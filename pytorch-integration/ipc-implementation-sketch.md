# Local Python IPC implementation sketch

Status: Proposed code structure and pseudocode; no new launcher crate or runnable implementation.
Date: 2026-10-31.
Related: [IPC comparison](ipc-design-comparison.md) and [launcher design](launcher-design.md).

This sketches the interactive Unix-socket option. The earlier file-based demo remains a separate, smaller milestone. The socket messages described here carry configuration and result metadata; model weights and logs remain files.

## The boundary

```text
mini-ray head
    | existing gRPC
Rust worker
    | registered handler, already running through spawn_blocking
    v
Launcher::launch(request)
    | owns listener, accepted socket, child handle, deadlines, output directory
    |                         Unix domain socket
    +----------------------------------------------------+
                                                         |
                                               Python bootstrap
                                                 | imports torch
                                                 | starts training thread
                                                 v
                                             training function
```

Rust creates and binds the endpoint before spawning the child. Python connects to it. A new child and a new endpoint belong to each invocation. Python never connects directly to the mini-ray head in this design.

## Suggested code layout

Paths below are proposed files, not files added by this sketch:

```text
crates/mini-ray-launcher/
  src/lib.rs          # LauncherConfig, LaunchRequest, LaunchResult, LaunchError
  src/protocol.rs     # Envelope, typed messages, frame codec, validation
  src/session.rs      # Legal message ordering and per-direction sequences
  src/supervisor.rs   # Spawn, IPC events, exit status, deadlines, kill/reap
  tests/ipc.rs        # Fixture-based subprocess checks

python/mini_ray_worker/
  bootstrap.py        # Connect, handshake, control loop, training thread
  protocol.py         # Matching schema and framing
  train_linear.py     # CPU example: train(config, report, cancel)
```

Keep the new launcher independent of the scheduler and gRPC crates. Its worker adapter uses `TaskRegistry::register_raw`. Rust-side dependencies would include serde, serde_json, and an OS-random token source; their versions and Rust 1.78 compatibility must be checked when implementing. The Python codec can use the standard library; the training module supplies the PyTorch dependency.

## Wire contract

Every message uses the same envelope. Message payloads are typed rather than arbitrary maps in the Rust protocol implementation. Directions have independent sequence counters starting at 1.

```json
{
  "wire_version": 1,
  "invocation_id": "<parent-generated UUID>",
  "sequence": 3,
  "kind": "metric",
  "payload": { "step": 20, "loss": 0.12 }
}
```

The enclosing frame is a four-byte unsigned big-endian length followed by that many UTF-8 JSON bytes. Reject empty frames and lengths above 64 KiB before allocating the body. A stream read may return part of a frame, and one read can contain multiple frames; the codec consumes exactly the required bytes.

Proposed Rust types, omitting imports and derives on supporting structures:

```rust
#[derive(Serialize, Deserialize)]
struct Envelope {
    wire_version: u32,
    invocation_id: String,
    sequence: u64,
    #[serde(flatten)]
    message: Message,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
enum Message {
    Hello { token: String },
    HelloAck {},
    Ready {},
    Run { config: TrainConfig },
    Started {},
    Metric { step: u64, loss: f64 },
    Result { initial_loss: f64, final_loss: f64, model_file: String },
    Error { code: String, message: String },
    Cancel { reason: String },
    Cancelled {},
}
```

`wire_version` versions the IPC envelope, independently of the training configuration's `protocol_version`. Validate invocation identity, version, direction, legal state, increasing sequence, finite floats, and field limits. A secret is sent only in `Hello`; never include it in diagnostic dumps. Counters are assigned by the single writer after optional metrics are coalesced. No reconnect or automatic replay of `Run` is supported.

The frame-codec algorithm is intentionally small:

```text
receive_frame(reader, total_frame_deadline):
    header = read_exactly(4, deadline)
    N = decode_u32_big_endian(header)
    require 1 <= N <= 65536
    body = read_exactly(N, same deadline)
    envelope = decode_strict_utf8_json(body)
    validate_schema(envelope)
    return envelope

send_frame(writer, envelope, total_write_deadline):
    validate_schema_and_finite_numbers(envelope)
    body = encode_utf8_json(envelope)
    require 1 <= len(body) <= 65536
    write_all(encode_u32_big_endian(len(body)), deadline)
    write_all(body, same deadline)
```

Helpers above must handle partial reads/writes and an overall monotonic deadline, not restart a full timeout for each byte. Waiting for the first byte during running can remain idle until the session/execution deadline; once a frame begins, a shorter frame deadline applies. Python must reject nonstandard JSON constants such as NaN and Infinity on decode and encode with `allow_nan=False`. Invalid protocol data fails the session instead of triggering a fallback parser.

## Rust: one supervisor and two I/O loops

The registered handler is synchronous and already executes on the worker's blocking pool. Keep that integration initially: use standard Unix socket/process APIs and dedicated I/O threads rather than creating a Tokio runtime inside the handler.

```text
socket reader thread --> bounded event queue --> supervisor / child.try_wait()
                                                  |
                                                  v
socket writer thread <-- bounded command queue ----+
```

Only the reader reads the socket; only the writer writes it. The supervisor owns protocol state, deadlines, and the child handle. I/O threads report errors through the event queue. Do not let a blocked queue send or socket operation prevent shutdown: use bounded operations, a shutdown flag/socket shutdown, and finite join behavior.

Skeleton, with helper functions deliberately left as interfaces:

```rust
impl Launcher {
    pub fn launch_with_cancel(
        &self,
        request: LaunchRequest,
        cancel: &AtomicBool,
    ) -> Result<LaunchResult, LaunchError> {
        let config = validate_request(request)?;
        let invocation = Invocation::create(&self.config)?;
        // Short private runtime dir; listener exists before the child starts.
        let listener = bind_private_socket(&invocation.socket_path)?;

        let child = Command::new(&self.config.python_executable)
            .arg("-u")
            .arg(&self.config.bootstrap_script)
            .arg("--socket").arg(&invocation.socket_path)
            .arg("--invocation").arg(&invocation.id)
            .env("MINIRAY_LAUNCH_TOKEN", invocation.secret())
            .current_dir(&invocation.output_dir)
            .stdin(Stdio::null())
            .stdout(invocation.open_stdout()?)
            .stderr(invocation.open_stderr()?)
            .spawn()?;

        let mut child = ChildGuard::new(child); // Best-effort kill/reap on early exit.
        let mut session = accept_expected_child(
            listener, &mut child, &invocation, self.config.startup_timeout,
        )?;

        let observed = supervise(
            &mut child, &mut session, &invocation, config, cancel, &self.config,
        );

        // Always attempt cleanup, preserving the original error if it failed.
        let cleanup = stop_io_reap_and_remove_socket(&mut child, &mut session);
        combine_outcome_with_cleanup(observed, cleanup)
    }
}
```

`launch(request)` is a convenience wrapper around `launch_with_cancel` with an initially unset cancellation flag. The flag in `launch_with_cancel` is for a local caller; connecting it to head-driven cancellation is separate work.

`Invocation::create` uses an exclusive directory and fresh invocation ID/token. It returns a cleanup guard that removes its owned socket/runtime endpoint on early failures as well as normal return; logs and artifacts are retained. Runtime/socket paths are separate from artifact paths if needed. `accept_expected_child` monitors child exit as well as socket acceptance, rejects unrelated connections within an overall startup deadline, and does not wait forever when Python fails before connecting.

`ChildGuard` is a fallback; normal cleanup explicitly records kill/wait failures, stops I/O threads, closes the listener, and removes only this invocation's endpoint. Configure frame and queue limits so cleanup need not drain unlimited data. Preserve logs and artifacts. Process-group/descendant cleanup is separate work; the initial training example must not spawn additional processes.

### The supervisor event loop

Use one serialized decision loop. A short receive timeout, such as 25 ms, lets it check `child.try_wait()` and deadlines regularly without treating every quiet interval as a failure.

```text
repeat until a final outcome is established:
    observe child status with try_wait()
    observe cancellation and deadlines
    receive one bounded I/O event, or wake on the short poll interval

    Hello(valid identity/token): send HelloAck
    Ready: send Run(config)
    Started: record local execution started
    Metric: publish bounded best-effort progress
    Result: validate/store terminal metadata; start exit/drain deadline
    Error: preserve application error; start exit/drain deadline
    Cancelled: preserve cancellation; start exit/drain deadline
    protocol error / early EOF: record failure and stop child

    if cancellation/timeout requires stopping:
        record reason, send Cancel when the session permits
        after grace expires, terminate child and reap

    once child is reaped and inbound terminal state is resolved:
        return the outcome defined below
```

Process-exit and socket events race. An observed successful exit does not by itself mean a terminal message is absent: the message can still be buffered in the socket or event queue. Continue a bounded drain through EOF, or until the exit/drain deadline, before deciding that no terminal message arrived. Conversely, a `Result` is provisional until exit status and artifacts are checked. Continue parsing through EOF to reject an unexpected second terminal message.

The protocol state machine distinguishes `Spawned`, `Handshaking`, `Ready`, `RunSent`, `Running`, `TerminalReceived`, and the final outcome. Cancellation creates a stopping state. It can be requested in any stage, but before a usable session exists the supervisor may have to stop the process directly. One child receives at most one Run.

If the supervisor accepts cancellation before accepting a terminal result, a late result cannot turn that invocation into success. If a result has already been accepted and validated, cancellation does not retroactively change a completed invocation. The serialized loop defines this ordering; a sent Cancel alone does not prove work stopped.

## Python: bootstrap, control loop, and training thread

Keep socket details out of the training function:

```python
class TrainingCancelled(Exception):
    pass


def train(config, report, cancel):
    import torch

    # Construct a CPU model, optimizer and generated data here.
    for step in range(config["steps"]):
        if cancel.is_set():
            raise TrainingCancelled()
        # Forward, loss, backward, optimizer update.
        report(step=step, loss=float(loss.item()))

    # Write model.pt before returning metadata.
    return {
        "initial_loss": initial_loss,
        "final_loss": final_loss,
        "model_file": "model.pt",
    }
```

This training fragment is illustrative: model/data construction, saving weights, and variables such as `loss` must be implemented in `train_linear.py`.

The bootstrap owns the connection. Its main thread receives control messages; a training thread runs user code; one writer loop serializes outbound messages. The following is pseudocode using the proposed `ControlChannel` helper:

```python
def bootstrap(socket_path, invocation_id):
    secret = os.environ.pop("MINIRAY_LAUNCH_TOKEN")
    channel = ControlChannel.connect(socket_path, invocation_id)
    channel.send_control("hello", {"token": secret})
    channel.expect("hello_ack")

    # Import before Ready so import errors are reported as startup failures.
    from train_linear import train, TrainingCancelled
    import torch
    torch.set_num_threads(1)
    torch.set_num_interop_threads(1)
    channel.send_control("ready", {})
    run = channel.expect("run")

    cancel = threading.Event()
    completed = queue.Queue(maxsize=1)

    def execute_training():
        try:
            channel.send_control("started", {})
            result = train(run["payload"]["config"], channel.report_metric, cancel)
            completed.put(("result", result))
        except TrainingCancelled:
            completed.put(("cancelled", {}))
        except Exception as error:
            # Bounded structured error; traceback goes to stderr.
            completed.put(("error", classify_and_bound(error)))

    trainer = threading.Thread(target=execute_training, daemon=False)
    trainer.start()
    # Receives Cancel while training is running; handles EOF and writer errors.
    kind, payload = channel.control_loop(cancel, completed)
    trainer.join()
    channel.send_terminal(kind, payload)
    channel.flush_terminal_and_close()
    # Exit 0 only for Result/Cancelled; reported Error exits nonzero.
    return channel.exit_code()
```

`ControlChannel` needs a bounded progress buffer and a reliable bounded control/terminal queue. Coalesce/drop progress before assigning outbound sequences; readiness/terminal messages cannot be silently lost. A failed terminal enqueue/write fails the invocation. The control loop checks the completion queue and control socket with bounded waits so training completion wakes it without another parent command. Before sending a terminal message, discard unsent optional metrics and join the training thread. The helper also reports writer failures and prevents concurrent writes from interleaving frames.

Put connection/import/validation failures inside an outer exception/cleanup boundary too. When possible, report a bounded startup Error before closing; Rust must still handle crashes that send no message. On socket EOF, set the cancellation event. A cooperative flag cannot interrupt every native PyTorch operation or recover from every GIL stall; Rust retains the execution deadline and termination authority. A non-daemon training thread must be joined before the child emits Cancelled and exits; if it hangs, the parent escalates.

## Worker registration adapter

The adapter enforces one input argument and turns launcher failures into actual task failures:

```rust
registry.register_raw("pytorch_train", move |args| {
    if args.len() != 1 {
        return Err(MiniRayError::TaskFailed("expected one config argument".into()));
    }
    let request: LaunchRequest = mini_ray_core::decode(&args[0])?;
    let output = launcher.launch(request)
        .map_err(|error| MiniRayError::TaskFailed(error.to_string()))?;
    mini_ray_core::encode(&output)
})?;
```

`LaunchRequest` and `LaunchResult` are serializable Rust types. Decode the request's JSON configuration deliberately at the launcher boundary. Do not expose private tokens in `Display`/`Debug` or publish `Result<LaunchResult, LaunchError>` as a successful task value.

No change to head polling, task submission, or result publication is required merely to use this adapter. A real head-driven cancellation API and task/attempt identity propagation would require additional worker/runtime protocol work. A child `Started` message is not an acknowledged scheduler ownership transition.

## Outcome rules

| Observations after bounded drain / cleanup | Outcome |
| --- | --- |
| One valid Result, exit zero, expected model file validates | Success; return metrics and artifact/log paths |
| Structured Error, with or without nonzero exit | Failure; retain application cause and process status |
| Child exit zero without a terminal result | Protocol failure |
| Result followed by nonzero exit | Failure; do not publish the result |
| Cancel accepted, training stopped, child reaped | Cancellation; currently maps to task failure because mini-ray has no cancellation outcome type |
| Deadline expiry followed by forced termination | Timeout/cancellation reason plus observed exit status |
| Unexpected EOF/reset before a terminal message, or signal termination without a reliable cause | Channel/process failure, not inferred OOM |
| Supervisor deliberately evicts for memory pressure in a future controller | Preserve supervisor's recorded reason; not implemented by the IPC channel |

Require the expected model file, a nonempty regular artifact, and a canonical path inside the invocation output directory. Only metadata crosses IPC. A local path remains local; moving checkpoints between machines is separate work.

## First implementation slices

1. Implement matching frame codecs and typed message validation in Rust/Python. Verify fragmentation, concatenation, size limits, invalid JSON and finite numbers.
2. Add a fixture bootstrap that completes Hello/Ready/Run/Started/Result and exits. Verify result/exit races and wrong identity/version handling without PyTorch.
3. Add supervisor deadlines, child exit monitoring, cooperative cancellation and forced cleanup. Verify heavy logs, metric bursts, import failure, hangs and early death with deterministic fixtures.
4. Register the worker adapter and run the real CPU PyTorch experiment through separate consumer/head/worker/Python processes.

This sketch has not been compiled or run. It defines proposed interfaces and the helper responsibilities needed to turn the design into implementation.
