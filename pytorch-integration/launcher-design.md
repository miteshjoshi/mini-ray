# Python launcher design

Status: Proposed; no launcher implementation or PyTorch verification yet.
Date: 2026-10-31

Companion: [IPC design comparison](ipc-design-comparison.md), covering Ray's worker/raylet channels and mini-ray transport choices. The first milestone retains the file protocol below; Unix sockets are proposed for a later interactive control channel.

## First milestone

Run a preinstalled CPU PyTorch training script through a registered mini-ray task and retrieve its metrics and artifact locations. Start with a head, one worker with one slot, and a Rust consumer on the same machine. Train a small linear regression model on generated data so the experiment needs no dataset download.

The launcher owns the Python process and the file protocol. The Python program owns training, metrics, and model serialization. mini-ray continues to own task scheduling and result publication.

Defer Python SDKs, code uploads, environment installation, GPUs, distributed training, live log streaming, checkpoint resume, and remote artifact transfer.

## Existing integration points

- `crates/mini-ray-runtime/src/lib.rs`: `TaskRegistry::register_raw` accepts a handler returning `Result<Vec<u8>>`. Typed registration helpers serialize the handler's return value, so using a typed helper with `Result` could publish an execution failure as a successful output.
- `crates/mini-ray-worker/src/lib.rs`: stateless handlers execute through `spawn_blocking`; heartbeats run in a separate async task. The launcher can use a synchronous child-process lifecycle here.
- `crates/mini-ray-client/src/lib.rs`: submission returns an `ObjectRef`, `get` requests an object immediately, and submission currently sets `max_retries = 1`.
- `crates/mini-ray-core/src/lib.rs`: Rust task payloads use serde/bincode. Python receives a separate JSON protocol.

These observations come from code inspection, not a new test run.

## Proposed module boundary

Implement a small `mini-ray-launcher` crate with no scheduler or gRPC dependencies. It exposes configuration, request/result types, a structured launcher error, and a synchronous `launch` function. The worker registers a `pytorch_train` adapter that decodes one task argument, calls the launcher, and encodes the successful result.

```text
Rust consumer
  put LaunchRequest -> submit "pytorch_train" -> ObjectRef<LaunchResult>
                                |
head -> worker -> raw handler -> launch -> Python subprocess
                                |             |
                                |       metrics + model file
                                v
                         LaunchResult -> head -> consumer
```

Keep the adapter thin. Launcher errors become `MiniRayError::TaskFailed` with an invocation ID, error category, and available log locations. Returning `Err` lets the worker use its existing failure-reporting path. Encoding or decoding failures remain errors too.

## Configuration and contracts

Worker-owned `LauncherConfig`:

| Field | Meaning |
| --- | --- |
| `python_executable` | Absolute path to the provisioned Python interpreter. |
| `script_path` | Absolute path to the provisioned training script. |
| `output_root` | Absolute directory under which invocations are created. |
| `execution_timeout` | Positive wall-clock deadline for the child; proposed default 120 seconds. |

Resolve and validate configured paths when enabling the handler. Each invocation still handles files or executables disappearing later. The consumer supplies training parameters, while executable selection, script selection, and timeout remain worker configuration.

Proposed Rust task types:

```rust
struct LaunchRequest {
    protocol_version: u32,
    config_json: String,
}

struct LaunchResult {
    protocol_version: u32,
    invocation_id: String,
    metrics: BTreeMap<String, f64>,
    model_path: String,
    stdout_path: String,
    stderr_path: String,
}
```

These types derive serde serialization for the existing Rust task boundary. `config_json` is UTF-8 JSON text, allowing Python interoperability without asking Python to decode bincode. Avoid carrying `serde_json::Value` directly through bincode; the JSON representation stays inside the string.

Version 1 training configuration contains `learning_rate`, `steps`, and `seed`. Require finite positive learning rate, `steps` between 1 and 100,000, and a seed between 0 and 2^32 - 1. Validate the request before spawning. Limit configuration text to 64 KiB. The script also validates its input and explicitly selects CPU execution.

## Files and Python protocol

Create a fresh UUID invocation directory with exclusive directory creation for every handler call, including retries:

```text
<output_root>/<invocation_id>/
  config.json
  stdout.log
  stderr.log
  result.json
  model.pt
```

Invoke the interpreter with a program/argument vector:

```text
<python_executable> -u <script_path> --config <absolute-config-path> --result <absolute-result-path>
```

Use the invocation directory as the working directory, null stdin, and redirect stdout/stderr directly to separate files. This avoids pipe-buffer deadlocks and accumulating logs in memory. Paths containing spaces stay single arguments. No shell is involved. The provisioned interpreter supplies the environment; this version does not create environments or install dependencies.

`config.json` contains a versioned envelope:

```json
{
  "protocol_version": 1,
  "training": { "learning_rate": 0.01, "steps": 200, "seed": 42 }
}
```

Python writes model weights, then writes `result.json` through a temporary sibling file and atomic rename:

```json
{
  "protocol_version": 1,
  "metrics": { "initial_loss": 12.4, "final_loss": 0.002 },
  "model_file": "model.pt"
}
```

The numbers above illustrate the schema; they are not measured results. A successful invocation requires both exit code zero and a valid result file. Read at most 64 KiB plus one byte to enforce the result-size limit. Require version 1, finite numeric `initial_loss` and `final_loss`, and the expected nonempty regular `model.pt` file. Resolve the model path and require it to remain inside the invocation directory. The launcher checks the artifact's presence; loading the weights is part of the real PyTorch smoke test.

The launcher builds `LaunchResult` using its own invocation and log paths. The Python result cannot choose log paths or another invocation directory.

## Process lifecycle

1. Validate the request and allocate the invocation directory.
2. Write configuration and open log files. Preserve the directory if setup fails after allocation.
3. Spawn Python and retain ownership of its child handle.
4. Poll `try_wait` at a short interval, such as 25 ms, using a monotonic deadline. Check for exit before deciding the deadline has expired.
5. On a successful exit, validate the result and artifact, then return the small result structure.
6. On nonzero exit or termination by signal, return an execution error even if a result file exists.
7. On timeout or an error while waiting, attempt to kill the child and wait to reap it before returning. Handle the race where the process exits during cleanup. Preserve cleanup errors alongside the original cause.

A child guard must attempt cleanup on early return or panic while the worker remains alive. Dropping a Rust `Child` alone does not terminate it. The timeout bounds allowed execution time; OS termination and reaping can add cleanup time, so it is not an absolute bound on handler latency.

Version 1 supports a single Python process. Configure the demo with no DataLoader subprocesses (`num_workers = 0`) and no subprocess spawning. Child-process trees and process-group termination need a separate design and test before accepting programs that spawn descendants.

The synchronous launcher runs on the worker's existing blocking pool. Blocking execution must not move into a Tokio runtime thread. Cap the demo's PyTorch intra-op/inter-op thread counts to one so a single worker slot has predictable CPU use.

## Errors and publication

| Category | Examples | Outcome |
| --- | --- | --- |
| Invalid request | Unsupported version, invalid parameters, oversized configuration | Fail before Python starts. |
| Setup | Directory/config/log creation fails | Fail and preserve available diagnostics. |
| Spawn | Interpreter missing, permissions, invalid executable | Fail with executable and OS error. |
| Execution | Nonzero exit or signal termination | Fail with exit information and log paths. |
| Timeout | Child exceeds execution deadline | Terminate/reap, then fail with diagnostics. |
| Protocol/artifact | Missing, oversized, malformed result; invalid metrics; missing model | Fail without publishing a successful task output. |
| Cleanup | Killing or reaping fails | Report original cause and cleanup failure. |

Keep error messages bounded; return paths instead of copying entire logs into gRPC errors. Preserve invocation files for inspection on success and failure. Retention, log quotas, and disk cleanup remain manual in the first experiment; file redirection bounds memory use but does not bound disk consumption.

## Retry, ownership, and accessibility limits

The launcher never retries internally. mini-ray currently permits one retry on submission, so the Python program may execute again. Every handler invocation gets a different directory; invocation IDs are local execution identifiers, not scheduler attempts or lease identities. The registry currently receives arguments without task/attempt metadata.

A fresh directory prevents accidental reuse of earlier output. It does not provide exactly-once execution, checkpoint resume, or fencing of external effects. The launcher publishes metrics and references through mini-ray; model bytes remain on disk.

Paths are usable by the consumer under the same-machine/shared-filesystem assumption. General remote workers will require shared storage or an artifact fetch/upload mechanism.

The current head has no acknowledged start RPC, and completion/failure messages do not carry attempt or lease identity. Implement acknowledged task start and stale execution-report fencing before claiming reliable multi-worker execution. This launcher does not repair those protocol gaps.

Worker heartbeat expiry does not kill Python. A forced worker death can orphan its child, and the current worker shutdown path does not supply a launcher cancellation token. The local deadline is enforced while the launcher remains alive. Graceful job cancellation and parent-death cleanup require worker lifecycle changes and separate-process tests.

## Verification and acceptance evidence

Start with lightweight subprocess fixtures so launcher behavior can be checked independently of PyTorch:

- Forward configuration and paths containing spaces; verify working directory and successful result decoding.
- Verify missing executable, nonzero exit, signal termination, absent/malformed/oversized result, invalid metrics, and absent model failures.
- Generate stdout/stderr beyond typical pipe capacity and verify file capture without hanging.
- Hold a fixture open, trigger timeout, and verify its process has exited and been reaped. Coordinate with an explicit readiness signal and bounded waits.
- Launch twice and verify distinct directories; retain diagnostics from a failed invocation.

Then run a separate-process CPU smoke experiment through the Rust consumer, head, worker, and real PyTorch. Assert finite loss with a documented substantial reduction, reload the saved weights, and check predictions on held-out generated inputs. Hold execution long enough to observe continued heartbeats. Poll only missing-output responses with a deadline; surface terminal producer failures and transport/decode errors immediately.

Report fixture tests and real PyTorch coverage separately. Missing PyTorch is an unmet prerequisite, not a passing smoke test. Use explicit readiness signals and clean up processes and temporary artifacts owned by tests.

Record the guarantee, evidence (test/experiment and commit), assumptions, limits, and lessons after implementation. Run relevant Rust tests and workspace checks then; this document alone establishes no runtime guarantee.

## Implementation order

1. Add launcher types, validation, and the JSON/file contract.
2. Implement process lifecycle and fixture checks, including timeout cleanup.
3. Register the raw worker handler and add the Rust consumer example.
4. Add the tiny PyTorch script and run the real smoke experiment.

Work through one module at a time. The first reviewable code change should establish the contract and launcher boundary.
