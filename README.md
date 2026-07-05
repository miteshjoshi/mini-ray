# mini-ray

A small Rust-native distributed task framework inspired by Ray.

This project is an educational MVP for learning how a Ray-like system can be built from first principles in Rust. The current design focuses on remote task submission, typed object references, centralized in-memory object storage, worker execution, actors, and a basic scheduler.

## API Sketch

```rust
let cluster = mini_ray_client::connect("http://127.0.0.1:50051").await?;

let input: ObjectRef<u64> = cluster.put(41u64).await?;
let output: ObjectRef<u64> = cluster.submit::<u64, u64>("add_one", vec![input]).await?;

let result: u64 = cluster.get(output).await?;
```

## Workspace Layout

- `mini-ray-core`: shared IDs, typed object refs, task specs, errors, and serialization helpers
- `mini-ray-proto`: gRPC protobuf contracts and generated tonic bindings
- `mini-ray-object-store`: in-memory object storage by object ID
- `mini-ray-scheduler`: task lifecycle, dependency tracking, worker leases, and work stealing
- `mini-ray-runtime`: worker-side task and actor registry plus execution engine
- `mini-ray-head`: head-node gRPC service for objects, task submission, actors, leases, and worker heartbeats
- `mini-ray-worker`: worker binary that registers task and actor handlers, polls leases, executes them, and reports results
- `mini-ray-client`: user-facing Rust client API

## Current Status

mini-ray can run simple remote tasks and single-threaded actors end to end. Actor method calls are sequenced per actor: later methods wait behind lower-sequence methods until those methods finish or fail terminally, while different actors and normal tasks can schedule independently.

The object store is still centralized and in-memory, worker execution is intentionally small, and the project remains an educational MVP rather than a production runtime.

## Running Locally

Start the head:

```sh
cargo run -p mini-ray-head
```

Start a worker in another shell:

```sh
cargo run -p mini-ray-worker
```

Run a task example:

```sh
cargo run -p mini-ray-client --example add_one
```

Run an actor method sequencing example:

```sh
cargo run -p mini-ray-client --example counter_actor
```

## Non-Goals for V1

- Python bindings
- Shared memory object storage
- Durable recovery
- Autoscaling
- Dashboard UI
