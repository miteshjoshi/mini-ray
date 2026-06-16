# mini-ray

A small Rust-native distributed task framework inspired by Ray.

This project is an educational MVP for learning how a Ray-like system can be built from first principles in Rust. The initial design focuses on remote task submission, typed object references, centralized in-memory object storage, worker execution, and a basic scheduler.

## Planned API

```rust
let cluster = mini_ray::connect("http://127.0.0.1:50051").await?;

let input: ObjectRef<u64> = cluster.put(41u64).await?;
let output: ObjectRef<u64> = cluster.submit("add_one", vec![input]).await?;

let result: u64 = cluster.get(output).await?;
```

## Workspace Layout

- `mini-ray-core`: shared IDs, typed object refs, task specs, errors, and serialization helpers
- `mini-ray-proto`: gRPC protobuf contracts and generated tonic bindings
- `mini-ray-object-store`: in-memory object storage by object ID
- `mini-ray-scheduler`: task lifecycle, dependency tracking, worker leases, and work stealing
- `mini-ray-runtime`: planned worker-side task registry and execution engine
- `mini-ray-head`: planned head-node service
- `mini-ray-worker`: planned worker binary
- `mini-ray-client`: planned user-facing Rust client API

## Current Status

Early scaffold. Core types, protobuf contracts, object store, and scheduler are being built one file at a time.

## Non-Goals for V1

- Actors
- Python bindings
- Shared memory object storage
- Durable recovery
- Autoscaling
- Dashboard UI
