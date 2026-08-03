# mini-ray Architecture

mini-ray is an educational Ray-like runtime. The implementation is intentionally small: one head service owns scheduling metadata and an in-memory object store, while workers poll the head for leases and execute registered Rust handlers.

## Components

- `mini-ray-client`: submits tasks, creates actors, calls actor methods, and reads objects from the head.
- `mini-ray-head`: exposes the gRPC API, owns the scheduler, actor metadata, actor-task lease metadata, and the in-memory object store.
- `mini-ray-scheduler`: tracks task state, dependency readiness, worker leases, retries, actor ordering, and object reference metadata.
- `mini-ray-worker`: registers task and actor handlers, polls leases, executes work, and reports completion or failure.
- `mini-ray-runtime`: stores task handlers, actor constructors, actor methods, and per-worker actor instances.

## Task Lifecycle

Tasks start as `Pending` when one or more dependencies are unavailable, otherwise they enter `Ready`. A worker polling the head can lease ready tasks up to its available capacity. Leased tasks move through:

```text
Pending -> Ready -> Leased -> Running -> Finished
                           \-> Failed
```

`Running` is currently available as a scheduler state, though the worker path completes leases directly after executing them. Failed tasks retry until `max_retries` is exhausted. Terminal tasks are `Finished` and `Failed`.

## Worker Execution

Workers request up to `poll_capacity` leases, bounded by configured slots.

Stateless task leases execute concurrently:

- each lease gets a cloned head client
- each lease uses a cloned task registry
- handler execution runs through `tokio::task::spawn_blocking`

Actor method leases are intentionally not spawned onto the stateless path. They execute through the worker's `ActorInstanceStore`, preserving a single mutable actor instance per actor id on that worker.

## Memory and Disk Pressure

Workers include best-effort memory and disk usage in heartbeats:

- memory used bytes
- memory total bytes
- memory pressure
- disk used bytes
- disk total bytes
- disk pressure

Pressure levels are `Unknown`, `Low`, `Moderate`, `High`, and `Critical`. The worker derives pressure from used/total ratios:

```text
< 70%  -> Low
>= 70% -> Moderate
>= 85% -> High
>= 95% -> Critical
```

The scheduler stores the latest resource usage for each worker. Workers reporting `High` or `Critical` memory or disk pressure do not receive new leases, even if they still have free execution slots. This keeps CPU slot availability separate from memory/disk safety.

Sampling is intentionally lightweight and best effort:

- Linux memory is read from `/proc/meminfo`.
- macOS memory is read from `sysctl` and `vm_stat`.
- Disk capacity is read with `df -Pk` for the worker's configured pressure path.

If sampling fails, the worker reports `Unknown`, which does not block leases.

## Actor Execution

Actors are single-threaded and ordered.

The head assigns each actor method an actor-wide sequence number when the method is submitted. The scheduler only leases an actor method when:

- dependencies are ready
- placement matches the actor owner worker, if one exists
- no same-actor method is currently `Leased` or `Running`
- no lower-sequence same-actor method is still nonterminal

Lower-sequence actor methods stop blocking later methods only after reaching `Finished` or `Failed`.

The first worker to lease an actor method becomes the actor owner. Later methods are pinned to that worker until actor recovery releases ownership.

## Object Ownership and Reference Tracking

mini-ray tracks object metadata in the scheduler:

- `available`: whether object data is known to exist in the object store
- `ref_count`: how many active task dependencies currently reference the object

When a task is submitted, each dependency increments that object's internal reference count. When the task reaches a terminal state, those dependency references are released.

Completed task outputs become available objects. A completed output is eligible for deletion only when its internal reference count is zero. This prevents deleting a producer output while an active downstream task still depends on it.

This is Ray-like for task dependency ownership, but it is not complete Ray-style distributed reference counting yet. Client-held `ObjectRef`s are not tracked because mini-ray has no protocol for clients or workers to report reference creation and drop events back to the head.

## Metadata Cleanup

The head exposes an internal cleanup hook:

```rust
cleanup_terminal_metadata(retain_terminal_tasks)
```

Cleanup removes old terminal task records and actor-task lease metadata. For finished tasks, it also removes the output object from the object store when the scheduler says the output has no active task references.

Cleanup deliberately keeps terminal producer metadata when its output is still referenced by active work. This preserves enough ownership information to clean the object once downstream tasks finish.

Failed task records are retained until cleanup so `get_object` can report producer failure for missing outputs. After failed task metadata is cleaned, that failure provenance is no longer available.

## Current Limitations

- The object store is centralized and in-memory.
- Client-held `ObjectRef`s are not reference-counted.
- There is no object spilling, lineage reconstruction, or durable recovery.
- Cleanup is explicit; there is no automatic background retention policy yet.
- Worker heartbeat payloads report memory and disk pressure, but `running` and `queued` counts are still placeholders.
- Actor restart behavior exists but is still intentionally simple.
