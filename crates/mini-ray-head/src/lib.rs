//! Head-node service wiring.
//!
//! This crate will own the gRPC service that combines the object store and
//! scheduler.

pub fn crate_ready() -> bool {
    true
}
