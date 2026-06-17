//! Worker-side task registration and execution.
//!
//! This crate is intentionally minimal while the workspace is being brought up.
//! The real runtime will map function IDs to Rust handlers.

pub fn crate_ready() -> bool {
    true
}
