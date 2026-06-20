//! Worker-side task registration and execution.

use mini_ray_core::{decode, encode, MiniRayError, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

type RawTaskHandler = Arc<dyn Fn(Vec<Vec<u8>>) -> Result<Vec<u8>> + Send + Sync + 'static>;

#[derive(Clone, Default)]
pub struct TaskRegistry {
    handlers: HashMap<String, RawTaskHandler>,
}

impl std::fmt::Debug for TaskRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRegistry")
            .field("handlers", &self.handlers.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl TaskRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    pub fn contains(&self, function_id: &str) -> bool {
        self.handlers.contains_key(function_id)
    }

    pub fn register_raw<F>(&mut self, function_id: impl Into<String>, handler: F) -> Result<()>
    where
        F: Fn(Vec<Vec<u8>>) -> Result<Vec<u8>> + Send + Sync + 'static,
    {
        let function_id = function_id.into();
        if self.handlers.contains_key(&function_id) {
            return Err(MiniRayError::TaskFailed(format!(
                "handler {function_id} is already registered"
            )));
        }

        self.handlers.insert(function_id, Arc::new(handler));
        Ok(())
    }

    pub fn register_nullary<F, Out>(
        &mut self,
        function_id: impl Into<String>,
        handler: F,
    ) -> Result<()>
    where
        F: Fn() -> Out + Send + Sync + 'static,
        Out: Serialize,
    {
        self.register_raw(function_id, move |args| {
            expect_arg_count(&args, 0)?;
            encode(&handler())
        })
    }

    pub fn register_unary<F, Arg, Out>(
        &mut self,
        function_id: impl Into<String>,
        handler: F,
    ) -> Result<()>
    where
        F: Fn(Arg) -> Out + Send + Sync + 'static,
        Arg: DeserializeOwned,
        Out: Serialize,
    {
        self.register_raw(function_id, move |args| {
            expect_arg_count(&args, 1)?;
            let arg = decode(&args[0])?;
            encode(&handler(arg))
        })
    }

    pub fn register_binary<F, A, B, Out>(
        &mut self,
        function_id: impl Into<String>,
        handler: F,
    ) -> Result<()>
    where
        F: Fn(A, B) -> Out + Send + Sync + 'static,
        A: DeserializeOwned,
        B: DeserializeOwned,
        Out: Serialize,
    {
        self.register_raw(function_id, move |args| {
            expect_arg_count(&args, 2)?;
            let first = decode(&args[0])?;
            let second = decode(&args[1])?;
            encode(&handler(first, second))
        })
    }

    pub fn execute(&self, function_id: &str, args: Vec<Vec<u8>>) -> Result<Vec<u8>> {
        let handler = self
            .handlers
            .get(function_id)
            .ok_or_else(|| MiniRayError::TaskFailed(format!("unknown handler {function_id}")))?;

        handler(args)
    }
}

fn expect_arg_count(args: &[Vec<u8>], expected: usize) -> Result<()> {
    if args.len() == expected {
        Ok(())
    } else {
        Err(MiniRayError::TaskFailed(format!(
            "expected {expected} args, got {}",
            args.len()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_execute_unary_handler() {
        let mut registry = TaskRegistry::new();
        registry
            .register_unary("add_one", |value: u64| value + 1)
            .unwrap();

        let output = registry
            .execute("add_one", vec![encode(&41u64).unwrap()])
            .unwrap();
        let value: u64 = decode(&output).unwrap();

        assert_eq!(value, 42);
    }

    #[test]
    fn register_and_execute_binary_handler() {
        let mut registry = TaskRegistry::new();
        registry
            .register_binary("add", |left: u64, right: u64| left + right)
            .unwrap();

        let output = registry
            .execute("add", vec![encode(&40u64).unwrap(), encode(&2u64).unwrap()])
            .unwrap();
        let value: u64 = decode(&output).unwrap();

        assert_eq!(value, 42);
    }

    #[test]
    fn register_and_execute_nullary_handler() {
        let mut registry = TaskRegistry::new();
        registry.register_nullary("answer", || 42u64).unwrap();

        let output = registry.execute("answer", vec![]).unwrap();
        let value: u64 = decode(&output).unwrap();

        assert_eq!(value, 42);
    }

    #[test]
    fn unknown_handler_returns_error() {
        let registry = TaskRegistry::new();
        let err = registry.execute("missing", vec![]).unwrap_err();

        assert!(matches!(err, MiniRayError::TaskFailed(_)));
    }

    #[test]
    fn duplicate_handler_is_rejected() {
        let mut registry = TaskRegistry::new();
        registry.register_nullary("answer", || 42u64).unwrap();
        let err = registry.register_nullary("answer", || 43u64).unwrap_err();

        assert!(matches!(err, MiniRayError::TaskFailed(_)));
    }

    #[test]
    fn wrong_arg_count_returns_error() {
        let mut registry = TaskRegistry::new();
        registry
            .register_unary("add_one", |value: u64| value + 1)
            .unwrap();

        let err = registry.execute("add_one", vec![]).unwrap_err();

        assert!(matches!(err, MiniRayError::TaskFailed(_)));
    }

    #[test]
    fn decode_error_is_returned() {
        let mut registry = TaskRegistry::new();
        registry
            .register_unary("add_one", |value: u64| value + 1)
            .unwrap();

        let err = registry
            .execute("add_one", vec![vec![1, 2, 3]])
            .unwrap_err();

        assert!(matches!(err, MiniRayError::Decode(_)));
    }
}
