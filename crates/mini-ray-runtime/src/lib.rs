//! Worker-side task registration and execution.

use mini_ray_core::{decode, encode, MiniRayError, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

type RawTaskHandler = Arc<dyn Fn(Vec<Vec<u8>>) -> Result<Vec<u8>> + Send + Sync + 'static>;
type RawActorConstructor =
    Arc<dyn Fn(Vec<Vec<u8>>) -> Result<Box<dyn Any + Send>> + Send + Sync + 'static>;
type RawActorMethod =
    Arc<dyn Fn(&mut dyn Any, Vec<Vec<u8>>) -> Result<Vec<u8>> + Send + Sync + 'static>;

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

#[derive(Clone, Default)]
pub struct ActorRegistry {
    constructors: HashMap<String, RawActorConstructor>,
    methods: HashMap<(String, String), RawActorMethod>,
}

impl std::fmt::Debug for ActorRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorRegistry")
            .field(
                "constructors",
                &self.constructors.keys().collect::<Vec<_>>(),
            )
            .field("methods", &self.methods.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ActorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_constructor_raw<F>(
        &mut self,
        actor_type: impl Into<String>,
        constructor: F,
    ) -> Result<()>
    where
        F: Fn(Vec<Vec<u8>>) -> Result<Box<dyn Any + Send>> + Send + Sync + 'static,
    {
        let actor_type = actor_type.into();
        if self.constructors.contains_key(&actor_type) {
            return Err(MiniRayError::TaskFailed(format!(
                "actor constructor {actor_type} is already registered"
            )));
        }

        self.constructors.insert(actor_type, Arc::new(constructor));
        Ok(())
    }

    pub fn register_constructor_nullary<F, Actor>(
        &mut self,
        actor_type: impl Into<String>,
        constructor: F,
    ) -> Result<()>
    where
        F: Fn() -> Actor + Send + Sync + 'static,
        Actor: Any + Send + 'static,
    {
        self.register_constructor_raw(actor_type, move |args| {
            expect_arg_count(&args, 0)?;
            Ok(Box::new(constructor()))
        })
    }

    pub fn register_constructor_unary<F, Arg, Actor>(
        &mut self,
        actor_type: impl Into<String>,
        constructor: F,
    ) -> Result<()>
    where
        F: Fn(Arg) -> Actor + Send + Sync + 'static,
        Arg: DeserializeOwned,
        Actor: Any + Send + 'static,
    {
        self.register_constructor_raw(actor_type, move |args| {
            expect_arg_count(&args, 1)?;
            let arg = decode(&args[0])?;
            Ok(Box::new(constructor(arg)))
        })
    }

    pub fn register_method_raw<F>(
        &mut self,
        actor_type: impl Into<String>,
        method_id: impl Into<String>,
        method: F,
    ) -> Result<()>
    where
        F: Fn(&mut dyn Any, Vec<Vec<u8>>) -> Result<Vec<u8>> + Send + Sync + 'static,
    {
        let key = (actor_type.into(), method_id.into());
        if self.methods.contains_key(&key) {
            return Err(MiniRayError::TaskFailed(format!(
                "actor method {}.{} is already registered",
                key.0, key.1
            )));
        }

        self.methods.insert(key, Arc::new(method));
        Ok(())
    }

    pub fn register_method_unary<F, Actor, Arg, Out>(
        &mut self,
        actor_type: impl Into<String>,
        method_id: impl Into<String>,
        method: F,
    ) -> Result<()>
    where
        F: Fn(&mut Actor, Arg) -> Out + Send + Sync + 'static,
        Actor: Any + Send + 'static,
        Arg: DeserializeOwned,
        Out: Serialize,
    {
        let actor_type = actor_type.into();
        self.register_method_raw(actor_type.clone(), method_id, move |actor, args| {
            expect_arg_count(&args, 1)?;
            let actor = actor.downcast_mut::<Actor>().ok_or_else(|| {
                MiniRayError::TaskFailed(format!("actor state type mismatch for {actor_type}"))
            })?;
            let arg = decode(&args[0])?;
            encode(&method(actor, arg))
        })
    }

    fn construct(&self, actor_type: &str, args: Vec<Vec<u8>>) -> Result<Box<dyn Any + Send>> {
        let constructor = self.constructors.get(actor_type).ok_or_else(|| {
            MiniRayError::TaskFailed(format!("unknown actor constructor {actor_type}"))
        })?;
        constructor(args)
    }

    fn execute_method(
        &self,
        actor_type: &str,
        method_id: &str,
        actor: &mut dyn Any,
        args: Vec<Vec<u8>>,
    ) -> Result<Vec<u8>> {
        let method = self
            .methods
            .get(&(actor_type.to_string(), method_id.to_string()))
            .ok_or_else(|| {
                MiniRayError::TaskFailed(format!("unknown actor method {actor_type}.{method_id}"))
            })?;
        method(actor, args)
    }
}

#[derive(Default)]
pub struct ActorInstanceStore {
    actors: HashMap<String, ActorInstance>,
}

struct ActorInstance {
    actor_type: String,
    state: Box<dyn Any + Send>,
}

impl std::fmt::Debug for ActorInstanceStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorInstanceStore")
            .field("actors", &self.actors.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ActorInstanceStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.actors.len()
    }

    pub fn execute(
        &mut self,
        registry: &ActorRegistry,
        actor_id: impl Into<String>,
        actor_type: &str,
        method_id: &str,
        constructor_args: Vec<Vec<u8>>,
        method_args: Vec<Vec<u8>>,
    ) -> Result<Vec<u8>> {
        let actor_id = actor_id.into();
        if !self.actors.contains_key(&actor_id) {
            let state = registry.construct(actor_type, constructor_args)?;
            self.actors.insert(
                actor_id.clone(),
                ActorInstance {
                    actor_type: actor_type.to_string(),
                    state,
                },
            );
        }

        let instance = self.actors.get_mut(&actor_id).expect("actor was inserted");
        if instance.actor_type != actor_type {
            return Err(MiniRayError::TaskFailed(format!(
                "actor {actor_id} is {}, not {actor_type}",
                instance.actor_type
            )));
        }

        registry.execute_method(actor_type, method_id, instance.state.as_mut(), method_args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Counter {
        value: u64,
    }

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

    #[test]
    fn actor_instance_preserves_state_across_method_calls() {
        let mut registry = ActorRegistry::new();
        registry
            .register_constructor_unary("Counter", |initial: u64| Counter { value: initial })
            .unwrap();
        registry
            .register_method_unary(
                "Counter",
                "increment",
                |counter: &mut Counter, delta: u64| {
                    counter.value += delta;
                    counter.value
                },
            )
            .unwrap();
        let mut instances = ActorInstanceStore::new();

        let first = instances
            .execute(
                &registry,
                "actor-1",
                "Counter",
                "increment",
                vec![encode(&10u64).unwrap()],
                vec![encode(&2u64).unwrap()],
            )
            .unwrap();
        let second = instances
            .execute(
                &registry,
                "actor-1",
                "Counter",
                "increment",
                vec![],
                vec![encode(&5u64).unwrap()],
            )
            .unwrap();

        assert_eq!(decode::<u64>(&first).unwrap(), 12);
        assert_eq!(decode::<u64>(&second).unwrap(), 17);
        assert_eq!(instances.len(), 1);
    }

    #[test]
    fn unknown_actor_method_returns_error() {
        let mut registry = ActorRegistry::new();
        registry
            .register_constructor_nullary("Counter", || Counter { value: 0 })
            .unwrap();
        let mut instances = ActorInstanceStore::new();

        let err = instances
            .execute(&registry, "actor-1", "Counter", "missing", vec![], vec![])
            .unwrap_err();

        assert!(matches!(err, MiniRayError::TaskFailed(_)));
    }
}
