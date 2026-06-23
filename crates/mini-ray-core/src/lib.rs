use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::marker::PhantomData;
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum MiniRayError {
    #[error("object {0} was not found")]
    MissingObject(ObjectId),
    #[error("failed to encode value: {0}")]
    Encode(String),
    #[error("failed to decode value: {0}")]
    Decode(String),
    #[error("task failed: {0}")]
    TaskFailed(String),
    #[error("scheduler error: {0}")]
    Scheduler(String),
    #[error("transport error: {0}")]
    Transport(String),
}

pub type Result<T> = std::result::Result<T, MiniRayError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectId(Uuid);

impl ObjectId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_string(value: &str) -> Result<Self> {
        Uuid::parse_str(value)
            .map(Self)
            .map_err(|err| MiniRayError::Decode(err.to_string()))
    }
}

impl Default for ObjectId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId(Uuid);

impl TaskId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_string(value: &str) -> Result<Self> {
        Uuid::parse_str(value)
            .map(Self)
            .map_err(|err| MiniRayError::Decode(err.to_string()))
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerId(Uuid);

impl WorkerId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_string(value: &str) -> Result<Self> {
        Uuid::parse_str(value)
            .map(Self)
            .map_err(|err| MiniRayError::Decode(err.to_string()))
    }
}

impl Default for WorkerId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for WorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorId(Uuid);

impl ActorId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_string(value: &str) -> Result<Self> {
        Uuid::parse_str(value)
            .map(Self)
            .map_err(|err| MiniRayError::Decode(err.to_string()))
    }
}

impl Default for ActorId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ActorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectRef<T> {
    id: ObjectId,
    type_name: &'static str,
    #[serde(skip)]
    _marker: PhantomData<T>,
}

impl<T> Copy for ObjectRef<T> {}

impl<T> Clone for ObjectRef<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: 'static> ObjectRef<T> {
    pub fn new(id: ObjectId) -> Self {
        Self {
            id,
            type_name: std::any::type_name::<T>(),
            _marker: PhantomData,
        }
    }

    pub fn id(&self) -> ObjectId {
        self.id
    }

    pub fn type_name(&self) -> &'static str {
        self.type_name
    }

    pub fn erase(&self) -> ObjectRef<()> {
        ObjectRef {
            id: self.id,
            type_name: "()",
            _marker: PhantomData,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorRef<T> {
    id: ActorId,
    type_name: &'static str,
    #[serde(skip)]
    _marker: PhantomData<T>,
}

impl<T> Copy for ActorRef<T> {}

impl<T> Clone for ActorRef<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: 'static> ActorRef<T> {
    pub fn new(id: ActorId) -> Self {
        Self {
            id,
            type_name: std::any::type_name::<T>(),
            _marker: PhantomData,
        }
    }

    pub fn id(&self) -> ActorId {
        self.id
    }

    pub fn type_name(&self) -> &'static str {
        self.type_name
    }

    pub fn erase(&self) -> ActorRef<()> {
        ActorRef {
            id: self.id,
            type_name: "()",
            _marker: PhantomData,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSpec {
    pub task_id: TaskId,
    pub function_id: String,
    pub dependencies: Vec<ObjectId>,
    pub output_id: ObjectId,
    pub target_worker: Option<WorkerId>,
    pub max_retries: u32,
    pub attempt: u32,
}

impl TaskSpec {
    pub fn new(
        function_id: impl Into<String>,
        dependencies: Vec<ObjectId>,
        output_id: ObjectId,
    ) -> Self {
        Self {
            task_id: TaskId::new(),
            function_id: function_id.into(),
            dependencies,
            output_id,
            target_worker: None,
            max_retries: 1,
            attempt: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorSpec {
    pub actor_id: ActorId,
    pub actor_type: String,
    pub constructor_dependencies: Vec<ObjectId>,
    pub max_restarts: u32,
}

impl ActorSpec {
    pub fn new(actor_type: impl Into<String>, constructor_dependencies: Vec<ObjectId>) -> Self {
        Self {
            actor_id: ActorId::new(),
            actor_type: actor_type.into(),
            constructor_dependencies,
            max_restarts: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorTaskSpec {
    pub task_id: TaskId,
    pub actor_id: ActorId,
    pub method_id: String,
    pub dependencies: Vec<ObjectId>,
    pub output_id: ObjectId,
    pub max_retries: u32,
    pub attempt: u32,
}

impl ActorTaskSpec {
    pub fn new(
        actor_id: ActorId,
        method_id: impl Into<String>,
        dependencies: Vec<ObjectId>,
        output_id: ObjectId,
    ) -> Self {
        Self {
            task_id: TaskId::new(),
            actor_id,
            method_id: method_id.into(),
            dependencies,
            output_id,
            max_retries: 1,
            attempt: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectData {
    pub id: ObjectId,
    pub bytes: Vec<u8>,
}

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    bincode::serialize(value).map_err(|err| MiniRayError::Encode(err.to_string()))
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    bincode::deserialize(bytes).map_err(|err| MiniRayError::Decode(err.to_string()))
}

pub const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_ref_carries_id_and_type() {
        let id = ObjectId::new();
        let object_ref = ObjectRef::<u64>::new(id);

        assert_eq!(object_ref.id(), id);
        assert!(object_ref.type_name().contains("u64"));
        assert_eq!(object_ref.erase().id(), id);
    }

    #[test]
    fn actor_ref_carries_id_and_type() {
        let id = ActorId::new();
        let actor_ref = ActorRef::<String>::new(id);

        assert_eq!(actor_ref.id(), id);
        assert!(actor_ref.type_name().contains("String"));
        assert_eq!(actor_ref.erase().id(), id);
    }

    #[test]
    fn actor_specs_capture_constructor_and_method_dependencies() {
        let constructor_input = ObjectId::new();
        let actor_spec = ActorSpec::new("Counter", vec![constructor_input]);
        let method_input = ObjectId::new();
        let output = ObjectId::new();
        let method_spec =
            ActorTaskSpec::new(actor_spec.actor_id, "increment", vec![method_input], output);

        assert_eq!(actor_spec.actor_type, "Counter");
        assert_eq!(actor_spec.constructor_dependencies, vec![constructor_input]);
        assert_eq!(method_spec.actor_id, actor_spec.actor_id);
        assert_eq!(method_spec.method_id, "increment");
        assert_eq!(method_spec.dependencies, vec![method_input]);
        assert_eq!(method_spec.output_id, output);
    }

    #[test]
    fn bincode_round_trip() {
        let encoded = encode(&41u64).unwrap();
        let decoded: u64 = decode(&encoded).unwrap();
        assert_eq!(decoded, 41);
    }
}
