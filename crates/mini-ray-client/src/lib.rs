//! User-facing Rust client API.

use mini_ray_core::{
    decode, encode, ActorId, ActorRef, MiniRayError, ObjectId, ObjectRef, Result, TaskId,
};
use mini_ray_proto::miniray::v1::{
    head_client::HeadClient, CreateActorRequest, GetObjectRequest, PutObjectRequest,
    SubmitActorTaskRequest, SubmitTaskRequest,
};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;

#[derive(Debug, Clone)]
pub struct Client {
    inner: Arc<Mutex<HeadClient<Channel>>>,
}

pub async fn connect(endpoint: impl AsRef<str>) -> Result<Client> {
    Client::connect(endpoint).await
}

impl Client {
    pub async fn connect(endpoint: impl AsRef<str>) -> Result<Self> {
        let inner = HeadClient::connect(endpoint.as_ref().to_string())
            .await
            .map_err(transport_error)?;

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub async fn put<T>(&self, value: T) -> Result<ObjectRef<T>>
    where
        T: Serialize + Send + Sync + 'static,
    {
        let object_id = ObjectId::new();
        let data = encode(&value)?;
        let request = PutObjectRequest {
            object_id: object_id.to_string(),
            data,
        };

        let response = self
            .inner
            .lock()
            .await
            .put_object(request)
            .await
            .map_err(status_error)?
            .into_inner();

        let stored_id = ObjectId::from_string(&response.object_id)?;
        Ok(ObjectRef::new(stored_id))
    }

    pub async fn get<T>(&self, object_ref: ObjectRef<T>) -> Result<T>
    where
        T: DeserializeOwned + 'static,
    {
        let request = GetObjectRequest {
            object_id: object_ref.id().to_string(),
        };

        let response = self
            .inner
            .lock()
            .await
            .get_object(request)
            .await
            .map_err(status_error)?
            .into_inner();

        decode(&response.data)
    }

    pub async fn submit<Arg, Out>(
        &self,
        function_id: impl Into<String>,
        dependencies: Vec<ObjectRef<Arg>>,
    ) -> Result<ObjectRef<Out>>
    where
        Arg: 'static,
        Out: 'static,
    {
        let dependencies = dependencies
            .into_iter()
            .map(|object_ref| object_ref.erase())
            .collect();

        self.submit_refs(function_id, dependencies).await
    }

    pub async fn submit_refs<Out>(
        &self,
        function_id: impl Into<String>,
        dependencies: Vec<ObjectRef<()>>,
    ) -> Result<ObjectRef<Out>>
    where
        Out: 'static,
    {
        let output_id = ObjectId::new();
        let request = SubmitTaskRequest {
            function_id: function_id.into(),
            dependencies: object_ref_ids(dependencies),
            output_id: output_id.to_string(),
            max_retries: 1,
        };

        let response = self
            .inner
            .lock()
            .await
            .submit_task(request)
            .await
            .map_err(status_error)?
            .into_inner();

        // Parse both IDs so malformed head responses fail at the API boundary.
        let _task_id = TaskId::from_string(&response.task_id)?;
        let output_id = ObjectId::from_string(&response.output_id)?;
        Ok(ObjectRef::new(output_id))
    }

    pub async fn create_actor<Actor, Arg>(
        &self,
        actor_type: impl Into<String>,
        constructor_dependencies: Vec<ObjectRef<Arg>>,
    ) -> Result<ActorRef<Actor>>
    where
        Actor: 'static,
        Arg: 'static,
    {
        let constructor_dependencies = constructor_dependencies
            .into_iter()
            .map(|object_ref| object_ref.erase())
            .collect();

        self.create_actor_refs(actor_type, constructor_dependencies)
            .await
    }

    pub async fn create_actor_with_restarts<Actor, Arg>(
        &self,
        actor_type: impl Into<String>,
        constructor_dependencies: Vec<ObjectRef<Arg>>,
        max_restarts: u32,
    ) -> Result<ActorRef<Actor>>
    where
        Actor: 'static,
        Arg: 'static,
    {
        let constructor_dependencies = constructor_dependencies
            .into_iter()
            .map(|object_ref| object_ref.erase())
            .collect();

        self.create_actor_refs_with_restarts(actor_type, constructor_dependencies, max_restarts)
            .await
    }

    pub async fn create_actor_refs<Actor>(
        &self,
        actor_type: impl Into<String>,
        constructor_dependencies: Vec<ObjectRef<()>>,
    ) -> Result<ActorRef<Actor>>
    where
        Actor: 'static,
    {
        self.create_actor_refs_with_restarts(actor_type, constructor_dependencies, 0)
            .await
    }

    pub async fn create_actor_refs_with_restarts<Actor>(
        &self,
        actor_type: impl Into<String>,
        constructor_dependencies: Vec<ObjectRef<()>>,
        max_restarts: u32,
    ) -> Result<ActorRef<Actor>>
    where
        Actor: 'static,
    {
        let actor_id = ActorId::new();
        let request = CreateActorRequest {
            actor_type: actor_type.into(),
            constructor_dependencies: object_ref_ids(constructor_dependencies),
            actor_id: actor_id.to_string(),
            max_restarts,
        };

        let response = self
            .inner
            .lock()
            .await
            .create_actor(request)
            .await
            .map_err(status_error)?
            .into_inner();

        let actor_id = ActorId::from_string(&response.actor_id)?;
        Ok(ActorRef::new(actor_id))
    }

    pub async fn call_actor<Actor, Arg, Out>(
        &self,
        actor_ref: ActorRef<Actor>,
        method_id: impl Into<String>,
        dependencies: Vec<ObjectRef<Arg>>,
    ) -> Result<ObjectRef<Out>>
    where
        Actor: 'static,
        Arg: 'static,
        Out: 'static,
    {
        let dependencies = dependencies
            .into_iter()
            .map(|object_ref| object_ref.erase())
            .collect();

        self.call_actor_refs(actor_ref.erase(), method_id, dependencies)
            .await
    }

    pub async fn call_actor_refs<Out>(
        &self,
        actor_ref: ActorRef<()>,
        method_id: impl Into<String>,
        dependencies: Vec<ObjectRef<()>>,
    ) -> Result<ObjectRef<Out>>
    where
        Out: 'static,
    {
        let output_id = ObjectId::new();
        let request = SubmitActorTaskRequest {
            actor_id: actor_ref.id().to_string(),
            method_id: method_id.into(),
            dependencies: object_ref_ids(dependencies),
            output_id: output_id.to_string(),
            max_retries: 1,
        };

        let response = self
            .inner
            .lock()
            .await
            .submit_actor_task(request)
            .await
            .map_err(status_error)?
            .into_inner();

        let _task_id = TaskId::from_string(&response.task_id)?;
        let output_id = ObjectId::from_string(&response.output_id)?;
        Ok(ObjectRef::new(output_id))
    }
}

fn object_ref_ids(refs: Vec<ObjectRef<()>>) -> Vec<String> {
    refs.into_iter()
        .map(|object_ref| object_ref.id().to_string())
        .collect()
}

fn transport_error(error: tonic::transport::Error) -> MiniRayError {
    MiniRayError::Transport(error.to_string())
}

fn status_error(error: tonic::Status) -> MiniRayError {
    MiniRayError::Transport(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_ref_ids_preserves_dependency_order() {
        let first = ObjectRef::<()>::new(ObjectId::new());
        let second = ObjectRef::<()>::new(ObjectId::new());

        let ids = object_ref_ids(vec![first, second]);

        assert_eq!(ids, vec![first.id().to_string(), second.id().to_string()]);
    }
}
