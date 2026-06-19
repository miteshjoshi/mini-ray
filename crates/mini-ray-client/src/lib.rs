//! User-facing Rust client API.

use mini_ray_core::{decode, encode, MiniRayError, ObjectId, ObjectRef, Result, TaskId};
use mini_ray_proto::miniray::v1::{
    head_client::HeadClient, GetObjectRequest, PutObjectRequest, SubmitTaskRequest,
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
