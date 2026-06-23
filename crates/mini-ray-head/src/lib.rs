//! Head-node service wiring.

use mini_ray_core::{ActorId, ActorSpec, MiniRayError, ObjectId, TaskId, TaskSpec, WorkerId};
use mini_ray_object_store::InMemoryObjectStore;
use mini_ray_proto::miniray::v1::{
    head_server::{Head, HeadServer},
    CompleteTaskRequest, CompleteTaskResponse, CreateActorRequest, CreateActorResponse,
    FailTaskRequest, FailTaskResponse, GetObjectRequest, GetObjectResponse, HeartbeatRequest,
    HeartbeatResponse, PollTaskRequest, PollTaskResponse, PutObjectRequest, PutObjectResponse,
    RegisterWorkerRequest, RegisterWorkerResponse, SubmitActorTaskRequest, SubmitActorTaskResponse,
    SubmitTaskRequest, SubmitTaskResponse, TaskLease,
};
use mini_ray_scheduler::Scheduler;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[derive(Debug, Clone)]
pub struct HeadService {
    object_store: InMemoryObjectStore,
    scheduler: Arc<Mutex<Scheduler>>,
    actors: Arc<Mutex<HashMap<ActorId, ActorSpec>>>,
}

impl HeadService {
    pub fn new(object_store: InMemoryObjectStore, scheduler: Scheduler) -> Self {
        Self {
            object_store,
            scheduler: Arc::new(Mutex::new(scheduler)),
            actors: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(InMemoryObjectStore::new(), Scheduler::default())
    }

    pub fn object_store(&self) -> InMemoryObjectStore {
        self.object_store.clone()
    }

    pub async fn actor_count(&self) -> usize {
        self.actors.lock().await.len()
    }
}

pub async fn serve(bind: SocketAddr) -> Result<(), tonic::transport::Error> {
    Server::builder()
        .add_service(HeadServer::new(HeadService::with_defaults()))
        .serve(bind)
        .await
}

#[tonic::async_trait]
impl Head for HeadService {
    async fn put_object(
        &self,
        request: Request<PutObjectRequest>,
    ) -> std::result::Result<Response<PutObjectResponse>, Status> {
        let request = request.into_inner();
        let object_id = parse_object_id(&request.object_id)?;

        self.object_store.put_bytes(object_id, request.data).await;
        self.scheduler.lock().await.object_available(object_id);

        Ok(Response::new(PutObjectResponse {
            object_id: object_id.to_string(),
        }))
    }

    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> std::result::Result<Response<GetObjectResponse>, Status> {
        let object_id = parse_object_id(&request.into_inner().object_id)?;
        let data = self
            .object_store
            .get_bytes(object_id)
            .await
            .map_err(status_from_error)?;

        Ok(Response::new(GetObjectResponse {
            object_id: object_id.to_string(),
            data,
        }))
    }

    async fn submit_task(
        &self,
        request: Request<SubmitTaskRequest>,
    ) -> std::result::Result<Response<SubmitTaskResponse>, Status> {
        let request = request.into_inner();
        let output_id = parse_object_id(&request.output_id)?;
        let dependencies = parse_object_ids(request.dependencies)?;
        let mut spec = TaskSpec::new(request.function_id, dependencies, output_id);
        spec.max_retries = request.max_retries;
        let task_id = spec.task_id;

        self.scheduler
            .lock()
            .await
            .submit(spec)
            .map_err(status_from_error)?;

        Ok(Response::new(SubmitTaskResponse {
            task_id: task_id.to_string(),
            output_id: output_id.to_string(),
        }))
    }

    async fn create_actor(
        &self,
        request: Request<CreateActorRequest>,
    ) -> std::result::Result<Response<CreateActorResponse>, Status> {
        let request = request.into_inner();
        let actor_id = parse_actor_id(&request.actor_id)?;
        let constructor_dependencies = parse_object_ids(request.constructor_dependencies)?;
        let spec = ActorSpec {
            actor_id,
            actor_type: request.actor_type,
            constructor_dependencies,
            max_restarts: request.max_restarts,
        };

        self.actors.lock().await.insert(actor_id, spec);

        Ok(Response::new(CreateActorResponse {
            actor_id: actor_id.to_string(),
        }))
    }

    async fn submit_actor_task(
        &self,
        request: Request<SubmitActorTaskRequest>,
    ) -> std::result::Result<Response<SubmitActorTaskResponse>, Status> {
        let request = request.into_inner();
        let actor_id = parse_actor_id(&request.actor_id)?;
        let actor_type = self
            .actors
            .lock()
            .await
            .get(&actor_id)
            .map(|spec| spec.actor_type.clone())
            .ok_or_else(|| Status::not_found(format!("actor {actor_id} was not found")))?;

        let output_id = parse_object_id(&request.output_id)?;
        let dependencies = parse_object_ids(request.dependencies)?;
        let mut spec = TaskSpec::new(
            format!("actor:{actor_id}:{actor_type}:{}", request.method_id),
            dependencies,
            output_id,
        );
        spec.max_retries = request.max_retries;
        let task_id = spec.task_id;

        self.scheduler
            .lock()
            .await
            .submit(spec)
            .map_err(status_from_error)?;

        Ok(Response::new(SubmitActorTaskResponse {
            task_id: task_id.to_string(),
            output_id: output_id.to_string(),
        }))
    }

    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> std::result::Result<Response<RegisterWorkerResponse>, Status> {
        let request = request.into_inner();
        let worker_id = parse_worker_id(&request.worker_id)?;
        self.scheduler
            .lock()
            .await
            .register_worker(worker_id, request.slots as usize);

        Ok(Response::new(RegisterWorkerResponse {
            worker_id: worker_id.to_string(),
        }))
    }

    async fn poll_task(
        &self,
        request: Request<PollTaskRequest>,
    ) -> std::result::Result<Response<PollTaskResponse>, Status> {
        let request = request.into_inner();
        let worker_id = parse_worker_id(&request.worker_id)?;
        let tasks = self
            .scheduler
            .lock()
            .await
            .lease_tasks(worker_id, request.capacity as usize)
            .map_err(status_from_error)?
            .into_iter()
            .map(task_lease_from_spec)
            .collect();

        Ok(Response::new(PollTaskResponse { tasks }))
    }

    async fn complete_task(
        &self,
        request: Request<CompleteTaskRequest>,
    ) -> std::result::Result<Response<CompleteTaskResponse>, Status> {
        let request = request.into_inner();
        let worker_id = parse_worker_id(&request.worker_id)?;
        let task_id = parse_task_id(&request.task_id)?;
        let output_id = parse_object_id(&request.output_id)?;

        let mut scheduler = self.scheduler.lock().await;
        scheduler
            .validate_task_owner(worker_id, task_id)
            .map_err(status_from_error)?;
        self.object_store.put_bytes(output_id, request.data).await;
        scheduler
            .complete(worker_id, task_id, output_id)
            .map_err(status_from_error)?;

        Ok(Response::new(CompleteTaskResponse {}))
    }

    async fn fail_task(
        &self,
        request: Request<FailTaskRequest>,
    ) -> std::result::Result<Response<FailTaskResponse>, Status> {
        let request = request.into_inner();
        let worker_id = parse_worker_id(&request.worker_id)?;
        let task_id = parse_task_id(&request.task_id)?;

        self.scheduler
            .lock()
            .await
            .fail(worker_id, task_id, request.error)
            .map_err(status_from_error)?;

        Ok(Response::new(FailTaskResponse {}))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> std::result::Result<Response<HeartbeatResponse>, Status> {
        let worker_id = parse_worker_id(&request.into_inner().worker_id)?;
        self.scheduler.lock().await.heartbeat(worker_id);

        Ok(Response::new(HeartbeatResponse {}))
    }
}

fn parse_object_id(value: &str) -> std::result::Result<ObjectId, Status> {
    ObjectId::from_string(value).map_err(status_from_error)
}

fn parse_object_ids(values: Vec<String>) -> std::result::Result<Vec<ObjectId>, Status> {
    values
        .into_iter()
        .map(|value| parse_object_id(&value))
        .collect()
}

fn parse_task_id(value: &str) -> std::result::Result<TaskId, Status> {
    TaskId::from_string(value).map_err(status_from_error)
}

fn parse_worker_id(value: &str) -> std::result::Result<WorkerId, Status> {
    WorkerId::from_string(value).map_err(status_from_error)
}

fn parse_actor_id(value: &str) -> std::result::Result<ActorId, Status> {
    ActorId::from_string(value).map_err(status_from_error)
}

fn task_lease_from_spec(spec: TaskSpec) -> TaskLease {
    TaskLease {
        task_id: spec.task_id.to_string(),
        function_id: spec.function_id,
        dependencies: spec
            .dependencies
            .into_iter()
            .map(|object_id| object_id.to_string())
            .collect(),
        output_id: spec.output_id.to_string(),
        attempt: spec.attempt,
    }
}

fn status_from_error(error: MiniRayError) -> Status {
    match error {
        MiniRayError::MissingObject(_) => Status::not_found(error.to_string()),
        MiniRayError::Decode(_) | MiniRayError::Encode(_) => {
            Status::invalid_argument(error.to_string())
        }
        MiniRayError::Scheduler(_) | MiniRayError::TaskFailed(_) | MiniRayError::Transport(_) => {
            Status::failed_precondition(error.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mini_ray_core::{decode, encode};

    #[tokio::test]
    async fn put_and_get_object_round_trip() {
        let service = HeadService::with_defaults();
        let object_id = ObjectId::new();
        let data = encode(&41u64).unwrap();

        service
            .put_object(Request::new(PutObjectRequest {
                object_id: object_id.to_string(),
                data,
            }))
            .await
            .unwrap();

        let response = service
            .get_object(Request::new(GetObjectRequest {
                object_id: object_id.to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        let value: u64 = decode(&response.data).unwrap();

        assert_eq!(response.object_id, object_id.to_string());
        assert_eq!(value, 41);
    }

    #[tokio::test]
    async fn submit_and_poll_ready_task() {
        let service = HeadService::with_defaults();
        let worker_id = WorkerId::new();
        let output_id = ObjectId::new();

        service
            .register_worker(Request::new(RegisterWorkerRequest {
                worker_id: worker_id.to_string(),
                slots: 1,
            }))
            .await
            .unwrap();
        let submitted = service
            .submit_task(Request::new(SubmitTaskRequest {
                function_id: "add_one".to_string(),
                dependencies: vec![],
                output_id: output_id.to_string(),
                max_retries: 1,
            }))
            .await
            .unwrap()
            .into_inner();

        let polled = service
            .poll_task(Request::new(PollTaskRequest {
                worker_id: worker_id.to_string(),
                capacity: 1,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(polled.tasks.len(), 1);
        assert_eq!(polled.tasks[0].task_id, submitted.task_id);
        assert_eq!(polled.tasks[0].function_id, "add_one");
        assert_eq!(polled.tasks[0].output_id, output_id.to_string());
    }

    #[tokio::test]
    async fn completion_stores_output_for_get() {
        let service = HeadService::with_defaults();
        let worker_id = WorkerId::new();
        let output_id = ObjectId::new();

        service
            .register_worker(Request::new(RegisterWorkerRequest {
                worker_id: worker_id.to_string(),
                slots: 1,
            }))
            .await
            .unwrap();
        let submitted = service
            .submit_task(Request::new(SubmitTaskRequest {
                function_id: "add_one".to_string(),
                dependencies: vec![],
                output_id: output_id.to_string(),
                max_retries: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        service
            .poll_task(Request::new(PollTaskRequest {
                worker_id: worker_id.to_string(),
                capacity: 1,
            }))
            .await
            .unwrap();

        service
            .complete_task(Request::new(CompleteTaskRequest {
                worker_id: worker_id.to_string(),
                task_id: submitted.task_id,
                output_id: output_id.to_string(),
                data: encode(&42u64).unwrap(),
            }))
            .await
            .unwrap();

        let response = service
            .get_object(Request::new(GetObjectRequest {
                object_id: output_id.to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        let value: u64 = decode(&response.data).unwrap();

        assert_eq!(value, 42);
    }

    #[tokio::test]
    async fn wrong_worker_completion_does_not_store_output() {
        let service = HeadService::with_defaults();
        let owner = WorkerId::new();
        let other = WorkerId::new();
        let output_id = ObjectId::new();

        service
            .register_worker(Request::new(RegisterWorkerRequest {
                worker_id: owner.to_string(),
                slots: 1,
            }))
            .await
            .unwrap();
        service
            .register_worker(Request::new(RegisterWorkerRequest {
                worker_id: other.to_string(),
                slots: 1,
            }))
            .await
            .unwrap();
        let submitted = service
            .submit_task(Request::new(SubmitTaskRequest {
                function_id: "add_one".to_string(),
                dependencies: vec![],
                output_id: output_id.to_string(),
                max_retries: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        service
            .poll_task(Request::new(PollTaskRequest {
                worker_id: owner.to_string(),
                capacity: 1,
            }))
            .await
            .unwrap();

        let err = service
            .complete_task(Request::new(CompleteTaskRequest {
                worker_id: other.to_string(),
                task_id: submitted.task_id,
                output_id: output_id.to_string(),
                data: encode(&42u64).unwrap(),
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(matches!(
            service.object_store().get_bytes(output_id).await,
            Err(MiniRayError::MissingObject(_))
        ));
    }

    #[tokio::test]
    async fn dependency_becomes_ready_after_put() {
        let service = HeadService::with_defaults();
        let worker_id = WorkerId::new();
        let input_id = ObjectId::new();
        let output_id = ObjectId::new();

        service
            .register_worker(Request::new(RegisterWorkerRequest {
                worker_id: worker_id.to_string(),
                slots: 1,
            }))
            .await
            .unwrap();
        service
            .submit_task(Request::new(SubmitTaskRequest {
                function_id: "add_one".to_string(),
                dependencies: vec![input_id.to_string()],
                output_id: output_id.to_string(),
                max_retries: 1,
            }))
            .await
            .unwrap();

        let before_put = service
            .poll_task(Request::new(PollTaskRequest {
                worker_id: worker_id.to_string(),
                capacity: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(before_put.tasks.is_empty());

        service
            .put_object(Request::new(PutObjectRequest {
                object_id: input_id.to_string(),
                data: encode(&41u64).unwrap(),
            }))
            .await
            .unwrap();

        let after_put = service
            .poll_task(Request::new(PollTaskRequest {
                worker_id: worker_id.to_string(),
                capacity: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(after_put.tasks.len(), 1);
    }

    #[tokio::test]
    async fn actor_creation_records_metadata_and_actor_task_can_be_polled() {
        let service = HeadService::with_defaults();
        let actor_id = ActorId::new();
        let worker_id = WorkerId::new();
        let output_id = ObjectId::new();

        service
            .register_worker(Request::new(RegisterWorkerRequest {
                worker_id: worker_id.to_string(),
                slots: 1,
            }))
            .await
            .unwrap();
        let created = service
            .create_actor(Request::new(CreateActorRequest {
                actor_type: "Counter".to_string(),
                constructor_dependencies: vec![],
                actor_id: actor_id.to_string(),
                max_restarts: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(created.actor_id, actor_id.to_string());
        assert_eq!(service.actor_count().await, 1);

        service
            .submit_actor_task(Request::new(SubmitActorTaskRequest {
                actor_id: actor_id.to_string(),
                method_id: "increment".to_string(),
                dependencies: vec![],
                output_id: output_id.to_string(),
                max_retries: 1,
            }))
            .await
            .unwrap();

        let polled = service
            .poll_task(Request::new(PollTaskRequest {
                worker_id: worker_id.to_string(),
                capacity: 1,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(polled.tasks.len(), 1);
        assert!(polled.tasks[0].function_id.contains("Counter"));
        assert!(polled.tasks[0].function_id.contains("increment"));
    }

    #[tokio::test]
    async fn unknown_actor_task_is_rejected() {
        let service = HeadService::with_defaults();
        let err = service
            .submit_actor_task(Request::new(SubmitActorTaskRequest {
                actor_id: ActorId::new().to_string(),
                method_id: "increment".to_string(),
                dependencies: vec![],
                output_id: ObjectId::new().to_string(),
                max_retries: 1,
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::NotFound);
    }
}
