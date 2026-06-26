//! Head-node service wiring.

use mini_ray_core::{ActorId, ActorSpec, MiniRayError, ObjectId, TaskId, TaskSpec, WorkerId};
use mini_ray_object_store::InMemoryObjectStore;
use mini_ray_proto::miniray::v1::{
    head_server::{Head, HeadServer},
    CompleteTaskRequest, CompleteTaskResponse, CreateActorRequest, CreateActorResponse,
    FailTaskRequest, FailTaskResponse, GetObjectRequest, GetObjectResponse, HeartbeatRequest,
    HeartbeatResponse, PollTaskRequest, PollTaskResponse, PutObjectRequest, PutObjectResponse,
    RegisterWorkerRequest, RegisterWorkerResponse, SubmitActorTaskRequest, SubmitActorTaskResponse,
    SubmitTaskRequest, SubmitTaskResponse, TaskLease, UnregisterWorkerRequest,
    UnregisterWorkerResponse,
};
use mini_ray_scheduler::Scheduler;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[derive(Debug, Clone)]
pub struct HeadService {
    object_store: InMemoryObjectStore,
    scheduler: Arc<Mutex<Scheduler>>,
    actors: Arc<Mutex<HashMap<ActorId, ActorRecord>>>,
    actor_tasks: Arc<Mutex<HashMap<TaskId, ActorTaskLeaseMeta>>>,
}

#[derive(Debug, Clone)]
struct ActorRecord {
    spec: ActorSpec,
    owner_worker: Option<WorkerId>,
}

#[derive(Debug, Clone)]
struct ActorTaskLeaseMeta {
    actor_id: ActorId,
    actor_type: String,
    method_id: String,
    dependencies: Vec<ObjectId>,
    constructor_dependencies: Vec<ObjectId>,
}

impl HeadService {
    pub fn new(object_store: InMemoryObjectStore, scheduler: Scheduler) -> Self {
        Self {
            object_store,
            scheduler: Arc::new(Mutex::new(scheduler)),
            actors: Arc::new(Mutex::new(HashMap::new())),
            actor_tasks: Arc::new(Mutex::new(HashMap::new())),
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

    pub async fn cleanup_expired_workers(&self) -> usize {
        let expiries = self.scheduler.lock().await.expire_workers();
        let expired_workers: Vec<WorkerId> =
            expiries.iter().map(|expiry| expiry.worker_id).collect();
        self.release_actor_owners(&expired_workers).await;
        expiries.len()
    }

    async fn remove_worker(&self, worker_id: WorkerId, reason: String) {
        let removed = self
            .scheduler
            .lock()
            .await
            .remove_worker(worker_id, reason)
            .is_some();
        if removed {
            self.release_actor_owners(&[worker_id]).await;
        }
    }

    async fn release_actor_owners(&self, worker_ids: &[WorkerId]) {
        if worker_ids.is_empty() {
            return;
        }

        for actor in self.actors.lock().await.values_mut() {
            if actor
                .owner_worker
                .is_some_and(|owner| worker_ids.contains(&owner))
            {
                actor.owner_worker = None;
            }
        }
    }
}

pub async fn serve(bind: SocketAddr) -> Result<(), tonic::transport::Error> {
    let service = HeadService::with_defaults();
    let cleanup_service = service.clone();
    let cleanup_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            cleanup_service.cleanup_expired_workers().await;
        }
    });
    let result = Server::builder()
        .add_service(HeadServer::new(service))
        .serve(bind)
        .await;
    cleanup_task.abort();
    result
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

        self.actors.lock().await.insert(
            actor_id,
            ActorRecord {
                spec,
                owner_worker: None,
            },
        );

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
        let actor = self
            .actors
            .lock()
            .await
            .get(&actor_id)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("actor {actor_id} was not found")))?;

        let output_id = parse_object_id(&request.output_id)?;
        let method_dependencies = parse_object_ids(request.dependencies)?;
        let mut scheduling_dependencies = method_dependencies.clone();
        if actor.owner_worker.is_none() {
            scheduling_dependencies.extend(actor.spec.constructor_dependencies.iter().copied());
        }
        let mut spec = TaskSpec::new(
            format!(
                "actor:{actor_id}:{}:{}",
                actor.spec.actor_type, request.method_id
            ),
            scheduling_dependencies,
            output_id,
        );
        spec.target_worker = actor.owner_worker;
        spec.max_retries = request.max_retries;
        let task_id = spec.task_id;

        self.actor_tasks.lock().await.insert(
            task_id,
            ActorTaskLeaseMeta {
                actor_id,
                actor_type: actor.spec.actor_type,
                method_id: request.method_id,
                dependencies: method_dependencies,
                constructor_dependencies: actor.spec.constructor_dependencies,
            },
        );

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

    async fn unregister_worker(
        &self,
        request: Request<UnregisterWorkerRequest>,
    ) -> std::result::Result<Response<UnregisterWorkerResponse>, Status> {
        let worker_id = parse_worker_id(&request.into_inner().worker_id)?;
        self.remove_worker(worker_id, "worker unregistered".to_string())
            .await;

        Ok(Response::new(UnregisterWorkerResponse {}))
    }

    async fn poll_task(
        &self,
        request: Request<PollTaskRequest>,
    ) -> std::result::Result<Response<PollTaskResponse>, Status> {
        let request = request.into_inner();
        let worker_id = parse_worker_id(&request.worker_id)?;
        self.cleanup_expired_workers().await;
        let leased_specs = self
            .scheduler
            .lock()
            .await
            .lease_tasks(worker_id, request.capacity as usize)
            .map_err(status_from_error)?;
        let mut tasks = Vec::with_capacity(leased_specs.len());
        for spec in leased_specs {
            let task_id = spec.task_id;
            let actor_meta = self.actor_tasks.lock().await.get(&task_id).cloned();
            if let Some(meta) = &actor_meta {
                let mut actors = self.actors.lock().await;
                if let Some(actor) = actors.get_mut(&meta.actor_id) {
                    if actor.owner_worker.is_none() {
                        actor.owner_worker = Some(worker_id);
                    }
                }
            }
            tasks.push(task_lease_from_spec(spec, actor_meta));
        }

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

fn task_lease_from_spec(spec: TaskSpec, actor_meta: Option<ActorTaskLeaseMeta>) -> TaskLease {
    let (actor_id, actor_type, method_id, dependencies, constructor_dependencies) =
        if let Some(meta) = actor_meta {
            (
                meta.actor_id.to_string(),
                meta.actor_type,
                meta.method_id,
                meta.dependencies
                    .into_iter()
                    .map(|object_id| object_id.to_string())
                    .collect(),
                meta.constructor_dependencies
                    .into_iter()
                    .map(|object_id| object_id.to_string())
                    .collect(),
            )
        } else {
            (
                String::new(),
                String::new(),
                String::new(),
                spec.dependencies
                    .iter()
                    .map(|object_id| object_id.to_string())
                    .collect(),
                Vec::new(),
            )
        };

    TaskLease {
        task_id: spec.task_id.to_string(),
        function_id: spec.function_id,
        dependencies,
        output_id: spec.output_id.to_string(),
        attempt: spec.attempt,
        actor_id,
        actor_type,
        method_id,
        constructor_dependencies,
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
    async fn actor_tasks_are_pinned_to_first_worker_that_polls_actor() {
        let service = HeadService::with_defaults();
        let actor_id = ActorId::new();
        let owner = WorkerId::new();
        let other = WorkerId::new();
        let first_output = ObjectId::new();
        let second_output = ObjectId::new();

        for worker_id in [owner, other] {
            service
                .register_worker(Request::new(RegisterWorkerRequest {
                    worker_id: worker_id.to_string(),
                    slots: 1,
                }))
                .await
                .unwrap();
        }
        service
            .create_actor(Request::new(CreateActorRequest {
                actor_type: "Counter".to_string(),
                constructor_dependencies: vec![],
                actor_id: actor_id.to_string(),
                max_restarts: 0,
            }))
            .await
            .unwrap();
        let first = service
            .submit_actor_task(Request::new(SubmitActorTaskRequest {
                actor_id: actor_id.to_string(),
                method_id: "increment".to_string(),
                dependencies: vec![],
                output_id: first_output.to_string(),
                max_retries: 1,
            }))
            .await
            .unwrap()
            .into_inner();

        let first_poll = service
            .poll_task(Request::new(PollTaskRequest {
                worker_id: owner.to_string(),
                capacity: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(first_poll.tasks.len(), 1);
        assert_eq!(first_poll.tasks[0].actor_id, actor_id.to_string());
        assert_eq!(first_poll.tasks[0].actor_type, "Counter");
        assert_eq!(first_poll.tasks[0].method_id, "increment");

        service
            .complete_task(Request::new(CompleteTaskRequest {
                worker_id: owner.to_string(),
                task_id: first.task_id,
                output_id: first_output.to_string(),
                data: encode(&1u64).unwrap(),
            }))
            .await
            .unwrap();

        service
            .submit_actor_task(Request::new(SubmitActorTaskRequest {
                actor_id: actor_id.to_string(),
                method_id: "increment".to_string(),
                dependencies: vec![],
                output_id: second_output.to_string(),
                max_retries: 1,
            }))
            .await
            .unwrap();

        let wrong_worker_poll = service
            .poll_task(Request::new(PollTaskRequest {
                worker_id: other.to_string(),
                capacity: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(wrong_worker_poll.tasks.is_empty());

        let owner_poll = service
            .poll_task(Request::new(PollTaskRequest {
                worker_id: owner.to_string(),
                capacity: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(owner_poll.tasks.len(), 1);
        assert_eq!(owner_poll.tasks[0].actor_id, actor_id.to_string());
    }

    #[tokio::test]
    async fn unregistering_worker_releases_actor_to_replacement_worker() {
        let service = HeadService::with_defaults();
        let actor_id = ActorId::new();
        let owner = WorkerId::new();
        let replacement = WorkerId::new();

        for worker_id in [owner, replacement] {
            service
                .register_worker(Request::new(RegisterWorkerRequest {
                    worker_id: worker_id.to_string(),
                    slots: 1,
                }))
                .await
                .unwrap();
        }
        service
            .create_actor(Request::new(CreateActorRequest {
                actor_type: "Counter".to_string(),
                constructor_dependencies: vec![],
                actor_id: actor_id.to_string(),
                max_restarts: 1,
            }))
            .await
            .unwrap();
        let first_output = ObjectId::new();
        let first = service
            .submit_actor_task(Request::new(SubmitActorTaskRequest {
                actor_id: actor_id.to_string(),
                method_id: "increment".to_string(),
                dependencies: vec![],
                output_id: first_output.to_string(),
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
        service
            .complete_task(Request::new(CompleteTaskRequest {
                worker_id: owner.to_string(),
                task_id: first.task_id,
                output_id: first_output.to_string(),
                data: encode(&1u64).unwrap(),
            }))
            .await
            .unwrap();

        service
            .unregister_worker(Request::new(UnregisterWorkerRequest {
                worker_id: owner.to_string(),
            }))
            .await
            .unwrap();
        service
            .submit_actor_task(Request::new(SubmitActorTaskRequest {
                actor_id: actor_id.to_string(),
                method_id: "increment".to_string(),
                dependencies: vec![],
                output_id: ObjectId::new().to_string(),
                max_retries: 1,
            }))
            .await
            .unwrap();

        let polled = service
            .poll_task(Request::new(PollTaskRequest {
                worker_id: replacement.to_string(),
                capacity: 1,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(polled.tasks.len(), 1);
        assert_eq!(polled.tasks[0].actor_id, actor_id.to_string());
    }

    #[tokio::test]
    async fn cleanup_expired_workers_retries_leased_task() {
        let service = HeadService::new(
            InMemoryObjectStore::new(),
            Scheduler::new(Duration::from_secs(1)),
        );
        let expired = WorkerId::new();
        let replacement = WorkerId::new();
        for worker_id in [expired, replacement] {
            service
                .register_worker(Request::new(RegisterWorkerRequest {
                    worker_id: worker_id.to_string(),
                    slots: 1,
                }))
                .await
                .unwrap();
        }
        let submitted = service
            .submit_task(Request::new(SubmitTaskRequest {
                function_id: "noop".to_string(),
                dependencies: vec![],
                output_id: ObjectId::new().to_string(),
                max_retries: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        service
            .poll_task(Request::new(PollTaskRequest {
                worker_id: expired.to_string(),
                capacity: 1,
            }))
            .await
            .unwrap();
        service.scheduler.lock().await.force_worker_last_heartbeat(
            expired,
            std::time::Instant::now() - Duration::from_secs(2),
        );

        assert_eq!(service.cleanup_expired_workers().await, 1);
        let polled = service
            .poll_task(Request::new(PollTaskRequest {
                worker_id: replacement.to_string(),
                capacity: 1,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(polled.tasks.len(), 1);
        assert_eq!(polled.tasks[0].task_id, submitted.task_id);
        assert_eq!(polled.tasks[0].attempt, 1);
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
