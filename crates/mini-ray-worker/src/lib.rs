//! Worker process loop and task execution.

use mini_ray_core::{MiniRayError, Result, WorkerId};
use mini_ray_proto::miniray::v1::{
    head_client::HeadClient, CompleteTaskRequest, FailTaskRequest, GetObjectRequest,
    HeartbeatRequest, PollTaskRequest, RegisterWorkerRequest, TaskLease,
};
use mini_ray_runtime::{ActorInstanceStore, ActorRegistry, TaskRegistry};
use std::time::Duration;
use tonic::transport::Channel;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub head_endpoint: String,
    pub slots: u32,
    pub poll_capacity: u32,
    pub poll_interval: Duration,
    pub heartbeat_interval: Duration,
}

impl WorkerConfig {
    pub fn new(head_endpoint: impl Into<String>, slots: u32) -> Self {
        let slots = slots.max(1);
        Self {
            head_endpoint: head_endpoint.into(),
            slots,
            poll_capacity: slots,
            poll_interval: Duration::from_millis(100),
            heartbeat_interval: Duration::from_secs(1),
        }
    }
}

#[derive(Debug)]
pub struct Worker {
    worker_id: WorkerId,
    config: WorkerConfig,
    registry: TaskRegistry,
    actor_registry: ActorRegistry,
    actor_instances: ActorInstanceStore,
    client: HeadClient<Channel>,
}

impl Worker {
    pub async fn connect(config: WorkerConfig, registry: TaskRegistry) -> Result<Self> {
        Self::connect_with_actors(config, registry, ActorRegistry::new()).await
    }

    pub async fn connect_with_actors(
        config: WorkerConfig,
        registry: TaskRegistry,
        actor_registry: ActorRegistry,
    ) -> Result<Self> {
        let client = HeadClient::connect(config.head_endpoint.clone())
            .await
            .map_err(transport_error)?;
        let worker = Self {
            worker_id: WorkerId::new(),
            config,
            registry,
            actor_registry,
            actor_instances: ActorInstanceStore::new(),
            client,
        };
        Ok(worker)
    }

    pub fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    pub async fn register(&mut self) -> Result<()> {
        self.client
            .register_worker(RegisterWorkerRequest {
                worker_id: self.worker_id.to_string(),
                slots: self.config.slots,
            })
            .await
            .map_err(status_error)?;
        Ok(())
    }

    pub async fn heartbeat(&mut self) -> Result<()> {
        self.client
            .heartbeat(HeartbeatRequest {
                worker_id: self.worker_id.to_string(),
                running: 0,
                queued: 0,
            })
            .await
            .map_err(status_error)?;
        Ok(())
    }

    pub async fn poll_and_execute_once(&mut self) -> Result<usize> {
        let response = self
            .client
            .poll_task(PollTaskRequest {
                worker_id: self.worker_id.to_string(),
                capacity: self.config.poll_capacity,
            })
            .await
            .map_err(status_error)?
            .into_inner();

        let task_count = response.tasks.len();
        for lease in response.tasks {
            self.execute_remote_lease(lease).await?;
        }

        Ok(task_count)
    }

    pub async fn run(mut self) -> Result<()> {
        self.register().await?;

        let mut poll = tokio::time::interval(self.config.poll_interval);
        let mut heartbeat = tokio::time::interval(self.config.heartbeat_interval);

        loop {
            tokio::select! {
                _ = poll.tick() => {
                    self.poll_and_execute_once().await?;
                }
                _ = heartbeat.tick() => {
                    self.heartbeat().await?;
                }
            }
        }
    }

    async fn execute_remote_lease(&mut self, lease: TaskLease) -> Result<()> {
        let execution = match self.fetch_args_and_execute(&lease).await {
            Ok(execution) => execution,
            Err(error) => {
                self.report_failure(&lease, error.to_string()).await?;
                return Ok(());
            }
        };

        self.client
            .complete_task(CompleteTaskRequest {
                worker_id: self.worker_id.to_string(),
                task_id: execution.task_id,
                output_id: execution.output_id,
                data: execution.data,
            })
            .await
            .map_err(status_error)?;

        Ok(())
    }

    async fn fetch_args_and_execute(&mut self, lease: &TaskLease) -> Result<TaskExecution> {
        let mut args = Vec::with_capacity(lease.dependencies.len());
        for object_id in &lease.dependencies {
            let response = self
                .client
                .get_object(GetObjectRequest {
                    object_id: object_id.clone(),
                })
                .await
                .map_err(status_error)?
                .into_inner();
            args.push(response.data);
        }

        if lease.actor_id.is_empty() {
            execute_lease(&self.registry, lease, args)
        } else {
            let constructor_args =
                fetch_objects(&mut self.client, &lease.constructor_dependencies).await?;
            execute_actor_lease(
                &self.actor_registry,
                &mut self.actor_instances,
                lease,
                constructor_args,
                args,
            )
        }
    }

    async fn report_failure(&mut self, lease: &TaskLease, error: String) -> Result<()> {
        self.client
            .fail_task(FailTaskRequest {
                worker_id: self.worker_id.to_string(),
                task_id: lease.task_id.clone(),
                error,
            })
            .await
            .map_err(status_error)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskExecution {
    pub task_id: String,
    pub output_id: String,
    pub data: Vec<u8>,
}

pub fn execute_lease(
    registry: &TaskRegistry,
    lease: &TaskLease,
    dependency_data: Vec<Vec<u8>>,
) -> Result<TaskExecution> {
    if dependency_data.len() != lease.dependencies.len() {
        return Err(MiniRayError::TaskFailed(format!(
            "task {} expected {} dependencies, got {} payloads",
            lease.task_id,
            lease.dependencies.len(),
            dependency_data.len()
        )));
    }

    let data = registry.execute(&lease.function_id, dependency_data)?;
    Ok(TaskExecution {
        task_id: lease.task_id.clone(),
        output_id: lease.output_id.clone(),
        data,
    })
}

pub fn execute_actor_lease(
    registry: &ActorRegistry,
    instances: &mut ActorInstanceStore,
    lease: &TaskLease,
    constructor_data: Vec<Vec<u8>>,
    dependency_data: Vec<Vec<u8>>,
) -> Result<TaskExecution> {
    if lease.actor_id.is_empty() {
        return Err(MiniRayError::TaskFailed(format!(
            "task {} is not an actor lease",
            lease.task_id
        )));
    }
    if constructor_data.len() != lease.constructor_dependencies.len() {
        return Err(MiniRayError::TaskFailed(format!(
            "actor task {} expected {} constructor dependencies, got {} payloads",
            lease.task_id,
            lease.constructor_dependencies.len(),
            constructor_data.len()
        )));
    }
    if dependency_data.len() != lease.dependencies.len() {
        return Err(MiniRayError::TaskFailed(format!(
            "actor task {} expected {} method dependencies, got {} payloads",
            lease.task_id,
            lease.dependencies.len(),
            dependency_data.len()
        )));
    }

    let data = instances.execute(
        registry,
        lease.actor_id.clone(),
        &lease.actor_type,
        &lease.method_id,
        constructor_data,
        dependency_data,
    )?;

    Ok(TaskExecution {
        task_id: lease.task_id.clone(),
        output_id: lease.output_id.clone(),
        data,
    })
}

async fn fetch_objects(
    client: &mut HeadClient<Channel>,
    object_ids: &[String],
) -> Result<Vec<Vec<u8>>> {
    let mut data = Vec::with_capacity(object_ids.len());
    for object_id in object_ids {
        let response = client
            .get_object(GetObjectRequest {
                object_id: object_id.clone(),
            })
            .await
            .map_err(status_error)?
            .into_inner();
        data.push(response.data);
    }
    Ok(data)
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
    use mini_ray_core::{decode, encode, ObjectId, TaskId};

    #[derive(Debug)]
    struct Counter {
        value: u64,
    }

    fn lease(function_id: &str, dependencies: Vec<ObjectId>, output_id: ObjectId) -> TaskLease {
        TaskLease {
            task_id: TaskId::new().to_string(),
            function_id: function_id.to_string(),
            dependencies: dependencies
                .into_iter()
                .map(|object_id| object_id.to_string())
                .collect(),
            output_id: output_id.to_string(),
            attempt: 0,
            actor_id: String::new(),
            actor_type: String::new(),
            method_id: String::new(),
            constructor_dependencies: Vec::new(),
        }
    }

    #[test]
    fn execute_lease_runs_registered_handler() {
        let input_id = ObjectId::new();
        let output_id = ObjectId::new();
        let lease = lease("add_one", vec![input_id], output_id);
        let mut registry = TaskRegistry::new();
        registry
            .register_unary("add_one", |value: u64| value + 1)
            .unwrap();

        let execution = execute_lease(&registry, &lease, vec![encode(&41u64).unwrap()]).unwrap();
        let value: u64 = decode(&execution.data).unwrap();

        assert_eq!(execution.task_id, lease.task_id);
        assert_eq!(execution.output_id, output_id.to_string());
        assert_eq!(value, 42);
    }

    #[test]
    fn execute_lease_rejects_missing_dependency_payloads() {
        let lease = lease("add_one", vec![ObjectId::new()], ObjectId::new());
        let registry = TaskRegistry::new();

        let err = execute_lease(&registry, &lease, vec![]).unwrap_err();

        assert!(matches!(err, MiniRayError::TaskFailed(_)));
    }

    #[test]
    fn execute_lease_returns_unknown_handler_error() {
        let lease = lease("missing", vec![], ObjectId::new());
        let registry = TaskRegistry::new();

        let err = execute_lease(&registry, &lease, vec![]).unwrap_err();

        assert!(matches!(err, MiniRayError::TaskFailed(_)));
    }

    #[test]
    fn execute_actor_lease_preserves_actor_state() {
        let constructor_id = ObjectId::new();
        let first_delta_id = ObjectId::new();
        let second_delta_id = ObjectId::new();
        let output_id = ObjectId::new();
        let mut lease = lease("actor:Counter:increment", vec![first_delta_id], output_id);
        lease.actor_id = "counter-1".to_string();
        lease.actor_type = "Counter".to_string();
        lease.method_id = "increment".to_string();
        lease.constructor_dependencies = vec![constructor_id.to_string()];

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

        let first = execute_actor_lease(
            &registry,
            &mut instances,
            &lease,
            vec![encode(&10u64).unwrap()],
            vec![encode(&2u64).unwrap()],
        )
        .unwrap();
        let mut second_lease = lease.clone();
        second_lease.dependencies = vec![second_delta_id.to_string()];
        let second = execute_actor_lease(
            &registry,
            &mut instances,
            &second_lease,
            vec![encode(&10u64).unwrap()],
            vec![encode(&5u64).unwrap()],
        )
        .unwrap();

        assert_eq!(decode::<u64>(&first.data).unwrap(), 12);
        assert_eq!(decode::<u64>(&second.data).unwrap(), 17);
    }
}
