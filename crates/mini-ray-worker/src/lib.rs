//! Worker process loop and task execution.

use mini_ray_core::{MiniRayError, Result, WorkerId};
use mini_ray_proto::miniray::v1::{
    head_client::HeadClient, CompleteTaskRequest, FailTaskRequest, GetObjectRequest,
    HeartbeatRequest, PollTaskRequest, RegisterWorkerRequest, TaskLease,
};
use mini_ray_runtime::TaskRegistry;
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
    client: HeadClient<Channel>,
}

impl Worker {
    pub async fn connect(config: WorkerConfig, registry: TaskRegistry) -> Result<Self> {
        let client = HeadClient::connect(config.head_endpoint.clone())
            .await
            .map_err(transport_error)?;
        let worker = Self {
            worker_id: WorkerId::new(),
            config,
            registry,
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

        execute_lease(&self.registry, lease, args)
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
}
