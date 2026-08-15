//! Worker process loop and task execution.

use mini_ray_core::{MiniRayError, Result, WorkerId};
use mini_ray_proto::miniray::v1::{
    head_client::HeadClient, CompleteTaskRequest, FailTaskRequest, GetObjectRequest,
    HeartbeatRequest, PollTaskRequest, RegisterWorkerRequest, TaskLease, UnregisterWorkerRequest,
};
use mini_ray_runtime::{ActorInstanceStore, ActorRegistry, TaskRegistry};
use std::path::PathBuf;
use std::process::Command;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use std::time::Duration;
use tonic::transport::Channel;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub head_endpoint: String,
    pub slots: u32,
    pub poll_capacity: u32,
    pub poll_interval: Duration,
    pub heartbeat_interval: Duration,
    pub pressure_path: PathBuf,
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
            pressure_path: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ResourcePressure {
    #[default]
    Unknown,
    Low,
    Moderate,
    High,
    Critical,
}

impl ResourcePressure {
    fn as_wire(self) -> u32 {
        match self {
            Self::Unknown => 0,
            Self::Low => 1,
            Self::Moderate => 2,
            Self::High => 3,
            Self::Critical => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResourceSample {
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub memory_pressure: ResourcePressure,
    pub disk_used_bytes: u64,
    pub disk_total_bytes: u64,
    pub disk_pressure: ResourcePressure,
}

#[derive(Debug)]
pub struct Worker {
    worker_id: WorkerId,
    config: WorkerConfig,
    registry: TaskRegistry,
    actor_registry: ActorRegistry,
    actor_instances: ActorInstanceStore,
    client: HeadClient<Channel>,
    running_tasks: Arc<AtomicU32>,
    queued_tasks: Arc<AtomicU32>,
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
            running_tasks: Arc::new(AtomicU32::new(0)),
            queued_tasks: Arc::new(AtomicU32::new(0)),
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
            .heartbeat(build_heartbeat_request(
                self.worker_id,
                &self.config.pressure_path,
                &self.running_tasks,
                &self.queued_tasks,
            ))
            .await
            .map_err(status_error)?;
        Ok(())
    }

    pub async fn unregister(&mut self) -> Result<()> {
        self.client
            .unregister_worker(UnregisterWorkerRequest {
                worker_id: self.worker_id.to_string(),
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
        self.queued_tasks
            .store(saturating_u32(task_count), Ordering::SeqCst);

        let mut normal_tasks = Vec::new();
        let mut first_error = None;
        for lease in response.tasks {
            if lease.actor_id.is_empty() {
                self.queued_tasks.fetch_sub(1, Ordering::SeqCst);
                let running_tasks = self.running_tasks.clone();
                normal_tasks.push(tokio::spawn(execute_remote_stateless_lease(
                    self.worker_id,
                    self.registry.clone(),
                    self.client.clone(),
                    lease,
                    running_tasks,
                )));
            } else {
                self.queued_tasks.fetch_sub(1, Ordering::SeqCst);
                let _running = RunningTaskGuard::new(self.running_tasks.clone());
                if let Err(error) = self.execute_remote_actor_lease(lease).await {
                    first_error.get_or_insert(error);
                }
            }
        }

        for task in normal_tasks {
            let result = task
                .await
                .map_err(|error| MiniRayError::Transport(error.to_string()))
                .and_then(|result| result);
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }

        if let Some(error) = first_error {
            return Err(error);
        }

        Ok(task_count)
    }

    pub async fn run(mut self) -> Result<()> {
        self.register().await?;

        let mut poll = tokio::time::interval(self.config.poll_interval);
        let mut heartbeat_handle = tokio::spawn(run_heartbeat_loop(
            self.client.clone(),
            self.worker_id,
            self.config.pressure_path.clone(),
            self.config.heartbeat_interval,
            self.running_tasks.clone(),
            self.queued_tasks.clone(),
        ));
        let shutdown = tokio::signal::ctrl_c();
        tokio::pin!(shutdown);

        let run_result = loop {
            tokio::select! {
                _ = poll.tick() => {
                    if let Err(error) = self.poll_and_execute_once().await {
                        break Err(error);
                    }
                }
                result = &mut shutdown => {
                    break result
                        .map_err(|error| MiniRayError::Transport(error.to_string()));
                }
                result = &mut heartbeat_handle => {
                    break result
                        .map_err(|error| MiniRayError::Transport(error.to_string()))
                        .and_then(|result| result);
                }
            }
        };

        heartbeat_handle.abort();
        let cleanup_result = self.unregister().await;
        run_result?;
        cleanup_result
    }

    async fn execute_remote_actor_lease(&mut self, lease: TaskLease) -> Result<()> {
        let execution = match self.fetch_actor_args_and_execute(&lease).await {
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

    async fn fetch_actor_args_and_execute(&mut self, lease: &TaskLease) -> Result<TaskExecution> {
        let args = fetch_objects(&mut self.client, &lease.dependencies).await?;
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

async fn execute_remote_stateless_lease(
    worker_id: WorkerId,
    registry: TaskRegistry,
    mut client: HeadClient<Channel>,
    lease: TaskLease,
    running_tasks: Arc<AtomicU32>,
) -> Result<()> {
    let _running = RunningTaskGuard::new(running_tasks);
    let execution = match fetch_stateless_args_and_execute(registry, &mut client, &lease).await {
        Ok(execution) => execution,
        Err(error) => {
            report_failure(&mut client, worker_id, &lease, error.to_string()).await?;
            return Ok(());
        }
    };

    client
        .complete_task(CompleteTaskRequest {
            worker_id: worker_id.to_string(),
            task_id: execution.task_id,
            output_id: execution.output_id,
            data: execution.data,
        })
        .await
        .map_err(status_error)?;

    Ok(())
}

async fn run_heartbeat_loop(
    mut client: HeadClient<Channel>,
    worker_id: WorkerId,
    pressure_path: PathBuf,
    heartbeat_interval: Duration,
    running_tasks: Arc<AtomicU32>,
    queued_tasks: Arc<AtomicU32>,
) -> Result<()> {
    let mut heartbeat = tokio::time::interval(heartbeat_interval);
    loop {
        heartbeat.tick().await;
        client
            .heartbeat(build_heartbeat_request(
                worker_id,
                &pressure_path,
                &running_tasks,
                &queued_tasks,
            ))
            .await
            .map_err(status_error)?;
    }
}

fn build_heartbeat_request(
    worker_id: WorkerId,
    pressure_path: &PathBuf,
    running_tasks: &AtomicU32,
    queued_tasks: &AtomicU32,
) -> HeartbeatRequest {
    let resources = sample_resources(pressure_path.clone());
    HeartbeatRequest {
        worker_id: worker_id.to_string(),
        running: running_tasks.load(Ordering::SeqCst),
        queued: queued_tasks.load(Ordering::SeqCst),
        memory_used_bytes: resources.memory_used_bytes,
        memory_total_bytes: resources.memory_total_bytes,
        memory_pressure: resources.memory_pressure.as_wire(),
        disk_used_bytes: resources.disk_used_bytes,
        disk_total_bytes: resources.disk_total_bytes,
        disk_pressure: resources.disk_pressure.as_wire(),
    }
}

fn saturating_u32(value: usize) -> u32 {
    value.try_into().unwrap_or(u32::MAX)
}

#[derive(Debug)]
struct RunningTaskGuard {
    running_tasks: Arc<AtomicU32>,
}

impl RunningTaskGuard {
    fn new(running_tasks: Arc<AtomicU32>) -> Self {
        running_tasks.fetch_add(1, Ordering::SeqCst);
        Self { running_tasks }
    }
}

impl Drop for RunningTaskGuard {
    fn drop(&mut self) {
        self.running_tasks.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn fetch_stateless_args_and_execute(
    registry: TaskRegistry,
    client: &mut HeadClient<Channel>,
    lease: &TaskLease,
) -> Result<TaskExecution> {
    let args = fetch_objects(client, &lease.dependencies).await?;
    let lease = lease.clone();
    tokio::task::spawn_blocking(move || execute_lease(&registry, &lease, args))
        .await
        .map_err(|error| MiniRayError::Transport(error.to_string()))?
}

async fn report_failure(
    client: &mut HeadClient<Channel>,
    worker_id: WorkerId,
    lease: &TaskLease,
    error: String,
) -> Result<()> {
    client
        .fail_task(FailTaskRequest {
            worker_id: worker_id.to_string(),
            task_id: lease.task_id.clone(),
            error,
        })
        .await
        .map_err(status_error)?;
    Ok(())
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

pub fn sample_resources(path: impl Into<PathBuf>) -> ResourceSample {
    let memory = sample_memory();
    let disk = sample_disk(path.into());
    let (memory_used_bytes, memory_total_bytes, memory_pressure) =
        resource_fields_from_usage(memory);
    let (disk_used_bytes, disk_total_bytes, disk_pressure) = resource_fields_from_usage(disk);

    ResourceSample {
        memory_used_bytes,
        memory_total_bytes,
        memory_pressure,
        disk_used_bytes,
        disk_total_bytes,
        disk_pressure,
    }
}

#[derive(Debug, Clone, Copy)]
struct UsageSample {
    used_bytes: u64,
    total_bytes: u64,
}

fn resource_fields_from_usage(sample: Option<UsageSample>) -> (u64, u64, ResourcePressure) {
    sample
        .map(|sample| {
            (
                sample.used_bytes,
                sample.total_bytes,
                pressure_from_usage(sample.used_bytes, sample.total_bytes),
            )
        })
        .unwrap_or((0, 0, ResourcePressure::Unknown))
}

fn pressure_from_usage(used_bytes: u64, total_bytes: u64) -> ResourcePressure {
    if total_bytes == 0 {
        return ResourcePressure::Unknown;
    }

    let used_percent = (used_bytes as u128).saturating_mul(100) / total_bytes as u128;
    if used_percent >= 95 {
        ResourcePressure::Critical
    } else if used_percent >= 85 {
        ResourcePressure::High
    } else if used_percent >= 70 {
        ResourcePressure::Moderate
    } else {
        ResourcePressure::Low
    }
}

#[cfg(target_os = "linux")]
fn sample_memory() -> Option<UsageSample> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let total_kib = parse_meminfo_kib(&meminfo, "MemTotal:")?;
    let available_kib = parse_meminfo_kib(&meminfo, "MemAvailable:")?;
    let used_kib = total_kib.saturating_sub(available_kib);
    Some(UsageSample {
        used_bytes: used_kib.saturating_mul(1024),
        total_bytes: total_kib.saturating_mul(1024),
    })
}

#[cfg(target_os = "linux")]
fn parse_meminfo_kib(meminfo: &str, key: &str) -> Option<u64> {
    meminfo.lines().find_map(|line| {
        let rest = line.strip_prefix(key)?;
        rest.split_whitespace().next()?.parse().ok()
    })
}

#[cfg(target_os = "macos")]
fn sample_memory() -> Option<UsageSample> {
    let total_bytes: u64 = command_stdout(Command::new("sysctl").args(["-n", "hw.memsize"]))?
        .trim()
        .parse()
        .ok()?;
    let vm_stat = command_stdout(&mut Command::new("vm_stat"))?;
    let page_size = parse_vm_stat_page_size(&vm_stat)?;
    let free_pages = parse_vm_stat_pages(&vm_stat, "Pages free:").unwrap_or(0);
    let inactive_pages = parse_vm_stat_pages(&vm_stat, "Pages inactive:").unwrap_or(0);
    let speculative_pages = parse_vm_stat_pages(&vm_stat, "Pages speculative:").unwrap_or(0);
    let available_bytes = free_pages
        .saturating_add(inactive_pages)
        .saturating_add(speculative_pages)
        .saturating_mul(page_size);
    Some(UsageSample {
        used_bytes: total_bytes.saturating_sub(available_bytes),
        total_bytes,
    })
}

#[cfg(target_os = "macos")]
fn parse_vm_stat_page_size(vm_stat: &str) -> Option<u64> {
    let first = vm_stat.lines().next()?;
    let digits: String = first.chars().filter(|ch| ch.is_ascii_digit()).collect();
    digits.parse().ok()
}

#[cfg(target_os = "macos")]
fn parse_vm_stat_pages(vm_stat: &str, key: &str) -> Option<u64> {
    vm_stat.lines().find_map(|line| {
        let rest = line.trim().strip_prefix(key)?;
        rest.trim()
            .trim_end_matches('.')
            .replace(',', "")
            .parse()
            .ok()
    })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn sample_memory() -> Option<UsageSample> {
    None
}

fn sample_disk(path: PathBuf) -> Option<UsageSample> {
    let output = command_stdout(Command::new("df").arg("-Pk").arg(path))?;
    parse_df_output(&output)
}

fn parse_df_output(output: &str) -> Option<UsageSample> {
    let line = output.lines().nth(1)?;
    let columns: Vec<&str> = line.split_whitespace().collect();
    let total_kib: u64 = columns.get(1)?.parse().ok()?;
    let used_kib: u64 = columns.get(2)?.parse().ok()?;
    Some(UsageSample {
        used_bytes: used_kib.saturating_mul(1024),
        total_bytes: total_kib.saturating_mul(1024),
    })
}

fn command_stdout(command: &mut Command) -> Option<String> {
    let output = command.output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
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
    fn pressure_from_usage_uses_expected_thresholds() {
        assert_eq!(pressure_from_usage(0, 0), ResourcePressure::Unknown);
        assert_eq!(pressure_from_usage(69, 100), ResourcePressure::Low);
        assert_eq!(pressure_from_usage(70, 100), ResourcePressure::Moderate);
        assert_eq!(pressure_from_usage(85, 100), ResourcePressure::High);
        assert_eq!(pressure_from_usage(95, 100), ResourcePressure::Critical);
        assert_eq!(
            pressure_from_usage(u64::MAX, u64::MAX),
            ResourcePressure::Critical
        );
    }

    #[test]
    fn parse_df_output_reads_total_and_used_kib() {
        let sample = parse_df_output(
            "Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/disk1 100 85 15 85% /\n",
        )
        .unwrap();

        assert_eq!(sample.total_bytes, 100 * 1024);
        assert_eq!(sample.used_bytes, 85 * 1024);
    }

    #[test]
    fn heartbeat_request_reports_running_tasks() {
        let running_tasks = Arc::new(AtomicU32::new(2));
        let queued_tasks = Arc::new(AtomicU32::new(3));
        let request = build_heartbeat_request(
            WorkerId::new(),
            &PathBuf::from("."),
            &running_tasks,
            &queued_tasks,
        );

        assert_eq!(request.running, 2);
        assert_eq!(request.queued, 3);
    }

    #[test]
    fn saturating_u32_caps_large_counts() {
        assert_eq!(saturating_u32(7), 7);
        assert_eq!(saturating_u32(usize::MAX), u32::MAX);
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
