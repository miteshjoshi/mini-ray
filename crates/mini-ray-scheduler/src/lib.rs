use mini_ray_core::{MiniRayError, ObjectId, Result, TaskId, TaskSpec, WorkerId};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Ready,
    Leased(WorkerId),
    Running(WorkerId),
    Finished,
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct TaskRecord {
    pub spec: TaskSpec,
    pub state: TaskState,
}

#[derive(Debug, Clone)]
struct WorkerRecord {
    slots: usize,
    last_heartbeat: Instant,
    leased: VecDeque<TaskId>,
}

#[derive(Debug)]
pub struct Scheduler {
    tasks: HashMap<TaskId, TaskRecord>,
    ready: VecDeque<TaskId>,
    known_objects: HashSet<ObjectId>,
    workers: HashMap<WorkerId, WorkerRecord>,
    heartbeat_timeout: Duration,
}

impl Scheduler {
    pub fn new(heartbeat_timeout: Duration) -> Self {
        Self {
            tasks: HashMap::new(),
            ready: VecDeque::new(),
            known_objects: HashSet::new(),
            workers: HashMap::new(),
            heartbeat_timeout,
        }
    }

    pub fn register_worker(&mut self, worker_id: WorkerId, slots: usize) {
        self.workers.insert(
            worker_id,
            WorkerRecord {
                slots: slots.max(1),
                last_heartbeat: Instant::now(),
                leased: VecDeque::new(),
            },
        );
    }

    pub fn heartbeat(&mut self, worker_id: WorkerId) {
        if let Some(worker) = self.workers.get_mut(&worker_id) {
            worker.last_heartbeat = Instant::now();
        }
    }

    pub fn submit(&mut self, spec: TaskSpec) -> Result<()> {
        if self.tasks.contains_key(&spec.task_id) {
            return Err(MiniRayError::Scheduler(format!(
                "task {} already exists",
                spec.task_id
            )));
        }

        let state = if self.dependencies_ready(&spec) {
            self.ready.push_back(spec.task_id);
            TaskState::Ready
        } else {
            TaskState::Pending
        };

        self.tasks.insert(spec.task_id, TaskRecord { spec, state });
        Ok(())
    }

    pub fn object_available(&mut self, object_id: ObjectId) {
        self.known_objects.insert(object_id);
        let newly_ready: Vec<TaskId> = self
            .tasks
            .iter()
            .filter_map(|(task_id, record)| {
                if record.state == TaskState::Pending && self.dependencies_ready(&record.spec) {
                    Some(*task_id)
                } else {
                    None
                }
            })
            .collect();

        for task_id in newly_ready {
            if let Some(record) = self.tasks.get_mut(&task_id) {
                record.state = TaskState::Ready;
                self.ready.push_back(task_id);
            }
        }
    }

    pub fn lease_tasks(&mut self, worker_id: WorkerId, capacity: usize) -> Result<Vec<TaskSpec>> {
        self.expire_workers();
        let worker = self
            .workers
            .get(&worker_id)
            .ok_or_else(|| MiniRayError::Scheduler(format!("unknown worker {worker_id}")))?;
        let available = worker
            .slots
            .saturating_sub(worker.leased.len())
            .min(capacity);
        let to_assign = available.max(steal_capacity(capacity, worker.leased.len()));

        let mut assigned = Vec::new();
        for _ in 0..to_assign {
            let Some(task_id) = self
                .ready
                .pop_front()
                .or_else(|| self.steal_from_busiest(worker_id))
            else {
                break;
            };
            if let Some(record) = self.tasks.get_mut(&task_id) {
                record.state = TaskState::Leased(worker_id);
                assigned.push(record.spec.clone());
            }
            if let Some(worker) = self.workers.get_mut(&worker_id) {
                worker.leased.push_back(task_id);
            }
        }

        Ok(assigned)
    }

    pub fn mark_running(&mut self, worker_id: WorkerId, task_id: TaskId) -> Result<()> {
        let record = self
            .tasks
            .get_mut(&task_id)
            .ok_or_else(|| MiniRayError::Scheduler(format!("unknown task {task_id}")))?;
        if record.state != TaskState::Leased(worker_id) {
            return Err(MiniRayError::Scheduler(format!(
                "task {task_id} is not leased to worker {worker_id}"
            )));
        }
        record.state = TaskState::Running(worker_id);
        Ok(())
    }

    pub fn complete(
        &mut self,
        worker_id: WorkerId,
        task_id: TaskId,
        output_id: ObjectId,
    ) -> Result<()> {
        self.ensure_owned_by_worker(worker_id, task_id)?;
        self.remove_worker_lease(worker_id, task_id);
        if let Some(record) = self.tasks.get_mut(&task_id) {
            record.state = TaskState::Finished;
        }
        self.object_available(output_id);
        Ok(())
    }

    pub fn validate_task_owner(&self, worker_id: WorkerId, task_id: TaskId) -> Result<()> {
        self.ensure_owned_by_worker(worker_id, task_id)
    }

    pub fn fail(&mut self, worker_id: WorkerId, task_id: TaskId, error: String) -> Result<()> {
        self.ensure_owned_by_worker(worker_id, task_id)?;
        self.remove_worker_lease(worker_id, task_id);
        let record = self
            .tasks
            .get_mut(&task_id)
            .ok_or_else(|| MiniRayError::Scheduler(format!("unknown task {task_id}")))?;
        if record.spec.attempt < record.spec.max_retries {
            record.spec.attempt += 1;
            record.state = TaskState::Ready;
            self.ready.push_back(task_id);
        } else {
            record.state = TaskState::Failed(error);
        }
        Ok(())
    }

    pub fn task_state(&self, task_id: TaskId) -> Option<TaskState> {
        self.tasks.get(&task_id).map(|record| record.state.clone())
    }

    pub fn force_worker_last_heartbeat(&mut self, worker_id: WorkerId, last_heartbeat: Instant) {
        if let Some(worker) = self.workers.get_mut(&worker_id) {
            worker.last_heartbeat = last_heartbeat;
        }
    }

    pub fn expire_workers(&mut self) {
        let now = Instant::now();
        let expired: Vec<WorkerId> = self
            .workers
            .iter()
            .filter_map(|(worker_id, worker)| {
                if now.duration_since(worker.last_heartbeat) > self.heartbeat_timeout {
                    Some(*worker_id)
                } else {
                    None
                }
            })
            .collect();

        for worker_id in expired {
            if let Some(worker) = self.workers.remove(&worker_id) {
                for task_id in worker.leased {
                    if let Some(record) = self.tasks.get_mut(&task_id) {
                        if matches!(record.state, TaskState::Leased(_) | TaskState::Running(_)) {
                            if record.spec.attempt < record.spec.max_retries {
                                record.spec.attempt += 1;
                                record.state = TaskState::Ready;
                                self.ready.push_back(task_id);
                            } else {
                                record.state =
                                    TaskState::Failed("worker heartbeat expired".to_string());
                            }
                        }
                    }
                }
            }
        }
    }

    fn dependencies_ready(&self, spec: &TaskSpec) -> bool {
        spec.dependencies
            .iter()
            .all(|object_id| self.known_objects.contains(object_id))
    }

    fn steal_from_busiest(&mut self, thief: WorkerId) -> Option<TaskId> {
        let victim_id = self
            .workers
            .iter()
            .filter(|(worker_id, worker)| **worker_id != thief && worker.leased.len() > 1)
            .max_by_key(|(_, worker)| worker.leased.len())
            .map(|(worker_id, _)| *worker_id)?;

        let victim = self.workers.get_mut(&victim_id)?;
        let index = victim.leased.iter().position(|task_id| {
            self.tasks
                .get(task_id)
                .is_some_and(|record| matches!(record.state, TaskState::Leased(_)))
        })?;
        let task_id = victim.leased.remove(index)?;
        if let Some(record) = self.tasks.get_mut(&task_id) {
            record.state = TaskState::Ready;
        }
        Some(task_id)
    }

    fn remove_worker_lease(&mut self, worker_id: WorkerId, task_id: TaskId) {
        if let Some(worker) = self.workers.get_mut(&worker_id) {
            worker.leased.retain(|leased| *leased != task_id);
        }
    }

    fn ensure_owned_by_worker(&self, worker_id: WorkerId, task_id: TaskId) -> Result<()> {
        let record = self
            .tasks
            .get(&task_id)
            .ok_or_else(|| MiniRayError::Scheduler(format!("unknown task {task_id}")))?;

        match record.state {
            TaskState::Leased(owner) | TaskState::Running(owner) if owner == worker_id => Ok(()),
            _ => Err(MiniRayError::Scheduler(format!(
                "task {task_id} is not owned by worker {worker_id}"
            ))),
        }
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new(Duration::from_secs(10))
    }
}

fn steal_capacity(requested: usize, held: usize) -> usize {
    if requested > 0 && held == 0 {
        requested
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependency_resolution_moves_task_to_ready() {
        let input = ObjectId::new();
        let output = ObjectId::new();
        let spec = TaskSpec::new("add_one", vec![input], output);
        let task_id = spec.task_id;
        let mut scheduler = Scheduler::default();

        scheduler.submit(spec).unwrap();
        assert_eq!(scheduler.task_state(task_id), Some(TaskState::Pending));

        scheduler.object_available(input);
        assert_eq!(scheduler.task_state(task_id), Some(TaskState::Ready));
    }

    #[test]
    fn duplicate_task_submission_is_rejected() {
        let spec = TaskSpec::new("noop", vec![], ObjectId::new());
        let mut scheduler = Scheduler::default();

        scheduler.submit(spec.clone()).unwrap();
        let err = scheduler.submit(spec).unwrap_err();

        assert!(matches!(err, MiniRayError::Scheduler(_)));
    }

    #[test]
    fn multiple_workers_get_independent_tasks() {
        let mut scheduler = Scheduler::default();
        let worker_a = WorkerId::new();
        let worker_b = WorkerId::new();
        scheduler.register_worker(worker_a, 1);
        scheduler.register_worker(worker_b, 1);

        for _ in 0..2 {
            scheduler
                .submit(TaskSpec::new("noop", vec![], ObjectId::new()))
                .unwrap();
        }

        assert_eq!(scheduler.lease_tasks(worker_a, 1).unwrap().len(), 1);
        assert_eq!(scheduler.lease_tasks(worker_b, 1).unwrap().len(), 1);
    }

    #[test]
    fn leasing_marks_task_as_leased_to_worker() {
        let mut scheduler = Scheduler::default();
        let worker = WorkerId::new();
        scheduler.register_worker(worker, 1);
        let spec = TaskSpec::new("noop", vec![], ObjectId::new());
        let task_id = spec.task_id;
        scheduler.submit(spec).unwrap();

        let leased = scheduler.lease_tasks(worker, 1).unwrap();

        assert_eq!(leased.len(), 1);
        assert_eq!(
            scheduler.task_state(task_id),
            Some(TaskState::Leased(worker))
        );
    }

    #[test]
    fn unknown_worker_cannot_lease_tasks() {
        let mut scheduler = Scheduler::default();
        let err = scheduler.lease_tasks(WorkerId::new(), 1).unwrap_err();

        assert!(matches!(err, MiniRayError::Scheduler(_)));
    }

    #[test]
    fn wrong_worker_cannot_mark_task_running() {
        let mut scheduler = Scheduler::default();
        let owner = WorkerId::new();
        let other = WorkerId::new();
        scheduler.register_worker(owner, 1);
        scheduler.register_worker(other, 1);
        scheduler
            .submit(TaskSpec::new("noop", vec![], ObjectId::new()))
            .unwrap();
        let task_id = scheduler.lease_tasks(owner, 1).unwrap()[0].task_id;

        let err = scheduler.mark_running(other, task_id).unwrap_err();

        assert!(matches!(err, MiniRayError::Scheduler(_)));
        assert_eq!(
            scheduler.task_state(task_id),
            Some(TaskState::Leased(owner))
        );
    }

    #[test]
    fn idle_worker_steals_queued_not_running_task() {
        let mut scheduler = Scheduler::default();
        let busy = WorkerId::new();
        let idle = WorkerId::new();
        scheduler.register_worker(busy, 4);
        scheduler.register_worker(idle, 1);

        for _ in 0..3 {
            scheduler
                .submit(TaskSpec::new("noop", vec![], ObjectId::new()))
                .unwrap();
        }
        let leased = scheduler.lease_tasks(busy, 3).unwrap();
        scheduler.mark_running(busy, leased[0].task_id).unwrap();

        let stolen = scheduler.lease_tasks(idle, 1).unwrap();
        assert_eq!(stolen.len(), 1);
        assert_ne!(stolen[0].task_id, leased[0].task_id);
        assert_eq!(
            scheduler.task_state(leased[0].task_id),
            Some(TaskState::Running(busy))
        );
    }

    #[test]
    fn failed_task_retries_once_then_fails() {
        let mut scheduler = Scheduler::default();
        let worker = WorkerId::new();
        scheduler.register_worker(worker, 1);
        let spec = TaskSpec::new("noop", vec![], ObjectId::new());
        let task_id = spec.task_id;
        scheduler.submit(spec).unwrap();

        scheduler.lease_tasks(worker, 1).unwrap();
        scheduler.fail(worker, task_id, "boom".to_string()).unwrap();
        assert_eq!(scheduler.task_state(task_id), Some(TaskState::Ready));

        scheduler.lease_tasks(worker, 1).unwrap();
        scheduler
            .fail(worker, task_id, "boom again".to_string())
            .unwrap();
        assert_eq!(
            scheduler.task_state(task_id),
            Some(TaskState::Failed("boom again".to_string()))
        );
    }

    #[test]
    fn complete_makes_output_available_for_dependent_task() {
        let mut scheduler = Scheduler::default();
        let worker = WorkerId::new();
        scheduler.register_worker(worker, 1);
        let output = ObjectId::new();
        let producer = TaskSpec::new("produce", vec![], output);
        let producer_id = producer.task_id;
        let consumer = TaskSpec::new("consume", vec![output], ObjectId::new());
        let consumer_id = consumer.task_id;
        scheduler.submit(producer).unwrap();
        scheduler.submit(consumer).unwrap();

        scheduler.lease_tasks(worker, 1).unwrap();
        scheduler.complete(worker, producer_id, output).unwrap();

        assert_eq!(scheduler.task_state(producer_id), Some(TaskState::Finished));
        assert_eq!(scheduler.task_state(consumer_id), Some(TaskState::Ready));
    }

    #[test]
    fn heartbeat_expiry_retries_task_once() {
        let mut scheduler = Scheduler::new(Duration::from_millis(1));
        let worker = WorkerId::new();
        let replacement = WorkerId::new();
        scheduler.register_worker(worker, 1);
        scheduler.register_worker(replacement, 1);
        let spec = TaskSpec::new("noop", vec![], ObjectId::new());
        let task_id = spec.task_id;
        scheduler.submit(spec).unwrap();
        scheduler.lease_tasks(worker, 1).unwrap();
        scheduler.force_worker_last_heartbeat(worker, Instant::now() - Duration::from_secs(1));

        scheduler.expire_workers();
        let retried = scheduler.lease_tasks(replacement, 1).unwrap();

        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].task_id, task_id);
        assert_eq!(retried[0].attempt, 1);
    }
}
