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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerExpiry {
    pub worker_id: WorkerId,
    pub retried_tasks: Vec<TaskId>,
    pub failed_tasks: Vec<TaskId>,
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

    pub fn submit(&mut self, mut spec: TaskSpec) -> Result<()> {
        if self.tasks.contains_key(&spec.task_id) {
            return Err(MiniRayError::Scheduler(format!(
                "task {} already exists",
                spec.task_id
            )));
        }
        if spec
            .target_worker
            .is_some_and(|worker_id| !self.workers.contains_key(&worker_id))
        {
            spec.target_worker = None;
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
                .pop_ready_for_worker(worker_id)
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

    pub fn fail_tasks(&mut self, task_ids: &[TaskId], error: String) -> Vec<TaskId> {
        let mut failed = Vec::new();

        for task_id in task_ids {
            let Some(state) = self.tasks.get(task_id).map(|record| record.state.clone()) else {
                continue;
            };

            match state {
                TaskState::Finished | TaskState::Failed(_) => {}
                TaskState::Pending | TaskState::Ready => {
                    self.ready.retain(|ready| ready != task_id);
                    if let Some(record) = self.tasks.get_mut(task_id) {
                        record.state = TaskState::Failed(error.clone());
                        failed.push(*task_id);
                    }
                }
                TaskState::Leased(worker_id) | TaskState::Running(worker_id) => {
                    self.remove_worker_lease(worker_id, *task_id);
                    if let Some(record) = self.tasks.get_mut(task_id) {
                        record.state = TaskState::Failed(error.clone());
                        failed.push(*task_id);
                    }
                }
            }
        }

        failed
    }

    pub fn task_state(&self, task_id: TaskId) -> Option<TaskState> {
        self.tasks.get(&task_id).map(|record| record.state.clone())
    }

    pub fn force_worker_last_heartbeat(&mut self, worker_id: WorkerId, last_heartbeat: Instant) {
        if let Some(worker) = self.workers.get_mut(&worker_id) {
            worker.last_heartbeat = last_heartbeat;
        }
    }

    pub fn expire_workers(&mut self) -> Vec<WorkerExpiry> {
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

        let mut expiries = Vec::with_capacity(expired.len());
        for worker_id in expired {
            if let Some(expiry) =
                self.remove_worker(worker_id, "worker heartbeat expired".to_string())
            {
                expiries.push(expiry);
            }
        }
        expiries
    }

    pub fn remove_worker(
        &mut self,
        worker_id: WorkerId,
        failure_reason: String,
    ) -> Option<WorkerExpiry> {
        let worker = self.workers.remove(&worker_id)?;
        let mut expiry = WorkerExpiry {
            worker_id,
            retried_tasks: Vec::new(),
            failed_tasks: Vec::new(),
        };

        for record in self.tasks.values_mut() {
            if record.spec.target_worker == Some(worker_id) {
                record.spec.target_worker = None;
            }
        }

        for task_id in worker.leased {
            if let Some(record) = self.tasks.get_mut(&task_id) {
                if matches!(record.state, TaskState::Leased(_) | TaskState::Running(_)) {
                    if record.spec.attempt < record.spec.max_retries {
                        record.spec.attempt += 1;
                        record.state = TaskState::Ready;
                        self.ready.push_back(task_id);
                        expiry.retried_tasks.push(task_id);
                    } else {
                        record.state = TaskState::Failed(failure_reason.clone());
                        expiry.failed_tasks.push(task_id);
                    }
                }
            }
        }

        Some(expiry)
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

        let index = self
            .workers
            .get(&victim_id)?
            .leased
            .iter()
            .position(|task_id| {
                self.tasks.get(task_id).is_some_and(|record| {
                    matches!(record.state, TaskState::Leased(_)) && self.can_run(record, thief)
                })
            })?;
        let victim = self.workers.get_mut(&victim_id)?;
        let task_id = victim.leased.remove(index)?;
        if let Some(record) = self.tasks.get_mut(&task_id) {
            record.state = TaskState::Ready;
        }
        Some(task_id)
    }

    fn pop_ready_for_worker(&mut self, worker_id: WorkerId) -> Option<TaskId> {
        let index = self.ready.iter().position(|task_id| {
            self.tasks
                .get(task_id)
                .is_some_and(|record| self.can_run(record, worker_id))
        })?;
        self.ready.remove(index)
    }

    fn can_run(&self, record: &TaskRecord, worker_id: WorkerId) -> bool {
        match record.spec.target_worker {
            Some(target) => target == worker_id,
            None => true,
        }
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
    fn pinned_task_only_leases_to_target_worker() {
        let mut scheduler = Scheduler::default();
        let target = WorkerId::new();
        let other = WorkerId::new();
        scheduler.register_worker(target, 1);
        scheduler.register_worker(other, 1);
        let mut spec = TaskSpec::new("actor_method", vec![], ObjectId::new());
        spec.target_worker = Some(target);
        let task_id = spec.task_id;
        scheduler.submit(spec).unwrap();

        assert!(scheduler.lease_tasks(other, 1).unwrap().is_empty());
        let leased = scheduler.lease_tasks(target, 1).unwrap();

        assert_eq!(leased.len(), 1);
        assert_eq!(leased[0].task_id, task_id);
    }

    #[test]
    fn pinned_task_is_not_stolen_by_non_target_worker() {
        let mut scheduler = Scheduler::default();
        let target = WorkerId::new();
        let other = WorkerId::new();
        scheduler.register_worker(target, 2);
        scheduler.register_worker(other, 1);
        let mut pinned = TaskSpec::new("actor_method", vec![], ObjectId::new());
        pinned.target_worker = Some(target);
        scheduler.submit(pinned).unwrap();
        let leased_to_target = scheduler.lease_tasks(target, 1).unwrap();

        assert!(scheduler.lease_tasks(other, 1).unwrap().is_empty());
        assert_eq!(
            scheduler.task_state(leased_to_target[0].task_id),
            Some(TaskState::Leased(target))
        );
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

        let expiries = scheduler.expire_workers();
        let retried = scheduler.lease_tasks(replacement, 1).unwrap();

        assert_eq!(
            expiries,
            vec![WorkerExpiry {
                worker_id: worker,
                retried_tasks: vec![task_id],
                failed_tasks: vec![],
            }]
        );
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].task_id, task_id);
        assert_eq!(retried[0].attempt, 1);
    }

    #[test]
    fn removing_worker_retries_tasks_and_releases_affinity() {
        let mut scheduler = Scheduler::default();
        let owner = WorkerId::new();
        let replacement = WorkerId::new();
        scheduler.register_worker(owner, 1);
        scheduler.register_worker(replacement, 1);
        let mut spec = TaskSpec::new("actor_method", vec![], ObjectId::new());
        spec.target_worker = Some(owner);
        spec.max_retries = 1;
        let task_id = spec.task_id;
        scheduler.submit(spec).unwrap();
        scheduler.lease_tasks(owner, 1).unwrap();

        let expiry = scheduler
            .remove_worker(owner, "worker unregistered".to_string())
            .unwrap();
        let retried = scheduler.lease_tasks(replacement, 1).unwrap();

        assert_eq!(expiry.retried_tasks, vec![task_id]);
        assert!(expiry.failed_tasks.is_empty());
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].task_id, task_id);
        assert_eq!(retried[0].target_worker, None);
    }

    #[test]
    fn removing_worker_releases_affinity_for_queued_tasks() {
        let mut scheduler = Scheduler::default();
        let owner = WorkerId::new();
        let replacement = WorkerId::new();
        scheduler.register_worker(owner, 1);
        scheduler.register_worker(replacement, 1);
        let mut spec = TaskSpec::new("actor_method", vec![], ObjectId::new());
        spec.target_worker = Some(owner);
        let task_id = spec.task_id;
        scheduler.submit(spec).unwrap();

        scheduler
            .remove_worker(owner, "worker unregistered".to_string())
            .unwrap();
        let leased = scheduler.lease_tasks(replacement, 1).unwrap();

        assert_eq!(leased.len(), 1);
        assert_eq!(leased[0].task_id, task_id);
        assert_eq!(leased[0].target_worker, None);
    }

    #[test]
    fn fail_tasks_removes_ready_and_leased_tasks_from_scheduling() {
        let mut scheduler = Scheduler::default();
        let worker = WorkerId::new();
        let replacement = WorkerId::new();
        scheduler.register_worker(worker, 1);
        scheduler.register_worker(replacement, 1);

        let ready = TaskSpec::new("ready", vec![], ObjectId::new());
        let ready_id = ready.task_id;
        scheduler.submit(ready).unwrap();

        let leased = TaskSpec::new("leased", vec![], ObjectId::new());
        let leased_id = leased.task_id;
        scheduler.submit(leased).unwrap();
        scheduler.lease_tasks(worker, 1).unwrap();

        let failed = scheduler.fail_tasks(&[ready_id, leased_id], "actor failed".to_string());
        let replacement_tasks = scheduler.lease_tasks(replacement, 2).unwrap();

        assert_eq!(failed, vec![ready_id, leased_id]);
        assert_eq!(
            scheduler.task_state(ready_id),
            Some(TaskState::Failed("actor failed".to_string()))
        );
        assert_eq!(
            scheduler.task_state(leased_id),
            Some(TaskState::Failed("actor failed".to_string()))
        );
        assert!(replacement_tasks.is_empty());
    }

    #[test]
    fn heartbeat_expiry_reports_task_when_retries_are_exhausted() {
        let mut scheduler = Scheduler::new(Duration::from_millis(1));
        let worker = WorkerId::new();
        scheduler.register_worker(worker, 1);
        let mut spec = TaskSpec::new("noop", vec![], ObjectId::new());
        spec.max_retries = 0;
        let task_id = spec.task_id;
        scheduler.submit(spec).unwrap();
        scheduler.lease_tasks(worker, 1).unwrap();
        scheduler.force_worker_last_heartbeat(worker, Instant::now() - Duration::from_secs(1));

        let expiries = scheduler.expire_workers();

        assert_eq!(
            expiries,
            vec![WorkerExpiry {
                worker_id: worker,
                retried_tasks: vec![],
                failed_tasks: vec![task_id],
            }]
        );
        assert_eq!(
            scheduler.task_state(task_id),
            Some(TaskState::Failed("worker heartbeat expired".to_string()))
        );
    }

    #[test]
    fn expire_workers_returns_empty_report_when_workers_are_healthy() {
        let mut scheduler = Scheduler::new(Duration::from_secs(10));
        scheduler.register_worker(WorkerId::new(), 1);

        assert!(scheduler.expire_workers().is_empty());
    }
}
