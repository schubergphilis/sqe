//! Fragment scheduler — assigns scan tasks to workers with load weighting.
//!
//! The [`FragmentScheduler`] trait defines the interface for scheduling
//! strategies. The default implementation, [`WeightedScheduler`], assigns
//! fragments to the worker with the lowest accumulated cost, skipping
//! unhealthy workers.

use sqe_planner::ScanTask;
use tracing::debug;

/// Information about a worker available for scheduling.
#[derive(Debug, Clone)]
pub struct WorkerInfo {
    /// The worker's gRPC endpoint URL.
    pub url: String,
    /// Whether the worker is currently healthy.
    pub healthy: bool,
    /// Number of fragments currently being executed on this worker.
    pub active_fragments: u32,
}

/// A single fragment-to-worker assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    /// Index into the original scan tasks slice.
    pub task_index: usize,
    /// URL of the assigned worker.
    pub worker_url: String,
}

/// Trait for fragment scheduling strategies.
///
/// Implementations receive a list of scan tasks and worker information,
/// and return an assignment of each task to exactly one worker.
pub trait FragmentScheduler: Send + Sync + std::fmt::Debug {
    /// Assign each scan task to a worker.
    ///
    /// Returns a Vec of [`Assignment`]s, one per input task, in the same
    /// order as the input `tasks` slice.
    ///
    /// # Errors
    ///
    /// Returns an error if no healthy workers are available.
    fn assign(
        &self,
        tasks: &[ScanTask],
        workers: &[WorkerInfo],
    ) -> Result<Vec<Assignment>, SchedulerError>;
}

/// Errors that can occur during fragment scheduling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerError {
    /// No healthy workers are available to receive fragments.
    NoHealthyWorkers,
}

impl std::fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchedulerError::NoHealthyWorkers => {
                write!(f, "No healthy workers available for scheduling")
            }
        }
    }
}

impl std::error::Error for SchedulerError {}

/// Estimates the cost of a scan task.
///
/// The cost is based on the number of data files in the task.
/// Each file is counted as 1 unit of work. Tasks with no files
/// still have a minimum cost of 1 to ensure they are accounted for.
fn estimate_cost(task: &ScanTask) -> u64 {
    let file_count = task.data_file_paths.len() as u64;
    file_count.max(1)
}

/// Weighted fragment scheduler that assigns tasks to the least-loaded worker.
///
/// Strategy:
/// 1. Filter out unhealthy workers.
/// 2. Initialize each worker's load from its `active_fragments` count.
/// 3. Sort tasks by estimated cost (descending) so the heaviest tasks
///    are assigned first ("largest-first" bin-packing heuristic).
/// 4. Assign each task to the worker with the currently lowest total load.
///
/// This produces a balanced distribution even when tasks have varying costs,
/// and naturally handles the case where some workers already have in-flight work.
#[derive(Debug, Default)]
pub struct WeightedScheduler;

impl WeightedScheduler {
    pub fn new() -> Self {
        Self
    }
}

impl FragmentScheduler for WeightedScheduler {
    fn assign(
        &self,
        tasks: &[ScanTask],
        workers: &[WorkerInfo],
    ) -> Result<Vec<Assignment>, SchedulerError> {
        // Filter to only healthy workers
        let healthy: Vec<&WorkerInfo> = workers.iter().filter(|w| w.healthy).collect();

        if healthy.is_empty() {
            return Err(SchedulerError::NoHealthyWorkers);
        }

        if tasks.is_empty() {
            return Ok(vec![]);
        }

        // Build a load tracker: (accumulated_load, worker_index_in_healthy)
        let mut loads: Vec<(u64, usize)> = healthy
            .iter()
            .enumerate()
            .map(|(i, w)| (u64::from(w.active_fragments), i))
            .collect();

        // Sort tasks by estimated cost descending (largest-first bin packing).
        // We track original indices so we can place assignments correctly.
        let mut indexed_tasks: Vec<(usize, u64)> = tasks
            .iter()
            .enumerate()
            .map(|(i, t)| (i, estimate_cost(t)))
            .collect();
        indexed_tasks.sort_by(|a, b| b.1.cmp(&a.1));

        // Pre-allocate the result vector with placeholders
        let mut assignments: Vec<Option<Assignment>> = vec![None; tasks.len()];

        for (task_idx, cost) in indexed_tasks {
            // Find the worker with the lowest current load.
            // On ties, the first worker (by position) wins, which provides
            // stable, deterministic ordering.
            let min_pos = loads
                .iter()
                .enumerate()
                .min_by_key(|(_, (load, _))| *load)
                .map(|(pos, _)| pos)
                .expect("healthy workers vec is non-empty");

            let worker_idx = loads[min_pos].1;
            let worker = &healthy[worker_idx];

            assignments[task_idx] = Some(Assignment {
                task_index: task_idx,
                worker_url: worker.url.clone(),
            });

            // Update the load for this worker
            loads[min_pos].0 += cost;
        }

        let result: Vec<Assignment> = assignments
            .into_iter()
            .map(|a| a.expect("BUG: every task must be assigned; healthy worker check passed above"))
            .collect();

        debug!(
            task_count = tasks.len(),
            worker_count = healthy.len(),
            assignments = ?result.iter().map(|a| (&a.worker_url, a.task_index)).collect::<Vec<_>>(),
            "Fragment scheduling complete"
        );

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task(id: &str, file_count: usize) -> ScanTask {
        ScanTask {
            fragment_id: id.to_string(),
            data_file_paths: (0..file_count)
                .map(|i| format!("s3://bucket/file{i}.parquet"))
                .collect(),
            projected_columns: vec![],
            s3_endpoint: String::new(),
            s3_region: String::new(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_session_token: String::new(),
            s3_path_style: false,
        }
    }

    fn make_worker(url: &str, healthy: bool, active: u32) -> WorkerInfo {
        WorkerInfo {
            url: url.to_string(),
            healthy,
            active_fragments: active,
        }
    }

    #[test]
    fn test_even_distribution() {
        let scheduler = WeightedScheduler::new();
        let tasks = vec![
            make_task("f1", 2),
            make_task("f2", 2),
            make_task("f3", 2),
            make_task("f4", 2),
        ];
        let workers = vec![
            make_worker("http://w1:50052", true, 0),
            make_worker("http://w2:50052", true, 0),
        ];

        let assignments = scheduler.assign(&tasks, &workers).unwrap();
        assert_eq!(assignments.len(), 4);

        // Count tasks per worker
        let w1_count = assignments
            .iter()
            .filter(|a| a.worker_url == "http://w1:50052")
            .count();
        let w2_count = assignments
            .iter()
            .filter(|a| a.worker_url == "http://w2:50052")
            .count();

        // With equal-cost tasks and equal-load workers, should be 2-2
        assert_eq!(w1_count, 2, "worker1 should get 2 tasks");
        assert_eq!(w2_count, 2, "worker2 should get 2 tasks");
    }

    #[test]
    fn test_skip_unhealthy_workers() {
        let scheduler = WeightedScheduler::new();
        let tasks = vec![make_task("f1", 3), make_task("f2", 3)];
        let workers = vec![
            make_worker("http://w1:50052", false, 0), // unhealthy
            make_worker("http://w2:50052", true, 0),
        ];

        let assignments = scheduler.assign(&tasks, &workers).unwrap();
        assert_eq!(assignments.len(), 2);

        // All tasks should go to w2 (the only healthy worker)
        for a in &assignments {
            assert_eq!(
                a.worker_url, "http://w2:50052",
                "unhealthy worker should not receive tasks"
            );
        }
    }

    #[test]
    fn test_no_healthy_workers_returns_error() {
        let scheduler = WeightedScheduler::new();
        let tasks = vec![make_task("f1", 1)];
        let workers = vec![
            make_worker("http://w1:50052", false, 0),
            make_worker("http://w2:50052", false, 0),
        ];

        let result = scheduler.assign(&tasks, &workers);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), SchedulerError::NoHealthyWorkers);
    }

    #[test]
    fn test_single_worker() {
        let scheduler = WeightedScheduler::new();
        let tasks = vec![
            make_task("f1", 5),
            make_task("f2", 3),
            make_task("f3", 1),
        ];
        let workers = vec![make_worker("http://w1:50052", true, 0)];

        let assignments = scheduler.assign(&tasks, &workers).unwrap();
        assert_eq!(assignments.len(), 3);

        for a in &assignments {
            assert_eq!(a.worker_url, "http://w1:50052");
        }
    }

    #[test]
    fn test_more_fragments_than_workers() {
        let scheduler = WeightedScheduler::new();
        let tasks = vec![
            make_task("f1", 2),
            make_task("f2", 2),
            make_task("f3", 2),
            make_task("f4", 2),
            make_task("f5", 2),
        ];
        let workers = vec![
            make_worker("http://w1:50052", true, 0),
            make_worker("http://w2:50052", true, 0),
        ];

        let assignments = scheduler.assign(&tasks, &workers).unwrap();
        assert_eq!(assignments.len(), 5);

        let w1_count = assignments
            .iter()
            .filter(|a| a.worker_url == "http://w1:50052")
            .count();
        let w2_count = assignments
            .iter()
            .filter(|a| a.worker_url == "http://w2:50052")
            .count();

        // 5 equal tasks across 2 workers: one gets 3, the other 2
        assert!(
            (w1_count == 3 && w2_count == 2) || (w1_count == 2 && w2_count == 3),
            "expected 3/2 split, got {w1_count}/{w2_count}"
        );
    }

    #[test]
    fn test_weighted_distribution_unequal_costs() {
        let scheduler = WeightedScheduler::new();
        // One heavy task (10 files) and two light tasks (1 file each)
        let tasks = vec![
            make_task("heavy", 10),
            make_task("light1", 1),
            make_task("light2", 1),
        ];
        let workers = vec![
            make_worker("http://w1:50052", true, 0),
            make_worker("http://w2:50052", true, 0),
        ];

        let assignments = scheduler.assign(&tasks, &workers).unwrap();
        assert_eq!(assignments.len(), 3);

        // The heavy task should be on one worker, both light tasks on the other.
        // This gives load 10 vs 2, which is the best possible split.
        let heavy_worker = &assignments[0].worker_url;
        let light1_worker = &assignments[1].worker_url;
        let light2_worker = &assignments[2].worker_url;

        assert_ne!(
            heavy_worker, light1_worker,
            "heavy and light tasks should be on different workers"
        );
        assert_eq!(
            light1_worker, light2_worker,
            "both light tasks should be on the same worker"
        );
    }

    #[test]
    fn test_existing_load_influences_assignment() {
        let scheduler = WeightedScheduler::new();
        let tasks = vec![make_task("f1", 2)];
        let workers = vec![
            make_worker("http://w1:50052", true, 10), // already loaded
            make_worker("http://w2:50052", true, 0),  // idle
        ];

        let assignments = scheduler.assign(&tasks, &workers).unwrap();
        assert_eq!(assignments.len(), 1);

        // Should assign to w2 since it has lower load
        assert_eq!(
            assignments[0].worker_url, "http://w2:50052",
            "task should go to the less-loaded worker"
        );
    }

    #[test]
    fn test_empty_tasks() {
        let scheduler = WeightedScheduler::new();
        let tasks: Vec<ScanTask> = vec![];
        let workers = vec![make_worker("http://w1:50052", true, 0)];

        let assignments = scheduler.assign(&tasks, &workers).unwrap();
        assert!(assignments.is_empty());
    }

    #[test]
    fn test_empty_workers() {
        let scheduler = WeightedScheduler::new();
        let tasks = vec![make_task("f1", 1)];
        let workers: Vec<WorkerInfo> = vec![];

        let result = scheduler.assign(&tasks, &workers);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), SchedulerError::NoHealthyWorkers);
    }

    #[test]
    fn test_task_index_preservation() {
        let scheduler = WeightedScheduler::new();
        let tasks = vec![
            make_task("f0", 1),
            make_task("f1", 5),
            make_task("f2", 1),
        ];
        let workers = vec![
            make_worker("http://w1:50052", true, 0),
            make_worker("http://w2:50052", true, 0),
        ];

        let assignments = scheduler.assign(&tasks, &workers).unwrap();

        // Each assignment should have the correct task_index
        for (i, a) in assignments.iter().enumerate() {
            assert_eq!(a.task_index, i, "task_index should match position");
        }
    }

    #[test]
    fn test_minimum_cost_for_empty_file_task() {
        // A task with 0 files should still have cost 1
        let task = make_task("empty", 0);
        assert_eq!(estimate_cost(&task), 1);
    }

    #[test]
    fn test_cost_proportional_to_files() {
        let task = make_task("many", 42);
        assert_eq!(estimate_cost(&task), 42);
    }

    #[test]
    fn test_all_unhealthy_except_one() {
        let scheduler = WeightedScheduler::new();
        let tasks = vec![
            make_task("f1", 2),
            make_task("f2", 2),
            make_task("f3", 2),
        ];
        let workers = vec![
            make_worker("http://w1:50052", false, 0),
            make_worker("http://w2:50052", false, 0),
            make_worker("http://w3:50052", true, 0), // only healthy one
        ];

        let assignments = scheduler.assign(&tasks, &workers).unwrap();
        assert_eq!(assignments.len(), 3);

        for a in &assignments {
            assert_eq!(a.worker_url, "http://w3:50052");
        }
    }
}
