//! Fragment scheduler — assigns scan tasks to workers with load weighting.
//!
//! [`assign`] hands each fragment to the worker with the lowest accumulated
//! cost, skipping unhealthy workers.

use sqe_planner::ScanTask;
use tracing::debug;

/// Information about a worker available for scheduling.
#[derive(Debug, Clone)]
pub struct WorkerInfo {
    /// The worker's gRPC endpoint URL.
    pub url: String,
    /// Whether the worker is currently healthy.
    pub healthy: bool,
    /// Load already in flight on this worker, in the same unit
    /// [`estimate_cost`] produces (megabytes of scan input, or file count
    /// when sizes are unknown). Finding 408: this used to be a fragment
    /// *count*, which the scheduler then added MB costs to, so a worker with
    /// three in-flight fragments looked 3 MB "busier" than an idle one and
    /// cross-query balancing was effectively off past a few MB per task.
    pub in_flight_load: u64,
}

/// A single fragment-to-worker assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    /// Index into the original scan tasks slice.
    pub task_index: usize,
    /// URL of the assigned worker.
    pub worker_url: String,
}

/// Estimates the cost of a scan task.
///
/// When `file_sizes_bytes` is populated, cost is measured in megabytes
/// (minimum 1) so that a 10 GB file costs 1000× more than a 10 MB file.
/// When sizes are unknown (empty vec), falls back to file count so that
/// existing behaviour is preserved for callers that haven't populated sizes.
///
/// `pub(crate)` so the dispatcher reserves the same number on the
/// [`crate::worker_registry::WorkerLoadTracker`] that the next scheduling
/// round reads back as [`WorkerInfo::in_flight_load`].
pub(crate) fn estimate_cost(task: &ScanTask) -> u64 {
    let total_bytes: u64 = task.file_sizes_bytes.iter().sum();
    if total_bytes > 0 {
        // Cost in megabytes, minimum 1
        (total_bytes / (1024 * 1024)).max(1)
    } else {
        // Fallback to file count if sizes not available
        (task.data_file_paths.len() as u64).max(1)
    }
}

/// Fold a byte slice into `hash` with the same 31-multiply polynomial used
/// for path affinity. A trailing 0 separator keeps adjacent strings from
/// colliding (`["ab","c"]` vs `["a","bc"]`).
fn hash_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash = hash.wrapping_mul(31).wrapping_add(*byte as u64);
    }
    hash.wrapping_mul(31)
}

/// Compute a preferred worker index for a set of files using consistent hashing.
///
/// Every path is hashed (not only `file_paths[0]`) so two tasks that share a
/// first file but differ later can land on different workers. Empty input or
/// zero workers maps to index 0.
fn preferred_worker(file_paths: &[String], num_workers: usize) -> usize {
    if num_workers == 0 || file_paths.is_empty() {
        return 0;
    }
    let mut hash: u64 = 0;
    for path in file_paths {
        hash = hash_bytes(hash, path.as_bytes());
    }
    // Mix in the count so `[a, b]` and `[a, b, a, b]` cannot collide by
    // concatenation of the same strings.
    hash = hash.wrapping_mul(31).wrapping_add(file_paths.len() as u64);
    (hash as usize) % num_workers
}

/// Weighted least-loaded assignment with file affinity: the biggest tasks go
/// first, each to its hash-preferred worker unless that worker is more than
/// 20% above the least-loaded one. `None` when no worker is healthy.
pub(crate) fn assign(tasks: &[ScanTask], workers: &[WorkerInfo]) -> Option<Vec<Assignment>> {
    // Filter to only healthy workers
    let healthy: Vec<&WorkerInfo> = workers.iter().filter(|w| w.healthy).collect();

    if healthy.is_empty() {
        return None;
    }

    if tasks.is_empty() {
        return Some(vec![]);
    }

    // Build a load tracker: (accumulated_load, worker_index_in_healthy).
    // `in_flight_load` is already in `estimate_cost` units, so adding task
    // costs below compares like with like.
    let mut loads: Vec<(u64, usize)> = healthy
        .iter()
        .enumerate()
        .map(|(i, w)| (w.in_flight_load, i))
        .collect();

    // Sort tasks by estimated cost descending (largest-first bin packing).
    // We track original indices so we can place assignments correctly.
    let mut indexed_tasks: Vec<(usize, u64)> = tasks
        .iter()
        .enumerate()
        .map(|(i, t)| (i, estimate_cost(t)))
        .collect();
    indexed_tasks.sort_by_key(|t| std::cmp::Reverse(t.1));

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

        // Cache affinity: prefer the consistent-hash worker if its load is within 20% of minimum
        let preferred = preferred_worker(&tasks[task_idx].data_file_paths, healthy.len());
        let preferred_load = loads[preferred].0;
        let min_load = loads[min_pos].0;
        let threshold = min_load + (min_load / 5).max(1); // 20% tolerance

        let chosen = if preferred_load <= threshold {
            preferred
        } else {
            min_pos
        };

        let worker_idx = loads[chosen].1;
        let worker = &healthy[worker_idx];

        assignments[task_idx] = Some(Assignment {
            task_index: task_idx,
            worker_url: worker.url.clone(),
        });

        // Update the load for this worker
        loads[chosen].0 += cost;
    }

    let result: Vec<Assignment> = assignments
        .into_iter()
        .map(|a| {
            a.expect("BUG: every task must be assigned; healthy worker check passed above")
        })
        .collect();

    debug!(
        task_count = tasks.len(),
        worker_count = healthy.len(),
        assignments = ?result.iter().map(|a| (&a.worker_url, a.task_index)).collect::<Vec<_>>(),
        "Fragment scheduling complete"
    );

    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: u64 = 1024 * 1024;

    fn storage() -> sqe_planner::ScanStorage {
        sqe_planner::ScanStorage {
            s3_allow_http: true,
            ..Default::default()
        }
    }

    fn make_task(id: &str, file_count: usize, file_size_mb: u64) -> ScanTask {
        ScanTask::builder(
            id,
            (0..file_count)
                .map(|i| format!("s3://bucket/file{i}.parquet"))
                .collect(),
            storage(),
        )
        .with_file_sizes((0..file_count).map(|_| file_size_mb * MB).collect())
        .build()
    }

    fn make_task_with_sizes(id: &str, sizes: &[u64]) -> ScanTask {
        ScanTask::builder(
            id,
            (0..sizes.len())
                .map(|i| format!("s3://bucket/file{i}.parquet"))
                .collect(),
            storage(),
        )
        .with_file_sizes(sizes.to_vec())
        .build()
    }

    fn make_worker(url: &str, healthy: bool, in_flight_load: u64) -> WorkerInfo {
        WorkerInfo {
            url: url.to_string(),
            healthy,
            in_flight_load,
        }
    }

    /// Finding 408 acceptance: two workers, one already carrying one
    /// in-flight 500 MB fragment (reserved in MB, the `estimate_cost` unit),
    /// two new 500 MB tasks land one per worker. With the old fragment-count
    /// initialisation the busy worker looked 1 MB loaded and, after the first
    /// 500 MB task went to the idle one, took the second task too.
    #[test]
    fn test_in_flight_load_is_in_cost_units() {
        let busy_reserved = estimate_cost(&make_task_with_sizes("inflight", &[500 * MB]));
        assert_eq!(busy_reserved, 500);
        let tasks = vec![
            make_task_with_sizes("a", &[500 * MB]),
            make_task_with_sizes("b", &[500 * MB]),
        ];
        let workers = vec![
            make_worker("http://w1:50052", true, busy_reserved),
            make_worker("http://w2:50052", true, 0),
        ];

        let assignments = assign(&tasks, &workers).unwrap();
        assert_eq!(assignments.len(), 2);
        assert_ne!(
            assignments[0].worker_url, assignments[1].worker_url,
            "500 MB in flight must count as 500 units, not 1: got {assignments:?}"
        );
    }

    #[test]
    fn test_even_distribution() {
        let tasks = vec![
            make_task("f1", 2, 10),
            make_task("f2", 2, 10),
            make_task("f3", 2, 10),
            make_task("f4", 2, 10),
        ];
        let workers = vec![
            make_worker("http://w1:50052", true, 0),
            make_worker("http://w2:50052", true, 0),
        ];

        let assignments = assign(&tasks, &workers).unwrap();
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
        let tasks = vec![make_task("f1", 3, 10), make_task("f2", 3, 10)];
        let workers = vec![
            make_worker("http://w1:50052", false, 0), // unhealthy
            make_worker("http://w2:50052", true, 0),
        ];

        let assignments = assign(&tasks, &workers).unwrap();
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
        let tasks = vec![make_task("f1", 1, 10)];
        let workers = vec![
            make_worker("http://w1:50052", false, 0),
            make_worker("http://w2:50052", false, 0),
        ];

        let result = assign(&tasks, &workers);
        assert!(result.is_none());
        assert!(result.is_none());
    }

    #[test]
    fn test_single_worker() {
        let tasks = vec![
            make_task("f1", 5, 10),
            make_task("f2", 3, 10),
            make_task("f3", 1, 10),
        ];
        let workers = vec![make_worker("http://w1:50052", true, 0)];

        let assignments = assign(&tasks, &workers).unwrap();
        assert_eq!(assignments.len(), 3);

        for a in &assignments {
            assert_eq!(a.worker_url, "http://w1:50052");
        }
    }

    #[test]
    fn test_more_fragments_than_workers() {
        let tasks = vec![
            make_task("f1", 2, 10),
            make_task("f2", 2, 10),
            make_task("f3", 2, 10),
            make_task("f4", 2, 10),
            make_task("f5", 2, 10),
        ];
        let workers = vec![
            make_worker("http://w1:50052", true, 0),
            make_worker("http://w2:50052", true, 0),
        ];

        let assignments = assign(&tasks, &workers).unwrap();
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
        // One heavy task (10 files × 100 MB) and two light tasks (1 file × 100 MB each)
        let tasks = vec![
            make_task("heavy", 10, 100),
            make_task("light1", 1, 100),
            make_task("light2", 1, 100),
        ];
        let workers = vec![
            make_worker("http://w1:50052", true, 0),
            make_worker("http://w2:50052", true, 0),
        ];

        let assignments = assign(&tasks, &workers).unwrap();
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
        let tasks = vec![make_task("f1", 2, 10)];
        let workers = vec![
            make_worker("http://w1:50052", true, 10), // already loaded
            make_worker("http://w2:50052", true, 0),  // idle
        ];

        let assignments = assign(&tasks, &workers).unwrap();
        assert_eq!(assignments.len(), 1);

        // Should assign to w2 since it has lower load
        assert_eq!(
            assignments[0].worker_url, "http://w2:50052",
            "task should go to the less-loaded worker"
        );
    }

    #[test]
    fn test_empty_tasks() {
        let tasks: Vec<ScanTask> = vec![];
        let workers = vec![make_worker("http://w1:50052", true, 0)];

        let assignments = assign(&tasks, &workers).unwrap();
        assert!(assignments.is_empty());
    }

    #[test]
    fn test_empty_workers() {
        let tasks = vec![make_task("f1", 1, 10)];
        let workers: Vec<WorkerInfo> = vec![];

        let result = assign(&tasks, &workers);
        assert!(result.is_none());
        assert!(result.is_none());
    }

    #[test]
    fn test_task_index_preservation() {
        let tasks = vec![
            make_task("f0", 1, 10),
            make_task("f1", 5, 10),
            make_task("f2", 1, 10),
        ];
        let workers = vec![
            make_worker("http://w1:50052", true, 0),
            make_worker("http://w2:50052", true, 0),
        ];

        let assignments = assign(&tasks, &workers).unwrap();

        // Each assignment should have the correct task_index
        for (i, a) in assignments.iter().enumerate() {
            assert_eq!(a.task_index, i, "task_index should match position");
        }
    }

    #[test]
    fn test_minimum_cost_for_empty_file_task() {
        // A task with 0 files and no sizes should still have cost 1 (fallback path)
        let task = make_task("empty", 0, 0);
        assert_eq!(estimate_cost(&task), 1);
    }

    #[test]
    fn test_cost_proportional_to_files_fallback() {
        // When file_sizes_bytes is empty, cost falls back to file count
        let mut task = make_task("many", 42, 0);
        task.file_sizes_bytes = vec![];
        assert_eq!(estimate_cost(&task), 42);
    }

    #[test]
    fn test_cost_uses_bytes_when_available() {
        // 2 files × 500 MB each = 1000 MB cost
        let task = make_task_with_sizes("t1", &[500 * MB, 500 * MB]);
        assert_eq!(estimate_cost(&task), 1000);
    }

    #[test]
    fn test_cost_falls_back_to_file_count() {
        // No file sizes → use file count
        let mut task = make_task("t1", 5, 0);
        task.file_sizes_bytes = vec![];
        assert_eq!(estimate_cost(&task), 5);
    }

    #[test]
    fn test_mixed_size_balancing() {
        // 1 GB = 1024 MB cost; 10 × 100 MB = 1000 MB cost.
        // Costs are proportional to bytes (in MiB), not equal.
        let heavy = make_task_with_sizes("heavy", &[1024 * MB]);
        let many_small = make_task_with_sizes("small", &[100 * MB; 10]);
        assert_eq!(estimate_cost(&heavy), 1024);
        assert_eq!(estimate_cost(&many_small), 1000);
        assert!(estimate_cost(&heavy) > estimate_cost(&many_small));
    }

    #[test]
    fn test_concurrent_scheduling_no_panic() {
        // Multiple concurrent calls to assign() should not panic or produce
        // overlapping assignments (each call is independent — no shared mutable state).
        let tasks: Vec<ScanTask> = (0..20)
            .map(|i| make_task(&format!("f{i}"), 3, 10))
            .collect();
        let workers = vec![
            make_worker("http://w1:50052", true, 0),
            make_worker("http://w2:50052", true, 0),
            make_worker("http://w3:50052", true, 0),
        ];

        // Run many concurrent assignments (simulated via threads since assign is sync)
        let results: Vec<_> = (0..50)
            .map(|_| {
                let t = tasks.clone();
                let w = workers.clone();
                std::thread::spawn(move || assign(&t, &w))
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();

        for result in &results {
            let assignments = result.as_ref().unwrap();
            assert_eq!(assignments.len(), 20);
            // Verify no overlapping: each task_index appears exactly once
            let mut seen = std::collections::HashSet::new();
            for a in assignments {
                assert!(
                    seen.insert(a.task_index),
                    "duplicate task_index {}",
                    a.task_index
                );
            }
        }
    }

    #[test]
    fn test_load_tracking_during_assignment() {
        // After assigning tasks, the internal load balancing should have produced
        // a fair distribution. We verify this by examining the final assignment
        // spread across workers.
        // 6 tasks with varying sizes: 1000, 800, 600, 400, 200, 100 MB
        let tasks = vec![
            make_task_with_sizes("heavy", &[1000 * MB]),
            make_task_with_sizes("mid_heavy", &[800 * MB]),
            make_task_with_sizes("mid", &[600 * MB]),
            make_task_with_sizes("mid_light", &[400 * MB]),
            make_task_with_sizes("light", &[200 * MB]),
            make_task_with_sizes("tiny", &[100 * MB]),
        ];
        let workers = vec![
            make_worker("http://w1:50052", true, 0),
            make_worker("http://w2:50052", true, 0),
            make_worker("http://w3:50052", true, 0),
        ];

        let assignments = assign(&tasks, &workers).unwrap();
        assert_eq!(assignments.len(), 6);

        // Compute the total cost assigned to each worker using estimate_cost
        let mut worker_loads: std::collections::HashMap<&str, u64> =
            std::collections::HashMap::new();
        for a in &assignments {
            let cost = estimate_cost(&tasks[a.task_index]);
            *worker_loads.entry(&a.worker_url).or_insert(0) += cost;
        }

        // Total cost = 1000 + 800 + 600 + 400 + 200 + 100 = 3100 MB
        // Ideal per worker = ~1033 MB
        // With largest-first bin packing, we expect reasonably balanced loads
        let loads: Vec<u64> = worker_loads.values().copied().collect();
        let max_load = *loads.iter().max().unwrap();
        let min_load = *loads.iter().min().unwrap();
        // The difference between max and min should be small relative to the total (≤600 MB)
        assert!(
            max_load - min_load <= 600,
            "load imbalance too high: max={max_load}, min={min_load}, loads={worker_loads:?}"
        );
    }

    #[test]
    fn test_rebalancing_after_failure() {
        // After marking a worker unhealthy, reassigning should skip it.
        let tasks = vec![
            make_task("f1", 3, 10),
            make_task("f2", 3, 10),
            make_task("f3", 3, 10),
            make_task("f4", 3, 10),
        ];

        // First assignment: 3 healthy workers
        let workers_before = vec![
            make_worker("http://w1:50052", true, 0),
            make_worker("http://w2:50052", true, 0),
            make_worker("http://w3:50052", true, 0),
        ];
        let assignments_before = assign(&tasks, &workers_before).unwrap();
        assert_eq!(assignments_before.len(), 4);

        // Now w2 has failed — simulate by passing it as unhealthy
        let workers_after = vec![
            make_worker("http://w1:50052", true, 0),
            make_worker("http://w2:50052", false, 0), // unhealthy
            make_worker("http://w3:50052", true, 0),
        ];
        let assignments_after = assign(&tasks, &workers_after).unwrap();
        assert_eq!(assignments_after.len(), 4);

        // No task should be assigned to the unhealthy worker
        for a in &assignments_after {
            assert_ne!(
                a.worker_url, "http://w2:50052",
                "unhealthy worker should not receive tasks after failure"
            );
        }

        // All tasks should go to w1 and w3 — 2 each
        let w1_count = assignments_after
            .iter()
            .filter(|a| a.worker_url == "http://w1:50052")
            .count();
        let w3_count = assignments_after
            .iter()
            .filter(|a| a.worker_url == "http://w3:50052")
            .count();
        assert_eq!(w1_count, 2);
        assert_eq!(w3_count, 2);
    }

    #[test]
    fn test_preferred_worker_deterministic() {
        let paths = vec!["s3://bucket/data/file1.parquet".to_string()];
        let w1 = preferred_worker(&paths, 3);
        let w2 = preferred_worker(&paths, 3);
        assert_eq!(w1, w2, "same files should hash to same worker");
    }

    #[test]
    fn test_preferred_worker_same_single_path_is_stable() {
        // Same single path must always prefer the same worker (cache affinity).
        let paths = vec!["s3://bucket/dashboard/metrics.parquet".to_string()];
        let first = preferred_worker(&paths, 7);
        for _ in 0..20 {
            assert_eq!(preferred_worker(&paths, 7), first);
        }
    }

    #[test]
    fn test_preferred_worker_distributes() {
        // Different file paths should distribute across workers
        let mut workers_hit = std::collections::HashSet::new();
        for i in 0..100 {
            let paths = vec![format!("s3://bucket/table/part-{i}.parquet")];
            workers_hit.insert(preferred_worker(&paths, 5));
        }
        assert!(workers_hit.len() >= 3, "should use at least 3 of 5 workers");
    }

    #[test]
    fn test_preferred_worker_hashes_all_paths() {
        // Two tasks that share the first file but differ later must be able
        // to land on different workers. Hashing only file_paths[0] would pin
        // every such task to the same worker.
        let shared_first = "s3://bucket/shared/file0.parquet".to_string();
        let mut workers_hit = std::collections::HashSet::new();
        for i in 0..100 {
            let paths = vec![
                shared_first.clone(),
                format!("s3://bucket/part-{i}/file1.parquet"),
                format!("s3://bucket/part-{i}/file2.parquet"),
            ];
            workers_hit.insert(preferred_worker(&paths, 8));
        }
        assert!(
            workers_hit.len() >= 3,
            "tasks that share only the first file should spread across workers, got {workers_hit:?}"
        );
    }

    #[test]
    fn test_cache_affinity_prefers_consistent_worker() {
        // When all workers have equal load, the scheduler should consistently
        // pick the same worker for the same file path (cache affinity).
        let file_path = "s3://bucket/dashboard/metrics.parquet".to_string();

        let task1 = ScanTask::builder("q1_scan", vec![file_path.clone()], storage())
            .with_file_sizes(vec![100 * MB])
            .build();
        let task2 = ScanTask::builder("q2_scan", vec![file_path.clone()], storage())
            .with_file_sizes(vec![100 * MB])
            .build();

        let workers = vec![
            make_worker("http://w1:50052", true, 0),
            make_worker("http://w2:50052", true, 0),
            make_worker("http://w3:50052", true, 0),
        ];

        // Both scans reference the same file; each is submitted as a separate single-task batch.
        let a1 = assign(&[task1], &workers).unwrap();
        let a2 = assign(&[task2], &workers).unwrap();

        // The same file path must consistently hash to the same worker
        assert_eq!(
            a1[0].worker_url, a2[0].worker_url,
            "repeated scans of the same file should go to the same worker for cache affinity"
        );
    }

    #[test]
    fn test_all_unhealthy_except_one() {
        let tasks = vec![
            make_task("f1", 2, 10),
            make_task("f2", 2, 10),
            make_task("f3", 2, 10),
        ];
        let workers = vec![
            make_worker("http://w1:50052", false, 0),
            make_worker("http://w2:50052", false, 0),
            make_worker("http://w3:50052", true, 0), // only healthy one
        ];

        let assignments = assign(&tasks, &workers).unwrap();
        assert_eq!(assignments.len(), 3);

        for a in &assignments {
            assert_eq!(a.worker_url, "http://w3:50052");
        }
    }
}
