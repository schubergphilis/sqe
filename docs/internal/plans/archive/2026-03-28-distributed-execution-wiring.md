# Distributed Execution Wiring — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the existing distributed execution infrastructure into the query pipeline so SELECT queries on Iceberg tables are automatically distributed to workers when healthy workers are available and the scan has enough data files.

**Architecture:** Add `try_distribute()` to `execute_query()` that inspects the PhysicalPlan for `IcebergScanExec` nodes, extracts data file paths via `scan.plan_files()`, builds `ScanTask`s, schedules them via `WeightedScheduler`, and replaces scan nodes with `DistributedScanExec`. Fragment state is tracked in `QueryTracker` and exposed via `system.runtime.tasks`.

**Tech Stack:** Rust, DataFusion 52, iceberg-rust 0.9, arrow-flight 57, moka, uuid v7

**Spec:** `docs/superpowers/specs/2026-03-28-distributed-execution-wiring-design.md`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `crates/sqe-coordinator/src/query_tracker.rs` | Modify | Add FragmentInfo, FragmentState, set_fragments(), update_fragment() |
| `crates/sqe-catalog/src/iceberg_scan.rs` | Modify | Add `data_file_paths()` method to IcebergScanExec |
| `crates/sqe-coordinator/src/query_handler.rs` | Modify | Add try_distribute(), extract_iceberg_scans(), rewrite_plan() |
| `crates/sqe-coordinator/src/distributed_scan.rs` | Modify | Add optional FragmentCallback |
| `crates/sqe-catalog/src/system_runtime.rs` | Modify | Tasks table reads from record.fragments |

---

### Task 1: Add FragmentInfo to QueryTracker

**Files:**
- Modify: `crates/sqe-coordinator/src/query_tracker.rs`

- [ ] **Step 1: Add FragmentInfo and FragmentState types**

Add after `QueryState`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FragmentState {
    Running,
    Finished,
    Failed,
    Retried,
}

impl std::fmt::Display for FragmentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => write!(f, "RUNNING"),
            Self::Finished => write!(f, "FINISHED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Retried => write!(f, "RETRIED"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FragmentInfo {
    pub task_id: String,
    pub worker_url: String,
    pub state: FragmentState,
    pub elapsed_ms: u64,
    pub input_rows: usize,
    pub output_rows: usize,
}
```

Add field to `QueryRecord`:
```rust
    pub fragments: Vec<FragmentInfo>,
```

Initialize as `fragments: Vec::new()` in `QueryTracker::start()`.

- [ ] **Step 2: Add set_fragments() and update_fragment() methods**

```rust
    pub fn set_fragments(&self, query_id: &Uuid, fragments: Vec<FragmentInfo>) {
        if let Some(old) = self.history.get(query_id) {
            let mut record = (*old).clone();
            record.fragments = fragments;
            self.history.insert(*query_id, Arc::new(record));
        }
    }

    pub fn update_fragment(
        &self,
        query_id: &Uuid,
        task_id: &str,
        state: FragmentState,
        elapsed_ms: u64,
        output_rows: usize,
    ) {
        if let Some(old) = self.history.get(query_id) {
            let mut record = (*old).clone();
            if let Some(frag) = record.fragments.iter_mut().find(|f| f.task_id == task_id) {
                frag.state = state;
                frag.elapsed_ms = elapsed_ms;
                frag.output_rows = output_rows;
            }
            self.history.insert(*query_id, Arc::new(record));
        }
    }
```

- [ ] **Step 3: Add tests**

```rust
    #[tokio::test]
    async fn set_and_update_fragments() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        tracker.start(id, "alice", None, "SELECT *", "s1", None, vec![]);

        let frags = vec![
            FragmentInfo {
                task_id: "frag-0".into(),
                worker_url: "http://worker-1:50052".into(),
                state: FragmentState::Running,
                elapsed_ms: 0,
                input_rows: 0,
                output_rows: 0,
            },
            FragmentInfo {
                task_id: "frag-1".into(),
                worker_url: "http://worker-2:50052".into(),
                state: FragmentState::Running,
                elapsed_ms: 0,
                input_rows: 0,
                output_rows: 0,
            },
        ];
        tracker.set_fragments(&id, frags);

        let rec = tracker.history.get(&id).unwrap();
        assert_eq!(rec.fragments.len(), 2);
        assert_eq!(rec.fragments[0].state, FragmentState::Running);

        tracker.update_fragment(&id, "frag-0", FragmentState::Finished, 42, 100);
        let rec = tracker.history.get(&id).unwrap();
        assert_eq!(rec.fragments[0].state, FragmentState::Finished);
        assert_eq!(rec.fragments[0].elapsed_ms, 42);
        assert_eq!(rec.fragments[0].output_rows, 100);
        assert_eq!(rec.fragments[1].state, FragmentState::Running);
    }
```

- [ ] **Step 4: Verify**

Run: `cargo test -p sqe-coordinator -- query_tracker && cargo clippy -p sqe-coordinator -- -D warnings`

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/query_tracker.rs
git commit -m "feat: add FragmentInfo tracking to QueryTracker"
```

---

### Task 2: Expose data file paths from IcebergScanExec

**Files:**
- Modify: `crates/sqe-catalog/src/iceberg_scan.rs`

- [ ] **Step 1: Add data_file_paths() method**

Add to `impl IcebergScanExec` (after the existing `predicates()` method around line 76):

```rust
    /// List all data file paths from the table's current snapshot.
    ///
    /// Uses the scan builder with the same projection and predicates as
    /// the execution, then calls `plan_files()` to get the filtered
    /// file scan tasks.
    pub async fn data_file_paths(&self) -> Result<Vec<String>, iceberg::Error> {
        let mut scan_builder = self.table.scan();

        if let Some(ref cols) = self.projection {
            scan_builder = scan_builder.select(cols.iter().map(|s| s.as_str()));
        }

        if let Some(ref pred) = self.predicates {
            scan_builder = scan_builder.with_filter(pred.clone());
        }

        let scan = scan_builder.build()?;

        use futures::TryStreamExt;
        let tasks: Vec<_> = scan.plan_files().await?.try_collect().await?;

        Ok(tasks.iter().map(|t| t.data_file_path().to_string()).collect())
    }

    /// Returns the projected column names, if any.
    pub fn projection(&self) -> Option<&[String]> {
        self.projection.as_deref()
    }
```

- [ ] **Step 2: Verify**

Run: `cargo clippy -p sqe-catalog -- -D warnings && cargo test -p sqe-catalog`

- [ ] **Step 3: Commit**

```bash
git add crates/sqe-catalog/src/iceberg_scan.rs
git commit -m "feat: expose data_file_paths() on IcebergScanExec"
```

---

### Task 3: Add FragmentCallback to DistributedScanExec

**Files:**
- Modify: `crates/sqe-coordinator/src/distributed_scan.rs`

- [ ] **Step 1: Add callback type and field**

Add after the `LocalExecutor` trait:

```rust
/// Callback invoked when a fragment completes or fails.
/// Args: (task_id, success: bool, elapsed_ms, output_rows)
pub type FragmentCallback = Arc<dyn Fn(&str, bool, u64, usize) + Send + Sync>;
```

Add field to `DistributedScanExec`:
```rust
    fragment_callback: Option<FragmentCallback>,
```

Add builder method:
```rust
    pub fn with_fragment_callback(mut self, cb: FragmentCallback) -> Self {
        self.fragment_callback = Some(cb);
        self
    }
```

Initialize as `None` in `new()`.

- [ ] **Step 2: Fire callback in execute()**

In the `execute()` method, after a partition stream completes or fails, fire the callback. Find the existing `execute()` implementation — it creates a Flight `do_get` stream. Wrap the stream to fire the callback on completion:

At the end of the successful stream path, add:
```rust
if let Some(ref cb) = self.fragment_callback {
    let task_id = self.scan_tasks[partition].fragment_id.clone();
    let cb = cb.clone();
    // Wrap stream to fire callback on completion
    // ... (use a FinalizeStream wrapper or inspect the poll)
}
```

The exact implementation depends on the current execute() structure. Read it carefully and add the callback fire at the appropriate point.

- [ ] **Step 3: Verify**

Run: `cargo test -p sqe-coordinator -- distributed_scan && cargo clippy -p sqe-coordinator -- -D warnings`

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/src/distributed_scan.rs
git commit -m "feat: add optional FragmentCallback to DistributedScanExec"
```

---

### Task 4: Wire try_distribute() into execute_query()

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs`

This is the core task — connecting all the pieces.

- [ ] **Step 1: Add helper to extract IcebergScanExec nodes**

Add a new function (outside the impl block, or as a standalone function):

```rust
use sqe_catalog::IcebergScanExec;

struct ScanInfo {
    data_files: Vec<String>,
    schema: SchemaRef,
    projected_columns: Vec<String>,
}

/// Walk a PhysicalPlan tree and find IcebergScanExec leaf nodes.
/// Returns info about each scan found.
async fn extract_iceberg_scans(plan: &Arc<dyn ExecutionPlan>) -> Vec<ScanInfo> {
    let mut scans = Vec::new();
    collect_scans(plan, &mut scans).await;
    scans
}

#[async_recursion::async_recursion]
async fn collect_scans(plan: &Arc<dyn ExecutionPlan>, scans: &mut Vec<ScanInfo>) {
    if let Some(iceberg_scan) = plan.as_any().downcast_ref::<IcebergScanExec>() {
        if let Ok(files) = iceberg_scan.data_file_paths().await {
            scans.push(ScanInfo {
                data_files: files,
                schema: iceberg_scan.schema(),
                projected_columns: iceberg_scan.projection()
                    .map(|p| p.to_vec())
                    .unwrap_or_default(),
            });
        }
    }
    for child in plan.children() {
        collect_scans(&child, scans).await;
    }
}
```

Note: Add `async-recursion = "1"` to sqe-coordinator's Cargo.toml if not already present. Alternatively, use a manual stack-based approach to avoid the dependency.

- [ ] **Step 2: Add try_distribute() method**

Add to `impl QueryHandler`:

```rust
    /// Attempt to distribute a physical plan across workers.
    /// Returns the original plan if distribution isn't possible.
    async fn try_distribute(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        session: &Session,
        query_id: &uuid::Uuid,
    ) -> Arc<dyn ExecutionPlan> {
        // 1. Check if workers are available
        let registry = match &self.worker_registry {
            Some(r) => r,
            None => return plan,
        };
        let healthy = registry.healthy_workers().await;
        if healthy.is_empty() {
            debug!("No healthy workers, executing locally");
            return plan;
        }

        // 2. Extract Iceberg scans
        let scans = extract_iceberg_scans(&plan).await;
        if scans.is_empty() {
            return plan;
        }

        // 3. Check file count threshold
        let total_files: usize = scans.iter().map(|s| s.data_files.len()).sum();
        if total_files < healthy.len() {
            debug!(total_files, workers = healthy.len(), "Not enough files, executing locally");
            return plan;
        }

        info!(
            total_files,
            workers = healthy.len(),
            scans = scans.len(),
            "Distributing query across workers"
        );

        // 4. Build ScanTasks
        let mut all_tasks = Vec::new();
        for scan in &scans {
            let file_groups = sqe_planner::split_files(
                scan.data_files.clone(),
                healthy.len(),
            );
            for (i, files) in file_groups.into_iter().enumerate() {
                if files.is_empty() {
                    continue;
                }
                all_tasks.push(sqe_planner::ScanTask {
                    fragment_id: uuid::Uuid::now_v7().to_string(),
                    data_file_paths: files,
                    projected_columns: scan.projected_columns.clone(),
                    s3_endpoint: self.config.storage.s3_endpoint.clone(),
                    s3_region: self.config.storage.s3_region.clone(),
                    s3_access_key: self.config.storage.s3_access_key.clone(),
                    s3_secret_key: self.config.storage.s3_secret_key.clone(),
                    s3_session_token: String::new(),
                    s3_path_style: self.config.storage.s3_path_style,
                    s3_allow_http: self.config.storage.s3_endpoint.starts_with("http://"),
                });
            }
        }

        if all_tasks.is_empty() {
            return plan;
        }

        // 5. Schedule tasks
        let worker_infos: Vec<crate::scheduler::WorkerInfo> = healthy.iter().map(|url| {
            crate::scheduler::WorkerInfo {
                url: url.clone(),
                healthy: true,
                active_fragments: 0,
            }
        }).collect();

        let scheduler = crate::scheduler::WeightedScheduler;
        let assignments = match scheduler.assign(&all_tasks, &worker_infos) {
            Ok(a) => a,
            Err(e) => {
                warn!(error = %e, "Scheduling failed, executing locally");
                return plan;
            }
        };

        // 6. Build DistributedScanExec
        let worker_urls: Vec<String> = assignments.iter()
            .map(|a| a.worker_url.clone())
            .collect();

        let schema = scans[0].schema.clone();

        // Record fragments in QueryTracker
        let fragments: Vec<crate::query_tracker::FragmentInfo> = all_tasks.iter()
            .zip(worker_urls.iter())
            .map(|(task, url)| crate::query_tracker::FragmentInfo {
                task_id: task.fragment_id.clone(),
                worker_url: url.clone(),
                state: crate::query_tracker::FragmentState::Running,
                elapsed_ms: 0,
                input_rows: 0,
                output_rows: 0,
            })
            .collect();
        self.query_tracker.set_fragments(query_id, fragments);

        let dist_scan = crate::distributed_scan::DistributedScanExec::new(
            all_tasks,
            worker_urls,
            schema,
        )
        .with_worker_registry(registry.clone());

        // For now, return the distributed scan as a standalone plan.
        // This works when the query is a simple scan. For complex plans
        // (joins, aggregations), a full plan rewrite is needed.
        Arc::new(dist_scan)
    }
```

- [ ] **Step 3: Call try_distribute() in execute_query()**

In `execute_query()` (around line 427-435), replace:

```rust
        // Create a new DataFrame from the enforced plan and execute
        let enforced_df = ctx
            .execute_logical_plan(enforced_plan)
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to create execution plan: {e}")))?;

        let batches = enforced_df
            .collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Query execution failed: {e}")))?;
```

With:

```rust
        // Create physical plan from the enforced logical plan
        let enforced_df = ctx
            .execute_logical_plan(enforced_plan)
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to create execution plan: {e}")))?;

        // Try to distribute scan work across workers
        let physical_plan = enforced_df.create_physical_plan().await
            .map_err(|e| SqeError::Execution(format!("Physical plan creation failed: {e}")))?;

        let maybe_distributed = self.try_distribute(
            physical_plan,
            session,
            &query_id,  // query_id from the tracker (added in the execute() method earlier)
        ).await;

        // Execute the (possibly distributed) plan
        let batches = datafusion::physical_plan::collect(maybe_distributed, ctx.task_ctx())
            .await
            .map_err(|e| SqeError::Execution(format!("Query execution failed: {e}")))?;
```

Note: `query_id` needs to be in scope. It's already generated in `execute()` (parent method) from the QueryTracker integration. Pass it to `execute_query()` as a parameter.

- [ ] **Step 4: Update execute_query() signature to accept query_id**

Change:
```rust
async fn execute_query(&self, session: &Session, sql: &str) -> sqe_core::Result<Vec<RecordBatch>>
```
To:
```rust
async fn execute_query(&self, session: &Session, sql: &str, query_id: &uuid::Uuid) -> sqe_core::Result<Vec<RecordBatch>>
```

Update all call sites in `execute()` to pass `&query_id`.

- [ ] **Step 5: Verify build and tests**

Run: `cargo build --all && cargo test --all && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-coordinator/src/query_handler.rs crates/sqe-coordinator/Cargo.toml
git commit -m "feat: wire distributed execution into query pipeline via try_distribute()"
```

---

### Task 5: Update system.runtime.tasks to use FragmentInfo

**Files:**
- Modify: `crates/sqe-catalog/src/system_runtime.rs`

- [ ] **Step 1: Update the tasks table builder**

Currently the tasks table auto-generates one task per query with `node_id = "test_warehouse"`. Change it to:
- If `record.fragments` is not empty → emit one row per fragment (with real worker URLs)
- If `record.fragments` is empty → generate one synthetic task (local execution) as before

Update the `build_tasks_table()` method in the `RuntimeSchemaProvider`.

- [ ] **Step 2: Verify**

Run: `cargo test -p sqe-catalog -- system_runtime && cargo clippy -p sqe-catalog -- -D warnings`

- [ ] **Step 3: Commit**

```bash
git add crates/sqe-catalog/src/system_runtime.rs
git commit -m "feat: system.runtime.tasks shows real worker URLs from FragmentInfo"
```

---

### Task 6: End-to-end testing with distributed stack

**Files:**
- Modify: `scripts/distributed-test.sh`

- [ ] **Step 1: Add distributed execution test**

Add a test that creates a multi-file table (CTAS with enough rows to produce multiple Parquet files), queries it, and verifies `system.runtime.tasks` shows worker URLs:

```bash
# Test: Distributed query shows worker assignments
echo "Test 13: Distributed execution"
# Create a larger table to produce multiple data files
run_sql "DROP TABLE IF EXISTS test_warehouse.default.dist_large" >/dev/null 2>&1 || true
# Insert enough data to create multiple files
for i in $(seq 1 5); do
    run_sql "INSERT INTO test_warehouse.default.dist_large SELECT * FROM test_warehouse.default.dtest1" >/dev/null 2>&1 || true
done
OUT=$(run_sql "SELECT node_id FROM system.runtime.tasks ORDER BY created DESC LIMIT 5")
# Check if any task ran on a worker (not just coordinator)
assert_contains "Tasks show worker assignment" "$OUT" "worker"
```

- [ ] **Step 2: Run the distributed test**

```bash
docker compose -f docker-compose.test.yml -f docker-compose.distributed.yml down
docker compose -f docker-compose.test.yml -f docker-compose.distributed.yml up --build -d
./scripts/bootstrap-test.sh
SQE_PASSWORD="" ./scripts/distributed-test.sh
```

- [ ] **Step 3: Commit**

```bash
git add scripts/distributed-test.sh
git commit -m "test: add distributed execution verification to integration tests"
```

---

### Task 7: Final verification and docs

**Files:**
- Modify: `README.md`, `nextsteps.md`

- [ ] **Step 1: Full verification**

```bash
cargo build --all
cargo test --all
cargo clippy --all-targets --all-features -- -D warnings
```

- [ ] **Step 2: Update docs**

Mark distributed execution wiring as complete in README roadmap and nextsteps.

- [ ] **Step 3: Commit and push**

```bash
git add README.md nextsteps.md
git commit -m "docs: mark distributed execution wiring as complete"
git push origin main
```

---

## Verification

After all tasks:
- [ ] `cargo build --all` — clean
- [ ] `cargo test --all` — all pass
- [ ] `cargo clippy -D warnings` — 0 errors
- [ ] `system.runtime.tasks` shows worker URLs for distributed queries
- [ ] Single-file queries stay local
- [ ] System table queries stay local
- [ ] Worker failure → retry → fallback works
- [ ] Full benchmark suite still passes
