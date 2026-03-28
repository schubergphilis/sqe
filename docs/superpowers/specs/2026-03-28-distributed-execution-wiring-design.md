# Distributed Execution Wiring — Design Spec

## Summary

Wire the existing distributed execution infrastructure (scheduler, DistributedScanExec, WorkerRegistry, worker Flight service) into the query execution pipeline. Currently all queries run locally on the coordinator. After this change, SELECT queries with Iceberg table scans are automatically distributed to workers when healthy workers are available and the scan has enough data files.

## Motivation

The coordinator, workers, scheduler, DistributedScanExec, and worker Flight service are all built and tested individually. But `execute_query()` in `query_handler.rs` never calls `should_distribute()` or replaces scan nodes with distributed scan nodes. All queries run locally regardless of worker availability.

## Architecture

```
execute_query(session, sql)
    │
    ├─► ctx.sql(sql) → LogicalPlan
    ├─► policy_enforcer.evaluate() → enforced LogicalPlan
    ├─► ctx.execute_logical_plan() → PhysicalPlan
    │
    ├─► should_distribute(physical_plan)?
    │       Conditions:
    │         1. WorkerRegistry is configured (not single-node mode)
    │         2. At least 1 healthy worker
    │         3. PhysicalPlan contains IcebergScanExec nodes
    │         4. Total data files >= number of healthy workers
    │       NO  → collect() locally (current path, unchanged)
    │       YES ↓
    │
    ├─► extract_iceberg_scans(physical_plan)
    │       Walk PhysicalPlan tree, find IcebergScanExec nodes
    │       For each: extract table metadata, data file paths, schema
    │       Returns Vec<ScanInfo { node, files, schema, projected_columns }>
    │
    ├─► Build ScanTasks for each scan
    │       For each ScanInfo:
    │         split_files(data_files, num_healthy_workers) → file groups
    │         For each group: ScanTask { fragment_id: UUID7, data_file_paths,
    │           s3 creds from session/config, projected_columns }
    │
    ├─► scheduler.assign(scan_tasks, worker_infos)
    │       WeightedScheduler: lowest-estimated-completion-time assignment
    │       Non-blocking, pure computation, O(n log n)
    │       Returns Vec<Assignment> { task_index, worker_url }
    │
    ├─► Replace IcebergScanExec nodes with DistributedScanExec in PhysicalPlan
    │       DistributedScanExec { scan_tasks, worker_urls, schema,
    │         worker_registry, credential_tracker }
    │       Each partition maps to one worker
    │
    ├─► Record fragment assignments in QueryTracker
    │       tracker.set_fragments(query_id, fragments)
    │
    └─► Execute the rewritten PhysicalPlan via DataFusion
        DataFusion drives the plan tree:
          - Non-scan nodes (filter, aggregate, join, sort) run on coordinator
          - DistributedScanExec.execute(partition_i) streams from worker[i]
            via Arrow Flight do_get
          - On worker failure: retry on different worker (up to max_retries),
            then fall back to local execution
          - On completion: update fragment state in QueryTracker
```

## Component 1: Plan Rewriting

**File:** `crates/sqe-coordinator/src/query_handler.rs`

New function `try_distribute()` that takes a PhysicalPlan and returns either a rewritten distributed plan or the original plan (for local execution).

```rust
async fn try_distribute(
    &self,
    plan: Arc<dyn ExecutionPlan>,
    session: &Session,
    query_id: &Uuid,
) -> Arc<dyn ExecutionPlan> {
    // 1. Check if distribution is possible
    let registry = match &self.worker_registry {
        Some(r) => r,
        None => return plan, // single-node mode
    };
    let healthy = registry.healthy_workers().await;
    if healthy.is_empty() {
        return plan; // no healthy workers
    }

    // 2. Extract Iceberg scans from the plan tree
    let scans = extract_iceberg_scans(&plan);
    if scans.is_empty() {
        return plan; // no table scans to distribute
    }

    // 3. Check file count threshold
    let total_files: usize = scans.iter().map(|s| s.data_files.len()).sum();
    if total_files < healthy.len() {
        return plan; // not enough files to justify distribution
    }

    // 4. Build ScanTasks and schedule
    // ... (see below)

    // 5. Replace scan nodes with DistributedScanExec
    // ... (see below)
}
```

### Extracting IcebergScanExec from PhysicalPlan

Walk the PhysicalPlan tree using `plan.children()` recursively. For each node, check if it's an `IcebergScanExec` by downcasting via `as_any()`. Extract:
- Data file paths from the Iceberg table metadata
- Schema from the scan node
- Projected columns

This requires accessing the table's metadata to list data files. The `IcebergScanExec` node in `sqe-catalog` holds a reference to the Iceberg `Table`. We may need to add a method to expose the data file list, or extract it from the table's current snapshot manifest.

### Replacing scan nodes

Use a recursive `rewrite_plan()` function that clones the plan tree, replacing `IcebergScanExec` nodes with `DistributedScanExec` nodes. DataFusion's `ExecutionPlan` trait has `with_new_children()` for rebuilding plan trees.

```rust
fn rewrite_plan(
    plan: Arc<dyn ExecutionPlan>,
    replacements: &HashMap<usize, Arc<DistributedScanExec>>,
) -> Arc<dyn ExecutionPlan> {
    // If this node is a scan to replace, return the replacement
    // Otherwise, recursively rewrite children and rebuild via with_new_children()
}
```

## Component 2: ScanTask Construction

**File:** `crates/sqe-coordinator/src/query_handler.rs` (in the `try_distribute` method)

For each `IcebergScanExec`:
1. Get data file paths from the table's current snapshot
2. Split files across healthy workers using `splitter::split_files()`
3. Build `ScanTask` for each group with S3 credentials from `config.storage`
4. Call `scheduler.assign()` to map tasks to workers

```rust
let worker_infos: Vec<WorkerInfo> = healthy.iter().map(|url| WorkerInfo {
    url: url.clone(),
    healthy: true,
    active_fragments: 0, // TODO: track real-time from registry
}).collect();

let assignments = self.scheduler.assign(&scan_tasks, &worker_infos)?;
```

## Component 3: Fragment Tracking in QueryTracker

**File:** `crates/sqe-coordinator/src/query_tracker.rs`

Add fragment tracking:

```rust
#[derive(Debug, Clone)]
pub struct FragmentInfo {
    pub task_id: String,
    pub worker_url: String,
    pub state: FragmentState,
    pub started: Option<DateTime<Utc>>,
    pub ended: Option<DateTime<Utc>>,
    pub elapsed_ms: u64,
    pub input_rows: usize,
    pub output_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FragmentState {
    Running,
    Finished,
    Failed,
    Retried,
}
```

Add to `QueryRecord`:
```rust
pub fragments: Vec<FragmentInfo>,
```

New methods on `QueryTracker`:
```rust
pub fn set_fragments(&self, query_id: &Uuid, fragments: Vec<FragmentInfo>);
pub fn update_fragment(&self, query_id: &Uuid, task_id: &str, state: FragmentState, elapsed_ms: u64, rows: usize);
```

## Component 4: Update system.runtime.tasks

**File:** `crates/sqe-catalog/src/system_runtime.rs`

The tasks table currently auto-generates one task per query. Change it to read from `record.fragments`:
- If `fragments` is empty (local execution): generate one synthetic task as before
- If `fragments` is populated (distributed): emit one row per fragment with real worker URLs

## Component 5: DistributedScanExec Fragment Callbacks

**File:** `crates/sqe-coordinator/src/distributed_scan.rs`

Add an optional callback for fragment completion/failure:

```rust
pub type FragmentCallback = Arc<dyn Fn(&str, FragmentState, u64, usize) + Send + Sync>;
```

`DistributedScanExec` stores this callback and calls it when a partition stream completes or fails. The coordinator passes a closure that updates the `QueryTracker`.

## Non-blocking Guarantees

1. **Scheduling** (`WeightedScheduler.assign()`) — pure computation, no I/O, O(n log n)
2. **Plan rewriting** — in-memory tree manipulation, no I/O
3. **Data file listing** — reads from Iceberg table metadata already loaded during planning (no extra Polaris calls)
4. **Fragment dispatch** — async Flight gRPC `do_get`, non-blocking
5. **Fragment streaming** — async `RecordBatchStream`, backpressure-aware via DataFusion's pull model
6. **Fragment callbacks** — fire-and-forget update to QueryTracker (moka insert is non-blocking)

## When NOT to distribute

- Single-node mode (`worker_urls` empty in config)
- No healthy workers available
- No `IcebergScanExec` nodes in the plan (system tables, SHOW commands, etc.)
- Fewer data files than workers (`files < workers`)
- DDL/DML statements (CREATE, INSERT, DROP — always local)
- EXPLAIN queries (always local)

## Configuration

No new config needed. Uses existing:
- `coordinator.worker_urls` — list of worker endpoints
- `storage.*` — S3 credentials for ScanTasks

## File Changes

| File | Action | Change |
|---|---|---|
| `crates/sqe-coordinator/src/query_handler.rs` | Modify | Add `try_distribute()`, call it in `execute_query()` after planning |
| `crates/sqe-coordinator/src/query_tracker.rs` | Modify | Add `FragmentInfo`, `FragmentState`, `set_fragments()`, `update_fragment()` |
| `crates/sqe-coordinator/src/distributed_scan.rs` | Modify | Add optional `FragmentCallback`, fire on partition complete/fail |
| `crates/sqe-catalog/src/system_runtime.rs` | Modify | Tasks table reads from `record.fragments` |
| `crates/sqe-catalog/src/iceberg_scan.rs` | Modify | Expose data file paths from IcebergScanExec |

## Testing Strategy

### Unit Tests
- `try_distribute()` returns original plan when no workers / no scans / few files
- `try_distribute()` returns rewritten plan with DistributedScanExec when conditions met
- `extract_iceberg_scans()` finds scan nodes in a plan tree
- `rewrite_plan()` correctly replaces scan nodes
- Fragment tracking: set_fragments, update_fragment, state transitions

### Integration Tests (distributed stack)
- Query with multi-file table → system.runtime.tasks shows workers, not coordinator
- system.runtime.nodes shows all 3 nodes
- Worker failure → retry on different worker → fragment state shows Retried
- SELECT on empty table → executes locally (no fragments)

## Success Criteria

1. SELECT queries on Iceberg tables with `files >= workers` are distributed
2. `system.runtime.tasks` shows actual worker URLs per fragment
3. No blocking I/O in the scheduling/dispatch path
4. Worker failures trigger retry, then local fallback
5. Single-file and system-table queries stay local
6. All existing tests continue to pass
