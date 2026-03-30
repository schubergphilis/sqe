# Neither Trusts the Other {#sec:coord-worker}

> The coordinator decides. The worker executes.
> Neither trusts the other more than necessary.
> Neither trusts the other more than necessary.

We have a physical plan. We have workers registered and sending heartbeats. Chapter 12 built the infrastructure for distributed execution -- worker registration, protobuf serialization, the Ballista heritage that gave us a head start. The question now is mechanical: how does a query plan get from the coordinator to a worker, execute under the right user's identity, and stream results back to the client?

The answer touches every trust boundary in the system. And the trust boundaries, it turns out, are the interesting part.

![Distributed execution: coordinator splits the physical plan at the scan boundary, dispatches fragments to workers, and collects Arrow batches](diagrams/rendered/13-distributed-execution.svg)


## The Plan Doesn't Travel Whole

A SQL query arrives at the coordinator as a string. The coordinator parses it, builds a logical plan, enforces policies (Chapter 8), optimizes it, and produces a physical plan. That physical plan is a tree of operators -- filter, projection, aggregation, sort -- rooted in a scan of Parquet files in S3.

The entire tree does not travel to workers. Only the scan travels.

This distinction matters. The coordinator keeps the upper plan tree -- the filters, the aggregations, the projections, the final sort. It sends only the leaf scan to workers. Workers read Parquet files and stream Arrow batches back. The coordinator applies everything else.

The split point is the `IcebergScanExec` node. When the coordinator has healthy workers available, it replaces this node with a `DistributedScanExec` that fans out the file reads across workers. Everything above -- every filter, every aggregation, every projection -- stays local.

```
Client SQL: SELECT region, SUM(amount) FROM orders WHERE year = 2025 GROUP BY region

Physical Plan (coordinator):
  AggregateExec [region, SUM(amount)]
    ProjectionExec [region, amount]
      FilterExec [year = 2025]
        DistributedScanExec           <-- replaced IcebergScanExec
          Worker 1: files 0..3        <-- reads Parquet, streams Arrow batches
          Worker 2: files 3..6
          Worker 3: files 6..9
```

The coordinator never touches S3. The workers never see the aggregation. Each side does exactly its job.


## Deciding Whether to Distribute

Not every query should be distributed. A `SHOW TABLES` query has no scan. A query touching a single small Parquet file gains nothing from shipping it to a worker -- the network round-trip costs more than the local read. The `try_distribute` method in `QueryHandler` makes this decision explicitly:

```rust
async fn try_distribute(
    &self,
    plan: Arc<dyn ExecutionPlan>,
    session: &Session,
    query_id: &uuid::Uuid,
) -> Arc<dyn ExecutionPlan> {
    // 1. Check if we have a worker registry (distributed mode)
    let registry = match self.worker_registry {
        Some(ref r) => r,
        None => return plan,
    };

    // 2. Get healthy workers -- if none, fall back to local
    let healthy = registry.healthy_workers().await;
    if healthy.is_empty() {
        debug!("No healthy workers available, executing locally");
        return plan;
    }

    // 3. Find IcebergScanExec node in the plan tree
    let scan_node = match find_iceberg_scan(&plan) {
        Some(node) => node,
        None => {
            debug!("No IcebergScanExec found in plan, executing locally");
            return plan;
        }
    };

    // ...

    // 5. Check if there are enough files to justify distribution
    let num_workers = healthy.len();
    if total_files < num_workers {
        debug!(
            total_files,
            num_workers,
            "Fewer files than workers, executing locally"
        );
        return plan;
    }

    // ... build scan tasks, schedule, replace scan node ...
}
```

Every check has a graceful fallback: return the original plan unchanged. If the registry is absent, the system runs single-node. If all workers are down, the coordinator handles the scan itself. If there are three files and five workers, there is no point creating two idle fragments. The coordinator falls back to local execution, and the query still succeeds.

This is deliberate. Distribution is an optimization, not a requirement. The system must work without it.

::: {.datafusion}
**DataFusion deep dive:** The `try_distribute` method operates on the physical plan *after* DataFusion's optimizer has run. This matters. Policy enforcement happens on the logical plan (Chapter 8), before optimization. By the time we reach `try_distribute`, row filters and column masks are already baked into the plan tree. The workers never see the original unfiltered plan. They get scan fragments that already reflect the user's policy-restricted view.
:::


## Splitting Files Across Workers

Once the coordinator decides to distribute, it needs to divide the work. An Iceberg table's scan resolves to a list of Parquet file paths in S3. The coordinator extracts these paths from the `IcebergScanExec` and splits them across workers.

The splitter is deliberately simple:

```rust
pub fn split_files(files: Vec<String>, num_workers: usize) -> Vec<Vec<String>> {
    if num_workers == 0 || files.is_empty() {
        return vec![];
    }

    let mut groups: Vec<Vec<String>> = (0..num_workers).map(|_| Vec::new()).collect();

    for (i, file) in files.into_iter().enumerate() {
        groups[i % num_workers].push(file);
    }

    groups
}
```

Round-robin. Each file goes to `worker[i % num_workers]`. A table with 12 Parquet files and 3 workers gets 4 files per worker. Not optimal -- there is no consideration of file size, data locality, or partition alignment. But it was correct, it was debuggable, and it shipped on day one.

The sophistication comes in the scheduler.


## The Weighted Scheduler

File splitting determines which files go together. The scheduler determines which worker gets each group. These are separate concerns. The split creates `ScanTask` objects; the scheduler assigns each task to a worker.

```rust
pub struct ScanTask {
    pub fragment_id: String,
    pub data_file_paths: Vec<String>,
    pub projected_columns: Vec<String>,
    pub s3_endpoint: String,
    pub s3_region: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_session_token: String,
    pub s3_path_style: bool,
    pub s3_allow_http: bool,
}
```

Each `ScanTask` is a self-contained unit of work. It carries everything a worker needs to read its files: the file paths, the S3 endpoint, the credentials, and the column projection. The worker does not need to contact the catalog. It does not need to resolve table metadata. Everything is already resolved.

The `WeightedScheduler` assigns tasks to workers using a largest-first bin-packing heuristic:

```rust
impl FragmentScheduler for WeightedScheduler {
    fn assign(
        &self,
        tasks: &[ScanTask],
        workers: &[WorkerInfo],
    ) -> Result<Vec<Assignment>, SchedulerError> {
        let healthy: Vec<&WorkerInfo> = workers.iter().filter(|w| w.healthy).collect();

        if healthy.is_empty() {
            return Err(SchedulerError::NoHealthyWorkers);
        }

        // Initialize each worker's load from its active_fragments count
        let mut loads: Vec<(u64, usize)> = healthy
            .iter()
            .enumerate()
            .map(|(i, w)| (u64::from(w.active_fragments), i))
            .collect();

        // Sort tasks by estimated cost descending (largest-first bin packing)
        let mut indexed_tasks: Vec<(usize, u64)> = tasks
            .iter()
            .enumerate()
            .map(|(i, t)| (i, estimate_cost(t)))
            .collect();
        indexed_tasks.sort_by(|a, b| b.1.cmp(&a.1));

        // Assign each task to the worker with the lowest current load
        for (task_idx, cost) in indexed_tasks {
            let min_pos = loads
                .iter()
                .enumerate()
                .min_by_key(|(_, (load, _))| *load)
                .map(|(pos, _)| pos)
                .expect("healthy workers vec is non-empty");

            let worker_idx = loads[min_pos].1;
            assignments[task_idx] = Some(Assignment {
                task_index: task_idx,
                worker_url: healthy[worker_idx].url.clone(),
            });

            loads[min_pos].0 += cost;
        }

        Ok(/* ... */)
    }
}
```

The cost function is file count: `estimate_cost` returns the number of Parquet files in the task, with a minimum of 1. A task with 10 files costs more than a task with 2. The heaviest tasks are assigned first, to the least-loaded worker. This is a classic approach from operations research -- largest-first decreases (LFD) -- and it produces balanced distributions even when tasks have wildly different costs.

The scheduler also considers existing load. If worker 1 is already executing 10 fragments from other queries and worker 2 is idle, new tasks go to worker 2. This is the `active_fragments` count in `WorkerInfo`, initialized from the worker registry.

Unhealthy workers are filtered out before scheduling begins. If a worker missed three consecutive health checks, it does not receive work. Period.

::: {.fieldreport}
**Field report:** Our first scheduler was pure round-robin -- task 0 to worker 0, task 1 to worker 1, and so on. It worked for uniform workloads. Then we ran a benchmark where one partition had 300MB of Parquet files and another had 2MB. The round-robin scheduler put both on different workers, but the worker with the 300MB partition became a bottleneck while the other worker sat idle for 12 seconds. The weighted scheduler fixed this in one afternoon. The bin-packing heuristic is not perfect, but it handles the common case of heterogeneous partition sizes without requiring per-file size metadata.
:::


## What the Worker Receives

The coordinator serializes the `ScanTask` to bytes and sends it as a Flight `Ticket` in a `do_get` call. The worker receives the ticket, deserializes the task, and starts reading Parquet files.

The worker's Flight service is minimal. It handles three operations:

```rust
#[tonic::async_trait]
impl FlightService for WorkerFlightService {
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        let scan_task = ScanTask::from_bytes(&ticket.ticket).map_err(|e| {
            Status::invalid_argument(format!("Failed to decode ScanTask: {e}"))
        })?;

        // Subscribe to credential updates for this fragment
        let cred_rx = credential_store.subscribe(&scan_task.fragment_id).await;

        let (schema, batches) =
            executor::execute_scan(&scan_task, Some(&metrics), &session_ctx, Some(cred_rx))
                .await
                .map_err(|e| Status::internal(format!("Scan execution failed: {e}")))?;

        // Stream results back as Arrow Flight data
        let batch_stream = stream::iter(batches.into_iter().map(Ok));
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(Status::from);

        Ok(Response::new(Box::pin(flight_stream)))
    }

    async fn do_action(&self, request: Request<Action>) -> /* ... */ {
        match action.r#type.as_str() {
            "health_check" => { /* return OK */ }
            "refresh_credentials" => { /* accept new S3 credentials */ }
            _ => Err(Status::unimplemented(/* ... */)),
        }
    }
}
```

Three actions. `do_get` executes a scan. `health_check` tells the coordinator the worker is alive. `refresh_credentials` accepts updated S3 credentials mid-scan. Everything else returns `UNIMPLEMENTED`. Workers do not support handshake, `do_put`, `do_exchange`, or any other Flight operation. They are executors, not servers.

The worker does not see the full query. It does not know about filters, aggregations, or projections that the coordinator will apply to its output. It reads the files it was told to read, in the columns it was told to project, and sends back Arrow batches. This is the fundamental contract: the coordinator thinks, the worker does.


## Executing the Scan

The executor is where Parquet bytes become Arrow batches. The `execute_scan` function walks through each file in the task, builds an S3 object store with the provided credentials, reads Parquet data, applies column projection, and collects the results.

```rust
pub async fn execute_scan(
    task: &ScanTask,
    metrics: Option<&Arc<WorkerMetricsRegistry>>,
    session_ctx: &SessionContext,
    credential_rx: Option<watch::Receiver<Option<RefreshableCredentials>>>,
) -> anyhow::Result<(SchemaRef, Vec<RecordBatch>)> {
    let store = build_object_store_with_creds(
        task,
        &task.s3_access_key,
        &task.s3_secret_key,
        &task.s3_session_token,
    )?;
    let mut store: Arc<dyn ObjectStore> = Arc::new(store);

    let pool = session_ctx.runtime_env().memory_pool.clone();
    let consumer = MemoryConsumer::new(format!("scan:{}", task.fragment_id));
    let mut reservation = consumer.register(&pool);

    for file_path in &task.data_file_paths {
        // Check for credential refresh before each file read
        if let Some(ref mut rx) = credential_rx {
            if rx.has_changed().unwrap_or(false) {
                let new_creds = rx.borrow_and_update().clone();
                if let Some(creds) = new_creds {
                    store = Arc::new(build_object_store_with_creds(
                        task, &creds.access_key_id,
                        &creds.secret_access_key, &creds.session_token,
                    )?);
                }
            }
        }

        // Read Parquet file, apply projection, collect batches
        let meta = store.head(&path).await?;
        let reader = ParquetObjectReader::new(store.clone(), meta.location);
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        // ... apply column projection mask ...
        let batches: Vec<RecordBatch> = builder.build()?.try_collect().await?;

        // Account for memory against the pool
        let batch_mem: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
        reservation.try_grow(batch_mem)?;

        all_batches.extend(batches);
    }

    Ok((schema, all_batches))
}
```

Two things stand out here.

First, the credential check happens *between files*, not between batches. A scan task with 10 files checks for refreshed credentials 10 times. A single large file is read with whatever credentials were current at the start. This is a pragmatic choice -- checking between individual Parquet row groups would require deeper integration with the async Parquet reader, and the credential refresh window (5 minutes before expiry) gives plenty of margin for even large files.

Second, memory accounting is explicit. Every batch is tracked against the `SessionContext`'s memory pool via a `MemoryConsumer` reservation. When `try_grow` fails -- because the worker has hit its memory limit -- the error propagates up as a DataFusion error. The scan stops. The query fails with a clear message about memory limits, not with an OOM kill.


## Memory Limits and Spill to Disk

Workers run with bounded memory. The `WorkerConfig` specifies a `memory_limit` (default: `8GB`) and whether to enable `spill_to_disk`:

```rust
pub fn build_session_context(config: &WorkerConfig) -> anyhow::Result<SessionContext> {
    let memory_bytes = parse_memory_limit(&config.memory_limit)?;

    let memory_pool = Arc::new(FairSpillPool::new(memory_bytes));

    let mut builder = RuntimeEnvBuilder::new().with_memory_pool(memory_pool);

    if config.spill_to_disk {
        builder = builder.with_temp_file_path(&config.spill_dir);
    } else {
        let disk_builder =
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled);
        builder = builder.with_disk_manager_builder(disk_builder);
    }

    let runtime = Arc::new(builder.build()?);
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);

    Ok(ctx)
}
```

DataFusion's `FairSpillPool` divides memory fairly among concurrent operators. When the pool is exhausted, operators that support spilling write intermediate data to disk. Operators that cannot spill (like our raw scan) fail with an error.

This is the right behaviour. A worker scanning too much data for its memory budget should fail fast, not silently swap until the kernel kills it. The coordinator can then reassign the fragment to a worker with more headroom, or fall back to local execution where the coordinator's larger memory pool might absorb the load.

::: {.deadend}
**Dead end: unlimited worker memory.** Our first deployment ran workers with no memory limit. The assumption was that S3 reads are streaming and memory consumption stays bounded. It was wrong. Column projection on wide tables with deeply nested Parquet schemas can temporarily require significant memory for decompression buffers. Two concurrent queries on a 200-column table caused the worker process to consume 14GB and get OOM-killed by Kubernetes. Adding `FairSpillPool` with explicit limits fixed this in one commit. The lesson: always bound your workers' resources, even when you think the workload is streaming.
:::


## The Trust Boundary

The coordinator and worker have an asymmetric trust relationship. Understanding this asymmetry is the key to the security model.

**The coordinator sends plans, not data.** It never reads from S3 itself in distributed mode. It constructs scan tasks -- which include S3 credentials -- and sends them to workers. The coordinator trusts the worker to execute the scan honestly and return correct results.

**The worker sends results, not plans.** It never modifies the query plan. It does not add filters, remove columns, or alter the aggregation. It reads files and streams batches. The coordinator trusts that the batches match the expected schema.

**The worker never sees the full query.** It does not know that the user is running `SELECT region, SUM(amount) FROM orders WHERE year = 2025 GROUP BY region`. It knows it should read files 3, 4, and 5, projecting columns `region`, `amount`, and `year`. The coordinator applies the filter and aggregation to the returned data. This limits what a compromised worker can infer about the user's intent.

| Component | Sends | Receives | Trusts the other to |
|-----------|-------|----------|---------------------|
| Coordinator | ScanTask (plan fragment + credentials) | Arrow RecordBatches | Execute honestly, return correct data |
| Worker | Arrow RecordBatches | ScanTask | Send valid tasks, provide valid credentials |

Neither side trusts the other more than necessary. The coordinator does not trust the worker with the full plan. The worker does not trust the coordinator to manage its resources -- it enforces its own memory limits.


## The Credential Problem We Almost Got Wrong

The first version of distributed execution embedded the coordinator's S3 credentials directly in the `ScanTask`. The coordinator had a static access key and secret key for the S3 endpoint, and it passed them through to workers. This worked. It was also wrong.

The problem is not technical -- it is operational. If the coordinator's S3 credentials are compromised via a worker, the attacker has access to *all* data in the S3 bucket. The coordinator's credentials are not scoped to a specific table or prefix. They are the keys to the kingdom.

The security review caught this in twelve minutes. The fix was conceptual, not mechanical: workers should obtain their own credentials from Polaris, scoped to the specific table and prefix they need to read.

In the current implementation, the coordinator still sends credentials in the `ScanTask`. For our deployment with a private S3-compatible endpoint (RustFS/MinIO), this is acceptable -- the credentials are static and the network is internal. But the architecture is designed for the production case: Polaris vends short-lived STS credentials scoped to the specific S3 prefix for each table. Those credentials travel in the `ScanTask`, expire in 15 minutes, and the credential refresh mechanism (described below) handles renewal.

The path from "coordinator passes its own credentials" to "Polaris vends scoped credentials per table" is a configuration change, not an architecture change. The `ScanTask` already has fields for `s3_access_key`, `s3_secret_key`, and `s3_session_token`. The only difference is where those values come from.

::: {.deadend}
**Dead end: workers calling Polaris directly.** We considered having workers contact Polaris themselves, using the user's JWT to obtain their own S3 credentials. This would eliminate credential passing entirely. The problem: every worker would need Polaris connectivity and the catalog URL. It would also mean N workers making N credential requests for the same table, when the coordinator already has the credentials from planning the query. The duplication was wasteful and the additional network dependency made workers less isolated. We kept credential passing via the `ScanTask`.
:::


## Heartbeats: How the Coordinator Knows Who Is Alive

Workers announce themselves to the coordinator with periodic heartbeats. The heartbeat is an Arrow Flight `do_action("heartbeat")` call -- the same protocol used for everything else. No separate discovery service, no etcd, no ZooKeeper.

```rust
pub fn start_heartbeat_task(coordinator_url: String, worker_url: String, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await; // skip first immediate tick

        loop {
            ticker.tick().await;
            if let Err(e) = send_heartbeat(&coordinator_url, &worker_url).await {
                warn!(
                    coordinator = %coordinator_url,
                    error = %e,
                    "Heartbeat to coordinator failed, will retry next interval"
                );
            }
        }
    });
}
```

The heartbeat body contains the worker's own Flight service URL. This is how the coordinator learns which workers exist and where to reach them. The `WorkerRegistry` on the coordinator side tracks health state:

```rust
pub async fn register_heartbeat(&self, url: &str) {
    let mut inner = self.inner.write().await;
    let state = inner.workers.entry(url.to_string()).or_insert_with(|| {
        info!(worker = url, "Discovered new worker via heartbeat");
        WorkerState {
            url: url.to_string(),
            healthy: false,
            consecutive_failures: 0,
            last_healthy: None,
        }
    });
    state.healthy = true;
    state.consecutive_failures = 0;
    state.last_healthy = Some(Instant::now());
}
```

Workers start unhealthy and become healthy after their first heartbeat. Three consecutive missed heartbeats mark a worker as unhealthy. A single successful heartbeat recovers it. The threshold is deliberately low -- in a sovereign deployment, you control the network, and three missed heartbeats (15 seconds at the default 5-second interval) is a clear signal.

There is a separate `mark_unhealthy` method that bypasses the consecutive-failure threshold. When a worker fails during query execution -- connection refused, timeout, gRPC error -- it is marked unhealthy immediately. Waiting for three more missed heartbeats when you already know the worker is down would waste queries.

The coordinator also runs an active health check loop that calls each worker's `health_check` action. This catches the case where a worker is running but stuck -- it is accepting connections but not processing them. The heartbeat (worker-initiated) proves the worker is trying. The health check (coordinator-initiated) proves it is responding.


## Streaming Results Back

When a worker finishes reading a Parquet file, it does not wait for all files to complete before responding. The Arrow Flight `do_get` response is a stream. Batches flow back to the coordinator as they are produced.

On the coordinator side, the `DistributedScanExec` implements DataFusion's `ExecutionPlan` trait. Each partition maps to one worker. When DataFusion calls `execute(partition)`, the exec sends the `ScanTask` to the assigned worker and returns a `SendableRecordBatchStream` backed by the Flight response:

```rust
impl ExecutionPlan for DistributedScanExec {
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let task = self.scan_tasks[partition].clone();
        let initial_worker_url = self.worker_urls[partition].clone();
        let schema = self.schema.clone();

        // ... setup retry logic, credential tracking, trace propagation ...

        let resolve_future = async move {
            match dispatch_to_worker(&task, &current_worker_url, &parent_cx).await {
                Ok(flight_stream) => {
                    // Project received batches to match the expected schema
                    let inner = Box::pin(
                        flight_stream
                            .map_err(|e| DataFusionError::External(Box::new(e)))
                            .map(move |batch_result| {
                                let batch = batch_result?;
                                // ... schema projection ...
                                Ok(batch)
                            }),
                    );
                    Ok(inner)
                }
                Err(e) => {
                    // Mark worker unhealthy, try another worker ...
                }
            }
        };
        // ...
    }
}
```

The dispatch is a simple Flight `do_get` call. The `ScanTask` is serialized to bytes and sent as the `Ticket`. The worker deserializes it and streams back results. OpenTelemetry trace context is injected into the gRPC metadata so the worker's span appears as a child of the coordinator's span -- one trace across the entire distributed query.

Schema projection on the coordinator side handles a subtle problem. Workers return full table columns because the Parquet reader applies only the column projection specified in the `ScanTask`. But the physical plan above the `DistributedScanExec` may expect fewer columns -- for example, a `COUNT(*)` query expects zero columns. The stream adapter matches incoming batches to the expected schema by selecting columns by name, or producing row-count-only batches for the zero-column case.

This projection mismatch caused one of our more confusing bugs. We ran `SELECT COUNT(*) FROM orders` distributed across two workers. Both workers returned correct batches with all projected columns. But DataFusion's `AggregateExec` for `COUNT(*)` expects zero input columns -- it only needs the row count. The `DistributedScanExec` was producing batches with 5 columns where the parent expected 0. DataFusion did not crash. It silently produced wrong results. The fix was the `expected_cols == 0` branch in the stream adapter, which strips all columns and preserves only the row count. One of those bugs where the system looks correct until you check the numbers.

::: {.datafusion}
**DataFusion deep dive:** `DistributedScanExec` implements `ExecutionPlan` with `Partitioning::UnknownPartitioning(n)` where `n` is the number of scan tasks. DataFusion's `collect()` function calls `execute(i)` for each partition and merges the results. This means all worker scans run in parallel -- DataFusion's task scheduler handles the concurrency. We did not need to build our own parallel dispatch loop. DataFusion did it for us.
:::


## Credential Refresh: The Push Model

STS credentials expire. In a production Polaris deployment, vended S3 credentials have a TTL -- typically 15 minutes to 1 hour. A long-running scan on a large table can easily exceed that window.

The naive fix would be to set a long TTL. But long-lived credentials are a security liability. The better fix is to refresh credentials before they expire and push the new ones to workers.

The `CredentialRefreshTracker` on the coordinator monitors active fragments:

```rust
pub struct CredentialRefreshTracker {
    fragments: Arc<RwLock<HashMap<String, ActiveFragment>>>,
    refresh_buffer_secs: i64,
}

pub struct ActiveFragment {
    pub fragment_id: String,
    pub worker_url: String,
    pub credential_expiry: Option<DateTime<Utc>>,
}
```

When the coordinator dispatches a scan fragment, it registers the fragment with the tracker, including the credential expiry time. A background task runs every 60 seconds and checks which fragments have credentials approaching expiry (within 5 minutes of the expiry time). For those fragments, it obtains fresh credentials from Polaris and pushes them to the appropriate worker.

The push happens via Arrow Flight `do_action("refresh_credentials")`:

```rust
pub async fn push_credentials_to_worker(
    worker_url: &str,
    credentials: &RefreshableCredentials,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let body = serde_json::to_vec(credentials)?;

    let channel = tonic::transport::Endpoint::new(worker_url.to_string())?
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(10))
        .connect()
        .await?;
    let mut client = FlightServiceClient::new(channel);

    let action = Action {
        r#type: "refresh_credentials".to_string(),
        body: bytes::Bytes::from(body),
    };

    client.do_action(tonic::Request::new(action)).await?;
    Ok(())
}
```

On the worker side, the `CredentialStore` uses a `tokio::sync::watch` channel per fragment. The executor subscribes before starting the scan. The Flight service publishes new credentials when they arrive. The executor checks the channel before each file read:

```rust
// In the executor's file loop:
if let Some(ref mut rx) = credential_rx {
    if rx.has_changed().unwrap_or(false) {
        let new_creds = rx.borrow_and_update().clone();
        if let Some(creds) = new_creds {
            store = Arc::new(build_object_store_with_creds(
                task, &creds.access_key_id,
                &creds.secret_access_key, &creds.session_token,
            )?);
        }
    }
}
```

The `watch` channel is ideal here. It always delivers the latest value, so if two refreshes happen between file reads, the executor picks up only the most recent credentials. There is no queue to drain, no ordering to maintain. Just "what are my current credentials?"

We considered `mpsc` channels initially. The problem with `mpsc` is that the receiver must consume every message in order. If the coordinator sends three rapid refreshes (each with newer credentials), the executor would consume the first, use it for one file read, consume the second, use it for another file read, and so on. With `watch`, it skips straight to the latest. For credentials, only the most recent value matters. Older credentials are strictly less useful than newer ones.

When a scan completes, the credential channel is cleaned up:

```rust
credential_store.remove(&scan_task.fragment_id).await;
```

And the tracker unregisters the fragment:

```rust
credential_tracker.unregister(&task.fragment_id).await;
```

The entire lifecycle is: register on dispatch, refresh if approaching expiry, unregister on completion. No leaked channels, no orphaned watchers.


## The DistributedScanExec in Full

The `DistributedScanExec` ties everything together. It is a DataFusion `ExecutionPlan` that replaces the local `IcebergScanExec` in the plan tree. Its configuration tells the full story of what distributed execution requires:

```rust
pub struct DistributedScanExec {
    scan_tasks: Vec<ScanTask>,
    worker_urls: Vec<String>,
    schema: SchemaRef,
    properties: PlanProperties,
    credential_expiry: Option<DateTime<Utc>>,
    credential_tracker: Option<Arc<CredentialRefreshTracker>>,
    worker_registry: Option<Arc<WorkerRegistry>>,
    max_retries: u32,
    local_executor: Option<Arc<dyn LocalExecutor>>,
    fragment_callback: FragmentCallbackOpt,
}
```

Every field is optional except the fundamentals (`scan_tasks`, `worker_urls`, `schema`). You can run distributed execution with no credential tracking, no retry logic, no local fallback, and no progress callbacks. Each feature is a layer that can be added or removed without affecting the others.

The builder pattern makes this explicit:

```rust
let exec = DistributedScanExec::new(scan_tasks, worker_urls, schema)
    .with_worker_registry(Arc::clone(registry))
    .with_credential_tracker(tracker)
    .with_fragment_callback(callback)
    .with_max_retries(2);
```

When a worker fails, the retry logic in `execute()` marks the worker unhealthy in the registry, picks a different healthy worker, and retries the fragment. If all workers fail, and a `local_executor` is configured, the fragment runs on the coordinator itself. The fallback chain is: assigned worker, then other healthy workers, then local execution, then error.

The `fragment_callback` fires when each fragment stream completes or fails, reporting the fragment ID, success status, elapsed time, and output row count. The coordinator uses this to update the query tracker -- the same data that powers the `system.runtime.tasks` virtual table where you can see, in real time, which fragments are running on which workers.


## Putting It All Together

Here is the complete flow when a user runs `SELECT region, SUM(amount) FROM orders GROUP BY region` against a table with 12 Parquet files and 3 healthy workers:

1. The coordinator parses the SQL, builds a logical plan, enforces policies, and produces a physical plan with an `IcebergScanExec` leaf.

2. `try_distribute` finds the `IcebergScanExec`, extracts 12 file paths, and calls `split_files` to create 3 groups of 4 files each.

3. Three `ScanTask` objects are created, each with a unique `fragment_id`, 4 file paths, and S3 credentials.

4. The `WeightedScheduler` assigns tasks to workers based on estimated cost and current load. Task 1 goes to Worker A (lowest load), Task 2 to Worker B, Task 3 to Worker C.

5. The coordinator builds a `DistributedScanExec` with the three tasks and worker URLs, and replaces the `IcebergScanExec` in the plan tree.

6. DataFusion calls `execute(0)`, `execute(1)`, `execute(2)` in parallel. Each call dispatches a `ScanTask` to its assigned worker via Arrow Flight `do_get`.

7. Each worker deserializes the task, builds an S3 object store, reads its 4 Parquet files, applies column projection, and streams Arrow batches back.

8. The coordinator's `FilterExec` applies `WHERE year = 2025` to the streamed batches. The `AggregateExec` computes the `GROUP BY region, SUM(amount)`. The `ProjectionExec` selects the final columns.

9. The client receives the aggregated result via Arrow Flight.

The workers never knew there was a filter. They read all years and let the coordinator discard the irrelevant ones. Future optimization could push the filter predicate into the `ScanTask` for Parquet predicate pushdown at the storage level -- but even without it, the architecture is correct and the query returns the right answer.

The total wall-clock time depends on the slowest worker. If Workers A and B finish in 2 seconds but Worker C takes 8 seconds (because its files are larger or S3 is throttling), the query takes 8 seconds. This is the price of parallel execution with a synchronization barrier. The weighted scheduler mitigates this by distributing heavier tasks to less-loaded workers, but it cannot eliminate it. Imbalance is inherent in real data. Iceberg partition pruning and manifest-level statistics will eventually help the scheduler make better decisions -- assigning based on file size, not just file count. That is a future optimization. The current system is correct, observable, and fast enough.


## What the Coordinator Cannot Do

The coordinator cannot read data. In distributed mode, it has no S3 connectivity (by design -- it does not need it). It cannot modify a running scan. Once a fragment is dispatched, the worker owns it until completion or failure. The coordinator cannot rearrange the plan mid-execution -- it is committed to the distribution it chose.

These constraints are features. A coordinator that cannot read data cannot leak data. A coordinator that cannot modify running scans cannot corrupt results. A coordinator that commits to a plan is predictable and debuggable.

There is one thing the coordinator *can* do after dispatch: push refreshed credentials and track progress via fragment callbacks. These are deliberate, narrow exceptions to the "fire and forget" model. They exist because the alternative -- letting credentials expire and queries fail, or having no visibility into fragment progress -- is worse. The coordinator touches running scans only to keep them alive and observable, never to change what they compute.


## What the Worker Cannot Do

The worker cannot see the full query. It cannot modify the plan it received. It cannot contact other workers -- there is no worker-to-worker communication. It cannot access tables it was not given credentials for. It cannot exceed its memory limit (the `FairSpillPool` enforces this, or the query fails).

These constraints are also features. A compromised worker that cannot see the full query cannot reconstruct what the user asked. A worker that cannot contact other workers cannot be used as a pivot point in a lateral network movement. A worker that cannot exceed its memory limit cannot destabilize the host.

::: {.sovereignty}
**Sovereignty principle:** In a distributed system, every trust boundary is an attack surface. The coordinator-worker boundary is designed so that compromising either side gives the attacker the least possible leverage. The coordinator cannot read data. The worker cannot see plans. Neither holds credentials beyond what the current query requires. This is not paranoia -- it is the minimum viable security posture for a system that handles production data.
:::


## The Lesson

Building distributed execution forced us to answer a question that single-node systems ignore: who knows what, and who trusts whom?

In a monolith, everything trusts everything. The query parser trusts the execution engine trusts the storage layer. There are no boundaries because there is no distance.

Distribution introduces distance, and distance introduces doubt. Can the coordinator trust the worker's results? Can the worker trust the coordinator's credentials? What happens when the network between them lies?

Our answer is minimal trust. The coordinator sends exactly what the worker needs and nothing more. The worker returns exactly what it computed and nothing more. Neither side holds state about the other beyond what is required for the current query. When the query completes, the relationship ends. Credentials expire. Channels close. The next query starts fresh.

This is more code than a naive implementation where the coordinator ships the entire plan and the worker returns the final answer. It is also more secure, more debuggable, and more resilient. When something goes wrong -- and Chapter 14 is about all the ways things go wrong -- the failure is contained to a single fragment, a single worker, a single credential scope. The blast radius is always bounded.

The coordinator decides. The worker executes. Neither trusts the other more than necessary. That is the contract. Everything else follows from it.

::: {.ailog}
**AI Logbook:** The AI implemented the `DistributedScanExec`, `WorkerFlightService`, `execute_scan`, the credential refresh push mechanism with `tokio::sync::watch` channels, and the `CredentialRefreshTracker` — all from a design doc that explicitly stated the trust boundary between coordinator and worker. The human drew that trust boundary; the AI couldn't derive it from the code. The `COUNT(*)` schema projection bug — workers returning five columns where the parent `AggregateExec` expected zero — produced silently wrong results that the human found by checking the numbers; the AI's fix was the `expected_cols == 0` branch in the stream adapter.
:::
