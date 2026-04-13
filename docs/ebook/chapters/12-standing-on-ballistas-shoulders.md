# Standing on Ballista's Shoulders {#sec:ballista}

> Don't build a distributed scheduler from scratch.
> Build on one that already works, then make it yours.

Apache Ballista exists for a reason. Building a distributed query execution framework is years of work. You need plan serialisation. You need worker registration. You need a scheduler that knows which workers are alive and which have died since the last heartbeat. You need a wire protocol that can move Arrow batches between processes without copying them into JSON and back. You need all of this before you write a single line of query logic.

Ballista already has all of it. It's DataFusion's official distributed execution layer, and it's built by the same people who build DataFusion. The protobuf serialisation understands DataFusion's plan nodes. The workers speak Arrow Flight. The scheduler tracks worker health with heartbeats.

So the question wasn't whether to use Ballista. The question was *how much* of Ballista to use.


## The Three Paths

When you need distributed execution for a DataFusion-based engine, you have three options. We considered all of them.

### Path 1: Use Ballista as-is

Ballista ships a `SchedulerServer` and an `ExecutorServer`. You configure them, point them at each other, and submit queries to the scheduler. The scheduler parses SQL, creates a physical plan, splits it into stages, assigns stages to executors, and collects results. It's a complete distributed query engine.

The problem is that it's *too* complete. Ballista's scheduler handles everything from SQL parsing to result collection. SQE's coordinator already does SQL parsing. SQE's auth layer already manages sessions and bearer tokens. SQE's policy engine already rewrites plans before optimisation.

Plugging Ballista in as-is would mean either duplicating all that work (running SQE's pipeline *and then* feeding the result into Ballista's pipeline) or abandoning SQE's pipeline and trying to inject auth and policy into Ballista's internals.

We tried the first approach for about a day. The coordinator would plan the query, enforce policy, produce a physical plan, then hand the plan to Ballista's scheduler for distribution. The scheduler would re-plan it. Filters that the policy engine had injected were being rearranged by Ballista's optimiser pass. Column masks that were carefully positioned to block predicate pushdown were getting pushed down anyway.

::: {.deadend}
**Dead end: Ballista as a black box.** We tried wrapping Ballista's scheduler as a backend for SQE's coordinator. The plan went in with policy filters attached. The plan came out with policy filters rearranged. Ballista's internal optimiser pass didn't know that certain filter nodes were security boundaries, not performance hints. Two days of work, and we learned that you can't treat a query planner as a transparent pass-through.
:::

### Path 2: Build from scratch

The other extreme. Ignore Ballista entirely. Write our own protobuf schema for plan serialisation. Write our own worker registration protocol. Write our own heartbeat mechanism. Write our own scheduler.

This is the approach that gives you the most control and the most work. We estimated three to four months for a production-quality distributed execution layer, based on the scope of what Ballista provides: protobuf codecs for every DataFusion plan node, Arrow Flight integration for result streaming, worker lifecycle management, and stage-based execution with shuffle support.

The protobuf codec alone is a significant undertaking. DataFusion 52 has over forty physical plan node types. Each needs a protobuf message definition, an encoder, and a decoder. Each encoder must handle all configuration variants -- a `HashJoinExec` alone has join type, join filter, null equality behaviour, partition mode, and projection. Getting any of these wrong means silent data corruption, because the deserialized plan produces different results than the original. Ballista's codebase has thousands of lines of codec tests for a reason.

We didn't have three months. We had the ambition to go from zero to distributed execution in under two weeks. The math didn't work.

### Path 3: Surgical fork

Take Ballista's ideas. Take its serialisation model. Take its execution philosophy. Don't take its scheduler. Don't take its auth model. Don't take its configuration surface.

This is the path we chose. Not a fork in the Git sense -- we didn't clone the Ballista repository and start modifying it. We studied Ballista's architecture, understood its codec design, and reimplemented the parts we needed with SQE's constraints baked in from the start.

The key insight: Ballista's most valuable contribution isn't its scheduler or its executor process. It's the *pattern*. The idea that a DataFusion physical plan can be serialised to protobuf, sent over the wire, deserialised on a different machine, and executed there with full fidelity. Once you understand that pattern, you can implement it in far fewer lines than Ballista uses, because you only need to serialise the plan nodes *your* engine actually produces.

::: {.antipattern}
**Antipattern: the partial fork.** There's a fourth path we didn't mention because it's the one you should never take: clone the repository, delete the parts you don't need, and start modifying what's left. This creates a codebase that looks like yours but inherits Ballista's internal assumptions, naming conventions, and coupling. Every upstream bugfix requires manual cherry-picking across diverged codebases. Every upstream API change requires understanding code you didn't write and only partially understand. A partial fork gives you the maintenance burden of both "build from scratch" and "use a dependency," with the benefits of neither. If you can't use a project's public API, study its architecture and reimplement what you need. Don't inhabit someone else's codebase.
:::


## What We Kept

Three things from Ballista's model survived into SQE essentially unchanged.

**The serialisation model.** DataFusion's `datafusion-proto` crate provides protobuf serialisation for LogicalPlans and PhysicalPlans. This is the same serialisation layer that Ballista uses internally. We use it directly -- `datafusion-proto = "52"` in our `Cargo.toml`. The protobuf schema defines how every built-in DataFusion plan node (Filter, Projection, Aggregate, Sort, HashJoin, and dozens more) maps to a protobuf message and back. This is thousands of lines of code we didn't have to write.

**The execution model.** Ballista's core insight is that a distributed query execution is just a set of independent scan tasks, each assigned to a worker, with results streamed back to the coordinator via Arrow Flight. The coordinator doesn't send the whole query to every worker. It sends each worker a *fragment* -- a subset of files to scan, with the credentials needed to read them. The worker executes the fragment, streams back RecordBatches, and the coordinator stitches the results together. SQE follows this pattern exactly.

In SQE, this model is embodied by `DistributedScanExec` -- a custom `ExecutionPlan` node that the coordinator injects into the physical plan tree in place of the local `IcebergScanExec`. Each partition of the `DistributedScanExec` maps to one worker. When DataFusion's execution engine calls `execute(partition)`, the node dispatches a `ScanTask` to the assigned worker via Flight `do_get` and returns the result stream. From DataFusion's perspective, the distributed scan behaves identically to a local scan -- it produces `RecordBatch` streams with the same schema. The distribution is invisible to every operator above it in the plan tree.

**The Flight interface.** Workers expose an Arrow Flight service. The coordinator dispatches work by calling `do_get` with a ticket that describes the scan task. Results come back as a stream of `FlightData` messages containing Arrow RecordBatches. Health checks use `do_action`. Credential refreshes use `do_action`. Everything over gRPC, everything using the Arrow Flight protocol. This is the same interface Ballista uses between its scheduler and executors.

The worker side of this interface is compact. The `WorkerFlightService` implements three operations and nothing else:

```rust
#[tonic::async_trait]
impl FlightService for WorkerFlightService {
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        let scan_task = ScanTask::from_bytes(&ticket.ticket)
            .map_err(|e| {
                Status::invalid_argument(
                    format!("Failed to decode ScanTask: {e}"))
            })?;

        let cred_rx = credential_store
            .subscribe(&scan_task.fragment_id).await;

        let (schema, batches) = executor::execute_scan(
            &scan_task, Some(&metrics),
            &session_ctx, Some(cred_rx),
        ).await.map_err(|e| {
            Status::internal(
                format!("Scan execution failed: {e}"))
        })?;

        let batch_stream =
            stream::iter(batches.into_iter().map(Ok));
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(Status::from);

        Ok(Response::new(Box::pin(flight_stream)))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        match action.r#type.as_str() {
            "health_check" => { /* return OK */ }
            "refresh_credentials" => {
                /* accept new S3 credentials */
            }
            _ => Err(Status::unimplemented(/* ... */)),
        }
    }
}
```

Three actions. `do_get` executes a scan. `health_check` tells the coordinator the worker is alive. `refresh_credentials` accepts updated S3 credentials mid-scan. Workers also implement `do_exchange` for shuffle data ingestion in distributed aggregation -- hash-partitioned or range-partitioned batches flow between workers during multi-stage execution. Everything else returns `UNIMPLEMENTED`. Workers do not support handshake, `do_put`, or any other Flight operation. They are executors, not servers.


## What We Replaced

Four things from Ballista didn't survive.

**The scheduler.** Ballista's scheduler is a standalone service that accepts SQL queries, plans them, and distributes work to executors. SQE's coordinator already does the planning. What SQE needs is a *fragment scheduler* -- something that takes a set of scan tasks and assigns them to workers based on load, health, and file count. That's a much simpler problem than what Ballista's scheduler solves.

Our scheduler is 170 lines:

```rust
/// Weighted fragment scheduler that assigns tasks to the
/// least-loaded worker.
///
/// Strategy:
/// 1. Filter out unhealthy workers.
/// 2. Initialize each worker's load from its active_fragments count.
/// 3. Sort tasks by estimated cost (descending) so the heaviest
///    tasks are assigned first ("largest-first" bin-packing).
/// 4. Assign each task to the worker with the currently lowest
///    total load.
#[derive(Debug, Default)]
pub struct WeightedScheduler;

impl FragmentScheduler for WeightedScheduler {
    fn assign(
        &self,
        tasks: &[ScanTask],
        workers: &[WorkerInfo],
    ) -> Result<Vec<Assignment>, SchedulerError> {
        let healthy: Vec<&WorkerInfo> =
            workers.iter().filter(|w| w.healthy).collect();

        if healthy.is_empty() {
            return Err(SchedulerError::NoHealthyWorkers);
        }

        // Build a load tracker: (accumulated_load, worker_index)
        let mut loads: Vec<(u64, usize)> = healthy
            .iter()
            .enumerate()
            .map(|(i, w)| (u64::from(w.active_fragments), i))
            .collect();

        // Sort tasks by cost descending (largest-first bin packing)
        let mut indexed_tasks: Vec<(usize, u64)> = tasks
            .iter()
            .enumerate()
            .map(|(i, t)| (i, estimate_cost(t)))
            .collect();
        indexed_tasks.sort_by(|a, b| b.1.cmp(&a.1));

        let mut assignments: Vec<Option<Assignment>> =
            vec![None; tasks.len()];

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

        Ok(assignments.into_iter().map(|a| a.unwrap()).collect())
    }
}
```

This is a largest-first bin-packing heuristic. The heaviest tasks get assigned first, to the least-loaded worker. It produces balanced distributions even when tasks have wildly different costs (one scan task covering 10 files vs another covering 1 file). It also accounts for workers that already have in-flight work from previous queries.

The scheduler also uses consistent hashing on file paths to prefer a "home" worker for each task. If the preferred worker's accumulated load exceeds the minimum-load worker by more than 20%, the task falls back to minimum-load assignment. This balances two goals: cache locality (a worker that has recently read a file may still have its Parquet footer cached) and load balance (no single worker becomes a bottleneck).

Ballista's scheduler is far more sophisticated. It handles multi-stage execution with shuffles, work stealing, and speculative execution. We don't need any of that -- SQE's distributed execution is scan-parallel only. Every scan task is independent. There's no shuffle stage. The coordinator handles final aggregation, sorting, and projection locally.

**Authentication.** Ballista has no concept of user identity. Its scheduler accepts queries anonymously. Its executors run with whatever ambient credentials the process has. This is the fundamental incompatibility.

SQE's distributed execution carries the user's bearer token through to every worker. The `ScanTask` struct includes S3 credentials vended specifically for that user:

```rust
pub struct ScanTask {
    pub fragment_id: String,
    pub data_file_paths: Vec<String>,
    pub file_sizes_bytes: Vec<u64>,
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

The `file_sizes_bytes` field enables byte-accurate cost estimation in the scheduler -- file count is a rough proxy, but actual file sizes let the `WeightedScheduler` balance I/O load precisely. Every field after `projected_columns` is about credentials. The worker never contacts Polaris. The worker never assumes a role. The worker reads the files it's told to read, with the credentials it's given, and returns the results. The coordinator is the only component that talks to the catalog.

Notice the `Debug` implementation on this struct. It redacts credentials:

```rust
impl std::fmt::Debug for ScanTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanTask")
            .field("fragment_id", &self.fragment_id)
            .field("data_file_paths", &self.data_file_paths)
            .field("projected_columns", &self.projected_columns)
            .field("s3_endpoint", &self.s3_endpoint)
            .field("s3_region", &self.s3_region)
            .field("s3_access_key", &"[REDACTED]")
            .field("s3_secret_key", &"[REDACTED]")
            .field("s3_session_token", &session_token_display)
            .field("s3_path_style", &self.s3_path_style)
            .field("s3_allow_http", &self.s3_allow_http)
            .finish()
    }
}
```

Small thing. Important thing. Log files are read by people who shouldn't see S3 credentials. The `#[derive(Debug)]` default would dump secrets into every debug log line. A custom `Debug` impl costs ten lines and prevents a class of security incident that's embarrassingly common.

**Configuration.** Ballista has its own configuration model -- command-line flags, environment variables, a scheduler config, an executor config. SQE has its TOML-based configuration with environment variable overlay. We didn't want two config models. Workers and coordinator share the same `SqeConfig` struct, with a `mode` field that determines which role the process plays:

```rust
pub enum Mode {
    Coordinator,
    Worker,
}
```

One binary, two modes. The coordinator starts a Flight SQL service for clients and a heartbeat listener for workers. The worker starts a Flight service for scan execution and a heartbeat sender pointed at the coordinator. Same Docker image, different entrypoint arguments.

**The codec for custom plan nodes.** Ballista's serialisation handles every built-in DataFusion plan node. It doesn't handle custom `ExecutionPlan` implementations -- because it doesn't know about them. SQE has `DistributedScanExec`, a custom plan node that replaces `IcebergScanExec` in the physical plan tree when distribution is active. This node needs to be serialisable too. That's where the `PhysicalExtensionCodec` comes in.


## The Protobuf Codec Deep Dive

DataFusion's protobuf serialisation is a two-layer system.

The first layer is `datafusion-proto`, which handles all built-in plan nodes. It converts a `FilterExec` to a protobuf `FilterExecNode`, a `ProjectionExec` to a `ProjectionExecNode`, and so on. This layer works out of the box. You call `physical_plan_to_bytes` and get protobuf bytes. You call `physical_plan_from_bytes` and get the plan back. Round-trip fidelity for built-in nodes is guaranteed by DataFusion's test suite.

The second layer is the `PhysicalExtensionCodec` trait. This is the escape hatch. When `datafusion-proto` encounters a plan node it doesn't recognise, it calls `try_encode` on the extension codec. When deserialising, it calls `try_decode`. This is how custom plan nodes participate in protobuf serialisation.

SQE's codec is `SqePhysicalCodec`:

```rust
#[derive(Debug, Default)]
pub struct SqePhysicalCodec;

impl PhysicalExtensionCodec for SqePhysicalCodec {
    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> DFResult<()> {
        if let Some(scan) =
            node.as_any().downcast_ref::<DistributedScanExec>()
        {
            let proto_schema: protobuf::Schema =
                scan.schema().as_ref().try_into().map_err(|e| {
                    DataFusionError::External(Box::new(
                        std::io::Error::other(
                            format!("Schema encoding failed: {e}")
                        ),
                    ))
                })?;

            let schema_bytes = proto_schema.encode_to_vec();
            let schema_proto_b64 = base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                &schema_bytes,
            );

            let encoded = EncodedDistributedScan {
                scan_tasks: scan.scan_tasks().to_vec(),
                worker_urls: scan.worker_urls().to_vec(),
                schema_proto_b64,
            };

            let json = serde_json::to_vec(&encoded)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            buf.extend_from_slice(&json);
            Ok(())
        } else {
            Err(DataFusionError::NotImplemented(format!(
                "SqePhysicalCodec: cannot encode '{}'",
                node.name()
            )))
        }
    }

    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let encoded: EncodedDistributedScan =
            serde_json::from_slice(buf)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Decode schema from base64-encoded protobuf
        let schema_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            &encoded.schema_proto_b64,
        ).map_err(|e| DataFusionError::External(
            Box::new(std::io::Error::other(
                format!("Schema base64 decode failed: {e}")
            ))
        ))?;

        let proto_schema =
            protobuf::Schema::decode(schema_bytes.as_slice())
                .map_err(|e| DataFusionError::External(
                    Box::new(std::io::Error::other(
                        format!("Schema proto decode failed: {e}")
                    ))
                ))?;

        let schema = Schema::try_from(&proto_schema)
            .map_err(|e| DataFusionError::External(
                Box::new(std::io::Error::other(
                    format!("Schema conversion failed: {e}")
                ))
            ))?;

        Ok(Arc::new(DistributedScanExec::new(
            encoded.scan_tasks,
            encoded.worker_urls,
            Arc::new(schema),
        )))
    }
}
```

Notice the mixed encoding strategy. The Arrow schema is serialised using DataFusion's protobuf schema codec -- the same one used for built-in plan nodes. The scan tasks and worker URLs are serialised using JSON via serde. The protobuf bytes are then base64-encoded so they can live inside the JSON payload.

Is this elegant? No. It's practical. The schema needs protobuf because Arrow schemas have complex nested types (maps, structs, lists of lists) that serde_json handles poorly. The scan tasks are flat structs with strings and booleans -- JSON handles them fine. Mixing the two encodings in one codec is ugly but correct, and the round-trip test proves it:

```rust
#[test]
fn test_roundtrip_distributed_scan_exec() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let original = Arc::new(DistributedScanExec::new(
        vec![make_task("f1"), make_task("f2")],
        vec!["http://w1:50052".to_string(),
             "http://w2:50052".to_string()],
        schema.clone(),
    ));

    let codec = SqePhysicalCodec::new();
    let mut buf = Vec::new();
    codec
        .try_encode(original.clone() as Arc<dyn ExecutionPlan>,
                     &mut buf)
        .expect("encode failed");

    let ctx = Arc::new(TaskContext::default());
    let decoded = codec
        .try_decode(&buf, &[], &ctx)
        .expect("decode failed");

    let decoded_scan = decoded
        .as_any()
        .downcast_ref::<DistributedScanExec>()
        .expect("Expected DistributedScanExec");

    assert_eq!(decoded_scan.scan_tasks().len(), 2);
    assert_eq!(decoded_scan.worker_urls(),
               &["http://w1:50052", "http://w2:50052"]);
    assert_eq!(*decoded_scan.schema(), *schema);
}
```

This test exists because we learned the hard way that it needed to.

::: {.datafusion}
**DataFusion deep dive:** The `PhysicalExtensionCodec` trait is DataFusion's official extension point for plan serialisation. When `datafusion-proto` serialises a physical plan and encounters a node that isn't in its built-in registry, it calls `try_encode` on whatever extension codec was provided. If no codec is registered, serialisation fails. If the codec returns `NotImplemented`, serialisation fails. The extension codec must handle every custom plan node in your engine, or the plan can't leave the coordinator. This is the contract: if you add a custom `ExecutionPlan`, you must teach the codec how to serialise it.
:::


## The Round-Trip Fidelity Challenge

Serialising a plan and deserialising it sounds straightforward. It's not.

The plan that arrives at the worker must be functionally identical to the plan that left the coordinator. Not structurally identical -- the Arc pointers will be different, the memory addresses will be different. But functionally identical: same schema, same partitioning, same data files, same projection.

The first version of our codec serialised scan tasks but didn't serialise the Arrow schema. We assumed the worker would infer the schema from the Parquet files it was reading. This worked for simple queries. It broke for queries with column projection.

The problem: when the coordinator plans a `SELECT id, name FROM users`, the physical plan's schema has two columns. But the Parquet file has twenty columns. The worker reads all twenty, because the projected schema was lost during serialisation. The coordinator then receives batches with twenty columns when it expects two. The downstream ProjectionExec fails.

The fix was serialising the schema as part of the codec payload. Four lines of encode, four lines of decode, and a base64 wrapper because protobuf bytes inside JSON need encoding. The round-trip test caught this in CI within a day of the codec being written. The lesson: round-trip tests for custom codecs aren't optional. They're the only thing standing between you and silent data corruption.

::: {.fieldreport}
**Field report:** The schema projection bug manifested as a `SchemaError` deep inside DataFusion's `ProjectionExec`. The error message said "column index 2 out of bounds for schema with 2 columns." The actual problem was 18 columns upstream where the schema had been lost during serialisation. We spent an afternoon tracing the error before adding the schema to the codec payload. The fix was small. The time to find it was not. After that, we wrote the round-trip test. Every codec change since has been validated by that test before it leaves the developer's machine.
:::


## Worker Registration and Heartbeat

Ballista's worker registration is a gRPC protocol between the scheduler and executors. Executors register on startup, send periodic heartbeats, and the scheduler removes them after a configurable number of missed heartbeats.

SQE's worker registration follows the same pattern with one difference: it's built on Arrow Flight rather than a custom gRPC protocol. Workers send heartbeats by calling `do_action("heartbeat")` on the coordinator's Flight service. The action body contains the worker's own URL so the coordinator knows who sent the heartbeat.

```rust
pub fn start_heartbeat_task(
    coordinator_url: String,
    worker_url: String,
    interval: Duration,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // First tick completes immediately; consume it so the first
        // real heartbeat fires after one full interval.
        ticker.tick().await;

        loop {
            ticker.tick().await;
            if let Err(e) =
                send_heartbeat(&coordinator_url, &worker_url).await
            {
                warn!(
                    coordinator = %coordinator_url,
                    error = %e,
                    "Heartbeat to coordinator failed"
                );
            }
        }
    });
}
```

The heartbeat is a fire-and-forget loop. No exponential backoff. No retry logic. If the coordinator is down, the heartbeat fails, the worker logs a warning, and tries again next interval. The coordinator's `WorkerRegistry` tolerates a configurable number of consecutive misses (three, by default) before marking a worker unhealthy.

```rust
pub async fn mark_failed(&self, url: &str) {
    let mut inner = self.inner.write().await;
    if let Some(state) = inner.workers.get_mut(url) {
        state.consecutive_failures += 1;
        if state.consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
            if state.healthy {
                warn!(
                    worker = url,
                    failures = state.consecutive_failures,
                    "Worker marked unhealthy"
                );
            }
            state.healthy = false;
        }
    }
}
```

Three misses is generous. With a 5-second heartbeat interval, a worker has 15 seconds of silence before it's removed from the pool. This handles brief network blips and GC pauses without triggering unnecessary failovers.

Recovery is instant. A single successful heartbeat resets the failure counter and marks the worker healthy:

```rust
pub async fn register_heartbeat(&self, url: &str) {
    let mut inner = self.inner.write().await;
    let state = inner.workers
        .entry(url.to_string())
        .or_insert_with(|| {
            info!(worker = url,
                  "Discovered new worker via heartbeat");
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

Notice that `register_heartbeat` uses `or_insert_with`. Workers that weren't in the initial configuration are automatically discovered when they start sending heartbeats. This means you can add workers to a running cluster without reconfiguring the coordinator. Scale out by starting a new worker process pointed at the coordinator's URL. The first heartbeat registers it. The next scheduling round includes it.

This is simpler than Ballista's registration protocol, which requires executors to explicitly register with the scheduler before they're eligible for work. SQE's approach is closer to service discovery -- if you're sending heartbeats, you exist.

The `WorkerRegistry` also supports immediate unhealthy marking, separate from the heartbeat-based failure threshold:

```rust
pub async fn mark_unhealthy(&self, url: &str) {
    let mut inner = self.inner.write().await;
    if let Some(state) = inner.workers.get_mut(url) {
        state.healthy = false;
        state.consecutive_failures = MAX_CONSECUTIVE_FAILURES;
    }
}
```

This is called when a worker fails *during query execution* -- a connection refused, a timeout, a transport error. A missed heartbeat is a soft signal. A failed scan dispatch is a hard signal. The worker is pulled from the pool immediately, and the fragment is retried on a different worker.

The two-tier health model -- gradual degradation for heartbeat misses, immediate removal for execution failures -- is the kind of nuance that doesn't appear in architecture diagrams. It matters in production. A worker that's restarting will miss a heartbeat or two but come back. A worker that returns connection refused during a scan is gone, and any fragments assigned to it need to be moved immediately.


## Credentials That Outlive the Scan

The ScanTask carries S3 credentials vended by Polaris. Those credentials have a lifetime -- typically an hour for STS session tokens. Most scans finish well within that window. But a query scanning hundreds of Parquet files across a large partition, on a worker that's also handling other queries, can take longer than expected.

When credentials expire mid-scan, the worker's S3 reads start returning `403 Forbidden`. The scan fails. The query fails. The user retries and gets a fresh token, but the experience is poor.

SQE handles this with a `CredentialRefreshTracker` on the coordinator side. When the coordinator dispatches scan fragments, it registers each fragment's credential expiry time. A background loop monitors these expiry times. When credentials are within five minutes of expiring, the coordinator refreshes them from Polaris and pushes the new credentials to the worker via `do_action("refresh_credentials")`.

```rust
/// Refreshed S3 credential payload pushed from coordinator to worker.
#[derive(Clone, Serialize, Deserialize)]
pub struct RefreshableCredentials {
    pub fragment_id: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub expiry: DateTime<Utc>,
}
```

On the worker side, the executor checks for refreshed credentials *between files* -- not between batches, not between row groups, but at each file boundary within the scan. A `tokio::sync::watch` channel carries the refresh from the Flight service handler to the running scan. The executor checks `has_changed()` before opening each new Parquet file, and rebuilds the S3 object store with the fresh credentials if a refresh arrived.

This is Ballista's execution model meeting SQE's auth model. Ballista doesn't need credential refresh because Ballista's workers use ambient credentials that don't expire. SQE's workers use vended, short-lived credentials that can expire mid-query. The coordinator-to-worker credential push is the mechanism that makes short-lived tokens work across long-running distributed scans. Ballista couldn't teach us this part.


## The Plan Surgery

The most delicate part of the distribution pipeline isn't the codec, the scheduler, or the heartbeat. It's the plan replacement.

When the coordinator decides to distribute a query, it has a physical plan tree that looks something like this:

```
ProjectionExec
  FilterExec (WHERE clause)
    IcebergScanExec (reads Parquet files from S3)
```

The `IcebergScanExec` is SQE's local scan node. It knows how to read Parquet files directly from S3 using the object store. For distributed execution, we need to replace it with a `DistributedScanExec` that fans out to workers instead.

But we can't just swap the root of the plan. The ProjectionExec and FilterExec need to stay. The aggregation nodes above them (if it's a `SELECT COUNT(*)`) need to stay. Only the leaf scan node changes.

The `try_distribute` method in `QueryHandler` does this in twelve steps. The critical ones:

1. Find the `IcebergScanExec` leaf in the plan tree.
2. Extract the list of data file paths from it.
3. Split the files across available workers.
4. Build `ScanTask` structs with credentials.
5. Schedule tasks to workers using the weighted scheduler.
6. Build a `DistributedScanExec` with the tasks and worker URLs.
7. Replace the `IcebergScanExec` leaf in the plan tree with the new `DistributedScanExec`.

Step 7 is the surgery:

```rust
fn replace_scan_in_plan(
    plan: &Arc<dyn ExecutionPlan>,
    target: &Arc<dyn ExecutionPlan>,
    replacement: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    if Arc::ptr_eq(plan, target) {
        return replacement;
    }

    let children = plan.children();
    if children.is_empty() {
        return Arc::clone(plan);
    }

    let new_children: Vec<Arc<dyn ExecutionPlan>> = children
        .iter()
        .map(|child| replace_scan_in_plan(
            child, target, Arc::clone(&replacement)
        ))
        .collect();

    let changed = new_children
        .iter()
        .zip(children.iter())
        .any(|(new, old)| !Arc::ptr_eq(new, old));

    if changed {
        plan.clone()
            .with_new_children(new_children)
            .unwrap_or_else(|_| Arc::clone(plan))
    } else {
        Arc::clone(plan)
    }
}
```

This walks the plan tree recursively. When it finds the target node (identified by `Arc` pointer equality), it returns the replacement. Every ancestor node is rebuilt via `with_new_children()` so the new leaf is properly wired into the tree.

The first version of this code didn't replace the leaf. It replaced the *entire plan* with a `DistributedScanExec`, discarding the filter and projection nodes above the scan. Queries returned wrong results -- all rows instead of filtered rows, all columns instead of projected columns. The fix was commit `3a123ea`: "fix: replace scan leaf in plan tree instead of replacing entire plan."

Three lines of logic in a recursive function. Two days of debugging wrong query results to get there.

The distribution also has guardrails. Not every query gets distributed:

```rust
// Skip if no worker registry (single-node mode)
// Skip if no healthy workers
// Skip if no IcebergScanExec in the plan
// Skip if total file count < number of workers
```

That last check is important. Distributing a single-file scan across two workers means one worker gets a file and the other gets nothing. The overhead of the Flight round-trip exceeds the benefit. The threshold is simple: if there are fewer files than workers, execute locally.


## The Distributed Compose

Seeing the pieces work together requires seeing them deployed. The distributed docker-compose file is short enough to include:

```yaml
services:
  coordinator:
    build: .
    entrypoint: ["sqe-server", "--config", "/config/coordinator.toml"]
    ports:
      - "60051:50051"   # Flight SQL
      - "28080:8080"    # Trino HTTP
      - "29090:9090"    # Prometheus metrics
    depends_on:
      polaris:
        condition: service_healthy

  worker-1:
    build: .
    entrypoint: ["sqe-worker", "/config/worker.toml"]
    ports:
      - "60061:50052"
    depends_on:
      - coordinator

  worker-2:
    build: .
    entrypoint: ["sqe-worker", "/config/worker.toml"]
    ports:
      - "60062:50052"
    depends_on:
      - coordinator
```

Same Docker image for coordinator and workers. Different entrypoint, different config file. The workers start after the coordinator, begin sending heartbeats, and within one interval they appear in the worker registry. The next query that hits the coordinator will consider them for distribution.

Adding a third worker is one more service block in the compose file. No coordinator reconfiguration needed.


## The Cost of the Fork

Every architectural shortcut has a maintenance cost. Here's what standing on Ballista's shoulders costs us.

**Dependency tracking.** We depend on `datafusion-proto`, which tracks DataFusion's version. When DataFusion releases a new version with new plan nodes or changed protobuf schemas, `datafusion-proto` updates. We update with it. If we'd built our own protobuf schema, we'd have to maintain parity manually. By depending on the official crate, we get this for free.

**Limited distribution scope.** Ballista supports multi-stage execution: hash shuffles, sort-merge joins distributed across workers, repartitioning between stages. SQE distributes scan work only. The coordinator handles joins, aggregations, and sorts locally. This is fine for our workload (analytical queries over partitioned Iceberg tables where the scan is the bottleneck), but it means SQE won't outperform a single node on shuffle-heavy queries.

**No work stealing.** Ballista's scheduler can detect when one worker finishes early and reassign pending work to it. SQE's scheduler assigns all fragments upfront and waits. If one worker is slower than the rest, the query is bottlenecked on that worker. The weighted scheduler mitigates this by front-loading heavy tasks, but it doesn't eliminate the problem.

**No speculative execution.** Spark and Ballista can speculatively launch a copy of a slow task on a different worker, using whichever result arrives first. SQE retries only on failure, not on slowness. Adding speculative execution would require tracking task progress, which we don't currently do.

These are deliberate trade-offs. The scanner-only distribution model is simpler to reason about, simpler to debug, and simpler to operate. When a query is slow, you look at the scan tasks. When a worker is overloaded, you look at the scheduler. There's no shuffle stage to investigate, no repartitioning to tune.

| Capability | Ballista | SQE |
|---|---|---|
| Plan serialisation | Full physical plan protobuf | datafusion-proto + custom extension codec |
| Distribution model | Multi-stage with shuffles | Scan-parallel only |
| Scheduler | Stage-aware, work-stealing | Weighted bin-packing |
| Worker discovery | Explicit registration | Heartbeat-based auto-discovery |
| Authentication | None | Bearer token passthrough per fragment |
| Credential lifecycle | Static / ambient | Vended, short-lived, refreshable mid-scan |
| Configuration | CLI flags + env vars | TOML config with env overlay |
| Custom plan nodes | Via extension codec | Via extension codec (same mechanism) |


## What Ballista Taught Us

The most important lesson from studying Ballista wasn't technical. It was architectural.

Ballista proves that DataFusion's extensibility model works. The `PhysicalExtensionCodec` trait, the `ExecutionPlan` trait, the `with_new_children` method -- these aren't theoretical extension points. They're production extension points that a real distributed execution framework uses daily.

When we built `DistributedScanExec`, we implemented `ExecutionPlan`. When we needed to serialise it, we implemented `PhysicalExtensionCodec`. When we needed to splice it into an existing plan tree, we used `with_new_children`. Every time, the trait design guided us to the right answer.

The second lesson: start with the simplest distribution model that solves your problem. Ballista's multi-stage execution is powerful, but it's complex. SQE's scan-parallel model handles the workloads we have today. If we need shuffle joins across workers tomorrow, we can add a stage to the scheduler. We don't need to redesign the whole system.

The third lesson: the protobuf round-trip is the contract. If a plan survives serialisation and deserialisation with the same semantics, the distributed system works. If it doesn't, nothing else matters. The round-trip test is the most important test in the distributed execution layer.

The fourth lesson is about knowing when heritage becomes baggage. Ballista gave us patterns and confidence that distributing DataFusion plans was feasible. But the moment we tried to use Ballista's scheduler with our auth model, it went from heritage to obstacle. The sign that you've crossed that line is when you spend more time working around the inherited code than working with it. We hit that point in two days. Two days was the right time to stop.


## Looking Forward

The scan-parallel model has a ceiling. When SQE needs to distribute a hash join across workers -- when the join inputs are too large to fit on a single coordinator -- we'll need to add shuffle stages. That means building a hash-repartitioning plan node, teaching the codec to serialise it, and teaching the scheduler to chain stages.

That's future work. It's also future work that's made easier by the foundation we've built. The `FragmentScheduler` trait is pluggable. The `SqePhysicalCodec` can handle additional plan nodes. The `WorkerRegistry` already knows which workers are healthy and how loaded they are.

The hard part of distributed execution isn't the execution itself. It's the plumbing: serialisation, health monitoring, credential management, plan manipulation. We built that plumbing in twelve days, because Ballista showed us which pipes to lay.

::: {.ailog}
**AI Logbook:** The human decided to fork Ballista's ideas rather than use it as-is or build from scratch. The AI implemented the `SqePhysicalCodec`, the `WeightedScheduler`, and the heartbeat protocol in three passes. The plan surgery function — `replace_scan_in_plan`, replacing a scan leaf in a tree while preserving all ancestor nodes — took the AI four attempts before the recursive traversal was correct. The first version replaced the entire plan; the fix was commit `3a123ea`.
:::

Next chapter, we'll walk through the coordinator and worker in detail -- how a query enters the coordinator, gets split into fragments, dispatched to workers, executed with the user's credentials, and reassembled into a result stream that the client receives as if the query ran locally.
