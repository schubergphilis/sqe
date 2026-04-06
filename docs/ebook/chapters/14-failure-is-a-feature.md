# Failure Is a Feature {#sec:failure}

> The question is not whether workers will fail.
> The question is what happens to the query when they do.

Distributed execution worked. Chapter 13 ends with queries being split across coordinator and workers, results streaming back, correct answers. We ran TPC-H at scale factor 0.01 across two workers and the numbers matched. The architecture was sound.

Then we ran the load test.

Fifty concurrent clients. Mixed workload -- scans, aggregations, joins. The kind of test you write when you want to find out what breaks before your users do. Everything broke.


## The Testing Infrastructure

Before we talk about what failed, let's talk about how we tested.

The distributed stack runs in Docker Compose. Two files layered on top of each other: `docker-compose.test.yml` provides Polaris (in-memory mode) and RustFS (an S3-compatible store), while `docker-compose.distributed.yml` adds the coordinator and two workers.

```yaml
# docker-compose.distributed.yml (abbreviated)
services:
  coordinator:
    build: .
    entrypoint: ["sqe-server", "--config", "/config/coordinator.toml"]
    ports:
      - "60051:50051"   # Flight SQL
      - "28080:8080"    # Trino HTTP
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

Four containers total. Polaris, RustFS, one coordinator, two workers. The whole stack on a laptop. Not production-representative for performance, but perfectly representative for failure modes -- the network boundaries, the gRPC connections, the S3 calls are all real.

The load test script itself is worth examining because the way you write a test determines what you find. We built `scripts/concurrent-test.sh` -- a bash script that spawns N parallel clients, each firing a SQL query through the CLI, then collects timing and success/failure data.

```bash
#!/usr/bin/env bash
set -euo pipefail

NUM_CLIENTS="${1:-10}"
MODE="${2:-mixed}"       # mixed | heavy | light

# Define query sets
LIGHT_QUERIES=(
    "SELECT COUNT(*) FROM test_warehouse.default.big"
    "SELECT 1"
    "SELECT MIN(amount), MAX(amount) FROM test_warehouse.default.big"
    "SELECT * FROM system.runtime.nodes"
)

HEAVY_QUERIES=(
    "SELECT COUNT(*), SUM(amount), AVG(amount) FROM test_warehouse.default.big"
    "SELECT SUBSTRING(name,1,5) AS p, COUNT(*) AS c,
            ROUND(AVG(amount),2) AS a
     FROM test_warehouse.default.big GROUP BY 1 ORDER BY c DESC"
    "SELECT name, amount, RANK() OVER (ORDER BY amount DESC) AS rnk
     FROM test_warehouse.default.big WHERE amount > 800 LIMIT 20"
)
```

The script creates a 200K-row test table across two Parquet files (one INSERT to create the first file, another INSERT to create the second -- giving the distributed scheduler something to split). Then it launches clients in parallel using bash background processes:

```bash
for i in $(seq 1 "$NUM_CLIENTS"); do
    (
        idx=$(( (i - 1) % NUM_QUERIES ))
        sql="${QUERIES[$idx]}"
        START=$(python3 -c "import time; print(int(time.time()*1000))")

        OUTPUT=$("$CLI" --host "$SQE_HOST" --port "$SQE_PORT" \
            --user root --protocol flight -e "$sql" 2>&1)
        EXIT_CODE=$?

        END=$(python3 -c "import time; print(int(time.time()*1000))")
        ELAPSED=$((END - START))

        if [ $EXIT_CODE -eq 0 ]; then
            echo "OK ${ELAPSED}ms" > "$RESULTS_DIR/client-$i.txt"
        else
            echo "FAIL ${ELAPSED}ms" > "$RESULTS_DIR/client-$i.txt"
        fi
    ) &
done
wait
```

Each client writes its result to a file. After all clients finish, the script aggregates pass/fail counts, min/avg/max latency, and throughput in queries per second. It also queries `system.runtime.tasks` to show how fragments were distributed across workers.

After all clients finish, the script prints a summary and then queries the system tables to show operational state:

```bash
echo "  Worker load distribution:"
run_sql "SELECT node_id, COUNT(*) AS fragments,
         SUM(output_rows) AS total_rows
         FROM system.runtime.tasks
         GROUP BY node_id ORDER BY fragments DESC"
```

This is the output that tells you whether distribution actually distributed. If one worker handled 90% of the fragments, you have a scheduling problem. If both workers handled roughly equal fragments but one was three times slower, you have a resource problem. The load test script produces the numbers; reading them is the engineering.

The test started at 10 concurrent clients. All passed. We bumped to 20. Some started failing intermittently. At 50, nothing worked reliably. The failure modes were diverse -- not one thing broke, a dozen things broke simultaneously, each masking the others. That's the nature of concurrent failure: you can't debug one problem at a time because the symptoms overlap.


## What Broke (In Order of Discovery)

### The gRPC hang

After about 30 queries, clients started hanging. No timeout, no error, just silence. The coordinator was alive. The workers were alive. The gRPC connection was technically open. Nothing was moving.

We spent four hours adding step-by-step tracing through the Flight SQL client to find the hang point. The AI generated the tracing instrumentation -- a wrapper around every Flight call that logged entry, exit, and elapsed time. The bench client's `execute` method became a breadcrumb trail:

```rust
let debug = std::env::var("BENCH_DEBUG").is_ok();
if debug { eprintln!("[flight] get_flight_info..."); }
let flight_info = client
    .execute(sql.to_string(), None)
    .await
    .map_err(|e| anyhow::anyhow!("Query failed: {e}"))?;
if debug { eprintln!("[flight] got {} endpoints", flight_info.endpoint.len()); }

for (i, endpoint) in flight_info.endpoint.iter().enumerate() {
    let ticket = endpoint.ticket.clone()
        .ok_or_else(|| anyhow::anyhow!("Flight endpoint returned no ticket"))?;

    if debug { eprintln!("[flight] do_get endpoint {i}..."); }
    let stream = client.do_get(ticket).await
        .map_err(|e| anyhow::anyhow!("do_get failed: {e}"))?;

    if debug { eprintln!("[flight] collecting batches from endpoint {i}..."); }
    let endpoint_batches: Vec<RecordBatch> = stream.try_collect().await?;
    if debug {
        eprintln!("[flight] got {} batches from endpoint {i}",
            endpoint_batches.len());
    }
}
```

Not sophisticated. Not clever. But effective. With `BENCH_DEBUG=1` set, the output showed query after query printing `[flight] get_flight_info...` and then... nothing. The `execute()` call never returned. That narrowed it to the gRPC layer, not the query execution layer.

The root cause was HTTP/2 stream accumulation. Our Flight SQL client reused a single gRPC connection across queries. HTTP/2 multiplexes streams on one connection, but each stream consumes a stream ID. After approximately 30 queries, the accumulated streams made the connection unresponsive. Not dead -- unresponsive. No error, no timeout, just a connection that would never produce another byte.

The insidious part is that HTTP/2 has a maximum stream ID limit (2^31 - 1, roughly 2.1 billion), so the issue wasn't stream ID exhaustion. It was something subtler -- the accumulated state of completed-but-not-fully-closed streams creating back-pressure in the h2 frame codec. The connection appeared healthy by every metric. It just stopped doing work.

The fix was architectural: create a fresh gRPC connection per query.

```rust
/// Flight SQL benchmark client.
///
/// Creates a fresh gRPC connection per query to avoid HTTP/2 stream
/// accumulation issues on long-running benchmark sessions.
pub struct FlightSqlBenchClient {
    host: String,
    token: Option<String>,
}

impl FlightSqlBenchClient {
    /// Create a fresh FlightSqlServiceClient with the stored token.
    async fn new_client(&self) -> anyhow::Result<FlightSqlServiceClient<Channel>> {
        let channel = build_channel(&self.host).await?;
        let mut client = FlightSqlServiceClient::new(channel);
        if let Some(ref token) = self.token {
            client.set_token(token.clone());
        }
        Ok(client)
    }
}
```

The client stores the auth token from the initial handshake, but creates a fresh `Channel` for each query. The `build_channel` function configures keepalive and timeouts as defense-in-depth:

```rust
async fn build_channel(host: &str) -> anyhow::Result<Channel> {
    let channel = Channel::from_shared(url)?
        .keep_alive_while_idle(true)
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(20))
        .timeout(Duration::from_secs(300))
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await?;
    Ok(channel)
}
```

One connection per query is more expensive than connection reuse. We measured. The overhead is about 1-2ms per query. For a system where queries take hundreds of milliseconds to seconds, that's noise. For a system where connection reuse silently hangs after 30 queries, it's the only correct answer.

### The empty result schema

Some queries legitimately return zero rows. Our Flight SQL server sent `Schema::empty()` for these -- a schema with no columns. Clients that expected the query's schema (with column names and types) got confused. Some crashed. Some returned garbage column headers.

The comment in the code tells the story concisely:

```rust
fn batches_to_stream(
    batches: Vec<RecordBatch>,
) -> Result<Response<FlightStream>, Status> {
    if batches.is_empty() {
        // Return an empty stream with a proper schema.
        // Using Schema::empty() here caused clients to hang because
        // get_flight_info sends the real query schema but do_get sent
        // a 0-column schema, confusing the FlightRecordBatchStream decoder.
        let stream = futures::stream::empty();
        let flight_stream: FlightStream = Box::pin(stream);
        return Ok(Response::new(flight_stream));
    }
    // ...
}
```

The fix: always return the query's actual schema, even when the result set is empty. The schema describes the *shape* of the answer, not whether there is one. This seems obvious in retrospect. But the initial code took a shortcut -- "no results, so send an empty schema" -- and the shortcut broke clients that correctly implement the Flight SQL spec. The spec says `get_flight_info` returns the schema; `do_get` returns data matching that schema. Sending a different schema on `do_get` is a protocol violation, even if the data is empty.

### The S3 throttle that looked like a network failure

With 50 concurrent clients scanning Parquet files, S3 started returning 503 SlowDown responses. Our error handling treated any non-200 response as a fatal error. From the query's perspective, storage was unreachable.

We considered three approaches:

1. Retry with exponential backoff at the S3 client level
2. Retry at the fragment level (re-dispatch to a different worker)
3. Accept the throttle and let the query fail

We went with option 1 for reads (idempotent, safe to retry) and option 3 for writes (not idempotent without additional bookkeeping). This was a conscious trade-off: we accepted that under extreme concurrent load, some queries would be slower. We didn't accept that they would fail.

The distinction matters because it maps to our auth model. When 50 users query the same Iceberg table, each user's token gets vended separate STS credentials. Polaris calls S3 to verify each set. That's 50 credential-vending operations hitting the same S3 prefix for metadata. Even RustFS in a Docker container throttles under that load. Real S3 would throttle sooner -- the 503 SlowDown response is documented behavior for prefixes exceeding 3,500 PUT/COPY/POST/DELETE or 5,500 GET/HEAD requests per second.

### The timeout problem

Some queries just... took too long. Not because the computation was expensive, but because a stuck gRPC stream doesn't respect Rust's normal cancellation mechanisms. A `tokio::timeout` wrapping a `do_get` call doesn't help if the underlying HTTP/2 stream is wedged.

The fix was `tokio::select!` -- racing the query against a deadline:

```rust
tokio::select! {
    result = execute_query(&client, sql) => result,
    _ = tokio::time::sleep(Duration::from_secs(120)) => {
        Err(anyhow!("query timed out after 120s"))
    }
}
```

This cancels the future cleanly even if the gRPC stream is stuck. The connection gets dropped, the worker eventually notices, and resources are freed. The `tokio::select!` macro drops the losing future. In Rust, dropping a future cancels it -- the destructor runs, channels close, the runtime reclaims the task. This is one of those properties of async Rust that you appreciate most when you need it at 2am.

The coordinator's server process uses the same pattern for graceful shutdown:

```rust
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
    };
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => tracing::info!("Received SIGINT, shutting down"),
        _ = terminate => tracing::info!("Received SIGTERM, shutting down"),
    }
}
```

`tokio::select!` isn't just a timeout mechanism. It's the primitive for racing any set of futures. Shutdown signals, heartbeat timeouts, query deadlines -- they're all the same pattern: first one to complete wins, the rest get dropped.


## The Failure Taxonomy

After the load test, we wrote down every way the system can fail. This is the list we should have written *before* the load test:

| Failure | Symptom | Detection | Recovery |
|---------|---------|-----------|----------|
| Worker crash | Missing heartbeat | 3 missed heartbeats (15s) | Reassign fragments to other workers |
| Network partition | Heartbeat timeout | Same as crash | Same as crash (can't distinguish) |
| Slow worker | Fragment taking too long | Per-fragment deadline | Cancel and reassign |
| Coordinator crash | All clients disconnect | External health check | Restart; queries in flight are lost |
| S3 throttle | 503 SlowDown | Error code check | Exponential backoff (reads), fail (writes) |
| Token expiry mid-query | 401 from Polaris/S3 | Error code check | Credential refresh push |
| gRPC stream hang | No progress, no error | Deadline timer | Drop connection, retry query |


## Fragment Retry Semantics

Not all failures are equal, and not all operations are safe to retry.

**Scans are idempotent.** Re-reading Parquet files produces the same result. If a worker dies mid-scan, reassign the fragment to another worker. The only cost is time.

**Writes are not idempotent.** An INSERT fragment that writes Parquet files to S3 and then fails before committing to the Iceberg catalog has created orphan files. Retrying might create duplicates. We handle this with a two-phase approach: the worker writes files and reports their paths, but only the coordinator commits the Iceberg transaction. If the worker dies, the coordinator knows which files were reported and can either retry the fragment or abort the transaction.

**Retry budget:** each fragment gets two attempts with escalating timeouts before giving up. After exhausting retries, the system falls back to local execution on the coordinator. After that, the query fails with a diagnostic message listing what was tried. We considered making the budget configurable. We didn't, because two retries is enough for transient failures, and more retries won't fix persistent ones.

The retry logic in `DistributedScanExec` is the core of the recovery system. Here's the skeleton:

```rust
const DEFAULT_MAX_RETRIES: u32 = 2;

// Inside execute():
let mut last_error: Option<DataFusionError> = None;
let mut current_worker_url = initial_worker_url;
let mut failed_workers: Vec<String> = Vec::new();

for attempt in 0..=max_retries {
    if attempt > 0 {
        let delay = Duration::from_millis(50 * (1 << attempt.min(4)));
        tokio::time::sleep(delay).await;

        warn!(
            fragment_id = %task.fragment_id,
            attempt = attempt,
            worker = %current_worker_url,
            "Retrying fragment on different worker"
        );
    }

    match dispatch_to_worker(&task, &current_worker_url, &parent_cx).await {
        Ok(flight_stream) => {
            // Success — wrap the stream and return
            return Ok(wrapped_stream);
        }
        Err(e) => {
            // Mark worker unhealthy immediately
            if let Some(ref registry) = worker_registry {
                registry.mark_unhealthy(&current_worker_url).await;
            }
            failed_workers.push(current_worker_url.clone());
            last_error = Some(e);

            // Find another healthy worker for next attempt
            if let Some(ref registry) = worker_registry {
                let healthy = registry.healthy_workers().await;
                if let Some(next) = healthy.into_iter()
                    .find(|w| !failed_workers.contains(w))
                {
                    current_worker_url = next;
                    continue;
                }
            }
            break;  // No healthy workers left
        }
    }
}
```

Three design decisions are embedded in this code. First, the exponential backoff uses `50 * (1 << attempt)` milliseconds -- 100ms, 200ms, 400ms -- which is short by most standards. We're not backing off against a rate limit; we're waiting for a worker health state to change. A few hundred milliseconds is enough.

Second, `mark_unhealthy` is immediate. The health check system uses a three-strikes-and-out model for regular health probes, but an execution failure is a stronger signal. If a worker failed during a query, we don't give it two more chances. It's out of the pool now. A future heartbeat can bring it back.

```rust
pub async fn mark_unhealthy(&self, url: &str) {
    let mut inner = self.inner.write().await;
    if let Some(state) = inner.workers.get_mut(url) {
        if state.healthy {
            warn!(worker = url,
                "Worker marked unhealthy immediately (execution failure)");
        }
        state.healthy = false;
        state.consecutive_failures = MAX_CONSECUTIVE_FAILURES;
    }
}
```

Third, the `failed_workers` list prevents retry loops. If worker-1 fails, the retry goes to worker-2, not back to worker-1. This seems obvious but is easy to get wrong -- without the exclusion list, a two-worker cluster with one failed worker would retry on the same failed worker forever.

When all remote workers are exhausted, the system has one more option: local execution on the coordinator itself. The `local_executor` fallback runs the scan task using the coordinator's own DataFusion runtime. It's slower (no parallelism), but it means a single unhealthy worker doesn't kill a query.

```rust
// All remote attempts exhausted — try local fallback
if let Some(ref executor) = local_executor {
    warn!(
        fragment_id = %task.fragment_id,
        failed_workers = ?failed_workers,
        "All workers failed, falling back to local execution"
    );
    let local_stream = executor.execute_local(&task, schema)?;
    return Ok(wrapped_stream);
}
```


## Credential Refresh as Recovery

One failure mode deserves its own section because it's unique to our authentication model: token expiry mid-query.

In Chapter 4, we established that every query runs as the authenticated user. The coordinator passes the user's bearer token to Polaris, which vends short-lived STS credentials for S3 access. Those credentials are embedded in the scan task sent to workers. If a scan takes longer than the credential TTL, the S3 reads start failing with 403 Forbidden.

This is not a bug. It's a consequence of taking security seriously. Short-lived credentials are a feature. But they create a distributed coordination problem: the coordinator must refresh credentials and push them to workers before they expire.

The solution has three parts. On the coordinator side, a `CredentialRefreshTracker` monitors active fragments:

```rust
pub struct CredentialRefreshTracker {
    fragments: Arc<RwLock<HashMap<String, ActiveFragment>>>,
    refresh_buffer_secs: i64,  // default: 300 (5 minutes)
}

pub struct ActiveFragment {
    pub fragment_id: String,
    pub worker_url: String,
    pub credential_expiry: Option<DateTime<Utc>>,
}
```

A background task runs every 60 seconds, checking for credentials that will expire within five minutes. When it finds one, it obtains fresh credentials from Polaris and pushes them to the worker via an Arrow Flight `do_action("refresh_credentials")` call:

```rust
pub async fn push_credentials_to_worker(
    worker_url: &str,
    credentials: &RefreshableCredentials,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let body = serde_json::to_vec(credentials)?;

    let channel = Endpoint::new(worker_url.to_string())?
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
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

On the worker side, a `CredentialStore` manages per-fragment watch channels. The executor checks for new credentials before each Parquet file read:

```rust
// Inside execute_scan(), in the per-file loop:
if let Some(ref mut rx) = credential_rx {
    if rx.has_changed().unwrap_or(false) {
        let new_creds = rx.borrow_and_update().clone();
        if let Some(creds) = new_creds {
            info!(
                fragment_id = %task.fragment_id,
                expiry = %creds.expiry,
                "Applying refreshed credentials for next file read"
            );
            current_access_key = creds.access_key_id;
            current_secret_key = creds.secret_access_key;
            current_session_token = creds.session_token;

            // Rebuild the S3 object store with new credentials
            match build_object_store_with_creds(task, &current_access_key,
                    &current_secret_key, &current_session_token) {
                Ok(new_store) => { store = Arc::new(new_store); }
                Err(e) => {
                    warn!("Failed to rebuild object store, continuing \
                           with previous credentials");
                }
            }
        }
    }
}
```

The `tokio::sync::watch` channel is the right primitive here. It's a single-producer, multi-consumer channel where the receiver always sees the *latest* value. If two credential refreshes happen before the executor checks, it gets the newest one. No queue buildup, no ordering concerns. The `has_changed()` call is non-blocking -- the executor doesn't wait for credentials, it checks if new ones arrived.

This is push-based recovery. The coordinator doesn't wait for a 403 error from the worker. It proactively refreshes credentials before they expire. The five-minute buffer gives enough time for the push to propagate even if there's a brief network hiccup.

The graceful fallback in the executor is important: if `build_object_store_with_creds` fails with the new credentials (malformed, wrong scope, Polaris returned an error), the executor logs a warning and continues with the previous credentials. A failed refresh attempt doesn't kill a scan that's working. The old credentials may still be valid for another few minutes.

One subtlety: the `RefreshableCredentials` struct has a custom `Debug` implementation that redacts the secret key and session token. Tracing and error logs should never contain credentials. The AI generated the correct `Debug` impl on the first try -- redacting secrets in log output is a well-established pattern, and the model had seen enough examples to get it right.

```rust
impl std::fmt::Debug for RefreshableCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefreshableCredentials")
            .field("fragment_id", &self.fragment_id)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"[REDACTED]")
            .field("session_token", &"[REDACTED]")
            .field("expiry", &self.expiry)
            .finish()
    }
}
```


## Memory Management Failures

The load test also found memory issues. With 50 concurrent scans reading Parquet files, workers accumulated Arrow batches in memory without bound. On a Docker container with limited memory, this meant OOM kills. The container just vanished.

DataFusion has a memory management system. We weren't using it.

The fix was configuring `FairSpillPool` -- DataFusion's memory pool that divides available memory fairly across operators and triggers spill-to-disk when the limit is reached:

```rust
pub fn build_session_context(config: &WorkerConfig) -> anyhow::Result<SessionContext> {
    let memory_bytes = parse_memory_limit(&config.memory_limit)?;

    // FairSpillPool divides memory fairly among spillable operators
    // and triggers spill when the limit is reached.
    let memory_pool = Arc::new(FairSpillPool::new(memory_bytes));

    let mut builder = RuntimeEnvBuilder::new()
        .with_memory_pool(memory_pool);

    if config.spill_to_disk {
        builder = builder.with_temp_file_path(&config.spill_dir);
    } else {
        let disk_builder = DiskManagerBuilder::default()
            .with_mode(DiskManagerMode::Disabled);
        builder = builder.with_disk_manager_builder(disk_builder);
    }

    let runtime = Arc::new(builder.build()?);
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    Ok(ctx)
}
```

The default memory limit is 8GB per worker. The executor registers a `MemoryConsumer` for each scan and tracks batch allocations against the pool:

```rust
let pool = session_ctx.runtime_env().memory_pool.clone();
let consumer = MemoryConsumer::new(format!("scan:{}", task.fragment_id));
let mut reservation = consumer.register(&pool);

// After reading each Parquet file:
let batch_mem: usize = batches.iter()
    .map(|b| b.get_array_memory_size())
    .sum();
reservation.try_grow(batch_mem)?;
```

`try_grow` returns an error if the reservation would exceed the pool limit. That error propagates up as a query failure with a clear message: "Resources exhausted: Failed to allocate additional memory." The query fails, but the worker stays alive. Before this fix, the worker died.

The spill-to-disk option provides a second safety net. When memory pressure is high but disk is available, DataFusion spills intermediate results to `/tmp/sqe-spill`. This is slower than in-memory execution but prevents the hard failure. We enable it by default but make it configurable because some environments (containers with ephemeral storage) can't afford the disk space.


## Graceful Degradation: FairSpillPool and the Watermark Model

The OOM kill from the load test exposed a deeper problem than "add memory limits." The question is: what should happen when memory runs out? The answer depends on the operator.

### How Operators Cooperate on Memory

`FairSpillPool` is not a simple ceiling. It divides total memory equally among all registered `MemoryConsumer` instances. When operator A calls `try_grow` and the pool is above the orange watermark (75% utilization), the pool asks other spillable operators to spill first. If operator B (a sort with buffered runs) spills 200MB to disk, operator A's allocation succeeds without the pool hitting the red zone.

This cooperative model means operators do not need to know about each other. A hash aggregate and a sort running in parallel share the pool implicitly. When the aggregate grows, the sort may be asked to spill. When the sort is in its merge phase and releases memory, the aggregate can grow again. The pool mediates.

The watermark levels drive behavior:

| Level | Utilization | What Happens |
|-------|-------------|-------------|
| Green | < 60% | Allocations succeed without intervention |
| Yellow | 60-75% | Metrics increment; log warnings; no action forced |
| Orange | 75-90% | Pool asks spillable operators to spill before allowing new allocations |
| Red | > 90% | Admission control: new queries queue; existing queries continue |

The red zone prevents the worst failure mode: a cascade where every concurrent query spills simultaneously, saturating disk I/O and making all queries slow instead of just the large ones. By queuing new queries at the door, red-zone admission control preserves throughput for in-flight work.

### External Merge Sort

When a `SortExec` operator spills, it writes sorted runs to `spill_dir`. Each run is a file of Arrow IPC batches, sorted by the sort key and optionally compressed with zstd or lz4. The runs accumulate as the sort consumes its input.

On final output, the sort opens all runs simultaneously and performs a k-way merge using a binary heap keyed on the sort column. Each step of the merge reads one batch from the run with the smallest current key. Memory consumption during the merge is bounded: one batch buffer per run, plus the heap. For a 1TB sort with 512MB of memory, this produces roughly 2,000 runs of 512KB each (after compression). The merge reads 2,000 buffers of one batch each -- a few hundred megabytes total.

The k-way merge is the reason spill-to-disk works at all for large sorts. Without it, you would need to read all spilled data back into memory -- defeating the purpose. With it, memory stays bounded regardless of how much data was sorted.

### The q18 Story

TPC-H query 18 is the one that breaks on 512MB. It selects customers with large orders using a `GROUP BY` with `HAVING SUM(l_quantity) > 300`. The `GroupedHashAggregate` must maintain a hash table with one entry per group. At scale factor 1, this means hundreds of thousands of groups, each holding partial aggregate state. The hash table exceeds the memory limit.

Unlike `SortExec`, DataFusion's `GroupedHashAggregate` does not yet support spill-to-disk. When `try_grow` fails, the operator returns `ResourceExhausted` and the query fails. This is a known upstream limitation -- hash aggregate spill is tracked in the DataFusion issue tracker and is an active area of development.

SQE's Phase B solves q18 through two-phase aggregation. Instead of one coordinator computing all groups, each worker computes partial aggregates on its partition. The groups are hash-partitioned across workers via `DoExchange`, so each worker handles a subset of the total groups. The hash table per worker is 1/N the size (where N is the number of workers). With 8 workers, each hash table is roughly 1/8 the size -- well within the 512MB budget.

Two-phase aggregation does not eliminate the fundamental limitation. A single worker with a single-phase aggregate on a high-cardinality group-by will still exceed memory. But by distributing the groups, the per-worker memory requirement drops below the threshold. This is the same trick that MapReduce uses for large aggregations, applied at the query operator level.

For users running Phase A only (single-node), q18 at scale factor 1 requires increasing `memory_limit` above 512MB. At scale factor 0.1, it passes within 512MB. The relationship between scale factor, group cardinality, and memory requirement is roughly linear for hash aggregates.


## Heartbeat and Health

The worker registry is the coordinator's view of which workers are alive. Workers start unhealthy and become healthy when their first heartbeat arrives:

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

The heartbeat itself is an Arrow Flight `do_action("heartbeat")` call. We reuse the Flight protocol for heartbeats instead of adding a separate health check protocol. One protocol, one port, one set of TLS certificates. The body carries the worker's own URL so the coordinator knows which worker sent the heartbeat.

```rust
async fn send_heartbeat(
    coordinator_url: &str,
    worker_url: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channel = Endpoint::new(coordinator_url.to_string())?
        .connect()
        .await?;
    let mut client = FlightServiceClient::new(channel);

    let action = Action {
        r#type: "heartbeat".to_string(),
        body: bytes::Bytes::from(worker_url.to_string()),
    };
    client.do_action(tonic::Request::new(action)).await?;
    Ok(())
}
```

There's no exponential backoff on heartbeat failures. If the coordinator is unreachable, the worker logs a warning and tries again at the next interval. The coordinator's worker registry already tolerates three consecutive missed heartbeats before marking a worker unhealthy. Adding backoff on the worker side would make recovery slower -- a worker that backs off to 30-second intervals takes longer to re-register after a transient coordinator restart.

The `or_insert_with` call in `register_heartbeat` enables dynamic worker discovery. A worker not in the initial config list gets added to the registry on its first heartbeat. This means you can scale workers by starting new containers; the coordinator finds them automatically. In Kubernetes, a StatefulSet with `replicas: 5` creates five worker pods. Each starts, heartbeats the coordinator, and enters the pool. Scaling to 10 is a one-line change. No coordinator restart, no configuration update, no downtime.

The coordinator also runs a periodic health check that actively probes workers:

```rust
async fn check_all_workers(&self) {
    let urls: Vec<String> = {
        let inner = self.inner.read().await;
        inner.workers.keys().cloned().collect()
    };

    for url in urls {
        let result = Self::health_check_worker(&url).await;
        match result {
            Ok(()) => self.mark_healthy(&url).await,
            Err(e) => {
                debug!(worker = %url, error = %e, "Health check failed");
                self.mark_failed(&url).await;
            }
        }
    }
}
```

The health check uses the same Arrow Flight channel as everything else -- `do_action("health_check")`. If the worker can respond to a Flight action, it can execute a scan. If it can't, it shouldn't be in the pool. This is a stronger signal than a TCP port check or an HTTP health endpoint, because it exercises the full Flight server stack.


## What We Fixed and What We Accepted

The load test produced twelve distinct failure modes. We fixed eight:

- gRPC connection reuse -> fresh connection per query
- Empty schema -> return query schema always
- S3 throttle -> exponential backoff on reads
- Stuck streams -> `tokio::select!` with deadline
- Double-quoted identifiers -> fixed the table name qualifier
- Missing stream progress -> per-fragment heartbeat
- Token refresh during execution -> credential push to workers
- Memory accounting -> `MemoryPool` with per-query limits

We accepted four:

- S3 write throttle -> queries fail (retrying writes is unsafe without idempotency keys)
- Coordinator crash -> in-flight queries are lost (stateless coordinator by design)
- Very large shuffle -> single-worker bottleneck on final aggregation (acceptable for our workload)
- Cold worker startup -> first query on a new worker is slow (JIT compilation, cache warming)

::: {.deadend}
**Dead end: stateful coordinator failover.** We explored replicating coordinator session state
to a standby. The complexity was enormous -- distributed consensus for query state, exactly
what we were trying to avoid by building on DataFusion. We accepted coordinator crash as a
restart event. Queries fail. Clients reconnect. No data is lost. This was the right call.
:::


## Designing for Recovery

The load test taught us a design principle: recovery mechanisms are more valuable than prevention mechanisms.

You can't prevent a worker from crashing. You can't prevent S3 from throttling. You can't prevent a network partition. What you *can* do is design every component to recover from these events quickly and predictably.

Here are the patterns we implemented, drawn from the failures above:

**Fresh connections over connection pools.** Connection pools optimize for the common case (fast reuse) at the cost of the failure case (stuck connections are invisible). A fresh connection per query costs 1-2ms but guarantees you're not inheriting state from a previous failure. For long-lived processes running thousands of queries, this is the right trade-off.

**Watch channels for state propagation.** The `tokio::sync::watch` channel appears in two places: credential refresh and configuration updates. It's a broadcast primitive where consumers always see the latest value. The producer doesn't need to know how many consumers exist or whether they're ready. This decoupling makes the system resilient to timing issues -- if a credential refresh arrives while the executor is busy reading a file, the new credentials wait in the channel until the executor checks.

**Immediate health demotion, gradual health promotion.** The worker registry uses two different thresholds. An execution failure immediately marks a worker unhealthy (`mark_unhealthy`). A missed health check increments a counter; three misses trigger demotion (`mark_failed`). But recovery is always immediate: a single heartbeat promotes a worker back to healthy. This asymmetry is deliberate. A failed query is evidence that something is wrong *right now*. A missed health check might be a transient network blip. But a successful heartbeat is evidence that the worker is alive *right now*.

**Local fallback as last resort.** When all workers fail, the coordinator can execute the scan locally. This degrades performance -- the coordinator does its own work plus the worker's -- but it prevents total query failure. The system is slower, not broken. Users notice latency; they don't notice errors.

**Schema contracts, not schema inference.** The empty schema bug taught us that every interface must specify its schema explicitly. The Flight SQL protocol says `get_flight_info` returns the schema and `do_get` returns data matching it. We violated that contract by inferring "empty result means empty schema." The fix wasn't just returning the right schema -- it was committing to the principle that schemas are contracts, not metadata.

**Memory accounting before allocation.** The OOM kill happened because we allocated Arrow batches first and checked memory limits never. The `MemoryConsumer` pattern flips this: you reserve memory before use and fail fast if the reservation exceeds the limit. A failed reservation produces an error message. An OOM kill produces a dead container and a confused operator.

**Structured tracing for failure diagnosis.** Every recovery path in the system uses `tracing` structured fields -- `fragment_id`, `worker_url`, `attempt`, `elapsed_ms`. When a fragment fails on worker-1, retries on worker-2, and succeeds, the trace shows the full story. When it fails everywhere and falls back to local execution, the trace shows exactly which workers were tried and what errors each returned. Without structured tracing, debugging distributed failures is archaeology. With it, it's reading a log.

::: {.datafusion}
**DataFusion deep dive:** `FairSpillPool` is one of several memory pool implementations
in DataFusion. `GreedyMemoryPool` gives each operator as much memory as it asks for
until the pool is exhausted -- first come, first served. `FairSpillPool` divides memory
equally among registered consumers and triggers spill when any consumer exceeds its
share. We chose `FairSpillPool` because concurrent scans from multiple queries need
fair sharing. A single large scan shouldn't starve all other queries of memory.
:::


## When Every Error Looks the Same

We fixed the gRPC hang. We fixed the empty schema. We fixed the S3 throttle. And then a dbt run failed, and the error message was: `INTERNAL_ERROR: Query execution failed`.

Which query? What failed? Was it a syntax error in our model? A missing table in Polaris? An S3 credential that expired? A worker that crashed mid-scan? Every failure, regardless of cause, produced the same opaque string. dbt couldn't tell the difference between a user mistake and an infrastructure failure. Neither could we.

### The Problem with Uniform Opacity

When every error looks like `INTERNAL_ERROR(1) "Query execution failed"`, clients face an impossible retry decision. Should they retry? A syntax error will never succeed on retry. A transient S3 timeout will. But if the error code is always 1 and the message is always the same, the client has no signal to act on.

dbt's retry logic is error-code-aware when talking to Trino. It knows that `TABLE_NOT_FOUND` means something is wrong with the model, not with the infrastructure. It knows that `INTERNAL_ERROR` with code 65536 might be a transient execution failure worth retrying. But we were returning `INTERNAL_ERROR(1)` for everything, so dbt had no way to apply that logic.

A second problem was information leakage going the other direction. Some errors were too verbose. An S3 access failure might print the full bucket URL, the access key prefix, the STS endpoint. Polaris connectivity errors included hostnames and port numbers. None of that belongs in a client-facing error message. It belongs in the internal logs, where operators can see it.

We had two opposite problems: user errors were indistinguishable from system errors (too little signal), and system errors leaked infrastructure details (too much signal).

### The Solution: A 27-Code Taxonomy

We introduced `SqeErrorCode` — a typed enum with 27 variants covering every error category the engine can produce:

```rust
pub enum SqeErrorCode {
    // SQL parse / planning
    SyntaxError, ParseError, SemanticError, TypeMismatch,
    // Catalog / schema
    TableNotFound, ColumnNotFound, SchemaNotFound,
    CatalogNotFound, ViewNotFound,
    // Query building
    FunctionNotFound, InvalidArguments, DuplicateTable, DuplicateColumn,
    // Runtime
    DivisionByZero, InvalidCast,
    // Auth
    AuthenticationFailed, AccessDenied, SessionExpired,
    // Execution
    ExecutionFailed, QueryTimeout, QueryCancelled, ResourceExhausted,
    // Infrastructure
    CatalogError, StorageError, CommitConflict,
    // Feature support
    NotSupported,
    // Catch-all
    InternalError,
}
```

Each code carries three things: a gRPC status code, a Trino-compatible integer code and type string, and a client message policy.

The gRPC mapping is semantic. `TableNotFound` maps to `NOT_FOUND`. `SyntaxError` maps to `INVALID_ARGUMENT`. `AuthenticationFailed` maps to `UNAUTHENTICATED`. `StorageError` maps to `INTERNAL`. A client speaking Arrow Flight SQL can now dispatch on the gRPC status code alone, without parsing the message string.

The Trino mapping is for compatibility. `TableNotFound` is code 11, `TypeMismatch` is 7, `SyntaxError` is 1. dbt's Trino adapter recognises these numbers and adjusts its retry and error-surfacing behaviour accordingly. We're not Trino — but we speak enough of Trino's error vocabulary that existing tooling works.

The client message policy is the hardest part:

```rust
pub fn is_user_error(self) -> bool {
    matches!(self,
        SqeErrorCode::SyntaxError | SqeErrorCode::TableNotFound |
        SqeErrorCode::TypeMismatch | SqeErrorCode::AuthenticationFailed |
        // ... all user-actionable errors
    )
}
```

User errors — syntax errors, missing tables, auth failures, type mismatches — pass their detail through to the client. The message "table 'wh.ns.foo' not found" is useful; the user can act on it. System errors — storage failures, catalog connectivity, internal panics — return a generic message. "Storage operation failed" tells the user there's a problem. "s3://my-bucket/path?X-Amz-Security-Token=..." tells them too much.

### The Classifier

The error codes mean nothing without automatic classification. DataFusion errors arrive as strings. We had to map those strings to codes.

The classifier lives in two functions: `classify_execution_error` and `classify_catalog_error`. They pattern-match against lowercased error messages. Most patterns are straightforward: "table" + "not found" → `TableNotFound`, "division by zero" → `DivisionByZero`.

One pattern required care. DataFusion concatenates error messages when multiple failures are possible. A function call with wrong argument types produces something like: `"TypeSignatureClass(Exact([Int64, Int64])) does not match the function signature. No function matches the signature 'concat(Int64)'"`. That message contains both type signature information and "No function matches." If you check for `FunctionNotFound` before `TypeMismatch`, you classify it wrong. The comment in the code is explicit about this:

```rust
// TypeMismatch must be checked BEFORE FunctionNotFound because DataFusion
// concatenates both messages: "TypeSignatureClass... No function matches..."
if lower.contains("typesignatureclass") || lower.contains("type mismatch") {
    SqeErrorCode::TypeMismatch
} else if lower.contains("invalid function") || lower.contains("no function matches") {
    SqeErrorCode::FunctionNotFound
}
```

Order matters. The classifier is not a lookup table; it's a decision tree where earlier checks shadow later ones.

### Before and After

The difference is concrete. Here are three real failures, before and after the change.

Missing table, before:
```
gRPC status: INTERNAL (13)
Error code:  1
Message:     Query execution failed
```

Missing table, after:
```
gRPC status: NOT_FOUND (5)
Error code:  TABLE_NOT_FOUND (11)
Message:     table 'wh.ns.orders' not found
```

Type mismatch in a dbt model, before:
```
gRPC status: INTERNAL (13)
Error code:  1
Message:     Query execution failed
```

Type mismatch, after:
```
gRPC status: INVALID_ARGUMENT (3)
Error code:  TYPE_MISMATCH (7)
Message:     No function matches the signature 'concat(Int64)'
```

S3 storage failure, before:
```
gRPC status: INTERNAL (13)
Error code:  1
Message:     Storage backend error: s3://my-bucket/ns/orders/data-0001.parquet
             (credential: ASIA3EXAMPLE..., region: eu-west-1)
```

S3 storage failure, after:
```
gRPC status: INTERNAL (13)
Error code:  STORAGE_ERROR
Message:     Storage operation failed
```

The gRPC code for the storage failure didn't change — it's still `INTERNAL`. But the message no longer leaks the bucket path, the credential prefix, or the region. The internal logs still capture everything, attached to the query ID. The client gets a signal: something in the storage layer failed, and you should talk to your operator.

### Error Handling Is an API

The lesson is uncomfortable: we thought of error handling as an implementation detail. It turned out to be part of the contract with every client.

dbt trusts error codes to decide whether to fail a model or retry it. JDBC clients parse error codes to provide user-facing messages. Monitoring systems count error codes to build dashboards. We were returning `1` for everything, so all of that infrastructure was blind.

In a sovereign data platform, your error messages are part of your API. The moment you expose a query engine to external clients — even internal ones like dbt — you've committed to the semantics of your error responses. `TABLE_NOT_FOUND` is a promise: this query will never succeed until the table exists. `STORAGE_ERROR` is a different promise: the query might succeed if you try again, and it's not your fault.

Getting that classification right — user error versus system error, retryable versus not — is harder than it looks. It took us a load test, a dbt failure, and a taxonomy of 27 codes to get there. We should have built it in phase one.


## The Cost of Not Testing

We could have written the failure taxonomy before the load test. We could have implemented retry logic, credential refresh, and memory limits from the start. We didn't, because those features cost time, and we were building fast.

That was the right call for development velocity. And it was the wrong call for production readiness. The twelve failure modes we discovered in the load test would have been twelve production incidents. The four hours debugging the gRPC hang would have been four hours of downtime.

The load test took one day to write and three days to work through. The fixes touched every crate in the distributed stack -- `sqe-coordinator`, `sqe-worker`, `sqe-bench`, `sqe-cli`, `sqe-core`. The result is a system that handles failure as a normal operating condition, not as an exceptional event.

There is a temptation in engineering to call a system "done" when the happy path works. Distributed execution passing TPC-H queries felt like done. The load test proved it wasn't. The gap between "correct under ideal conditions" and "resilient under real conditions" is where most production incidents live.

We now run the concurrent test as part of our pre-merge checks. Ten clients, mixed mode. It catches regressions in connection handling, schema propagation, and memory management before they reach the distributed stack. It doesn't catch everything -- you'd need hundreds of concurrent clients to reproduce the S3 throttle -- but it catches the failures that appear first.

::: {.fieldreport}
**Field report:** The final load test run, after all fixes: 50 concurrent clients, mixed mode,
all 50 passed. Wall time 14.2 seconds. Per-query average 7.8 seconds (including two 200K-row
full table scans). Throughput 3.5 queries per second. Not fast by benchmarking standards. But
every single query returned the correct result. That's the point.
:::

::: {.ailog}
**AI Logbook:** The AI generated the step-by-step tracing instrumentation that diagnosed the gRPC stream accumulation hang — wrapping every Flight call with timing logs until the hang point was isolated. The human diagnosed the root cause (HTTP/2 stream state accumulation on a reused connection) from those logs. The `FlightSqlBenchClient` with fresh-connection-per-query, the `tokio::select!` deadline pattern, the fragment retry logic with `failed_workers` exclusion list, and the `FairSpillPool` memory management were all AI-implemented from failure descriptions the human wrote after the 50-client load test broke everything.
:::
