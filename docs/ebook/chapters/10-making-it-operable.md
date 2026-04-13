# Making It Operable {#sec:operations}

> A query engine that surprises you is a query engine nobody will trust.

The engine worked. Queries went in, Arrow batches came out, the numbers were correct. We had authentication, catalog integration, a write path, policy enforcement. By any reasonable definition, we had a functioning SQL engine.

We also had no idea what was happening inside it.

A query took 4.2 seconds. Was that slow? We didn't know. Where did the time go -- parsing, planning, scanning Parquet files, network transfer? We didn't know that either. A user reported that their dashboard queries were "sometimes slow." Sometimes. We couldn't reproduce it, couldn't measure it, couldn't even confirm it was real.

This is the gap between "it works" and "we can operate it." And it has a twin: the gap between "it runs on my machine" and "someone else can deploy it." Observability tells you what's happening. Configuration determines what *can* happen. Both are prerequisites for trust, and trust is what the teams running production workloads through your engine actually need.


## The Three Pillars, Applied

Everyone knows the three pillars of observability: metrics, traces, logs. The concept is not new. What's new is applying it to a distributed query engine where a single SQL statement might touch a coordinator, two workers, a REST catalog, and S3 -- all within the same second.

**Metrics** answer "how much" and "how fast." Counters and histograms, scraped by Prometheus, aggregated over time. They tell you the system's vital signs: queries per second, latency percentiles, error rates. They don't tell you why a specific query was slow.

**Traces** answer "where did the time go." OpenTelemetry spans, linked across process boundaries, showing the full timeline from SQL parse to Arrow batch delivery. They tell you that query Q spent 200ms in planning and 3.8 seconds waiting for S3. They don't tell you that 40% of your queries are hitting the same pattern.

**Logs** answer "what happened." Structured JSON, correlated with trace IDs, recording the facts: who ran what query, when, how long it took, whether it succeeded. They're the audit trail, the debugging breadcrumb, and the compliance record.

Each pillar is incomplete alone. Together, they give you the ability to answer any question about your system's behavior -- past, present, and (with good alerting) future.

We built all three in a single day. The implementation was straightforward. Getting the *right* metrics took considerably longer.


## What to Measure

The first version of our metrics was wrong. Not broken -- wrong. We measured what was easy to measure, not what mattered.

We started with a single counter: total queries executed. Then a latency histogram. Then error counts. Basic stuff, straight from every "how to add Prometheus to your Rust service" tutorial.

The problem became clear the first time we tried to debug a slow query report. We knew the p99 latency was 5.2 seconds. We didn't know whether that was parsing, planning, scanning, or network transfer. We knew 2% of queries failed. We didn't know whether they failed during authentication, catalog resolution, or execution.

The metrics registry we ended up with tells a different story:

```rust
pub struct MetricsRegistry {
    pub registry: Registry,
    pub query_count: CounterVec,       // labels: status, statement_type
    pub query_duration: HistogramVec,  // labels: statement_type
    pub rows_returned: Counter,
    pub active_sessions: IntGauge,
    pub healthy_workers: IntGauge,
    pub cache_hits: Counter,
    pub cache_misses: Counter,
    pub cache_invalidations: Counter,
    pub cache_size_bytes: Gauge,
    pub cache_entries: Gauge,
}
```

The `query_count` counter has two label dimensions: `status` (success or error) and `statement_type` (query, insert, ctas, show, explain). This single counter answers: "How many SELECT queries succeeded in the last hour? How many INSERTs failed?" Without the `statement_type` label, you're averaging reads and writes together, which hides everything useful.

The `query_duration` histogram uses custom bucket boundaries tuned for query engine latencies, not HTTP request latencies. A 10ms query is a metadata lookup. A 250ms query is a well-optimized scan. A 5-second query is a multi-table join. A 60-second query is either a big analytical scan or something is wrong. The default Prometheus buckets are heavily weighted toward sub-second values -- useless for distinguishing "normal slow" from "abnormal slow" in a SQL engine.

Workers have their own registry, tuned to what matters at the execution layer: fragments executed, rows scanned, bytes read, fragment duration. Fragment-level metrics are the only way to answer the question that matters most in a distributed engine: "Is this query slow because the work is genuinely large, or because one worker is struggling?" If all workers report similar fragment durations, the query is just big. If one worker's fragments take 10x longer, you have a hot spot.

DataFusion's `ExecutionPlan` trait exposes `metrics()` on every physical operator, returning per-operator row counts and elapsed compute time. We surface these through `EXPLAIN ANALYZE`, which executes the query and returns a tree of operators with actual row counts, elapsed milliseconds, and output sizes. This complements Prometheus metrics -- Prometheus shows system-level trends, `EXPLAIN ANALYZE` shows query-level detail. When an operator reports scanning 10 million rows but only 500 survive the filter, you know the predicate pushdown isn't reaching the Parquet reader.

Both registries implement a `HasRegistry` trait, which means the metrics server code is generic -- same endpoint shape, same scrape config for coordinator and worker. The coordinator runs Prometheus on port 9090, workers on 9091. In docker-compose these map to host ports 29090, 29091, 29092, all scrapable by a single Prometheus instance. The only difference is the prefix: coordinator metrics use `sqe_`, worker metrics use `sqe_worker_`. Grafana dashboards use the same panel layouts for both, swapping the prefix.

The recording happens inside `QueryHandler::execute`, unconditionally -- success or failure. The `status` field comes from whether the result was `Ok` or `Err`, and the `kind_name` comes from the SQL classifier, not from the execution result. We record the statement type even when the query fails, which is critical for answering "are SELECT queries failing more than INSERTs?"

The metrics fields are `Option<Arc<MetricsRegistry>>` rather than always-present. In integration tests, we often create a `QueryHandler` without a metrics registry because the test doesn't care about metrics. Making them optional avoids the overhead of maintaining a Prometheus registry in test contexts. In production, they're always present.


## The Dashboard That Matters

After weeks of running the engine, we settled on six panels that tell you almost everything:

| Panel | Metric | Why |
|-------|--------|-----|
| Query rate | `rate(sqe_query_count_total[5m])` by status | Is the engine being used? Are queries failing? |
| Query latency | `histogram_quantile(0.95, sqe_query_duration_seconds)` | Are queries fast enough? Is latency drifting? |
| Active sessions | `sqe_active_sessions` | How many users are connected right now? |
| Healthy workers | `sqe_healthy_workers` | Is the cluster capacity what we expect? |
| Cache hit rate | `rate(sqe_cache_hits_total[5m]) / (rate(sqe_cache_hits_total[5m]) + rate(sqe_cache_misses_total[5m]))` | Is the result cache helping? |
| Worker scan throughput | `rate(sqe_worker_bytes_read_total[5m])` | Are workers keeping up with scan demand? |

The seventh panel -- added after an incident -- is `rate(sqe_query_count_total{status="error"}[5m])` with an alert threshold. When more than 5% of queries fail over a 5-minute window, something is wrong. During normal operation, this number is zero. When it's not zero, you want to know immediately.

Not everything that can be measured should be alerted on. We learned this by over-alerting during the first week and then ignoring all alerts because there were too many. The alerts that survived: error rate above 5% for 5 minutes (catches auth failures, catalog failures, S3 throttling), p95 latency above 30 seconds for 10 minutes (catches systemic slowness), healthy workers below expected count for 3 minutes (catches crashes and network partitions), and zero queries for 15 minutes during business hours (catches the "everything looks fine but nothing is happening" failure mode).

The alerts we removed: cache hit rate below 50% (fired constantly, depends entirely on workload) and individual fragment duration above 10 seconds (some fragments are legitimately large). Noise that obscures real problems is worse than no alerting at all.


## The Query That Scanned Too Much

This is the story that justified every hour spent on observability.

During load testing, one particular TPC-H query was taking 8x longer than expected. The Prometheus dashboard showed the p95 climbing. But the average latency was fine. Something was wrong with a specific query pattern.

We pulled up the OTel trace for one of the slow executions. The coordinator spent 50ms on parsing and planning. The two workers together spent 7.3 seconds on scanning. But the query was simple -- a filtered aggregation on a small table. It should have scanned a few files, not taken 7 seconds.

The worker metrics told the rest of the story. `sqe_worker_bytes_read_total` for that fragment was 10x higher than expected. The worker was reading every Parquet file in the table instead of only the ones matching the filter.

The root cause: the Iceberg manifest filter wasn't being applied. Our scan task was sending all data file paths to the worker instead of only the files whose partition values matched the query predicate. The fix was in the planner -- apply partition pruning before constructing the scan task. The observability stack found the bug. Without per-worker bytes-read metrics and per-query traces, we'd have been guessing.


## Traces Across Process Boundaries

Prometheus metrics tell you how the system is performing. OpenTelemetry traces tell you how a specific request flows through it. For a distributed query engine, this distinction is the difference between "our p95 is 5 seconds" and "this specific query spent 4.7 of those 5 seconds waiting for a worker to read from S3."

The hardest part of distributed tracing is not generating spans. It's connecting them. When the coordinator dispatches a scan fragment to a worker, the worker's execution span needs to be a child of the coordinator's dispatch span. Otherwise you get two disconnected traces that happen to overlap in time, and the whole point is lost.

We solved this with W3C TraceContext propagation over gRPC metadata. The injector wraps tonic's `MetadataMap` and inserts `traceparent`/`tracestate` headers at the point of dispatch in the `DistributedScanExec`:

```rust
let ticket = Ticket::new(ticket_bytes);
let mut request = tonic::Request::new(ticket);
inject_trace_context(parent_cx, request.metadata_mut());
```

On the worker side, `do_get` extracts the parent context from the incoming gRPC metadata and links its execution span to the coordinator's trace:

```rust
let parent_cx = extract_trace_context(request.metadata());
let worker_span = info_span!(
    "worker_execute_scan",
    fragment_id = %scan_task.fragment_id,
    file_count = scan_task.data_file_paths.len(),
);
worker_span.set_parent(parent_cx);
```

The result: a single trace shows the full lifecycle of a distributed query. SQL parse on the coordinator. Plan optimization. Fragment dispatch. Worker execution. Parquet reads. Arrow batch transfer. All connected, all with accurate timing.

We also propagate trace context to Polaris REST catalog calls via HTTP headers, so that catalog operations appear in the same trace as the query that triggered them. When `EXPLAIN ANALYZE` says planning took 800ms, the trace shows you that 750ms was waiting for Polaris to return table metadata. That tells you the fix is to warm the catalog cache, not to optimize the scan.

One subtle detail in the OTel initialization: the filter that prevents telemetry-induced-telemetry loops. Without filtering `hyper`, `tonic`, `h2`, `reqwest`, and `tower` from the OTel log bridge, every export generates HTTP logs, which get exported via OTel, which generate more HTTP logs. The system enters an infinite loop that saturates the network. We found this during the first integration test. The symptom -- exponentially growing memory usage -- was alarming before we traced it to the log bridge.

The `OtelGuard` RAII type holds the tracer, meter, and logger providers for the lifetime of the process. On drop, it flushes all pending spans and metrics to the OTLP endpoint. The shutdown order matters: meter first, tracer second, logger last. The tracer might generate log events during its shutdown. The logger needs to be alive to capture them. We got this wrong initially and lost shutdown-related log entries. Not a production outage, but the kind of thing that makes debugging harder exactly when it matters.


## Audit Logging

Metrics and traces serve the operations team. Audit logs serve a different audience: security, compliance, and forensics.

Every query that executes in SQE produces an audit entry: timestamp, username, session ID, query hash, statement type, duration, rows returned, status, and client IP. Every field is there for a reason. `session_id` links to the Flight SQL session. `statement_type` enables filtering -- show me all DDL operations. `duration_ms` catches slow queries. `rows_returned` catches queries that return suspiciously large result sets.

The `query_hash` field deserves explanation. It's a SHA-256 of the normalized SQL -- whitespace collapsed, keywords uppercased. `SELECT 1 FROM t` and `select  1  from   t` produce the same hash. This lets you correlate queries across time without storing raw SQL in every entry. If you want to find all executions of a specific query pattern, hash the pattern and search for the hash. If you need the actual SQL, the optional `query_text` field has it -- configurable to omit in production if the SQL itself contains sensitive data.

The audit logger is append-only JSONL to a file, with flush-after-every-write to ensure a coordinator crash doesn't lose the last few entries. When no audit log path is configured, the logger is a no-op -- no allocation, no I/O, no lock contention. The `Option` pattern again: zero cost when disabled, full fidelity when enabled.

::: {.sovereignty}
**Sovereignty principle:** Audit logging is non-negotiable for sovereign infrastructure. If you can't answer "who queried what, when, and what did they see?" then you don't control your data platform -- you're just hosting it. The audit log is the proof that your access policies are being enforced. Without it, policies are promises.
:::


## Health Endpoints

Kubernetes needs to know three things about your pod: is it alive, is it ready, and what's its status? We serve these on a dedicated port.

**`/healthz`** (liveness probe) returns `"ok"` unconditionally. If the HTTP server can respond, the process is alive. No logic, no checks.

**`/readyz`** (readiness probe) returns 200 only when initialization is complete -- auth provider configured, catalog reachable, workers (if any) registered. The `ready` flag is an `AtomicBool` set after all initialization completes. The health server starts *before* initialization, so Kubernetes sees the pod as alive but not ready during the startup window.

**`/api/v1/status`** (cluster status) returns JSON with node role, version, uptime, and worker health. For human consumption and dashboards, not Kubernetes probes.

The health port is always `prometheus_port + 1` -- computed, not configured. One less thing to configure, one less thing to get wrong. The validation step checks that the Prometheus port doesn't collide with the Flight SQL port, which is the only plausible conflict.

Workers have a simpler health model, but the coordination protocol adds its own dimension. Workers send heartbeats every 5 seconds; the coordinator considers a worker healthy if it has sent a heartbeat within the last 15 seconds. The coordinator also runs active health checks via Arrow Flight. Belt and suspenders: heartbeats prove the worker can reach the coordinator, active checks prove the coordinator can reach the worker. Both directions matter in a network where firewalls or service mesh policies might allow traffic in one direction but not the other.

The `sqe_healthy_workers` gauge on the coordinator tracks how many workers are currently healthy. This is the single most important metric for capacity planning. If you have 4 workers and this gauge drops to 2, your scan capacity just halved.

::: {.fieldreport}
**Field report:** During distributed docker-compose testing, we initially had the readiness probe checking `/healthz` instead of `/readyz`. The coordinator would accept connections before worker registration completed, which meant the first few queries executed locally instead of distributed. The symptom was confusing: "Why are my distributed queries not distributing?" Because the load balancer started sending traffic before workers were ready.
:::


## From Observability to Configuration

Observability tells you what's happening. Configuration determines what *can* happen. And the uncomfortable truth about prototyping is that the hardcoded version ships faster. Every constant you pull out into configuration is a design decision you have to make explicit. What's the default? What's the range? What happens when someone puts in garbage?

The first version of SQE had about a dozen constants scattered across three crates:

```rust
// Early version -- don't do this
const FLIGHT_SQL_PORT: u16 = 50051;
const POLARIS_URL: &str = "http://localhost:8181/api/catalog";
const KEYCLOAK_URL: &str = "http://localhost:8080";
```

These worked perfectly for local development against the quickstart Docker Compose stack. They were also completely useless for anything else.

The first time someone else tried to run SQE -- a colleague who had a different Polaris endpoint and a different OIDC provider -- it didn't compile for them. Not "didn't work." Didn't *compile*. Because the Keycloak URL was baked into the binary. They had to clone the repo, find the right constant, change it, and wait for `cargo build` to finish.

That was the moment I understood: configuration isn't a feature you add after the engine works. Configuration *is* the product. The engine itself is implementation detail. The configuration surface is what operators actually interact with.

The Twelve-Factor App methodology calls this "storing config in constants" and lists it under things to stop doing immediately. But knowing the principle and feeling the pain are different things. The pain arrived when we tried to run integration tests in CI against a different stack and realised we'd need conditional compilation just to change an endpoint URL.

::: {.antipattern}
**Antipattern: Constants as configuration.** When you hardcode values because "we'll fix it later," you're making a bet that later comes before someone else needs to deploy your software. That bet almost always loses. The first external user showed up two days before we planned to extract the config.
:::


## Why TOML, and the Norway Problem

The format choice was quick. YAML lost for one specific reason: the Norway problem. In YAML, the string `no` is interpreted as a boolean `false`. The value `3.10` becomes the float `3.1`. Port `8080` could be an integer or a string depending on context. These implicit coercions are bugs waiting to happen in a config file where port numbers, boolean flags, and string identifiers all live together.

TOML has explicit types. `8080` is always an integer. `"8080"` is always a string. `true` is always a boolean. No helpful type guessing that silently converts your region string into something else.

The Rust ecosystem sealed the decision. `serde` plus `toml` gives you typed deserialization with error messages that point to the exact line and column. Every section in the TOML maps to a subsystem in the engine:

```toml
[coordinator]
flight_sql_port = 50051
trino_http_port = 8080
worker_urls = ["http://worker-1:50052", "http://worker-2:50052"]

[auth]
token_endpoint = "http://polaris:8181/api/catalog/v1/oauth/tokens"
client_id = "root"
client_secret = "s3cr3t"

[catalog]
polaris_url = "http://polaris:8181/api/catalog"
warehouse = "test_warehouse"

[storage]
s3_endpoint = "http://rustfs:9000"
s3_access_key = "s3admin"
s3_secret_key = "s3admin"
s3_region = "us-east-1"
s3_path_style = true
```

An operator reading this file sees the architecture. `[coordinator]` is the Flight SQL server. `[auth]` is OIDC. `[catalog]` is Polaris. `[storage]` is S3. You don't need to read the source to understand what these sections do.


## The Config Struct

The TOML deserializes directly into a typed Rust struct:

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct SqeConfig {
    pub coordinator: CoordinatorConfig,
    #[serde(default)]
    pub worker: WorkerConfig,
    pub auth: AuthConfig,
    pub catalog: CatalogConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub policy: PolicyConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    #[serde(default)]
    pub session: SessionConfig,
    #[serde(default)]
    pub query: QueryConfig,
    #[serde(default)]
    pub query_cache: QueryCacheConfig,
    #[serde(default)]
    pub query_history: QueryHistoryConfig,
}
```

Two things to notice. First, `#[serde(default)]` on most sections means a minimal config file only needs `[coordinator]`, `[auth]`, and `[catalog]`. Everything else gets sensible defaults. A developer running locally doesn't need to configure metrics, rate limits, session timeouts, or query caching. They just work.

Second, every field has a concrete type. Ports are `u16`. Timeouts are `u64`. Memory limits are strings with a parser that handles `"512MB"`, `"8GB"`, `"1TB"` -- case-insensitive, with or without the B suffix. The type system catches misconfigurations at startup, not at 3am when a query hits the wrong code path.

The subsection structs each carry their own defaults via `serde(default = "fn_name")`. Every default is a function, not an attribute value -- a serde requirement for non-trivial defaults that has a side benefit: all defaults live in one place at the bottom of the config module. Worker memory defaults to 8GB, heartbeat to 5 seconds, spill directory to `/tmp/sqe-spill`. Changing a default is a one-line change that shows up clearly in diffs.

The zero-config experience matters. A developer should be able to clone the repo, start the quickstart stack, and run `cargo run` with a minimal config file. No `[worker]` section needed -- defaults handle it. No `[policy]` section -- defaults to passthrough. No `[session]` section -- 15 minute idle timeout and 8 hour absolute timeout. No `[query_cache]` section -- caching enabled by default with 256MB and 5 minute TTL. Required sections -- the ones that depend on your specific infrastructure -- don't have defaults. If you forget `catalog.polaris_url`, the TOML parser fails immediately with a clear error. Not at runtime when the first query tries to reach Polaris.


## Environment Variable Overlay

TOML files work well for base configuration. They don't work for secrets, and they don't work for per-deployment overrides in Kubernetes.

The solution is a layered model: TOML first, then environment variable overrides. Every config field can be overridden by setting `SQE_<SECTION>__<FIELD>`. Double underscore as the separator, because single underscore is already used within field names.

```bash
SQE_CATALOG__POLARIS_URL="http://polaris-prod:8181/api/catalog"
SQE_AUTH__CLIENT_SECRET="production-secret-from-vault"
SQE_COORDINATOR__FLIGHT_SQL_PORT=50052
```

The implementation is explicit -- every overridable field is listed in one function. We rejected a reflection-based approach that would auto-map any `SQE_*` variable. The explicit listing means typos in variable names are silently ignored rather than silently applied to the wrong field, and we control which fields are overridable. Some fields, like TLS certificate paths, should only come from the config file or a mounted secret.

A bad override logs a warning and keeps the TOML value. It doesn't crash the process. In Kubernetes, you might have a stale ConfigMap with a variable referencing a renamed field. Crashing on that would take down the service during a rolling upgrade. Warning is the right response.

The Helm chart makes secrets explicit:

```yaml
env:
  - name: SQE_AUTH__CLIENT_SECRET
    valueFrom:
      secretKeyRef:
        name: sqe-secrets
        key: SQE_AUTH__CLIENT_SECRET
        optional: true
```

Secret values flow from Kubernetes Secrets into environment variables, which override whatever the TOML has. The TOML in the ConfigMap never contains secrets. Custom `Debug` implementations on `AuthConfig` and `StorageConfig` redact sensitive fields -- `client_secret` renders as `"[REDACTED]"` -- so even a startup config dump to logs won't leak credentials.

The type-specific override functions handle parsing carefully. A boolean override accepts `"true"`, `"1"`, `"yes"` and their negatives; anything else logs a warning and keeps the TOML value. This matters: a misconfigured environment variable shouldn't silently change behavior, but it also shouldn't crash the service during a rolling upgrade where a stale ConfigMap references a field that's been renamed.

The docker-compose file for the distributed test stack shows the layered model in action:

```yaml
services:
  sqe:
    command: ["--config", "/etc/sqe/sqe.toml"]
    environment:
      SQE_CATALOG__POLARIS_URL: "http://polaris:8181/api/catalog"
      SQE_AUTH__CLIENT_SECRET: "${SQE_CLIENT_SECRET:-sqe-secret-change-me}"
      SQE_METRICS__OTLP_ENDPOINT: "http://jaeger:4317"
```

The TOML in the container image provides the base structure and defaults. The environment variables override deployment-specific values. The `${SQE_CLIENT_SECRET:-sqe-secret-change-me}` syntax means the secret comes from the host environment if set, falling back to a development default. Same model the Helm chart uses, scaled up.


## Plugin Points

The config surface is real. Every struct shown here is production code. The plugin architecture -- the extension points for external contributors -- is designed but not fully populated.

The pattern is consistent across subsystems. The policy engine has a `PolicyEnforcer` trait:

```rust
#[async_trait]
pub trait PolicyEnforcer: Send + Sync {
    async fn evaluate(
        &self,
        user: &SessionUser,
        plan: LogicalPlan,
    ) -> sqe_core::Result<LogicalPlan>;
}
```

Today `[policy] engine = "passthrough"` instantiates the only implementation. The plan is that `engine = "opa"` would instantiate an OPA-backed enforcer, `engine = "cedar"` a Cedar-backed one. The trait is the plugin point. The config key selects which implementation loads.

The same pattern applies to auth and catalog. DataFusion's `CatalogProvider` trait is the extension point for catalog backends. Our Polaris implementation is the first. Adding a second -- Gravitino, Unity, in-memory -- would follow the same trait, selected by a config key.

We defined the traits before having multiple implementations because the open-source target demands it. The traits are the promise: "you can swap this out." The config keys are the interface: "here's how you select the swap." The risk is that the traits are slightly wrong for what OPA or Cedar actually need. We accept this. The trait is four lines. Changing it is a breaking change in a pre-1.0 project. The cost of getting it slightly wrong is low. The cost of not having it -- of shipping a monolithic engine that can't be extended without forking -- is the end of the open-source goal.


## Rate Limits and Sessions

Two config sections arrived later, when we started thinking about multi-tenant deployments:

```toml
[rate_limit]
enabled = true
per_user_queries_per_minute = 60
global_queries_per_minute = 1000

[session]
idle_timeout_secs = 900          # 15 minutes
absolute_timeout_secs = 28800    # 8 hours
```

Rate limiting is off by default. An internal deployment where all users are trusted doesn't need it. A shared deployment where a runaway dbt job could monopolize the engine does. The config key is the toggle.

Session timeouts are on by default with values that work for interactive use. A dbt batch job that runs longer than 8 hours needs the absolute timeout increased. A query dashboard open all day needs the idle timeout extended. These are operator decisions, not engine decisions. The defaults are sensible; the overrides are available.


## Failing Fast on Bad Config

The engine calls `validate()` immediately after loading the config, before starting any subsystem. The validation accumulates all errors before reporting:

```rust
let config = SqeConfig::load(&config_path)?;
config.validate()?;  // Fail here, not later
```

An operator with five misconfigurations gets all five reported at once, not one at a time across five restarts. Validation runs before any network I/O -- the engine doesn't try to connect to Polaris to see if the URL is valid. Connection problems are runtime errors; config problems are startup errors. The port conflict check prevents a class of bug otherwise diagnosed at the OS level as "address already in use" with no hint about which config keys conflict. The TLS check ensures that if one of cert_file and key_file is set, both must be set.

The full config loading sequence runs from binary startup to ready state: CLI parsing (`--config` flag or `SQE_CONFIG` env var), file read, TOML parse with serde, environment overlay, deprecation warnings for renamed keys, validation, and subsystem initialization. Step 5 matters: we renamed `auth.keycloak_url` when we realised the auth system works with any OIDC provider. The old key still works -- it maps to the same field -- but the engine logs a deprecation warning. This is how config keys evolve in a sovereign system. You don't break existing deployments. You warn, give time, and remove in a major version.

Of approximately 45 individual config keys across 12 sections, only 3 are truly required: `auth.client_id`, `catalog.polaris_url`, and one of `auth.keycloak_url` or `auth.token_endpoint`. Everything else has a default that works for local development.

The config surface also has tests. The `valid_config()` helper constructs a known-good configuration, and each validation test mutates one field to confirm the correct error fires. Every default gets a test confirming its value. This is important for a subtle reason: defaults are documentation. When an operator reads the test suite and sees `assert_eq!(config.memory_limit, "8GB")`, they know that omitting the memory limit from their TOML gives them 8GB. The tests are the spec.


## The Twelve-Factor Connection

The Twelve-Factor App methodology was written for web services. A query engine isn't a web service, but five of the twelve factors apply directly.

**III. Config** -- Store config in the environment. The TOML handles the base, environment variables handle per-deployment overrides, secrets never touch disk.

**IV. Backing services** -- Treat backing services as attached resources. Polaris, S3, Keycloak -- each configured by URL. Swapping from test to production Polaris is a config change, not a code change. This matters more for a query engine than for a typical web app, because the backing services are the entire reason the engine exists.

**X. Dev/prod parity** -- The same binary runs everywhere. The same TOML structure, different values. SQE doesn't have a "development mode." It has config keys like `auth.ssl_verification = false` that you can set explicitly, and a deprecation warning if you do.

**XI. Logs** -- Treat logs as event streams. SQE logs to stdout/stderr and sends traces to an OTLP collector when configured. No log file management in the engine.

**VI. Processes** -- Execute the app as stateless processes. Workers are completely stateless. The coordinator holds session state in memory but is designed for session affinity, not sticky state.

SQE builds as a single binary -- `sqe-server` -- that runs as either coordinator or worker based on a `--mode` flag, an `SQE_MODE` environment variable, or a config key. Both modes share the same config file structure. A coordinator ignores the `[worker]` section. A worker ignores `[coordinator].worker_urls`. In Kubernetes, the same ConfigMap is mounted to both coordinator and worker pods. The only difference is the `SQE_MODE` environment variable. Same image, same config, different mode. One config surface to understand.

::: {.ailog}
*[To be completed by AI Logbook agent]*
:::


## Tying It Together

The observability stack in SQE is not a separate system. It's wired into the query pipeline at every step. The `execute` method on `QueryHandler` instruments with `#[tracing::instrument]`, which creates an OTel span. Inside that span, it records Prometheus metrics. After execution, it writes the audit log. The worker's `do_get` extracts the parent trace context and creates a child span. The executor records per-fragment metrics.

Every query that flows through the engine produces:

- A Prometheus counter increment and histogram observation
- An OTel trace with spans for parse, plan, dispatch, and execute
- A structured audit log entry

None of these require manual instrumentation at the call site. The `QueryHandler` and `WorkerFlightService` handle it. If you add a new statement type, the metrics and audit log pick it up automatically because they're driven by the SQL classifier, not by statement-specific code.

This matters more than any individual metric choice. The best observability stack is the one that works without the developer remembering to add it. The worst is the one with gaps because someone forgot to instrument a code path.

The configuration surface follows the same principle. Every new feature adds config keys following an established pattern: add a section, define defaults, add validation, add tests, add env override support. The pattern scales because it's mechanical. No judgment calls about where to put the config key or how to name it. The TOML section is the subsystem. The field is the behavior selector. The default is what "normal" looks like.

The full configuration surface as of this writing has 12 sections and roughly 45 individual keys:

| Section | Keys | Required |
|---------|------|----------|
| `coordinator` | 7 | No (has defaults) |
| `worker` | 6 | No |
| `auth` | 7 | Yes (client_id, one endpoint) |
| `catalog` | 4 | Yes (polaris_url) |
| `storage` | 6 | No |
| `policy` | 1 | No |
| `metrics` | 3 | No |
| `rate_limit` | 3 | No |
| `session` | 2 | No |
| `query` | 2 | No |
| `query_cache` | 4 | No |
| `query_history` | 2 | No |

The surface will grow. Each new feature -- pluggable auth backends, OPA integration, Cedar rules, compaction scheduling -- will add config keys. The pattern is established. The pattern scales.


## The Enterprise Checklist

We spent months adding the pieces that make an engine observable and configurable. Then we spent more months on the pieces that make it trustworthy in an enterprise context — the ones that matter when the security team asks for a review or when a production incident happens at 2am and you need to answer specific questions fast.

None of these features were in the original design. They arrived one by one, each time someone asked a question we couldn't answer or revealed an assumption we hadn't examined.


### Structured Error Codes

The first version of error handling was honest about its limitations: errors came back as strings. A DataFusion parse error looked exactly like a catalog connection failure, which looked exactly like an OPA policy denial. All of them returned HTTP 500 or a generic gRPC INTERNAL status, because that was the path of least resistance.

The problem surfaced during integration testing with the dbt adapter. When a model failed, dbt logged the error string. The string was useful for debugging but useless for automated handling. How do you distinguish "table not found" from "access denied" from "out of memory" if they all say `Internal error`?

We introduced 27 `SqeErrorCode` variants that carry semantics rather than just messages:

| Category | Examples | gRPC Status | Trino Code |
|---|---|---|---|
| Auth | `Unauthorized`, `Forbidden`, `SessionExpired` | UNAUTHENTICATED, PERMISSION_DENIED | 65536, 65537 |
| Catalog | `TableNotFound`, `SchemaNotFound`, `CatalogUnavailable` | NOT_FOUND, UNAVAILABLE | 65540, 65541 |
| Execution | `InvalidQuery`, `TypeMismatch`, `DivisionByZero` | INVALID_ARGUMENT | 65536+n |
| Resources | `QueryTimeout`, `MemoryLimitExceeded`, `TooManyRequests` | RESOURCE_EXHAUSTED | 65550+n |
| System | `InternalError`, `SerializationError`, `ConfigError` | INTERNAL | 65535 |

The classifier that assigns codes is built around a simple principle: user errors get detail, system errors get redaction. If a table isn't found, the error message says which table. If Polaris returns a 503, the message says "catalog unavailable" — not the internal details of which HTTP call failed or what the retry sequence looked like. The user can't fix a Polaris outage. The operator can, and they have the logs.

An auto-classifier parses DataFusion error message strings to assign error codes. DataFusion doesn't yet have a typed error enum stable enough to match on, so we pattern-match on the string representations. It's not elegant, but it's isolated to one module and tested against the actual error strings DataFusion produces. When DataFusion's error types stabilize, swapping the classifier for a type-match is a contained change.


### Security Hardening at Startup

We added startup warnings for three conditions: TLS disabled on the Flight SQL port, rate limiting disabled, and SSL certificate verification disabled. Each logs at `WARN` level during initialization, before accepting any connections.

The philosophy is fail-open for development, fail-loud for production. We don't refuse to start without TLS — the quickstart stack runs over plain HTTP and that's intentional. But we print a warning that is hard to miss:

```
WARN sqe_coordinator: TLS is DISABLED on Flight SQL port 50051 -- do not use in production
WARN sqe_coordinator: Rate limiting is DISABLED -- concurrent queries are unlimited
WARN sqe_coordinator: SSL certificate verification is DISABLED -- catalog connections are insecure
```

These are the settings a developer enables for a local run against a self-signed cert and forgets to turn off before deploying. The warnings are there because that exact scenario happened during our first staging deployment. Everything worked. The security team noticed the unencrypted Flight SQL port during a network scan two weeks later.

What we don't do yet: block startup on these conditions. We discussed it. The argument for blocking is that it prevents the accidental insecure deployment. The argument against is that it breaks legitimate airgapped deployments and internal-only environments. We landed on warning, with a documented `--allow-insecure` flag for environments where the operator has made a deliberate choice. The flag exists in the config schema. We haven't added it to the validation yet.


### Client IP Logging

Every request — Flight SQL and Trino HTTP — now logs the client IP address alongside the query audit entry. The implementation has two layers: `x-forwarded-for` header parsing for requests arriving through a reverse proxy or ingress, and TCP peer address fallback for direct connections.

The ordering matters. A request arriving through an nginx ingress carries the real client IP in `x-forwarded-for`; the TCP peer address is the ingress pod. If we logged the TCP address, we'd have a perfect record of which ingress pod received the traffic and nothing about who sent the query.

We take the leftmost address in `x-forwarded-for`, which is the one set by the client, not the one appended by intermediate proxies. This is correct when the ingress is trusted. In a deployment where the network perimeter isn't controlled, a client can spoof the header. The fix is to strip untrusted `x-forwarded-for` headers at the ingress level — not something the query engine can solve. We document this limitation.


### Circuit Breaker for Polaris

Catalog availability is not guaranteed. Polaris goes through rolling restarts. Network partitions happen. The default behavior — retry with backoff — has a failure mode that took us a while to fully appreciate: during a Polaris outage, the coordinator accumulates threads blocked waiting for HTTP responses. When Polaris returns, those threads all try to reconnect simultaneously. The thundering herd effect can cause a second outage immediately after recovery.

The circuit breaker solves this by separating "is Polaris healthy?" from "should I try to reach Polaris?"

```
Closed  ──5 failures──▶  Open  ──30s──▶  Half-Open  ──success──▶  Closed
                                                       ──failure──▶  Open
```

In the closed state, requests reach Polaris normally. Five consecutive failures open the circuit. In the open state, requests fail immediately without making a network call — fast failure rather than blocked threads. After 30 seconds, the circuit moves to half-open, allowing a single probe request. If the probe succeeds, the circuit closes. If it fails, the circuit reopens.

The thresholds are configurable, but the defaults reflect what we found worked during outage testing: 5 failures is enough signal, 30 seconds is enough recovery time for a Polaris pod restart, a single probe avoids the thundering herd. The circuit breaker state is local to each coordinator process. In a multi-coordinator deployment, each coordinator makes its own circuit decisions. This is deliberate — a circuit that's shared across coordinators requires distributed state, which is a harder problem than the one the circuit breaker is solving.


### PII Redaction in Audit Logs

The audit log captures `query_hash` (SHA-256 of normalized SQL, safe to store) and optionally `query_text` (the raw SQL). The hash supports correlation without reconstruction — you can find all executions of a query pattern without storing the query itself. The text supports debugging.

The problem with `query_text`: SQL WHERE clauses contain user data. `SELECT * FROM orders WHERE customer_email = 'alice@example.com'` is a realistic query. That email address doesn't belong in the audit log.

We added regex-based stripping of four PII categories before writing `query_text` to the audit entry:

| Pattern | What it strips |
|---|---|
| Email addresses | `\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z]{2,}\b` |
| Phone numbers | Common formats: +1-555-0100, (555) 010-0100, etc. |
| SSNs | `\b\d{3}-\d{2}-\d{4}\b` |
| Credit card numbers | 13-16 digit sequences with Luhn-plausible formatting |

Each match is replaced with `[REDACTED:<TYPE>]`. The redacted SQL is still readable. The pattern of the query is preserved. The personal data is not.

This is not a complete solution. SQL can encode PII in ways that don't match these patterns — base64-encoded data, numeric IDs that happen to be SSNs without dashes, string constants in languages other than English. The redaction is best-effort defense-in-depth, not a compliance guarantee. The guarantee comes from having `query_text` configurable-off for high-sensitivity deployments. If you can't afford PII in audit logs at all, disable `query_text`. The hash is always safe.


### Per-Query Resource Limits

Multi-tenancy requires protecting the engine from individual queries that consume disproportionate resources. Four limits apply to every query:

| Limit | Default | Enforcement |
|---|---|---|
| `max_result_rows` | 1,000,000 rows | Post-execution count; query cancelled if exceeded |
| `max_concurrent_queries` | 100 | Semaphore at `QueryHandler::execute` entry |
| `max_query_memory` | 256 MB | DataFusion `GreedyMemoryPool` |
| `slow_query_threshold_secs` | 30 seconds | WARN log after threshold; query continues |

The `max_result_rows` limit is intentionally post-execution because we can't know the result size before executing. The trade-off: a query that generates 2M rows uses the compute budget to produce them, then gets cancelled when we count the output. This wastes work. The alternative — stopping mid-scan — requires streaming result counting during execution, which DataFusion doesn't expose cleanly. We document this as a known limitation and recommend users add LIMIT clauses for exploratory queries.

The concurrency semaphore has one subtle behavior: when the engine is at capacity, new queries wait rather than fail immediately. The wait timeout is 5 seconds. A query that can't acquire the semaphore in 5 seconds gets a `TooManyRequests` error. This gives burst capacity for genuine traffic spikes while protecting against indefinite queue growth.

The `GreedyMemoryPool` in DataFusion allocates memory greedily and cancels the query if allocation would exceed the limit. "Cancel" means the next allocation attempt returns an error, which propagates up through the execution tree. The cancellation is clean — DataFusion unwinds the execution future — but it's abrupt. The user sees `MemoryLimitExceeded`. They don't see a partial result. For analytics workloads this is correct behavior. For streaming results (large Arrow batches delivered incrementally), we return what we have and signal cancellation on the next batch request.

Slow query logging is the most operational of the four. A query that takes 31 seconds doesn't fail — it completes and produces results. But the coordinator logs a structured warning with the query ID, username, duration, and a truncated query text. This gives operators a passive alert that something is running long, without interrupting it. The alerts we removed from Prometheus included individual query duration; this replaced them. Prometheus tracks the distribution. The slow query log identifies the specific offenders.


### Session Persistence

Session state — authenticated users, open Flight SQL sessions, per-session settings — lives in memory on the coordinator. A coordinator restart means every active session is lost. Users see authentication failures and reconnection prompts. For a dbt job mid-run, this means the job fails and must restart.

The initial implementation had a file-based snapshot: every 5 minutes (configurable), the coordinator serializes all sessions to a JSONL file at a configured path. On startup, if the file exists, sessions are restored. The user reconnection window is the restart duration, not the full re-authentication cycle.

What we have is not HA. It's warm restart. The distinction matters: a crash still causes disruption; the persistence only helps with planned restarts (upgrades, config reloads). True HA requires session state in an external store — Redis being the obvious choice — and a leader election mechanism so multiple coordinators share state without conflicts. The `SessionStore` trait is defined and the file-based implementation is the first backend. A Redis implementation would follow the same trait. We're not there yet. We know what "there" looks like.


### The `/readyz` Evolution

The readiness probe started as a boolean: is initialization complete? That answered one question but not the one operators actually need: "Is this coordinator capable of serving queries right now?"

A coordinator that has initialized but can't reach Polaris is not ready. It will accept connections and then fail every query with `CatalogUnavailable`. Kubernetes won't remove it from load balancer rotation because `/readyz` still returns 200.

The evolved `/readyz` makes a lightweight Polaris reachability check — an HTTP HEAD to the catalog base URL — before returning success. If Polaris is unreachable, `/readyz` returns 503 with a JSON body describing which dependency failed:

```json
{
  "status": "not_ready",
  "checks": {
    "initialized": true,
    "polaris_reachable": false,
    "workers_registered": true
  },
  "failed": ["polaris_reachable"]
}
```

Kubernetes removes the pod from rotation on 503. Traffic stops flowing to a coordinator that can't serve it. When Polaris recovers, the check passes, the pod re-enters rotation.

The cost: every readiness check makes a network call. With Kubernetes polling every 5 seconds, that's 12 catalog pings per minute per coordinator pod. We added a 10-second cache on the Polaris reachability result to reduce this to at most 6 pings per minute during normal operation, with a fresh check guaranteed when the cached result is "not reachable."

What we don't have yet: leader election, so that only one coordinator is "primary" for write operations. For read-only query traffic this doesn't matter — all coordinators are equivalent. For session state and write coordination, it matters significantly. The HA story is the next chapter we haven't written yet.


## The Security Audit That Changed the Defaults

After the engine was fast, correct, and observable, we asked: would a bank approve this for production?

The answer was 43 findings across six categories. Two critical, thirteen high, twenty-one medium, seven low. The engine ran 221 out of 222 benchmark queries correctly and beat Trino by 2.5x to 8.8x. It still failed the audit.

The critical finding was the session context cache. Keyed by username. Two users sharing a username from different identity providers would share a Polaris catalog session. Cross-user data access. The fix was straightforward: key by `username:sha256(token)[..16]`. But it required switching moka's `get`/`insert` to `try_get_with` for atomic cache population, which also eliminated a TOCTOU race condition where concurrent requests from the same user would build redundant SessionContexts.

The most tedious finding was panic safety. Sixteen call sites in date extraction functions used `.unwrap()` on `date32_to_datetime()`, which returns `Option<NaiveDateTime>`. A query calling `year()` on a Date32 column with an extreme value would panic and kill the coordinator. Not return an error. Kill the coordinator. Every one of those sixteen sites needed individual attention because the containing functions had different error handling patterns. Some were in `.map()` closures where `?` does not work. Those needed conversion to explicit loops.

The finding that taught us the most was adaptive sort. We initially set the default to `partition_only`. Safest possible: never sort on non-partition columns, never OOM from unbounded sorts. Eight integration tests immediately failed. Every test that used `ORDER BY salary DESC` got unsorted results. The lesson: the safest default and the correct default are not always the same.

The right default is `adaptive`. Sort normally when memory is available. Strip non-partition sorts when memory pressure rises. Never crash. Never silently return wrong results on small data. The FairSpillPool memory limit is the backstop. If the sort exceeds memory, DataFusion spills to disk. If spill is not configured, the adaptive stripper removes the sort before the executor runs out of memory. Two layers of protection, transparent to the user.

Other findings were smaller but cumulative. Nine Flight SQL metadata endpoints with no authentication check. The cancel-query endpoint letting any client cancel any other user's query. The OPA policy cache ignoring role changes. OIDC error bodies forwarded verbatim to clients, enabling user enumeration. Blocking `std::fs::write` on Tokio worker threads. The `checksum()` UDF using `DefaultHasher`, which is not stable across Rust versions. The audit logger silently dropping records on mutex poison.

We fixed all 43. Thirty-three files changed. +1,272 / -372 lines. Every unit test still passed. All sixty integration tests passed.

The audit doc lives at `docs/issues.md`. It lists every finding by severity, with file:line references, what was wrong, how it was fixed, and why it matters. The document is not a badge. It is a maintenance artifact. When someone changes the session cache or the auth chain or the sort logic, they can check the audit doc to understand why the code looks the way it does.

::: {.sovereignty}
**Sovereignty principle:** A production security audit is not optional for sovereign infrastructure. If you skip it, you are deploying hope. The 43 findings were not surprising. They are the normal output of building software quickly and then reviewing it carefully. The difference between a prototype and a production system is not the code. It is the review.
:::


## The Lesson

We started with no observability and hardcoded constants. We added counters. Then histograms. Then traces. Then audit logs. Then trace propagation. Then health probes. Then a config file. Then environment overlays. Then validation. Then a full security audit. Each addition was prompted by a specific question we could not answer or a specific deployment we could not support.

The engine that "worked" without any of this was the same code, executing the same queries. The difference is that now we know what it is doing, and someone other than the person who wrote it can deploy it. No surprises.

Configuration taught us something we did not expect: it forced architectural clarity. When you name a config section, you decide what that subsystem is. When you define defaults, you decide what "normal" looks like. When you write validation, you decide what is required and what is optional. The TOML file is not a reflection of the architecture. It *is* the architecture, expressed for operators.

If your query engine surprises you, your metrics are wrong. If nobody else can deploy it, your config is wrong. If it panics on user input, your error handling is wrong. Fix all three.
