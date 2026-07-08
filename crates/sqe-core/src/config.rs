use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::SecretString;

#[derive(Debug, Deserialize, Clone)]
pub struct SqeConfig {
    pub coordinator: CoordinatorConfig,
    #[serde(default)]
    pub worker: WorkerConfig,
    pub auth: AuthConfig,
    /// Legacy single-catalog block. Always present for backwards
    /// compatibility. Operators starting fresh should usually use
    /// `[catalogs.NAME]` blocks instead and leave `[catalog]` minimal
    /// (a placeholder REST URL satisfies the deserializer; flatten
    /// drops it when `catalogs` is non-empty and the operator hasn't
    /// named the legacy block via `default_catalog`).
    pub catalog: CatalogConfig,
    /// Named catalog map. Each entry is a full `CatalogConfig`,
    /// including its own `[catalogs.<name>.backend]` block, so you
    /// can attach Polaris alongside Nessie, AWS Glue, S3 Tables, HMS,
    /// JDBC, or Hadoop in the same coordinator. The map is keyed
    /// by the SQL identifier the catalog is exposed as. Empty
    /// (the default) means "use the legacy `[catalog]` block only".
    #[serde(default)]
    pub catalogs: HashMap<String, CatalogConfig>,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub policy: PolicyConfig,
    #[serde(default)]
    pub access_control: AccessControlConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    #[serde(default)]
    pub security: SecurityConfig,
    #[serde(default)]
    pub session: SessionConfig,
    #[serde(default)]
    pub query: QueryConfig,
    #[serde(default)]
    pub query_cache: QueryCacheConfig,
    #[serde(default)]
    pub query_history: QueryHistoryConfig,
}

/// Controls adaptive sort stripping behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortMode {
    /// Always sort. Spill to disk if needed.
    Strict,
    /// Only sort when keys match Iceberg partition columns.
    PartitionOnly,
    /// Sort when memory allows; strip non-partition sorts under pressure.
    Adaptive,
}

impl SortMode {
    /// Parse from config string. Returns `Adaptive` for unknown values.
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "strict" => Self::Strict,
            "partition_only" | "partition-only" => Self::PartitionOnly,
            "adaptive" => Self::Adaptive,
            _ => {
                tracing::warn!(sort_mode = s, "Unknown sort_mode, defaulting to partition_only");
                Self::PartitionOnly
            }
        }
    }
}


/// Controls passive per-query profiling: after a streaming query finishes
/// (or fails), the coordinator renders the executed physical plan with the
/// per-operator metrics DataFusion populated during normal execution. No
/// re-run under EXPLAIN ANALYZE is needed to see where the time went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileMode {
    /// No profiles are captured.
    Off,
    /// Profile queries that cross `slow_query_threshold_secs`, and every
    /// failed query (failures are exactly when evidence is wanted).
    Slow,
    /// Profile every query.
    All,
}

impl ProfileMode {
    /// Parse from config string. Unknown values fall back to `Off` with a
    /// WARN (profiling is opt-in; lenient like `SortMode::parse`).
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "off" => Self::Off,
            "slow" => Self::Slow,
            "all" => Self::All,
            _ => {
                tracing::warn!(query_profile = s, "Unknown query_profile, defaulting to off");
                Self::Off
            }
        }
    }
}

/// How the coordinator resolves a catalog name that is not statically
/// declared in `[catalogs.*]`. `Static` (default) errors on an unknown
/// 3-part identifier. `PolarisAuto` lazily probes Polaris for a warehouse
/// of that name using the caller's bearer (see the catalog-discovery design).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CatalogDiscovery {
    #[default]
    Static,
    PolarisAuto,
}

impl CatalogDiscovery {
    /// Parse from a config string; unknown values fall back to `Static`
    /// (fail-closed — discovery is opt-in).
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "polaris-auto" => Self::PolarisAuto,
            _ => Self::Static,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct QueryConfig {
    /// Default catalog for unqualified SQL identifiers. When unset
    /// (the common case), SQE picks `iceberg` if the legacy
    /// `[catalog]` block is the sole catalog, otherwise the first
    /// alphabetically-sorted name from `[catalogs.*]`. Set this
    /// when you've named the legacy `[catalog]` block via the
    /// flattened name (see `SqeConfig::flattened_catalogs`) and want
    /// it to be the default rather than the alphabetic winner.
    #[serde(default)]
    pub default_catalog: Option<String>,
    /// How catalogs not declared in `[catalogs.*]` resolve. `"static"`
    /// (default) errors on an unknown 3-part identifier; `"polaris-auto"`
    /// lazily probes Polaris for a warehouse of that name with the caller's
    /// bearer. See `docs/superpowers/specs/2026-05-29-polaris-catalog-discovery-design.md`.
    #[serde(default)]
    pub catalog_discovery: CatalogDiscovery,
    /// Maximum query execution time in seconds. Default: 300 (5 minutes).
    #[serde(default = "default_query_timeout")]
    pub timeout_secs: u64,
    /// Per-role timeout overrides. Keys are role names, values are timeout in
    /// seconds. When a user has multiple matching roles the highest value wins.
    #[serde(default)]
    pub role_overrides: std::collections::HashMap<String, u64>,
    /// Maximum number of rows returned per query. Default: 1_000_000. Set to 0 for unlimited.
    #[serde(default = "default_max_result_rows")]
    pub max_result_rows: usize,
    /// Maximum concurrent queries. Default: 100. Set to 0 for unlimited.
    #[serde(default = "default_max_concurrent_queries")]
    pub max_concurrent_queries: usize,
    /// Maximum concurrent queries from a single authenticated user. Defaults to
    /// the global `max_concurrent_queries` value (no per-user differentiation).
    /// Set to a smaller positive number to prevent one tenant from holding
    /// every global permit; set to 0 to disable per-user accounting entirely.
    #[serde(default = "default_max_concurrent_per_user")]
    pub max_concurrent_per_user: usize,
    /// Maximum reserved memory per authenticated user, summed across all of
    /// their in-flight queries. When a new query would push the user above
    /// this limit, it is rejected with a per-user pressure error even if
    /// the global pool is below the red-band. Supports "B", "KB", "MB",
    /// "GB" suffixes. Default: "1GB". Set to "0" to disable per-user
    /// memory accounting (admission falls back to the global FairSpillPool
    /// pressure check alone).
    #[serde(default = "default_per_user_memory_budget")]
    pub per_user_memory_budget: String,
    /// Queries taking longer than this are logged at WARN level. Default: 30. Set to 0 to disable.
    #[serde(default = "default_slow_query_threshold")]
    pub slow_query_threshold_secs: u64,
    /// Passive per-query profiling: log the executed physical plan with the
    /// per-operator metrics DataFusion populated during normal execution.
    ///
    /// - `"off"`: no profiles (default).
    /// - `"slow"`: log a profile for queries crossing `slow_query_threshold_secs`,
    ///   and for every failed query.
    /// - `"all"`: log every query's profile.
    ///
    /// Unknown values are treated as `"off"` with a WARN at startup.
    #[serde(default = "default_query_profile")]
    pub query_profile: String,
    /// Maximum memory per query. Default: "256MB". Supports: B, KB, MB, GB. Set to "0" for unlimited.
    #[serde(default = "default_max_query_memory")]
    pub max_query_memory: String,
    /// Minimum total scan size to distribute across workers. Below this, execute on coordinator.
    /// Default: "128MB". Set to "0" to always distribute.
    #[serde(default = "default_distribution_threshold")]
    pub distribution_threshold: String,
    /// Minimum number of data files to distribute. Below this, execute locally on coordinator.
    /// Default: 4. Used as a fast check when file sizes are not yet available.
    #[serde(default = "default_distribution_file_threshold")]
    pub distribution_file_threshold: usize,
    /// Target size per scan task for bin-packing. Default: "256MB".
    #[serde(default = "default_target_task_size")]
    pub target_task_size: String,
    /// Controls when ORDER BY clauses are preserved vs stripped to save memory.
    ///
    /// - `"strict"`: Always sort. Spill to disk if needed.
    /// - `"partition_only"`: Only sort when keys match Iceberg partition columns.
    ///   Safest for TB-scale data from mixed writers (Spark, Trino, SQE).
    /// - `"adaptive"`: Sort when memory is available (Green); strip non-partition
    ///   sorts under memory pressure. Best default: correct for small data,
    ///   safe for large data.
    ///
    /// Default: `"adaptive"` — tries to sort in memory, falls back to
    /// partition-only under pressure. Never crashes from unbounded sorts.
    #[serde(default = "default_sort_mode")]
    pub sort_mode: String,
    /// Minimum number of projection-only columns required for late materialization
    /// to be applied. Late materialization uses a two-phase Parquet scan: Phase 1
    /// reads only predicate columns, Phase 2 reads remaining columns for surviving
    /// rows. When the number of deferrable columns is below this threshold, the
    /// overhead of two-phase scanning may exceed the I/O savings.
    ///
    /// Default: 1 (apply whenever there is at least one projection-only column).
    /// Set to 0 to disable late materialization entirely.
    #[serde(default = "default_late_mat_min_projection_cols")]
    pub late_materialization_min_projection_cols: usize,
    /// Enable star-schema join reordering. When enabled, chains of inner
    /// equi-joins are reordered so small dimension tables are joined first
    /// (building small hash tables) and the large fact table is probed last.
    ///
    /// Default: true.
    #[serde(default = "default_true")]
    pub star_schema_reorder: bool,
    /// Minimum ratio between the largest and smallest table row counts
    /// required to trigger star-schema join reordering. Only applies when
    /// `star_schema_reorder` is enabled.
    ///
    /// Default: 10 (fact table must be at least 10x larger than the smallest dimension).
    #[serde(default = "default_star_schema_min_ratio")]
    pub star_schema_min_ratio: usize,
    /// Parallelize the probe-side Iceberg scan of `CollectLeft` (broadcast)
    /// hash joins so the fact-table decode runs across cores while the build
    /// side stays single-partition (issue #235, follow-up to #131). Build-side
    /// scans are never touched, so the q72 regression cannot recur.
    ///
    /// Default: true (since 2026-07-08). Validated on the clean SF10 rig with
    /// split-level scan partitioning in place: SSB 20.0s vs Trino 21.4s, TPC-H
    /// neutral, TPC-DS 99/99 correct at +6% total (the residual is the real
    /// cost of parallel window/rollup plans on q67/q47/q57, not skew). The
    /// 2026-06 "perf-neutral" verdict predated the dynamic-filter and
    /// dim-build-swap fixes and was measured on a contended box. Set to false
    /// to restore fully serial CollectLeft probes.
    #[serde(default = "default_true")]
    pub parallel_probe_scan: bool,
    /// Parallelize single-node Iceberg scans across cores by giving each scan
    /// N output partitions, where the operator above the scan can consume the
    /// parallelism without a redundant gather (issue #131 follow-up). The scan
    /// advertises `RoundRobinBatch(N)`; where it feeds a `Partitioned` hash
    /// join the pass inserts an explicit `RepartitionExec(Hash(key), N)` so the
    /// join stays `Partitioned` instead of falling back to `CollectLeft` +
    /// `CoalescePartitionsExec` (the q72 regression shape). Scans on a
    /// `CollectLeft` build side, under a global sort, or under an unrecognized
    /// parent are left serial. N comes from
    /// `datafusion.execution.target_partitions`; only scans whose cached
    /// manifest byte size reaches `distribution_threshold` are parallelized.
    ///
    /// Default: false. Distinct from `parallel_probe_scan`, which keeps the
    /// `CollectLeft` shape and parallelizes only the probe side; enable one at
    /// a time. Stays opt-in until the q72 benchmark gate passes.
    #[serde(default)]
    pub parallel_scan: bool,
    /// Swap the dimension scan onto the build side of star-tail CollectLeft
    /// joins when the join-output side has no byte statistics but the dim
    /// scan has a known size under the broadcast threshold. Fixes the
    /// SSB q4.x class where cascaded cardinality underestimates keep the
    /// fact stream as the build and the dim's semijoin filter never reaches
    /// the fact scan. Default true; bounded downside by construction (a
    /// wrong swap collects at most broadcast-threshold bytes).
    #[serde(default = "default_true")]
    pub dim_build_swap: bool,
    /// Idle-timeout (seconds) for an active result stream. When the gRPC
    /// client has not pulled a batch within this window the coordinator
    /// aborts the stream and releases its concurrency permit. Bounds the
    /// damage from slow or malicious clients holding open Flight streams
    /// to pin every slot in `max_concurrent_queries`. Set to 0 to disable.
    /// Default: 300 (5 minutes). Issue #75.
    #[serde(default = "default_stream_idle_timeout")]
    pub stream_idle_timeout_secs: u64,
    /// Push the scan's filter predicate and (where safe) the query LIMIT into
    /// each distributed `ScanTask` so workers prune rows before shipping them
    /// over Flight (#233). The coordinator always keeps the authoritative
    /// `FilterExec` / `GlobalLimitExec` above `DistributedScanExec`, so this is
    /// a pure optimization: workers double-filtering or over-counting a limit
    /// cannot change results. Set to `false` to ship every projected row and
    /// rely solely on coordinator-side filtering.
    ///
    /// Default: true.
    #[serde(default = "default_true")]
    pub distributed_scan_pushdown: bool,
    /// Maximum distinct build-side join keys materialized as an IN-list
    /// dynamic filter (DataFusion's
    /// `optimizer.hash_join_inlist_pushdown_max_distinct_values`). Above
    /// this the filter degrades to an opaque hash-table probe that cannot
    /// be pushed into Iceberg scans or shipped to workers, so only min/max
    /// bounds prune -- which is worthless on uniformly distributed join
    /// keys (every SSB star query). DataFusion's default is 150; SSB-scale
    /// dimension filters carry 160-6500 keys.
    ///
    /// Default: 65536. Set to 0 to keep DataFusion's default.
    #[serde(default = "default_runtime_filter_inlist_max_values")]
    pub runtime_filter_inlist_max_values: usize,
    /// Companion byte cap for the IN-list dynamic filter (DataFusion's
    /// `optimizer.hash_join_inlist_pushdown_max_size`, default 128KB).
    /// Sized so `runtime_filter_inlist_max_values` keys of any fixed-width
    /// type fit. Default: "4MB". Set to "0" to keep DataFusion's default.
    #[serde(default = "default_runtime_filter_inlist_max_size")]
    pub runtime_filter_inlist_max_size: String,
    /// Cap on concurrently open partition writers per partitioned write (the
    /// bounded fanout writer). When a batch arrives for a new partition and
    /// the map is full, the least-recently-written writer is closed and
    /// flushed first, then the new one opens. Default 0 = auto (derived from
    /// the pool size, floored at 8 and capped at 64). Small deployments with
    /// few partitions never reach it. Cutover trades bounded memory for
    /// small-file debt, repaired by `system.rewrite_data_files`.
    #[serde(default)]
    pub fanout_max_open_writers: usize,
    /// Byte budget for total buffered fanout memory across open partition
    /// writers, same string format as `max_query_memory` ("512MB"). When the
    /// tracked estimate exceeds the budget, writers flush in
    /// least-recently-written order until under budget. Default "0" = auto
    /// (a fraction of the node pool, bounded below so one writer at full
    /// row-group size always fits).
    #[serde(default = "default_fanout_buffer_budget")]
    pub fanout_buffer_budget: String,
    /// Whether Layer A write-buffer pool reservations are active. Default
    /// true. Escape hatch: set false to disable the write-side memory
    /// accounting (never the streaming paths) if a deployment hits an
    /// accounting false positive. Documented as a diagnostic, not a tuning
    /// knob.
    #[serde(default = "default_true")]
    pub write_buffer_tracking: bool,
    /// Whether a copy-on-write MERGE streams its target from the pinned data
    /// files instead of materialising the whole target into a MemTable
    /// (write-path memory safety, Layer B phase B2). Default false: the
    /// buffered path (bounded by the Layer A `merge-target-buffer`) is used.
    /// When true, the target flows through the merge join as governed operator
    /// memory. Opt-in until validated against a live catalog. Requires
    /// `write_buffer_tracking` (ignored when tracking is off).
    #[serde(default)]
    pub merge_target_streaming: bool,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            default_catalog: None,
            catalog_discovery: CatalogDiscovery::default(),
            timeout_secs: default_query_timeout(),
            role_overrides: std::collections::HashMap::new(),
            max_result_rows: default_max_result_rows(),
            max_concurrent_queries: default_max_concurrent_queries(),
            max_concurrent_per_user: default_max_concurrent_per_user(),
            per_user_memory_budget: default_per_user_memory_budget(),
            slow_query_threshold_secs: default_slow_query_threshold(),
            query_profile: default_query_profile(),
            max_query_memory: default_max_query_memory(),
            distribution_threshold: default_distribution_threshold(),
            distribution_file_threshold: default_distribution_file_threshold(),
            target_task_size: default_target_task_size(),
            sort_mode: default_sort_mode(),
            late_materialization_min_projection_cols: default_late_mat_min_projection_cols(),
            star_schema_reorder: default_true(),
            star_schema_min_ratio: default_star_schema_min_ratio(),
            parallel_probe_scan: true,
            parallel_scan: false,
            dim_build_swap: true,
            stream_idle_timeout_secs: default_stream_idle_timeout(),
            distributed_scan_pushdown: default_true(),
            runtime_filter_inlist_max_values: default_runtime_filter_inlist_max_values(),
            runtime_filter_inlist_max_size: default_runtime_filter_inlist_max_size(),
            fanout_max_open_writers: 0,
            fanout_buffer_budget: default_fanout_buffer_budget(),
            write_buffer_tracking: default_true(),
            merge_target_streaming: false,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct QueryCacheConfig {
    #[serde(default = "default_cache_enabled")]
    pub enabled: bool,
    #[serde(default = "default_cache_max_memory_mb")]
    pub max_memory_mb: u64,
    #[serde(default = "default_cache_max_entry_mb")]
    pub max_entry_mb: u64,
    #[serde(default = "default_cache_ttl_secs")]
    pub ttl_secs: u64,
}

impl Default for QueryCacheConfig {
    fn default() -> Self {
        Self {
            enabled: default_cache_enabled(),
            max_memory_mb: default_cache_max_memory_mb(),
            max_entry_mb: default_cache_max_entry_mb(),
            ttl_secs: default_cache_ttl_secs(),
        }
    }
}

fn default_cache_enabled() -> bool { true }
fn default_cache_max_memory_mb() -> u64 { 256 }
fn default_cache_max_entry_mb() -> u64 { 5 }
fn default_cache_ttl_secs() -> u64 { 300 }

#[derive(Debug, Deserialize, Clone)]
pub struct QueryHistoryConfig {
    #[serde(default = "default_history_max_entries")]
    pub max_entries: u64,
    #[serde(default = "default_history_ttl_secs")]
    pub ttl_secs: u64,
}

impl Default for QueryHistoryConfig {
    fn default() -> Self {
        Self {
            max_entries: default_history_max_entries(),
            ttl_secs: default_history_ttl_secs(),
        }
    }
}

fn default_history_max_entries() -> u64 { 10000 }
fn default_history_ttl_secs() -> u64 { 1800 }

#[derive(Deserialize, Clone)]
pub struct CoordinatorConfig {
    #[serde(default = "default_flight_port")]
    pub flight_sql_port: u16,
    #[serde(default = "default_trino_port")]
    pub trino_http_port: u16,
    /// Port the DuckDB Quack RPC server listens on. Zero disables the Quack
    /// endpoint entirely. DuckDB's documented default for `quack:host` URIs
    /// is 9494, so enabling this with `quack_port = 9494` lets a DuckDB CLI
    /// attach without specifying a port.
    #[serde(default)]
    pub quack_port: u16,
    #[serde(default = "default_mode")]
    pub mode: String,
    /// List of worker Flight server URLs for distributed execution.
    /// Empty = single-node mode (all queries execute locally).
    #[serde(default)]
    pub worker_urls: Vec<String>,
    /// When `true`, error responses include the full error chain (dev only).
    /// When `false` (default / production), only sanitised messages are returned.
    #[serde(default)]
    pub debug: bool,
    /// Optional TLS configuration for the Flight SQL listener.
    #[serde(default)]
    pub tls: TlsConfig,
    /// Shared secret that workers must supply in the `x-sqe-worker-secret`
    /// metadata header when sending heartbeats. Must be non-empty whenever
    /// `worker_urls` is non-empty unless `allow_unauthenticated_workers` is
    /// explicitly set. Validation rejects the empty case at startup —
    /// configuring distributed mode without a secret used to be a logged
    /// warning, but that let any client on the cluster network register as
    /// a worker and exfiltrate user bearers along with query plans.
    #[serde(default)]
    pub worker_secret: SecretString,
    /// Opt-in escape hatch for the `worker_secret` requirement. Leaving this
    /// `false` (the default) makes the coordinator refuse to start when
    /// distributed mode is configured without a secret. Setting it `true`
    /// is visible in config diffs and acknowledges that any TCP-reachable
    /// client may register as a worker.
    #[serde(default)]
    pub allow_unauthenticated_workers: bool,
    /// Memory limit for the coordinator's DataFusion runtime.
    /// Accepts human-readable sizes: "8GB", "512MB", "4096MB".
    /// Default: "8GB". Applies to all query operator memory (sorts, joins, aggregates).
    #[serde(default = "default_coordinator_memory")]
    pub memory_limit: String,
    /// Memory pool strategy for the shared DataFusion runtime.
    ///
    /// - `"greedy"` (default): first-come first-served up to `memory_limit`;
    ///   spillable operators spill when the pool is genuinely full. A single
    ///   large consumer (a wide hash aggregate) may use the whole pool.
    /// - `"fair"`: the previous behavior (`FairSpillPool`). Divides the pool
    ///   evenly across every REGISTERED spillable consumer; wide plans
    ///   (many partitions x many operators) shrink each consumer's cap to
    ///   pool/N even when most consumers allocate nothing. TPC-DS q39 at
    ///   SF10 registers ~90 spillable consumers, capping each at ~95 MB of
    ///   an 8 GB pool, and DataFusion 53's partial aggregate cannot emit
    ///   early under a constant GROUP BY ordering key -- the raw
    ///   ResourcesExhausted error surfaces instead of spilling.
    ///   Cross-query fairness is enforced separately by
    ///   `query.per_user_memory_budget`.
    #[serde(default = "default_memory_pool")]
    pub memory_pool: String,
    /// Enable spill-to-disk when memory limit is reached. Default: true.
    #[serde(default = "default_true")]
    pub spill_to_disk: bool,
    /// Directory for spill files. Must be on fast local storage (SSD recommended).
    /// Default: "/tmp/sqe-coordinator-spill".
    #[serde(default = "default_coordinator_spill_dir")]
    pub spill_dir: String,
    /// Compression for spill files. "none", "lz4" (default), or "zstd".
    #[serde(default = "default_spill_compression")]
    pub spill_compression: String,
    /// Arrow Flight IPC compression for client-facing DoGet responses.
    /// Supported values: `"lz4"` (default), `"zstd"`, `"none"`.
    #[serde(default = "default_flight_compression")]
    pub flight_compression: String,
    /// Arrow Flight IPC compression for internal shuffle (DoExchange) transfers.
    /// Supported values: `"zstd"` (default), `"lz4"`, `"none"`.
    #[serde(default = "default_shuffle_compression")]
    pub shuffle_compression: String,
    /// Maximum number of workers the registry will track. Heartbeats from
    /// previously-unknown URLs are rejected once this cap is reached. Bounds
    /// the memory footprint of the registry when workers cycle through pod
    /// IPs in Kubernetes or report unstable URLs.
    #[serde(default = "default_max_workers")]
    pub max_workers: usize,
    /// HTTP/2 / gRPC transport tuning. Lifts the receiver windows off
    /// tonic's 64 KB default so Flight SQL DoGet streams are not forced
    /// to send a WINDOW_UPDATE every ~64 KB on a multi-GB result set.
    #[serde(default)]
    pub transport: GrpcTransportConfig,
    /// gRPC connect timeout (seconds) used when dispatching scan tasks to
    /// workers. Caps the time the coordinator will wait for TCP+TLS+HTTP/2
    /// handshake before failing the worker over. Issue #29.
    #[serde(default = "default_worker_connect_timeout")]
    pub worker_connect_timeout_secs: u64,
    /// gRPC request timeout (seconds) applied to each `do_get` from coordinator
    /// to worker. Must exceed `worker.scan_timeout_secs` (default 600s) so the
    /// worker's own abort path fires first and the coordinator sees a clean
    /// `DeadlineExceeded` instead of an unbounded await. Issue #29.
    #[serde(default = "default_worker_rpc_timeout")]
    pub worker_rpc_timeout_secs: u64,
    /// Maximum time the Flight SQL handshake will block waiting for the
    /// auth provider. Default: 30 s. Increase for slow OIDC providers.
    #[serde(default = "default_auth_handshake_timeout_secs")]
    pub auth_handshake_timeout_secs: u64,
    /// Interval between worker health checks. Default: 5 s.
    #[serde(default = "default_health_check_interval_secs")]
    pub health_check_interval_secs: u64,
    /// Consecutive failed health checks before a worker is marked unhealthy. Default: 3.
    #[serde(default = "default_health_check_max_failures")]
    pub health_check_max_failures: u32,
    /// How often the credential-refresh background loop runs. Default: 60 s.
    #[serde(default = "default_credential_refresh_interval_secs")]
    pub credential_refresh_interval_secs: u64,
    /// Connect timeout used when pushing refreshed credentials to a worker. Default: 5 s.
    #[serde(default = "default_credential_push_connect_timeout_secs")]
    pub credential_push_connect_timeout_secs: u64,
    /// Per-request timeout used when pushing refreshed credentials to a worker. Default: 10 s.
    #[serde(default = "default_credential_push_request_timeout_secs")]
    pub credential_push_request_timeout_secs: u64,
    /// Grace period (seconds) to keep the process alive after SIGTERM before
    /// shutting down the Flight server. On SIGTERM the readiness probe flips to
    /// NOT-ready first so the Kubernetes Service stops routing new work, then
    /// the process sleeps this long to let already-routed connections drain
    /// before the tonic graceful boundary cuts remaining streams. Keep this
    /// shorter than the pod's `terminationGracePeriodSeconds` so the process
    /// exits cleanly before SIGKILL. Default: 25 s. Issue #250.
    #[serde(default = "default_shutdown_drain_secs")]
    pub shutdown_drain_secs: u64,
}

/// HTTP/2 + TCP knobs applied to every tonic Server / Client this
/// binary opens.
///
/// Defaults: 8 MB stream window, 16 MB connection window, 1 MB frame,
/// 30 s HTTP/2 keepalive interval (10 s timeout), 60 s TCP keepalive.
/// They lift Flight throughput on SF10+ workloads and keep long-running
/// connections alive across NAT / load-balancer idle timeouts.
#[derive(Debug, Deserialize, Clone)]
pub struct GrpcTransportConfig {
    /// Per-stream receive window in bytes.
    #[serde(default = "default_initial_stream_window_size")]
    pub initial_stream_window_size: u32,
    /// Connection-level receive window in bytes.
    #[serde(default = "default_initial_connection_window_size")]
    pub initial_connection_window_size: u32,
    /// Maximum HTTP/2 frame size in bytes.
    #[serde(default = "default_max_frame_size")]
    pub max_frame_size: u32,
    /// HTTP/2 keepalive ping interval in seconds.
    #[serde(default = "default_http2_keepalive_interval_secs")]
    pub http2_keepalive_interval_secs: u64,
    /// HTTP/2 keepalive ping timeout in seconds.
    #[serde(default = "default_http2_keepalive_timeout_secs")]
    pub http2_keepalive_timeout_secs: u64,
    /// TCP keepalive in seconds. 0 disables.
    #[serde(default = "default_tcp_keepalive_secs")]
    pub tcp_keepalive_secs: u64,
}

impl Default for GrpcTransportConfig {
    fn default() -> Self {
        Self {
            initial_stream_window_size: default_initial_stream_window_size(),
            initial_connection_window_size: default_initial_connection_window_size(),
            max_frame_size: default_max_frame_size(),
            http2_keepalive_interval_secs: default_http2_keepalive_interval_secs(),
            http2_keepalive_timeout_secs: default_http2_keepalive_timeout_secs(),
            tcp_keepalive_secs: default_tcp_keepalive_secs(),
        }
    }
}

fn default_initial_stream_window_size() -> u32 {
    8 * 1024 * 1024
}
fn default_initial_connection_window_size() -> u32 {
    16 * 1024 * 1024
}
fn default_max_frame_size() -> u32 {
    1024 * 1024
}
fn default_http2_keepalive_interval_secs() -> u64 {
    30
}
fn default_http2_keepalive_timeout_secs() -> u64 {
    10
}
fn default_tcp_keepalive_secs() -> u64 {
    60
}

impl std::fmt::Debug for CoordinatorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoordinatorConfig")
            .field("flight_sql_port", &self.flight_sql_port)
            .field("trino_http_port", &self.trino_http_port)
            .field("quack_port", &self.quack_port)
            .field("mode", &self.mode)
            .field("worker_urls", &self.worker_urls)
            .field("debug", &self.debug)
            .field("tls", &self.tls)
            .field("worker_secret", &self.worker_secret)
            .field(
                "allow_unauthenticated_workers",
                &self.allow_unauthenticated_workers,
            )
            .field("memory_limit", &self.memory_limit)
            .field("spill_to_disk", &self.spill_to_disk)
            .field("spill_dir", &self.spill_dir)
            .field("spill_compression", &self.spill_compression)
            .field("flight_compression", &self.flight_compression)
            .field("shuffle_compression", &self.shuffle_compression)
            .field("max_workers", &self.max_workers)
            .field("transport", &self.transport)
            .field("worker_connect_timeout_secs", &self.worker_connect_timeout_secs)
            .field("worker_rpc_timeout_secs", &self.worker_rpc_timeout_secs)
            .field("auth_handshake_timeout_secs", &self.auth_handshake_timeout_secs)
            .field("health_check_interval_secs", &self.health_check_interval_secs)
            .field("health_check_max_failures", &self.health_check_max_failures)
            .field(
                "credential_refresh_interval_secs",
                &self.credential_refresh_interval_secs,
            )
            .field(
                "credential_push_connect_timeout_secs",
                &self.credential_push_connect_timeout_secs,
            )
            .field(
                "credential_push_request_timeout_secs",
                &self.credential_push_request_timeout_secs,
            )
            .field("shutdown_drain_secs", &self.shutdown_drain_secs)
            .finish()
    }
}

/// IPC body compression codec for Arrow Flight transfers.
///
/// Maps directly to `arrow_ipc::CompressionType` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlightCompression {
    /// No compression.
    None,
    /// LZ4 Frame -- fast decompression, good for client-facing responses.
    Lz4,
    /// Zstandard -- better compression ratio, good for internal shuffle.
    Zstd,
}

impl FlightCompression {
    /// Parse a config string into a `FlightCompression`.
    ///
    /// Accepted values (case-insensitive): `"lz4"`, `"zstd"`, `"none"`.
    pub fn from_config(s: &str) -> crate::error::Result<Self> {
        match s.trim().to_lowercase().as_str() {
            "lz4" | "lz4_frame" => Ok(Self::Lz4),
            "zstd" | "zstandard" => Ok(Self::Zstd),
            "none" | "" => Ok(Self::None),
            other => Err(crate::error::SqeError::Config(format!(
                "Unknown Flight compression codec '{other}'. Expected: lz4, zstd, or none"
            ))),
        }
    }
}

/// TLS configuration for gRPC (Flight SQL) and worker listeners.
///
/// When `cert_file` and `key_file` are both set, the server enables TLS.
/// If omitted, the server runs in plaintext (suitable for development).
#[derive(Debug, Deserialize, Clone, Default)]
pub struct TlsConfig {
    /// Path to PEM-encoded server certificate.
    #[serde(default)]
    pub cert_file: String,
    /// Path to PEM-encoded private key.
    #[serde(default)]
    pub key_file: String,
    /// Path to PEM-encoded CA certificate for client verification (mTLS).
    /// When set, the server requires clients to present a valid certificate
    /// signed by this CA.
    #[serde(default)]
    pub ca_file: String,
}

impl TlsConfig {
    /// Returns `true` when both cert and key are configured.
    pub fn is_enabled(&self) -> bool {
        !self.cert_file.is_empty() && !self.key_file.is_empty()
    }
}

#[derive(Deserialize, Clone)]
pub struct WorkerConfig {
    #[serde(default)]
    pub coordinator_url: String,
    #[serde(default = "default_worker_flight_port")]
    pub flight_port: u16,
    /// URL the coordinator should use to reach this worker's Flight service,
    /// sent verbatim in every heartbeat. When empty (the default) the worker
    /// derives a routable address at startup: the `POD_IP` / `HOSTNAME` env
    /// var (set on Kubernetes via the downward API), else the first
    /// non-loopback local interface address. Set this explicitly when the
    /// auto-derived address is wrong (NAT, multi-homed hosts, overlay
    /// networks). Never advertise 0.0.0.0: the coordinator rejects it because
    /// every worker would collide on one bogus loopback registry entry.
    #[serde(default)]
    pub advertise_url: String,
    #[serde(default = "default_heartbeat")]
    pub heartbeat_interval_secs: u64,
    #[serde(default = "default_memory")]
    pub memory_limit: String,
    #[serde(default = "default_true")]
    pub spill_to_disk: bool,
    #[serde(default = "default_spill_dir")]
    pub spill_dir: String,
    /// Maximum duration in seconds for a single scan task. Default: 600 (10 minutes).
    /// Set to 0 to disable the timeout.
    #[serde(default = "default_scan_timeout")]
    pub scan_timeout_secs: u64,
    /// Shared secret that the coordinator must supply in the
    /// `x-sqe-worker-secret` metadata header on every `do_get` and
    /// `do_action("refresh_credentials")` call. Must match
    /// `coordinator.worker_secret`. When empty the worker refuses to start
    /// unless `allow_unauthenticated = true` is set: a worker that accepts
    /// unauthenticated scan tickets leaks user S3 credentials to anyone with
    /// network reach to the Flight port.
    #[serde(default)]
    pub worker_secret: String,
    /// Opt-in escape hatch for the `worker_secret` requirement. Leaving
    /// `false` (the default) makes the worker refuse to start with an empty
    /// secret. Setting `true` accepts the documented risk: any TCP-reachable
    /// client may push scan tasks or swap S3 credentials.
    #[serde(default)]
    pub allow_unauthenticated: bool,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            coordinator_url: String::new(),
            flight_port: default_worker_flight_port(),
            advertise_url: String::new(),
            heartbeat_interval_secs: default_heartbeat(),
            memory_limit: default_memory(),
            spill_to_disk: true,
            spill_dir: default_spill_dir(),
            scan_timeout_secs: default_scan_timeout(),
            worker_secret: String::new(),
            allow_unauthenticated: false,
        }
    }
}

// Hand-written Debug so `worker_secret` (a live shared credential) is never
// printed by `{:?}`, an anyhow chain, or a panic message (CORE-01). A bool
// presence sentinel is enough for operators to tell whether it is set.
impl std::fmt::Debug for WorkerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerConfig")
            .field("coordinator_url", &self.coordinator_url)
            .field("flight_port", &self.flight_port)
            .field("advertise_url", &self.advertise_url)
            .field("heartbeat_interval_secs", &self.heartbeat_interval_secs)
            .field("memory_limit", &self.memory_limit)
            .field("spill_to_disk", &self.spill_to_disk)
            .field("spill_dir", &self.spill_dir)
            .field("scan_timeout_secs", &self.scan_timeout_secs)
            .field(
                "worker_secret",
                if self.worker_secret.is_empty() {
                    &"<empty>"
                } else {
                    &"[REDACTED]"
                },
            )
            .field("allow_unauthenticated", &self.allow_unauthenticated)
            .finish()
    }
}

/// Parse a human-readable memory size string (e.g. "1GB", "512MB", "1024") into bytes.
///
/// Supported suffixes (case-insensitive): `B`, `KB`/`K`, `MB`/`M`, `GB`/`G`, `TB`/`T`.
/// A bare number without a suffix is interpreted as bytes.
pub fn parse_memory_limit(s: &str) -> crate::error::Result<usize> {
    let s = s.trim();
    if s.is_empty() {
        return Err(crate::error::SqeError::Config(
            "Empty memory limit string".to_string(),
        ));
    }

    // Find where the numeric part ends and the suffix begins
    let (num_str, suffix) = match s.find(|c: char| !c.is_ascii_digit() && c != '.') {
        Some(idx) => (&s[..idx], s[idx..].trim().to_uppercase()),
        None => (s, String::new()),
    };

    let num: f64 = num_str.parse().map_err(|e| {
        crate::error::SqeError::Config(format!("Invalid memory limit number '{num_str}': {e}"))
    })?;

    let multiplier: f64 = match suffix.as_str() {
        "" | "B" => 1.0,
        "K" | "KB" => 1024.0,
        "M" | "MB" => 1024.0 * 1024.0,
        "G" | "GB" => 1024.0 * 1024.0 * 1024.0,
        "T" | "TB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        other => {
            return Err(crate::error::SqeError::Config(format!(
                "Unknown memory limit suffix '{other}' in '{s}'"
            )))
        }
    };

    // Bound the product before the `as usize` cast. A bare `as usize` on an
    // oversized value (e.g. "99999999TB") saturates to `usize::MAX` rather than
    // erroring, and that value feeds task sizing and pool budgets downstream.
    // Reject non-finite, negative, or out-of-range results instead (CORE-02).
    // The f64 parse is kept so fractional sizes ("1.5GB") still work.
    let product = num * multiplier;
    if !product.is_finite() || product < 0.0 || product > usize::MAX as f64 {
        return Err(crate::error::SqeError::Config(format!(
            "Memory limit '{s}' is out of range"
        )));
    }

    Ok(product as usize)
}

/// Configuration for a single auth provider in the `[[auth.providers]]` array.
///
/// Each variant maps to a concrete `AuthProvider` implementation in `sqe-auth`.
/// The `type` field in TOML selects the variant via the serde tag.
#[derive(Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthProviderConfig {
    /// OIDC Resource Owner Password Credentials (ROPC) grant.
    /// Works with any OIDC provider (Keycloak, Auth0, Okta, Zitadel, etc.).
    OidcPassword {
        /// Full token endpoint URL.
        token_url: String,
        /// OAuth2 client_id.
        client_id: String,
        /// OAuth2 client_secret. Empty string for public clients.
        #[serde(default)]
        client_secret: String,
        /// Dot-separated JSON path to the roles array in the JWT payload.
        /// Default: `"realm_access.roles"`.
        #[serde(default = "default_roles_claim")]
        roles_claim: String,
        /// JWT claim for the subject identifier (`sub`). Default: `"sub"`.
        /// Distinct from `user_claim`/`user_id`; used to populate `Identity::subject`.
        #[serde(default = "default_user_claim")]
        subject_claim: String,
        /// JWT claim path for the user's email address. Empty string disables extraction.
        #[serde(default)]
        email_claim: String,
        /// Dot-separated JSON path to the groups array in the JWT payload.
        /// Empty string disables extraction. Separate from `roles_claim`.
        #[serde(default)]
        groups_claim: String,
        /// When `true`, a token-endpoint *rejection* of the ROPC grant returns
        /// `NotMyCredentials` (defer to the next provider) instead of
        /// `AuthFailed` (stop the chain). Set this on a mixed Basic-auth
        /// listener so a `client_id`/`client_secret` that is not a valid user
        /// falls through to `client_credentials_passthrough`. Default `false`
        /// preserves single-provider behavior. (#276)
        #[serde(default)]
        fallthrough_on_reject: bool,
    },
    /// Generic OAuth2 client_credentials grant (e.g. Polaris service token).
    ClientCredentials {
        /// OAuth2 token endpoint URL.
        token_endpoint: String,
        /// OAuth2 client_id.
        client_id: String,
        /// OAuth2 client_secret.
        client_secret: String,
    },
    /// Per-connection OAuth2 `client_credentials` passthrough. The end-user
    /// client presents its OWN `client_id`/`client_secret` on the connection
    /// (Basic auth: username = client_id, password = client_secret); SQE runs
    /// the grant per connection and forwards the resulting token to the catalog.
    /// No credentials live in config: that is the difference from
    /// `ClientCredentials`. It consumes username/password, so to share a
    /// listener with `OidcPassword` (mixed human + service-principal Basic
    /// auth) set `fallthrough_on_reject = true` on the `OidcPassword` provider
    /// that precedes it; otherwise deploy it as the sole username/password
    /// provider (service-principal-only access). (#276)
    ClientCredentialsPassthrough {
        /// Full OAuth2 token endpoint URL.
        token_url: String,
        /// Dot-separated JSON path to the roles array in the JWT payload.
        /// Default: `"realm_access.roles"`.
        #[serde(default = "default_roles_claim")]
        roles_claim: String,
        /// JWT claim for the subject identifier (`sub`). Default: `"sub"`.
        #[serde(default = "default_user_claim")]
        subject_claim: String,
        /// Optional OAuth `scope`, sent only when set. No Polaris-specific
        /// default.
        #[serde(default)]
        scope: Option<String>,
        /// When `true`, a token-endpoint *rejection* of the client_credentials
        /// grant returns `NotMyCredentials` instead of `AuthFailed`, so a
        /// human ROPC credential that is not a valid client falls through to a
        /// following `oidc_password` provider. Default `false`. (#276)
        #[serde(default)]
        fallthrough_on_reject: bool,
    },
    /// OAuth2 Token Exchange (RFC 8693) — exchanges an incoming credential for a
    /// user-scoped JWT via an OIDC token endpoint. Catch-all; place last in chain.
    TokenExchange {
        /// Full token endpoint URL.
        token_url: String,
        /// OAuth2 client_id.
        client_id: String,
        /// OAuth2 client_secret. Optional for public clients.
        #[serde(default)]
        client_secret: Option<String>,
        /// Target audience for the exchanged token (e.g. `"polaris"`).
        #[serde(default)]
        audience: Option<String>,
        /// JWT claim that carries the user identifier. Default: `"sub"`.
        #[serde(default = "default_user_claim")]
        user_claim: String,
        /// Dot-separated JSON path to the roles array in the JWT payload.
        /// Default: `"realm_access.roles"`.
        #[serde(default = "default_roles_claim")]
        roles_claim: String,
    },
    /// Pre-obtained JWT validated via JWKS. For programmatic clients, SSO flows, Airflow.
    BearerToken {
        /// JWKS endpoint URL for signature verification.
        jwks_url: String,
        /// Expected issuer (`iss` claim). Optional.
        #[serde(default)]
        issuer: Option<String>,
        /// Expected audience (`aud` claim). Required by default; see
        /// `allow_unbounded_audience` for the explicit opt-out. Without
        /// audience binding, any JWT signed by the configured JWKS issuer
        /// would be accepted (confused-deputy across SaaS apps sharing
        /// the IdP). Issue #8.
        #[serde(default)]
        audience: Option<String>,
        /// JWT claim for user identity. Default: `"sub"`.
        #[serde(default = "default_user_claim")]
        user_claim: String,
        /// Dot-separated JSON path to roles. Default: `"realm_access.roles"`.
        #[serde(default = "default_roles_claim")]
        roles_claim: String,
        /// JWT claim for the subject identifier (`sub`). Default: `"sub"`.
        /// Distinct from `user_claim`/`user_id`; used to populate `Identity::subject`.
        #[serde(default = "default_user_claim")]
        subject_claim: String,
        /// JWT claim path for the user's email address. Empty string disables extraction.
        #[serde(default)]
        email_claim: String,
        /// Dot-separated JSON path to the groups array in the JWT payload.
        /// Empty string disables extraction. Separate from `roles_claim`.
        #[serde(default)]
        groups_claim: String,
        /// Explicit opt-in to accept tokens with any audience. Default
        /// `false`: a missing/empty `audience` then errors at startup.
        /// Setting `true` acknowledges that tokens issued for any service
        /// sharing the IdP will be accepted.
        #[serde(default)]
        allow_unbounded_audience: bool,
        /// Explicit opt-in to allow a non-`https` `jwks_url`. Default
        /// `false`: an `http://` JWKS endpoint then errors at startup. The
        /// JWKS is the highest-trust input in the auth path; over plaintext
        /// an on-path attacker can substitute the signing keys and forge
        /// identities. Set `true` only for local/dev setups.
        #[serde(default)]
        allow_insecure_jwks: bool,
    },
    /// AWS IAM authentication via STS GetCallerIdentity.
    AwsIam {
        /// AWS region for STS endpoint. Default: `"us-east-1"`.
        #[serde(default = "default_aws_region")]
        region: String,
        /// Whether to validate credentials via STS call. Default: true.
        #[serde(default = "default_true")]
        validate_with_sts: bool,
    },
    /// API key authentication from a keys file.
    ApiKey {
        /// Path to the TOML file containing API key entries.
        keys_file: String,
        /// Prefix that identifies an API key (default: `"sqe_"`).
        #[serde(default = "default_api_key_prefix")]
        key_prefix: String,
    },
    /// Mutual TLS client certificate authentication.
    Mtls {
        /// Whether to extract OU from the cert subject as a group.
        #[serde(default = "default_true")]
        extract_ou: bool,
        /// Whether to extract SAN DNS names as groups.
        #[serde(default)]
        extract_san: bool,
    },
    /// Fixed-identity provider for development/testing.
    Anonymous {
        /// User name to assign. Default: `"anonymous"`.
        #[serde(default = "default_anonymous_user")]
        user: String,
        /// Roles to assign. Default: empty.
        #[serde(default)]
        roles: Vec<String>,
    },
    /// Accepts any non-empty bearer and forwards it to the catalog without
    /// local validation. Intended for deployments where an upstream proxy
    /// has already validated the JWT, or for dev environments where the
    /// catalog itself is the source of truth.
    BearerPassthrough {
        /// User name to assign. Default: `"bearer-passthrough"`.
        #[serde(default = "default_bearer_passthrough_user")]
        user: String,
        /// Roles to assign. Default: empty.
        #[serde(default)]
        roles: Vec<String>,
    },
}

// Hand-written Debug so the OAuth `client_secret`s on the three secret-bearing
// variants are never printed by `{:?}`, an anyhow chain, or a panic message
// (CORE-01). Non-secret variants fall through to a variant-name-only summary
// (the catch-all also guards against a future variant leaking a secret without
// a matching arm). Note `AuthConfig`'s own Debug already summarizes the
// provider list as a count; this protects a stray `{:?}` of a single provider.
impl std::fmt::Debug for AuthProviderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthProviderConfig::OidcPassword {
                token_url,
                client_id,
                roles_claim,
                ..
            } => f
                .debug_struct("OidcPassword")
                .field("token_url", token_url)
                .field("client_id", client_id)
                .field("client_secret", &"[REDACTED]")
                .field("roles_claim", roles_claim)
                .finish(),
            AuthProviderConfig::ClientCredentials {
                token_endpoint,
                client_id,
                ..
            } => f
                .debug_struct("ClientCredentials")
                .field("token_endpoint", token_endpoint)
                .field("client_id", client_id)
                .field("client_secret", &"[REDACTED]")
                .finish(),
            AuthProviderConfig::TokenExchange {
                token_url,
                client_id,
                ..
            } => f
                .debug_struct("TokenExchange")
                .field("token_url", token_url)
                .field("client_id", client_id)
                .field("client_secret", &"[REDACTED]")
                .field("..", &"<other fields elided>")
                .finish(),
            other => {
                let name = match other {
                    AuthProviderConfig::BearerToken { .. } => "BearerToken",
                    AuthProviderConfig::AwsIam { .. } => "AwsIam",
                    AuthProviderConfig::ApiKey { .. } => "ApiKey",
                    AuthProviderConfig::Mtls { .. } => "Mtls",
                    AuthProviderConfig::Anonymous { .. } => "Anonymous",
                    AuthProviderConfig::ClientCredentialsPassthrough { .. } => {
                        "ClientCredentialsPassthrough"
                    }
                    AuthProviderConfig::BearerPassthrough { .. } => "BearerPassthrough",
                    // The three secret-bearing variants are handled above.
                    _ => "AuthProviderConfig",
                };
                f.debug_struct(name).finish_non_exhaustive()
            }
        }
    }
}

fn default_bearer_passthrough_user() -> String {
    "bearer-passthrough".to_string()
}

fn default_roles_claim() -> String {
    "realm_access.roles".to_string()
}
fn default_aws_region() -> String {
    "us-east-1".to_string()
}
fn default_user_claim() -> String {
    "sub".to_string()
}
fn default_anonymous_user() -> String {
    "anonymous".to_string()
}
fn default_api_key_prefix() -> String {
    "sqe_".to_string()
}

#[derive(Deserialize, Clone)]
pub struct AuthConfig {
    /// Legacy: Keycloak base URL. Deprecated in favor of `[[auth.providers]]`.
    #[serde(default)]
    pub keycloak_url: String,
    /// Legacy: Keycloak realm. Deprecated in favor of `[[auth.providers]]`.
    #[serde(default)]
    pub realm: String,
    /// Legacy: OAuth2 client_id (used when `providers` is empty for backward compat).
    #[serde(default)]
    pub client_id: String,
    /// Legacy: OAuth2 client_secret.
    #[serde(default)]
    pub client_secret: SecretString,
    /// Legacy: Generic OAuth2 token endpoint for client_credentials grant.
    /// When set (and keycloak_url is empty), the engine uses client_credentials mode.
    #[serde(default)]
    pub token_endpoint: String,
    #[serde(default = "default_refresh_buffer")]
    pub token_refresh_buffer_secs: u64,
    /// Verify TLS certificates for OIDC/OAuth endpoints.
    /// Deprecated: use `tls_skip_verify = true` instead of `ssl_verification = false`.
    #[serde(default = "default_true")]
    pub ssl_verification: bool,
    /// Skip TLS certificate verification (default: false).
    /// When true, equivalent to `ssl_verification = false`.
    /// Clearer naming: `true` means "skip verification" (insecure).
    #[serde(default)]
    pub tls_skip_verify: bool,
    /// Dot-separated JSON path to the roles claim in the legacy OIDC password
    /// grant JWT payload. Default: `"realm_access.roles"` (Keycloak shape).
    /// Set to e.g. `"groups"` for Auth0/Okta/AzureAD whose tokens carry roles
    /// at the top level rather than under `realm_access`. Mirrors the
    /// per-provider `roles_claim` on `[[auth.providers]]` so the legacy
    /// authenticator path also reads custom claim paths (issue #13).
    #[serde(default = "default_roles_claim")]
    pub roles_claim: String,
    /// Explicit provider chain. When non-empty, takes precedence over the
    /// legacy `keycloak_url` / `token_endpoint` fields.
    #[serde(default)]
    pub providers: Vec<AuthProviderConfig>,
    /// Group/ARN → roles mapping shared across providers that support it.
    /// Keys are group names or ARN patterns, values are role lists.
    #[serde(default)]
    pub role_mappings: std::collections::HashMap<String, Vec<String>>,
    /// Interactive OIDC flows (auth code + PKCE, device code).
    /// Maps to `[auth.external]` in TOML.
    #[serde(default)]
    pub external: Option<ExternalAuthConfig>,
    /// Roles that may execute coordinator-wide DDL: ATTACH, DETACH,
    /// CREATE SECRET, DROP SECRET, SHOW SECRETS. Every authenticated
    /// session used to be able to mount arbitrary catalog backends or
    /// stash arbitrary credentials in process memory; that surface is
    /// now gated behind the roles listed here (issue #3). Default:
    /// `["service_admin", "catalog_admin"]`.
    #[serde(default = "default_admin_roles")]
    pub admin_roles: Vec<String>,
}

fn default_admin_roles() -> Vec<String> {
    vec!["service_admin".to_string(), "catalog_admin".to_string()]
}

impl AuthConfig {
    /// Returns true if TLS certificate verification should be skipped.
    /// Combines `tls_skip_verify` (new, clear) with `ssl_verification` (legacy, inverted).
    /// Either `tls_skip_verify = true` OR `ssl_verification = false` triggers skip.
    pub fn should_skip_tls_verify(&self) -> bool {
        self.tls_skip_verify || !self.ssl_verification
    }

    /// Returns `true` if any of the given role names is in `admin_roles`.
    /// Empty `admin_roles` (an operator who explicitly disabled the gate)
    /// returns `false` — admin-only statements then fail closed.
    pub fn has_admin_role(&self, roles: &[String]) -> bool {
        if self.admin_roles.is_empty() {
            return false;
        }
        roles
            .iter()
            .any(|r| self.admin_roles.iter().any(|admin| admin == r))
    }
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthConfig")
            .field("keycloak_url", &self.keycloak_url)
            .field("realm", &self.realm)
            .field("client_id", &self.client_id)
            .field("client_secret", &self.client_secret)
            .field("token_endpoint", &self.token_endpoint)
            .field("token_refresh_buffer_secs", &self.token_refresh_buffer_secs)
            .field("ssl_verification", &self.ssl_verification)
            .field("providers", &format!("[{} provider(s)]", self.providers.len()))
            .field("role_mappings", &format!("[{} mapping(s)]", self.role_mappings.len()))
            .field("external", &self.external.as_ref().map(|e| format!("issuer={}", e.issuer)))
            .finish()
    }
}

/// Configuration for interactive OIDC flows (device code, Trino external auth).
#[derive(Debug, Deserialize, Clone)]
pub struct ExternalAuthConfig {
    pub issuer: String,
    pub client_id: String,
    #[serde(default)]
    pub client_secret: Option<String>,
    #[serde(default = "default_redirect_uri")]
    pub redirect_uri: String,
    #[serde(default = "default_external_scopes")]
    pub scopes: Vec<String>,
    #[serde(default = "default_challenge_timeout")]
    pub challenge_timeout_secs: u64,
    #[serde(default)]
    pub authorization_endpoint: Option<String>,
    #[serde(default)]
    pub token_endpoint: Option<String>,
    #[serde(default)]
    pub device_authorization_endpoint: Option<String>,
    #[serde(default)]
    pub device: Option<DeviceAuthConfig>,
    #[serde(default)]
    pub accept_invalid_certs: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DeviceAuthConfig {
    pub client_id: String,
    #[serde(default = "default_external_scopes")]
    pub scopes: Vec<String>,
}

fn default_redirect_uri() -> String {
    "http://localhost:8080/oauth2/callback".to_string()
}
fn default_external_scopes() -> Vec<String> {
    vec!["openid".to_string(), "profile".to_string()]
}
fn default_challenge_timeout() -> u64 {
    900
}

/// Selectable catalog backend. Defaults to `Rest` so existing TOML
/// configurations keep working unchanged.
///
/// The non-REST variants point at the per-backend constructors in
/// `crates/sqe-catalog/src/backends/`; selecting one routes the
/// engine session manager through that backend's iceberg::Catalog
/// implementation instead of through the REST client. REST-specific
/// SessionCatalog methods (view DDL, commit_schema_update through
/// raw REST) error out under non-REST backends.
#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum CatalogBackend {
    /// Iceberg REST (Polaris, Nessie, Unity OSS, AWS Glue REST,
    /// AWS S3 Tables). Configured via `catalog_url` + `warehouse` on
    /// CatalogConfig. AWS endpoints engage SigV4 automatically when
    /// the server's /v1/config response advertises
    /// `rest.sigv4-enabled=true`.
    #[default]
    Rest,
    /// Hive Metastore over Thrift. Requires the `hms` cargo feature
    /// on sqe-catalog. `uri` is `host:port` of the metastore.
    Hms {
        uri: String,
        #[serde(default)]
        warehouse: String,
    },
    /// AWS Glue Data Catalog over the AWS SDK. Requires the `glue`
    /// cargo feature. `region` mandatory; `endpoint` optional for
    /// LocalStack.
    Glue {
        region: String,
        #[serde(default)]
        warehouse: String,
        #[serde(default)]
        endpoint: Option<String>,
    },
    /// JDBC catalog (Postgres, MySQL, SQLite) via iceberg-catalog-sql.
    /// Requires the `sql-postgres` cargo feature. `url` is the JDBC
    /// connection string; `warehouse` is the on-disk path used for
    /// new tables when the DB doesn't carry one.
    Jdbc {
        url: String,
        #[serde(default)]
        warehouse: String,
    },
    /// AWS S3 Tables (managed Iceberg). Requires the `s3tables`
    /// cargo feature on sqe-catalog. `table_bucket_arn` is the
    /// fully-qualified ARN of the S3 Tables bucket
    /// (`arn:aws:s3tables:REGION:ACCOUNT:bucket/NAME`).
    /// `endpoint_url` is optional and only needed when targeting a
    /// non-default S3 Tables endpoint (LocalStack, custom region
    /// override).
    S3tables {
        table_bucket_arn: String,
        #[serde(default)]
        endpoint_url: Option<String>,
    },
}

/// Per-catalog auth strategy. When present on a `CatalogConfig`,
/// overrides the session bearer token for that catalog only. Used
/// for federation where one catalog speaks to Polaris (session
/// token) and a sibling speaks to a partner Iceberg REST endpoint
/// behind its own OAuth client.
#[derive(Debug, Deserialize, Clone, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CatalogAuthConfig {
    /// Use the user's session bearer token from the top-level
    /// `[auth]` block. This is the default and matches V6
    /// behaviour. Configured explicitly only when the operator
    /// wants the intent visible in the TOML.
    #[default]
    SessionBearer,
    /// A pre-issued static bearer token. Read from a separate
    /// secrets file or env override at deploy time. Useful for
    /// internal gateways and integration tests.
    Static { token: String },
    /// No `Authorization` header at all. Used for public Nessie
    /// read-only access and other anonymous endpoints.
    Anonymous,
    /// OAuth2 `client_credentials` grant against a per-catalog
    /// token endpoint. The token is fetched at session-build time
    /// and reused for the session lifetime; refresh on expiry is
    /// a future change.
    ClientCredentials {
        /// Full URL of the token endpoint (e.g.
        /// `https://partner.com/oauth/tokens`).
        token_endpoint: String,
        client_id: String,
        client_secret: String,
        /// Optional scope passed in the form body. Defaults to
        /// `PRINCIPAL_ROLE:ALL` for Polaris compatibility.
        #[serde(default)]
        scope: Option<String>,
    },
    /// Rely on the AWS SDK provider chain. Used by `glue` and
    /// `s3tables` backends and by AWS REST endpoints that engage
    /// SigV4 based on `/v1/config`. No token is fetched at the
    /// SQE level; the AWS SDK handles credentials.
    Aws,
}


#[derive(Debug, Deserialize, Clone)]
pub struct CatalogConfig {
    /// REST catalog endpoint URL. Used by `CatalogBackend::Rest`
    /// (the default) for Polaris, Nessie, Unity OSS, AWS Glue REST,
    /// and AWS S3 Tables. Other backends (`Hms`, `Glue`, `Jdbc`,
    /// `S3tables`) carry their own connection details on the enum
    /// variant and ignore this field.
    ///
    /// Old configs that used `polaris_url` continue to deserialize
    /// thanks to the serde alias.
    #[serde(alias = "polaris_url")]
    pub catalog_url: String,
    #[serde(default)]
    pub warehouse: String,
    /// Backend selector. When omitted from TOML, deserialises to
    /// `CatalogBackend::Rest` so the existing `catalog_url` field
    /// keeps driving the engine. Non-REST variants source their own
    /// connection details from the enum.
    #[serde(default)]
    pub backend: CatalogBackend,
    #[serde(default = "default_cache_ttl")]
    pub metadata_cache_ttl_secs: u64,
    /// Default Iceberg table format version for new tables (2 or 3).
    #[serde(default = "default_table_format_version")]
    pub default_table_format_version: u8,
    /// Trust Iceberg sort order metadata for ALL columns, not just partition keys.
    /// When true, DataFusion may skip redundant sorts based on Iceberg metadata.
    /// Default false: safer for mixed-writer environments (Spark, Trino, SQE).
    /// Only enable when you know all data files are physically sorted.
    #[serde(default)]
    pub trust_sort_order: bool,
    /// Hide namespace NAMES the caller holds no grants in from metadata
    /// listings (`SHOW SCHEMAS`, `information_schema.schemata`, Flight SQL
    /// `GetDbSchemas`). REST/Polaris backend only: each listed namespace is
    /// probed once per session-catalog build with the caller's bearer
    /// (Polaris `LOAD_NAMESPACE_METADATA`); a 403 drops the name. Any other
    /// probe failure fails OPEN and keeps the name — namespace contents stay
    /// protected by the per-operation checks regardless. Single-identity
    /// backends (Glue/HMS/JDBC/Hadoop) skip the filter: there is no
    /// per-caller identity to scope the list to. Default: true.
    #[serde(default = "default_true")]
    pub namespace_visibility_filter: bool,
    /// Maximum file size in MB for the direct-read fast path.
    ///
    /// When all data files in a scan are smaller than this threshold, SQE reads
    /// each file entirely in a single S3 GET and parses Parquet from memory,
    /// bypassing iceberg-rust's `scan.to_arrow()` pipeline (which issues
    /// additional HEAD, footer, and manifest requests). For ClickBench-style
    /// queries this reduces per-query S3 overhead from 5–7 requests to 1.
    ///
    /// Set to 0 to disable the fast path and always use iceberg-rust's pipeline.
    /// Default: 3 MB.
    #[serde(default = "default_small_file_threshold_mb")]
    pub small_file_threshold_mb: u64,
    /// Parquet compression codec for writes (CTAS, INSERT, etc.).
    ///
    /// Supported values: `"zstd"` (default), `"lz4"`, `"snappy"`, `"none"`.
    /// ZSTD level 3 is used when `"zstd"` is selected — a good balance of
    /// compression ratio vs. speed for S3.
    #[serde(default = "default_parquet_compression")]
    pub parquet_compression: String,
    /// Concurrency for loading Iceberg manifests during query-time column
    /// statistics pruning and CoW write paths.
    ///
    /// Each manifest is a separate S3 GET. On wide snapshots the sequential
    /// walk dominates cold-cache plan latency; loading manifests in parallel
    /// collapses that to roughly one round trip. Warm-cache reads are served
    /// from the iceberg-rust `ObjectCache` and ignore this knob.
    ///
    /// Default: 64.
    #[serde(default = "default_manifest_concurrency")]
    pub manifest_concurrency: usize,
    /// Two-tier dynamic runtime-filter pushdown tuning (issue #132).
    #[serde(default)]
    pub runtime_filters: RuntimeFiltersConfig,
    /// Per-catalog auth override. When unset (the common case),
    /// SQE uses the user's session bearer token from the top-level
    /// `[auth]` block, matching V6 behaviour. Set this on
    /// `[catalogs.<name>.auth]` blocks when one catalog needs a
    /// different token source: a partner Polaris with its own
    /// OAuth client, an anonymous Nessie read-only endpoint, an
    /// AWS-SDK-provider-chain Glue endpoint, etc.
    #[serde(default)]
    pub auth: Option<CatalogAuthConfig>,
    /// Per-catalog storage override. When unset, the global
    /// `[storage]` block applies. Set this on
    /// `[catalogs.<name>.storage]` to point a single catalog at a
    /// different S3 endpoint, region, or credential pair (e.g.
    /// one Ceph cluster + one AWS S3, or a partner bucket with
    /// its own access key). Iceberg credential vending from REST
    /// catalogs still wins per-table over both this and the
    /// global block.
    #[serde(default)]
    pub storage: Option<StorageConfig>,
}

/// Tuning for the two-tier dynamic runtime-filter pushdown (MR #220, issue #132).
///
/// Tier 1 feeds runtime filters into iceberg-rust's manifest / row-group /
/// page-index pruning at file open; Tier 2 re-applies them per batch. Tier 1 is
/// a large win on fact tables clustered on the filter column (tight per-file
/// min/max) and pure overhead on uniformly-distributed tables, where bounds
/// pruning cannot skip anything. The clustering gate below inspects the
/// already-loaded manifest bounds and skips Tier-1 registration when every
/// filter column is effectively uniform.
#[derive(Debug, Deserialize, Clone)]
pub struct RuntimeFiltersConfig {
    /// When true, skip Tier-1 registration on scans whose planned files are
    /// effectively uniform on every filter column. Tier-2 still applies.
    ///
    /// Default `false`: always register Tier-1 (the MR #220 behavior). The gate
    /// is additive and benchmark-tuned per workload before being enabled; a
    /// wrong answer only falls back to the current behavior.
    #[serde(default)]
    pub clustering_skip_enabled: bool,
    /// A file whose per-column `[lower, upper]` covers more than this fraction
    /// of the snapshot-wide range is "uniform" on that column. A scan is gated
    /// out of Tier-1 only when every decidable filter column's median per-file
    /// spread is at or above this threshold. Default `0.8`.
    #[serde(default = "default_runtime_filter_uniform_threshold")]
    pub uniform_threshold: f64,
    /// Bounded wait (milliseconds) at scan-stream open for pending hash-join
    /// dynamic filters to seal, mirroring the distributed dispatch wait and
    /// Trino's split-generation wait. Dimension build sides typically seal
    /// within ~100ms; waiting lets the scan apply their key sets BEFORE
    /// decoding instead of filtering decoded batches afterwards. Set to 0
    /// to open scans immediately (the pre-existing behavior).
    ///
    /// Default: 100.
    #[serde(default = "default_runtime_filter_wait_ms")]
    pub wait_ms: u64,
}

impl Default for RuntimeFiltersConfig {
    fn default() -> Self {
        Self {
            clustering_skip_enabled: false,
            uniform_threshold: default_runtime_filter_uniform_threshold(),
            wait_ms: default_runtime_filter_wait_ms(),
        }
    }
}

fn default_runtime_filter_uniform_threshold() -> f64 {
    0.8
}

fn default_runtime_filter_wait_ms() -> u64 {
    100
}

#[derive(Deserialize, Clone)]
pub struct StorageConfig {
    #[serde(default)]
    pub s3_endpoint: String,
    #[serde(default)]
    pub s3_region: String,
    #[serde(default)]
    pub s3_access_key: String,
    #[serde(default)]
    pub s3_secret_key: SecretString,
    #[serde(default)]
    pub s3_path_style: bool,
    /// Allow plaintext HTTP for S3 endpoints. Only enable for dev/test (e.g., MinIO).
    #[serde(default)]
    pub s3_allow_http: bool,
    /// Byte-range coalescing threshold. Ranges separated by a gap of at most
    /// this many bytes are merged into a single S3 GET request. Default: "1MB".
    #[serde(default = "default_coalesce_threshold")]
    pub coalesce_threshold: String,
    /// Maximum size of the Parquet footer (metadata) cache. Default: "256MB".
    #[serde(default = "default_footer_cache_size")]
    pub footer_cache_size: String,
    /// Maximum number of concurrent byte-range requests per file. Default: 4.
    #[serde(default = "default_concurrent_requests_per_file")]
    pub concurrent_requests_per_file: usize,
    /// Maximum number of files fetched concurrently. Default: 8.
    #[serde(default = "default_max_concurrent_files")]
    pub max_concurrent_files: usize,
    /// Prefetch buffer size for overlapping footer reads. Default: "32MB".
    #[serde(default = "default_prefetch_buffer")]
    pub prefetch_buffer: String,
    /// Maximum number of S3 files to prefetch concurrently during scan.
    /// Higher values improve throughput on high-latency S3 connections.
    /// Default: 4. Set higher (8-16) for WAN or high-latency storage.
    #[serde(default = "default_prefetch_concurrency")]
    pub prefetch_concurrency: usize,

    // ── Azure ADLS Gen2 / Blob ──────────────────────────────────────────
    /// Azure storage account name (required for shared-key / SAS auth).
    #[serde(default)]
    pub azure_account: String,
    /// Azure storage account access key (shared-key auth).
    #[serde(default)]
    pub azure_access_key: String,
    /// Azure SAS token (alternative to shared key).
    #[serde(default)]
    pub azure_sas_token: String,
    /// Use the Azurite storage emulator (for local development).
    #[serde(default)]
    pub azure_use_emulator: bool,

    // ── Google Cloud Storage ────────────────────────────────────────────
    /// Path to a GCP service-account JSON key file.
    #[serde(default)]
    pub gcs_service_account_path: String,
    /// Inline GCP service-account JSON key contents.
    #[serde(default)]
    pub gcs_service_account_key: String,

    // ── Table-valued function security (issue #10) ──────────────────────
    /// File TVF (`read_parquet`, `read_csv`, `read_json`) policy. Defaults
    /// deny local-fs and arbitrary HTTP hosts so an authenticated user
    /// cannot exfiltrate `/etc/shadow`, `/proc/self/environ`, or the
    /// coordinator's service-account token, nor pivot to cloud-metadata
    /// endpoints (IMDS / GCP / Azure IMDS) on `http://169.254.169.254`.
    #[serde(default)]
    pub tvf: TvfPolicy,
}

/// Security policy for table-valued-function path arguments.
///
/// All defaults are fail-closed: cloud object stores (`s3://`, `abfss://`,
/// `gs://`, `hf://`) keep working out of the box; local paths and arbitrary
/// `http(s)://` hosts are rejected unless explicitly enabled.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct TvfPolicy {
    /// When `true`, file TVFs may read absolute paths on the coordinator /
    /// worker filesystem (e.g. `/var/data/foo.parquet`). Default: `false`
    /// because an authenticated user could read `/etc/shadow`, a mounted
    /// secret, or `/var/run/secrets/kubernetes.io/serviceaccount/token`.
    /// Enable on single-tenant deployments where every user is trusted to
    /// read the local filesystem (rare; production deployments should
    /// stage files in an object store instead).
    #[serde(default)]
    pub allow_local_paths: bool,
    /// When `true`, file TVFs may fetch from arbitrary `http(s)://` hosts.
    /// Default: `false` because IMDS lives at `http://169.254.169.254`
    /// and worker pods often run with cloud-side privileges higher than
    /// the user's. When `false`, only `allowed_http_hosts` are reachable
    /// (empty list = no HTTP at all).
    #[serde(default)]
    pub allow_http: bool,
    /// When `allow_http` is `false`, this allowlist names the hosts that
    /// file TVFs may reach. Compared case-insensitively against the URL's
    /// host (no port, no path). Examples: `["data.example.com",
    /// "huggingface.co"]`. Wildcards are not supported; add each fully-
    /// qualified host explicitly.
    #[serde(default)]
    pub allowed_http_hosts: Vec<String>,
    /// Object-store URL prefixes that file TVFs may read **with the
    /// engine's own storage credentials**. Default: empty = DENY all
    /// engine-credentialed object-store TVF reads. Without this gate any
    /// authenticated user could `read_csv('s3://<any-bucket>/<any-key>')`
    /// and the engine would fetch it with its static S3 key — a complete
    /// bypass of the catalog authorization plane (Polaris/OPA), which only
    /// covers catalog tables.
    ///
    /// Entries are compared on URL-path-segment boundaries (so
    /// `"s3://staging"` does NOT match `s3://staging-evil/...`) after
    /// scheme canonicalisation (`s3a://`→`s3://`, `abfs://`→`abfss://`,
    /// `az://`→`azure://`, `gcs://`→`gs://`). The literal placeholder
    /// `{user}` expands to the authenticated username, enabling per-user
    /// staging areas:
    ///
    /// ```toml
    /// [storage.tvf]
    /// allowed_object_store_prefixes = [
    ///   "s3://data-platform-staging/_table-load-staging/",
    ///   "s3://notebook-scratch/{user}/",
    /// ]
    /// ```
    ///
    /// Paths whose TVF call carries complete *inline* credentials
    /// (`access_key` + `secret_key`, an Azure key/SAS token, or an inline
    /// GCS service-account key) bypass this list: the engine's storage key
    /// is not used, so the object store itself enforces access.
    #[serde(default)]
    pub allowed_object_store_prefixes: Vec<String>,
    /// Roles (matched case-insensitively against the caller's JWT roles)
    /// that may read **any** object-store path with the engine's storage
    /// credentials, bypassing `allowed_object_store_prefixes`. Default:
    /// empty = no role-based override. Intended for platform
    /// administrators / service identities that own the storage anyway.
    #[serde(default)]
    pub object_store_admin_roles: Vec<String>,
}

/// Identity of the caller invoking a file TVF, resolved at session-context
/// build time from the authenticated session. Drives
/// [`TvfPolicy::check_path`]'s object-store gating.
#[derive(Debug, Clone, Default)]
pub struct TvfCaller {
    /// Authenticated username (JWT `user_claim`). `None` for callers with
    /// no resolvable identity — `{user}` prefixes never match for them.
    pub username: Option<String>,
    /// Roles from the caller's JWT (`roles_claim`), matched against
    /// `[storage.tvf] object_store_admin_roles`.
    pub roles: Vec<String>,
    /// `true` only for the embedded (in-process, single-tenant) CLI where
    /// the caller already owns the process, its config, and its
    /// credentials. Skips object-store prefix gating entirely. The
    /// coordinator must NEVER set this for remote sessions.
    pub trusted: bool,
}

impl TvfCaller {
    /// Caller identity for an authenticated remote session.
    pub fn for_user(username: String, roles: Vec<String>) -> Self {
        Self {
            username: Some(username),
            roles,
            trusted: false,
        }
    }

    /// Trusted local caller (embedded CLI). Object-store prefix gating is
    /// skipped — the local user owns the config and the credentials in it.
    pub fn trusted_local() -> Self {
        Self {
            username: None,
            roles: Vec::new(),
            trusted: true,
        }
    }
}

/// Canonicalise an object-store URL for prefix comparison: scheme aliases
/// collapse to one spelling, and the scheme is lowercased. Any path segment
/// that could be re-interpreted as a `.`/`..` traversal or a hidden segment
/// boundary by a downstream URL/HTTP layer AFTER this prefix check is
/// rejected outright: a literal `.`/`..`, the percent-encodings of `.`
/// (`%2e`) and `/` (`%2f`), and double-encoding (`%25…`). We reject rather
/// than decode-and-renormalise because object_store / reqwest decoding
/// behaviour varies per backend, so the only sound stance is to refuse
/// anything ambiguous.
fn canonicalize_object_url(path: &str) -> Result<String, String> {
    const ALIASES: &[(&str, &str)] = &[
        ("s3a://", "s3://"),
        ("abfs://", "abfss://"),
        ("az://", "azure://"),
        ("gcs://", "gs://"),
    ];
    let lower = path.to_lowercase();
    let mut out = path.to_string();
    for (from, to) in ALIASES {
        if lower.starts_with(from) {
            // Don't rewrite `abfss://` via the `abfs://` alias.
            if *from == "abfs://" && lower.starts_with("abfss://") {
                continue;
            }
            out = format!("{to}{}", &path[from.len()..]);
            break;
        }
    }
    if let Some(idx) = out.find("://") {
        let (scheme, rest) = out.split_at(idx);
        out = format!("{}{}", scheme.to_ascii_lowercase(), rest);
    }
    let after_scheme = out.split_once("://").map(|x| x.1).unwrap_or("");
    for seg in after_scheme.split('/') {
        let lower = seg.to_ascii_lowercase();
        // A percent-encoded slash (`%2f`) or double-encoding (`%25…`) could
        // smuggle an extra segment boundary or a dot segment past this
        // check; reject any segment carrying them.
        if lower.contains("%2f") || lower.contains("%25") {
            return Err(format!(
                "TVF: object-store path '{path}' contains a percent-encoded \
                 slash or double-encoding in a path segment, which is not allowed"
            ));
        }
        let decoded = lower.replace("%2e", ".");
        if decoded == "." || decoded == ".." {
            return Err(format!(
                "TVF: object-store path '{path}' contains a dot path segment \
                 ('.' / '..'), which is not allowed"
            ));
        }
    }
    Ok(out)
}

/// `true` when `path` falls under `prefix` on a path-segment boundary.
/// `s3://staging` matches `s3://staging` and `s3://staging/x` but NOT
/// `s3://staging-evil/x`.
fn object_prefix_matches(path: &str, prefix: &str) -> bool {
    if prefix.is_empty() {
        return false;
    }
    match path.strip_prefix(prefix) {
        Some(rest) => prefix.ends_with('/') || rest.is_empty() || rest.starts_with('/'),
        None => false,
    }
}

/// Expand the `{user}` placeholder in a configured prefix. Returns `None`
/// when the prefix needs a username that is absent or contains characters
/// that could rewrite the URL shape (anything outside `[A-Za-z0-9._@+-]`).
fn substitute_user_placeholder(prefix: &str, username: Option<&str>) -> Option<String> {
    if !prefix.contains("{user}") {
        return Some(prefix.to_string());
    }
    let user = username?;
    let safe = !user.is_empty()
        && !user.chars().all(|c| c == '.')
        && user
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '@' | '+'));
    if !safe {
        return None;
    }
    Some(prefix.replace("{user}", user))
}

impl TvfPolicy {
    /// Return `Ok(())` if a TVF invoked by `caller` may reference `path`,
    /// or an error describing why it cannot.
    ///
    /// - Object-store schemes (`s3://`, `s3a://`, `abfss://`, `abfs://`,
    ///   `azure://`, `az://`, `gs://`, `gcs://`) use the engine's own
    ///   storage credentials, so they are gated per caller identity:
    ///   allowed only under `allowed_object_store_prefixes` (with `{user}`
    ///   substitution), for a role in `object_store_admin_roles`, for a
    ///   trusted local caller, or when the call carries complete inline
    ///   credentials (`inline_credentials = true`). Default: DENY.
    /// - `hf://` resolves to anonymous HTTPS downloads from
    ///   huggingface.co; no engine storage credentials are involved, so it
    ///   stays allowed.
    /// - Local paths (`/...`, `file://`, no scheme) need
    ///   `allow_local_paths = true`.
    /// - `http(s)://` needs either `allow_http = true` or an exact match
    ///   in `allowed_http_hosts`.
    pub fn check_path(
        &self,
        path: &str,
        caller: &TvfCaller,
        inline_credentials: bool,
    ) -> Result<(), String> {
        let lower = path.to_lowercase();
        if lower.starts_with("s3://")
            || lower.starts_with("s3a://")
            || lower.starts_with("abfss://")
            || lower.starts_with("abfs://")
            || lower.starts_with("azure://")
            || lower.starts_with("az://")
            || lower.starts_with("gs://")
            || lower.starts_with("gcs://")
        {
            return self.check_object_store(path, caller, inline_credentials);
        }
        if lower.starts_with("hf://") {
            // Resolved upstream to https://huggingface.co downloads; the
            // engine's storage credentials are never attached.
            return Ok(());
        }

        if lower.starts_with("http://") || lower.starts_with("https://") {
            if self.allow_http {
                return Ok(());
            }
            // Parse out the host (lowercased) and check the allowlist.
            // Keeping this local + total avoids panicking on malformed input.
            let after_scheme = lower
                .split_once("://")
                .map(|x| x.1)
                .unwrap_or("");
            let host_with_port = after_scheme.split('/').next().unwrap_or("");
            let host = host_with_port
                .split(':')
                .next()
                .unwrap_or("")
                .trim();
            if host.is_empty() {
                return Err(format!(
                    "TVF: malformed URL '{path}' (missing host)"
                ));
            }
            if self
                .allowed_http_hosts
                .iter()
                .any(|h| h.eq_ignore_ascii_case(host))
            {
                return Ok(());
            }
            return Err(format!(
                "TVF: HTTP host '{host}' is not in `[storage.tvf] allowed_http_hosts`. \
                 Add the host or set `allow_http = true` to permit arbitrary hosts."
            ));
        }

        // Everything else — bare path, `file://`, `/...` — is a local path.
        if self.allow_local_paths {
            return Ok(());
        }
        Err(format!(
            "TVF: local filesystem paths are disabled. \
             Path '{path}' is not an object-store URL (s3://, abfss://, gs://, hf://, ...). \
             Set `[storage.tvf] allow_local_paths = true` to permit local reads, \
             or stage the file in an object store."
        ))
    }

    /// Identity-aware gate for engine-credentialed object-store TVF reads.
    /// Fail-closed: with no configuration, every `s3://` / `abfss://` /
    /// `gs://` (and alias-scheme) path is denied unless the call carries
    /// complete inline credentials or the caller is trusted-local.
    fn check_object_store(
        &self,
        path: &str,
        caller: &TvfCaller,
        inline_credentials: bool,
    ) -> Result<(), String> {
        if caller.trusted || inline_credentials {
            return Ok(());
        }
        if !self.object_store_admin_roles.is_empty()
            && caller.roles.iter().any(|r| {
                self.object_store_admin_roles
                    .iter()
                    .any(|a| a.eq_ignore_ascii_case(r))
            })
        {
            return Ok(());
        }

        let principal = caller.username.as_deref().unwrap_or("<anonymous>");
        let canon = match canonicalize_object_url(path) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    principal = %principal,
                    path = %path,
                    "TVF object-store access denied: {e}"
                );
                return Err(e);
            }
        };
        for raw_prefix in &self.allowed_object_store_prefixes {
            let Some(substituted) =
                substitute_user_placeholder(raw_prefix, caller.username.as_deref())
            else {
                continue;
            };
            // A misconfigured prefix (e.g. containing dot segments) is
            // skipped rather than widened.
            let Ok(canon_prefix) = canonicalize_object_url(&substituted) else {
                continue;
            };
            if object_prefix_matches(&canon, &canon_prefix) {
                return Ok(());
            }
        }

        tracing::warn!(
            principal = %principal,
            path = %path,
            "TVF object-store access denied: no matching \
             `[storage.tvf] allowed_object_store_prefixes` entry"
        );
        Err(format!(
            "TVF: object-store path '{path}' denied for user '{principal}'. \
             Reads with the engine's storage credentials are limited to \
             `[storage.tvf] allowed_object_store_prefixes` ({} configured). \
             Add a matching prefix (the `{{user}}` placeholder expands to the \
             authenticated username), grant a role listed in \
             `[storage.tvf] object_store_admin_roles`, or pass complete inline \
             credentials (e.g. access_key/secret_key) so the engine's storage \
             key is not used.",
            self.allowed_object_store_prefixes.len()
        ))
    }

    /// Validate an S3-style endpoint override (the `endpoint =>` argument
    /// on `read_parquet` / `read_csv` / `read_json`). Issue #46 closed
    /// the SSRF gap left by issue #10: the path argument was checked but
    /// the endpoint override flowed straight into `AmazonS3Builder`, so
    /// `read_parquet('s3://x/y', endpoint => 'http://169.254.169.254/...')`
    /// still pivoted to IMDS.
    ///
    /// Empty endpoints are allowed (the operator's storage config kicks
    /// in). HTTP/HTTPS endpoints are validated against the same allowlist
    /// as path arguments. Bare-host endpoints (e.g. `minio.local:9000`)
    /// are allowed since they cannot reach IMDS over its expected scheme.
    pub fn check_endpoint(&self, endpoint: &str) -> Result<(), String> {
        if endpoint.is_empty() {
            return Ok(());
        }
        let lower = endpoint.to_lowercase();
        if !lower.starts_with("http://") && !lower.starts_with("https://") {
            return Ok(());
        }
        if self.allow_http {
            return Ok(());
        }
        let after_scheme = lower
            .split_once("://")
            .map(|x| x.1)
            .unwrap_or("");
        let host_with_port = after_scheme.split('/').next().unwrap_or("");
        let host = host_with_port
            .split(':')
            .next()
            .unwrap_or("")
            .trim();
        if host.is_empty() {
            return Err(format!(
                "TVF: malformed endpoint '{endpoint}' (missing host)"
            ));
        }
        if self
            .allowed_http_hosts
            .iter()
            .any(|h| h.eq_ignore_ascii_case(host))
        {
            return Ok(());
        }
        Err(format!(
            "TVF: endpoint host '{host}' is not in `[storage.tvf] allowed_http_hosts`. \
             Inline `endpoint => '{endpoint}'` was rejected to prevent SSRF \
             to metadata services. Add the host or set `allow_http = true` \
             to permit arbitrary hosts."
        ))
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            s3_endpoint: String::new(),
            s3_region: String::new(),
            s3_access_key: String::new(),
            s3_secret_key: SecretString::default(),
            s3_path_style: false,
            s3_allow_http: false,
            coalesce_threshold: default_coalesce_threshold(),
            footer_cache_size: default_footer_cache_size(),
            concurrent_requests_per_file: default_concurrent_requests_per_file(),
            max_concurrent_files: default_max_concurrent_files(),
            prefetch_buffer: default_prefetch_buffer(),
            prefetch_concurrency: default_prefetch_concurrency(),
            azure_account: String::new(),
            azure_access_key: String::new(),
            azure_sas_token: String::new(),
            azure_use_emulator: false,
            gcs_service_account_path: String::new(),
            gcs_service_account_key: String::new(),
            tvf: TvfPolicy::default(),
        }
    }
}

fn default_coalesce_threshold() -> String { "1MB".to_string() }
fn default_footer_cache_size() -> String { "256MB".to_string() }
fn default_concurrent_requests_per_file() -> usize { 4 }
fn default_max_concurrent_files() -> usize { 8 }
fn default_prefetch_buffer() -> String { "32MB".to_string() }
fn default_prefetch_concurrency() -> usize { 4 }

impl std::fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageConfig")
            .field("s3_endpoint", &self.s3_endpoint)
            .field("s3_region", &self.s3_region)
            .field("s3_access_key", &"[REDACTED]")
            .field("s3_secret_key", &self.s3_secret_key)
            .field("s3_path_style", &self.s3_path_style)
            .field("s3_allow_http", &self.s3_allow_http)
            .field("coalesce_threshold", &self.coalesce_threshold)
            .field("footer_cache_size", &self.footer_cache_size)
            .field("concurrent_requests_per_file", &self.concurrent_requests_per_file)
            .field("max_concurrent_files", &self.max_concurrent_files)
            .field("prefetch_buffer", &self.prefetch_buffer)
            .field("prefetch_concurrency", &self.prefetch_concurrency)
            .field("azure_account", &self.azure_account)
            .field("azure_access_key", &"[REDACTED]")
            .field("azure_sas_token", &"[REDACTED]")
            .field("azure_use_emulator", &self.azure_use_emulator)
            .field("gcs_service_account_path", &self.gcs_service_account_path)
            .field("gcs_service_account_key", &"[REDACTED]")
            .finish()
    }
}

/// Access control backend for GRANT/REVOKE/SHOW GRANTS SQL.
///
/// Supports multiple backends:
/// - `"chameleon"` -- Chameleon platform API (GROUP/USER grantees)
/// - `"polaris"` -- Apache Polaris 1.3 native (PRINCIPAL/PRINCIPAL_ROLE/CATALOG_ROLE)
/// - `"none"` -- disabled (default)
///
/// ```toml
/// [access_control]
/// backend = "chameleon"
/// url = "http://backend:8080/api/platform/v1/access"
/// ```
/// Access control backend selected for GRANT/REVOKE/SHOW GRANTS dispatch.
///
/// Deserialized from TOML strings. Unknown values fail at config-load
/// rather than silently disabling access control at the dispatch site.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AccessControlBackend {
    /// No access control. GRANT/REVOKE statements are accepted by the
    /// parser but rejected at execution. Default.
    #[default]
    None,
    /// Chameleon platform API (`GROUP` / `USER` grantees).
    Chameleon,
    /// Apache Polaris 1.3 native (`PRINCIPAL` / `PRINCIPAL_ROLE` / `CATALOG_ROLE`).
    Polaris,
    /// Apache Ranger via Polaris 1.5 embedded authorizer. SQE writes grants to
    /// Ranger Admin; Polaris enforces. Requires `[access_control.ranger]`.
    Ranger,
}

impl std::str::FromStr for AccessControlBackend {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "none" | "" => Ok(Self::None),
            "chameleon" => Ok(Self::Chameleon),
            "polaris" => Ok(Self::Polaris),
            "ranger" => Ok(Self::Ranger),
            other => Err(format!(
                "unknown access_control.backend {other:?}; expected one of none, chameleon, polaris, ranger"
            )),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct AccessControlConfig {
    /// Backend type: `chameleon`, `polaris`, or `none` (disabled).
    #[serde(default)]
    pub backend: AccessControlBackend,
    /// Backend API URL.
    /// Chameleon: http://backend:port/api/platform/v1/access
    /// Polaris: http://polaris:8181/api/management/v1 (Polaris management API)
    #[serde(default)]
    pub url: String,
    /// Request timeout in seconds.
    #[serde(default = "default_access_control_timeout")]
    pub timeout_secs: u64,
    /// Optional: Polaris service account client_id for management API.
    /// When absent, the user's passthrough OIDC token is used.
    #[serde(default)]
    pub client_id: Option<String>,
    /// Optional: Polaris service account client_secret for management API.
    /// When absent, the user's passthrough OIDC token is used.
    #[serde(default)]
    pub client_secret: Option<String>,
    /// Apache Ranger backend tuning. Used only when `backend = "ranger"`.
    #[serde(default)]
    pub ranger: RangerConfig,
}

impl Default for AccessControlConfig {
    fn default() -> Self {
        Self {
            backend: AccessControlBackend::default(),
            url: String::new(),
            timeout_secs: 30,
            client_id: None,
            client_secret: None,
            ranger: RangerConfig::default(),
        }
    }
}

fn default_access_control_timeout() -> u64 { 30 }

/// Apache Ranger backend configuration (Polaris 1.5 embedded authorizer).
///
/// The Ranger Admin base URL is taken from `access_control.url`
/// (e.g. `http://ranger-admin:6080`), consistent with the Polaris backend.
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct RangerConfig {
    /// Ranger service instance name. Must match the Polaris
    /// `polaris.authorization.ranger.service-name` setting.
    #[serde(default = "default_ranger_service_name")]
    pub service_name: String,
    /// Ranger Admin user for HTTP basic auth.
    #[serde(default = "default_ranger_admin_user")]
    pub admin_user: String,
    /// Ranger Admin password. Set via `SQE_ACCESS_CONTROL__RANGER__ADMIN_PASSWORD`.
    #[serde(default)]
    pub admin_password: SecretString,
    /// Value for the top-level `root` resource (the Polaris realm/context the
    /// embedded authorizer prefixes onto resource paths). When empty, the
    /// `root` level is omitted from written policies. See Task 14 for how to
    /// determine the correct value for a deployment.
    #[serde(default)]
    pub realm: String,
    /// HTTP timeout for a single Ranger Admin call, in seconds.
    #[serde(default = "default_ranger_timeout_secs")]
    pub timeout_secs: u64,
    /// Accept self-signed TLS certs on the Ranger Admin endpoint.
    #[serde(default)]
    pub accept_invalid_certs: bool,
}

impl Default for RangerConfig {
    fn default() -> Self {
        Self {
            service_name: default_ranger_service_name(),
            admin_user: default_ranger_admin_user(),
            admin_password: SecretString::default(),
            realm: String::new(),
            timeout_secs: default_ranger_timeout_secs(),
            accept_invalid_certs: false,
        }
    }
}

fn default_ranger_service_name() -> String {
    "polaris".to_string()
}
fn default_ranger_admin_user() -> String {
    "admin".to_string()
}
fn default_ranger_timeout_secs() -> u64 {
    30
}

/// Policy engine backend used to compute row filters and column masks.
///
/// Deserialized from TOML strings. Unknown values fail at config-load.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PolicyEngine {
    /// No policy enforcement: every plan passes through unmodified.
    /// Default for the open-source distribution.
    #[default]
    Passthrough,
    /// In-memory `PolicyStore` for tests and local dev.
    InMemory,
    /// Open Policy Agent over HTTP. Requires `[policy.opa]`.
    Opa,
    /// Cedar policy engine (experimental).
    Cedar,
    /// Apache Ranger fine-grained policies (hive service-def). Requires
    /// `[policy.ranger]`. Reads row-filter + data-mask policies and feeds the
    /// PlanRewriter. Separate from `access_control.backend = "ranger"`.
    Ranger,
}

impl std::str::FromStr for PolicyEngine {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "passthrough" | "" => Ok(Self::Passthrough),
            "in-memory" | "inmemory" | "in_memory" => Ok(Self::InMemory),
            "opa" => Ok(Self::Opa),
            "cedar" => Ok(Self::Cedar),
            "ranger" => Ok(Self::Ranger),
            other => Err(format!(
                "unknown policy.engine {other:?}; expected one of passthrough, in-memory, opa, cedar, ranger"
            )),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct PolicyConfig {
    #[serde(default)]
    pub engine: PolicyEngine,
    /// Per-deployment secret keyed into the SHA-256 column mask UDF.
    ///
    /// When set, the `sha256(col)` mask runs as HMAC-SHA256 with this key,
    /// blocking the offline rainbow-table attack against low-entropy
    /// columns (SSN, phone, employee ID). When empty, the UDF falls back
    /// to plain SHA-256 and emits a startup warning. Rotating the key
    /// changes every masked digest, so the same key must persist across
    /// coordinator restarts and across all coordinators in an HA setup.
    ///
    /// This applies to Ranger `MASK_HASH` and OPA `hash` column masks alike.
    /// We warn rather than reject on `engine = ranger` + empty key (issue #37):
    /// default-denying Hash without a key is the stronger control but is
    /// breaking for deployments already relying on the unkeyed behaviour, so it
    /// is deferred. Setting this key is the recommended hardening step.
    ///
    /// Can be set via the `SQE_POLICY__MASK_KEY` environment variable.
    #[serde(default)]
    pub mask_key: String,
    /// OPA backend tuning. Empty defaults are sensible for most deployments
    /// (5 s timeout, 10 000 cache entries, 5 consecutive failures opens the
    /// breaker, 30 s recovery window).
    #[serde(default)]
    pub opa: OpaConfig,
    /// Ranger fine-grained backend tuning. Used only when `engine = "ranger"`.
    #[serde(default)]
    pub ranger: RangerPolicyConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OpaConfig {
    /// HTTP timeout for a single OPA evaluate call in seconds.
    #[serde(default = "default_opa_timeout_secs")]
    pub timeout_secs: u64,
    /// Maximum cached `ResolvedPolicy` entries.
    #[serde(default = "default_opa_cache_max_entries")]
    pub cache_max_entries: u64,
    /// Cache TTL in seconds; entries older than this are revalidated.
    #[serde(default = "default_opa_cache_ttl_secs")]
    pub cache_ttl_secs: u64,
    /// Consecutive OPA failures before the circuit breaker opens.
    #[serde(default = "default_opa_breaker_failure_threshold")]
    pub breaker_failure_threshold: u32,
    /// How long to keep the breaker open before probing again, in seconds.
    #[serde(default = "default_opa_breaker_recovery_secs")]
    pub breaker_recovery_secs: u64,
}

impl Default for OpaConfig {
    fn default() -> Self {
        Self {
            timeout_secs: default_opa_timeout_secs(),
            cache_max_entries: default_opa_cache_max_entries(),
            cache_ttl_secs: default_opa_cache_ttl_secs(),
            breaker_failure_threshold: default_opa_breaker_failure_threshold(),
            breaker_recovery_secs: default_opa_breaker_recovery_secs(),
        }
    }
}

fn default_opa_timeout_secs() -> u64 {
    5
}
fn default_opa_cache_max_entries() -> u64 {
    10_000
}
fn default_opa_cache_ttl_secs() -> u64 {
    60
}
fn default_opa_breaker_failure_threshold() -> u32 {
    5
}
fn default_opa_breaker_recovery_secs() -> u64 {
    30
}

/// Fine-grained policy engine backed by a `hive`-type Apache Ranger service.
///
/// The Ranger Admin base URL is taken from `policy.ranger.url`. This is the
/// ENFORCEMENT path (row filters + column masks), distinct from
/// `access_control.ranger` (the GRANT/REVOKE write path on the `polaris`
/// service). `service_name` defaults to `hive` so policies are shared with
/// Apache Spark / Kyuubi.
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct RangerPolicyConfig {
    /// Ranger Admin base URL, e.g. `http://ranger-admin:6080`.
    #[serde(default)]
    pub url: String,
    /// The `hive` Ranger service instance to read. Shared with Spark/Kyuubi.
    #[serde(default = "default_ranger_policy_service_name")]
    pub service_name: String,
    /// Ranger Admin user for HTTP basic auth.
    #[serde(default = "default_ranger_admin_user")]
    pub admin_user: String,
    /// Ranger Admin password. Set via `SQE_POLICY__RANGER__ADMIN_PASSWORD`.
    #[serde(default)]
    pub admin_password: SecretString,
    /// HTTP timeout for a single Ranger download call, in seconds.
    #[serde(default = "default_ranger_policy_timeout_secs")]
    pub timeout_secs: u64,
    /// Maximum cached `ResolvedPolicy` entries.
    #[serde(default = "default_ranger_policy_cache_max_entries")]
    pub cache_max_entries: u64,
    /// Cache TTL in seconds for resolved row-filter / data-mask policies.
    ///
    /// Masks and filters edited directly in Ranger Admin (the normal authoring
    /// path) are not honored until this TTL elapses, leaving a bounded
    /// over-permissive window of up to `cache_ttl_secs`. This is asymmetric with
    /// the tag path, which re-fetches the column->tags map every call. A future
    /// improvement is `lastKnownVersion` / HTTP-304 polling so external edits
    /// are picked up promptly without a short TTL; until then, lower this value
    /// if prompt propagation of Admin-side edits matters more than fetch load.
    #[serde(default = "default_ranger_policy_cache_ttl_secs")]
    pub cache_ttl_secs: u64,
    /// Consecutive failures before the circuit breaker opens.
    #[serde(default = "default_opa_breaker_failure_threshold")]
    pub breaker_failure_threshold: u32,
    /// How long to keep the breaker open before probing again, in seconds.
    #[serde(default = "default_opa_breaker_recovery_secs")]
    pub breaker_recovery_secs: u64,
    /// Accept self-signed TLS certs on the Ranger Admin endpoint.
    #[serde(default)]
    pub accept_invalid_certs: bool,
}

impl Default for RangerPolicyConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            service_name: default_ranger_policy_service_name(),
            admin_user: default_ranger_admin_user(),
            admin_password: SecretString::default(),
            timeout_secs: default_ranger_policy_timeout_secs(),
            cache_max_entries: default_ranger_policy_cache_max_entries(),
            cache_ttl_secs: default_ranger_policy_cache_ttl_secs(),
            breaker_failure_threshold: default_opa_breaker_failure_threshold(),
            breaker_recovery_secs: default_opa_breaker_recovery_secs(),
            accept_invalid_certs: false,
        }
    }
}

fn default_ranger_policy_service_name() -> String {
    "hive".to_string()
}

fn default_ranger_policy_timeout_secs() -> u64 {
    5
}

fn default_ranger_policy_cache_max_entries() -> u64 {
    10_000
}

fn default_ranger_policy_cache_ttl_secs() -> u64 {
    30
}

/// Audit-log output configuration. Nested under `[metrics.audit]` in config files
/// and via `SQE_METRICS__AUDIT__*` env overrides.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AuditConfig {
    /// Output format: "native" (canonical AuditEvent JSON), "ocsf", or "both".
    #[serde(default = "default_audit_format")]
    pub format: String,
    /// Tag names that mark a column as GDPR-sensitive. Any column in a queried
    /// Iceberg table whose tag set intersects this list has its identifier and
    /// adjacent literal values removed from the logged SQL text. Empty list
    /// disables GDPR column masking. Consumed at startup by the coordinator to
    /// configure GDPR masking on the audit writer thread.
    #[serde(default)]
    pub gdpr_tags: Vec<String>,
    /// How tagged column identifiers appear after masking: "tokenize" | "drop" | "keep".
    /// "tokenize" replaces the identifier with a stable per-column token so log
    /// lines remain correlatable without leaking the column name. Consumed at
    /// startup alongside `gdpr_tags`.
    #[serde(default = "default_gdpr_identifier_mode")]
    pub gdpr_identifier_mode: String,
    /// Log full result sets for debugging. NEVER enable in production.
    #[serde(default)]
    pub superdebug_log_results: bool,
    /// Coalesce dashboard auth-SUCCESS audit events per principal.
    ///
    /// Within each window, at most ONE audit line is written per principal;
    /// subsequent hits increment the `sqe_dashboard_auth_success_total` counter
    /// but do NOT write a second audit line.  Set to 0 to disable dedup and
    /// audit every request (restores pre-coalesce behavior).
    ///
    /// Default: 300 seconds.
    #[serde(default = "default_dashboard_access_audit_window_secs")]
    pub dashboard_access_audit_window_secs: u64,
}

fn default_audit_format() -> String {
    "native".to_string()
}

fn default_gdpr_identifier_mode() -> String {
    "tokenize".to_string()
}

fn default_dashboard_access_audit_window_secs() -> u64 {
    300
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            format: default_audit_format(),
            gdpr_tags: Vec::new(),
            gdpr_identifier_mode: default_gdpr_identifier_mode(),
            superdebug_log_results: false,
            dashboard_access_audit_window_secs: default_dashboard_access_audit_window_secs(),
        }
    }
}

/// OTLP audit export configuration. Nested under `[metrics.audit_export]` in
/// config files and via `SQE_METRICS__AUDIT_EXPORT__*` env overrides.
///
/// Disabled by default (`enabled = false`) to preserve existing behavior.
/// When enabled, audit events are spooled to `spool_path` and flushed to the
/// configured OTLP endpoint in batches.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AuditExportConfig {
    /// Enable the OTLP audit exporter. Default: false.
    #[serde(default)]
    pub enabled: bool,
    /// Export target. Currently only "otlp" is supported. Default: "otlp".
    #[serde(default = "default_export_target")]
    pub target: String,
    /// OTLP endpoint URL for audit log export (e.g. "http://collector:4317").
    #[serde(default)]
    pub otlp_endpoint: String,
    /// Path for the on-disk spool used to buffer events before export.
    #[serde(default)]
    pub spool_path: String,
    /// Maximum number of events per export batch. Default: 512.
    #[serde(default = "default_export_batch_max")]
    pub batch_max: usize,
    /// Flush interval in milliseconds. Default: 2000.
    #[serde(default = "default_export_flush_ms")]
    pub flush_interval_ms: u64,
    /// Maximum spool size in bytes before back-pressure applies. Default: 1 GiB.
    #[serde(default = "default_export_max_spool")]
    pub max_spool_bytes: u64,
    /// Where to start replaying the spool on restart: "now" skips historical
    /// events; "beginning" replays from the oldest spooled event. Default: "now".
    #[serde(default = "default_export_start_at")]
    pub start_at: String,
}

fn default_export_target() -> String { "otlp".into() }
fn default_export_batch_max() -> usize { 512 }
fn default_export_flush_ms() -> u64 { 2000 }
fn default_export_max_spool() -> u64 { 1_073_741_824 }
fn default_export_start_at() -> String { "now".into() }

impl Default for AuditExportConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            target: default_export_target(),
            otlp_endpoint: String::new(),
            spool_path: String::new(),
            batch_max: default_export_batch_max(),
            flush_interval_ms: default_export_flush_ms(),
            max_spool_bytes: default_export_max_spool(),
            start_at: default_export_start_at(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetricsConfig {
    #[serde(default = "default_prometheus_port")]
    pub prometheus_port: u16,
    #[serde(default)]
    pub otlp_endpoint: String,
    #[serde(default)]
    pub audit_log_path: String,
    /// OTel trace sampling rate (0.0 to 1.0). Default: 0.01 (1%).
    /// Set to 1.0 to trace all queries (expensive). Set to 0.0 to disable tracing.
    #[serde(default = "default_trace_sample_rate")]
    pub trace_sample_rate: f64,
    #[serde(default)]
    pub openlineage: OpenLineageConfig,
    /// Serve the read-only web UI (HTML dashboard + /api/v1/queries* endpoints)
    /// on the internal health port (metrics_port + 1). Default OFF (WEB-01):
    /// the server binds `0.0.0.0` and the UI has NO authentication, so it must
    /// not be enabled implicitly. When `true`, the coordinator emits a startup
    /// WARN. Leave off to keep only /healthz, /readyz, /api/v1/status.
    #[serde(default)]
    pub web_ui: bool,
    /// Audit-log format and GDPR knobs. See `AuditConfig` for field docs.
    #[serde(default)]
    pub audit: AuditConfig,
    /// OTLP audit export configuration. See `AuditExportConfig` for field docs.
    #[serde(default)]
    pub audit_export: AuditExportConfig,
}

fn default_trace_sample_rate() -> f64 {
    0.01
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            prometheus_port: 9090,
            otlp_endpoint: String::new(),
            audit_log_path: String::new(),
            trace_sample_rate: default_trace_sample_rate(),
            openlineage: OpenLineageConfig::default(),
            web_ui: false,
            audit: AuditConfig::default(),
            audit_export: AuditExportConfig::default(),
        }
    }
}

/// OpenLineage emitter configuration.
///
/// Controls whether SQE emits OpenLineage events for executed statements
/// and where those events are delivered (file sink, HTTP endpoint, or both).
#[derive(Clone, Deserialize, Serialize)]
pub struct OpenLineageConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_ol_job_namespace")]
    pub job_namespace: String,
    #[serde(default)]
    pub producer: String,
    #[serde(default)]
    pub emit_selects: bool,
    #[serde(default)]
    pub file_path: String,
    #[serde(default)]
    pub http_endpoint: String,
    #[serde(default = "default_ol_auth_mode")]
    pub auth_mode: String,
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "default_ol_http_timeout")]
    pub http_timeout_ms: u64,
    #[serde(default = "default_ol_http_retry")]
    pub http_retry_attempts: u32,
    #[serde(default)]
    pub spool_path: String,
    #[serde(default = "default_ol_spool_cap")]
    pub spool_max_bytes: u64,
    #[serde(default = "default_ol_replay_secs")]
    pub replay_interval_secs: u64,
    #[serde(default = "default_ol_channel_cap")]
    pub channel_capacity: usize,
}

impl Default for OpenLineageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            job_namespace: default_ol_job_namespace(),
            producer: String::new(),
            emit_selects: false,
            file_path: String::new(),
            http_endpoint: String::new(),
            auth_mode: default_ol_auth_mode(),
            api_key: String::new(),
            http_timeout_ms: default_ol_http_timeout(),
            http_retry_attempts: default_ol_http_retry(),
            spool_path: String::new(),
            spool_max_bytes: default_ol_spool_cap(),
            replay_interval_secs: default_ol_replay_secs(),
            channel_capacity: default_ol_channel_cap(),
        }
    }
}

// Hand-written Debug so `api_key` (an OpenLineage HTTP-sink bearer credential,
// reachable via `MetricsConfig`'s derived Debug) is never printed by `{:?}`
// (CORE-01). `Serialize` is retained for config round-tripping.
impl std::fmt::Debug for OpenLineageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenLineageConfig")
            .field("enabled", &self.enabled)
            .field("job_namespace", &self.job_namespace)
            .field("producer", &self.producer)
            .field("emit_selects", &self.emit_selects)
            .field("file_path", &self.file_path)
            .field("http_endpoint", &self.http_endpoint)
            .field("auth_mode", &self.auth_mode)
            .field(
                "api_key",
                if self.api_key.is_empty() {
                    &"<empty>"
                } else {
                    &"[REDACTED]"
                },
            )
            .field("http_timeout_ms", &self.http_timeout_ms)
            .field("http_retry_attempts", &self.http_retry_attempts)
            .field("spool_path", &self.spool_path)
            .field("spool_max_bytes", &self.spool_max_bytes)
            .field("replay_interval_secs", &self.replay_interval_secs)
            .field("channel_capacity", &self.channel_capacity)
            .finish()
    }
}

impl OpenLineageConfig {
    /// Validate that the OpenLineage configuration is internally consistent.
    ///
    /// Rules:
    /// - When enabled, at least one sink (file_path or http_endpoint) must be set.
    /// - `auth_mode = "bearer"` requires `api_key` to be set.
    /// - `spool_path` only makes sense when `http_endpoint` is set (the spool
    ///   buffers events that fail to deliver over HTTP).
    /// - When enabled, `spool_max_bytes` must be at least 1 MiB so the spool
    ///   has room for at least a few events.
    pub fn validate(&self) -> Result<(), String> {
        if self.enabled && self.file_path.is_empty() && self.http_endpoint.is_empty() {
            return Err(
                "openlineage.enabled requires at least one of file_path or http_endpoint".into(),
            );
        }
        if self.auth_mode == "bearer" && self.api_key.is_empty() {
            return Err(
                "openlineage.auth_mode = \"bearer\" requires api_key to be set".into(),
            );
        }
        if !self.spool_path.is_empty() && self.http_endpoint.is_empty() {
            return Err(
                "openlineage.spool_path requires http_endpoint to be set".into(),
            );
        }
        if self.enabled && self.spool_max_bytes < 1024 * 1024 {
            return Err("openlineage.spool_max_bytes must be at least 1 MiB".into());
        }
        Ok(())
    }
}

fn default_ol_job_namespace() -> String {
    "sqe".into()
}
fn default_ol_auth_mode() -> String {
    "none".into()
}
fn default_ol_http_timeout() -> u64 {
    5000
}
fn default_ol_http_retry() -> u32 {
    1
}
fn default_ol_spool_cap() -> u64 {
    100 * 1024 * 1024
}
fn default_ol_replay_secs() -> u64 {
    30
}
fn default_ol_channel_cap() -> usize {
    10_000
}

#[derive(Debug, Deserialize, Clone)]
pub struct RateLimitConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_per_user_rpm")]
    pub per_user_queries_per_minute: u32,
    #[serde(default = "default_global_rpm")]
    pub global_queries_per_minute: u32,
    /// Pre-auth rate limit on handshake attempts, keyed by (peer-ip,
    /// username). Defaults to 10 attempts per minute, enough for a
    /// human to fat-finger a password a few times, low enough to make
    /// credential stuffing impractical against the upstream IdP.
    #[serde(default = "default_auth_rpm")]
    pub auth_attempts_per_minute: u32,
    /// Pre-auth rate limit on metadata browse paths (Flight catalog,
    /// schemas, tables, prepared-statement schema lookup). Each call
    /// triggers a Polaris fan-out and was previously uncapped.
    /// Defaults to 120 per minute per user.
    #[serde(default = "default_metadata_rpm")]
    pub metadata_per_user_per_minute: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            per_user_queries_per_minute: default_per_user_rpm(),
            global_queries_per_minute: default_global_rpm(),
            auth_attempts_per_minute: default_auth_rpm(),
            metadata_per_user_per_minute: default_metadata_rpm(),
        }
    }
}

/// Security policy applied to wire-protocol surfaces.
///
/// Currently covers the trusted-proxy allowlist used when extracting
/// the client IP for audit logs and pre-auth rate limiting. Issue #74.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct SecurityConfig {
    /// IP literals of upstream proxies that are allowed to set
    /// `x-forwarded-for`. When the request's peer address is in this
    /// list, the rightmost untrusted hop from `x-forwarded-for` is
    /// recorded as the client IP. Otherwise the peer address is used
    /// directly and `x-forwarded-for` is ignored. Empty (the default)
    /// means no proxy is trusted, audit IPs always come from the peer.
    ///
    /// IPv4 and IPv6 literals are supported. CIDR ranges are not (yet);
    /// list each proxy IP explicitly. Hostnames are not resolved.
    #[serde(default)]
    pub trusted_proxies: Vec<String>,
    /// Opt-in escape hatch for the TLS-on-the-wire requirement. Leaving this
    /// `false` (the default) makes the engine refuse to start when distributed
    /// mode targets a non-loopback peer without TLS configured. Setting it
    /// `true` is visible in config diffs and acknowledges that user bearer
    /// tokens, the worker secret, and vended S3 credentials travel in
    /// cleartext and can be captured and replayed by an on-path observer.
    #[serde(default)]
    pub allow_insecure_transport: bool,
}

impl SecurityConfig {
    /// Choose the client IP given the directly-observed peer address
    /// and the `x-forwarded-for` header value (if any). When `peer` is
    /// in `trusted_proxies`, the rightmost untrusted hop from the
    /// header chain is returned; otherwise `peer` itself is returned.
    ///
    /// The "rightmost untrusted hop" rule walks the chain right-to-left
    /// and skips IPs that are themselves in the trusted_proxies list,
    /// stopping at the first untrusted address. This survives chains of
    /// known proxies that prepend to one another.
    pub fn resolve_client_ip(
        &self,
        peer: Option<&str>,
        forwarded_for: Option<&str>,
    ) -> String {
        let peer = peer.unwrap_or("unknown");
        let peer_host = strip_port(peer);
        let trusted = !self.trusted_proxies.is_empty()
            && self
                .trusted_proxies
                .iter()
                .any(|p| p.eq_ignore_ascii_case(peer_host));
        if !trusted {
            return peer.to_string();
        }
        let chain = match forwarded_for {
            Some(v) if !v.trim().is_empty() => v,
            _ => return peer.to_string(),
        };
        // Walk right to left, skipping known trusted proxies.
        let hops: Vec<&str> = chain.split(',').map(str::trim).collect();
        for hop in hops.iter().rev() {
            let hop_host = strip_port(hop);
            if hop_host.is_empty() {
                continue;
            }
            let is_trusted = self
                .trusted_proxies
                .iter()
                .any(|p| p.eq_ignore_ascii_case(hop_host));
            if !is_trusted {
                return hop.to_string();
            }
        }
        peer.to_string()
    }
}

/// Strip the `:port` suffix from a peer-address-like string for
/// allowlist comparison. IPv4 is `host:port`; IPv6 is `[host]:port`.
///
/// Public so wire-protocol surfaces can derive a port-stable rate-limit
/// key from a resolved client IP: the source port is ephemeral, so a key
/// that kept it would hand every new TCP connection a fresh bucket and
/// defeat per-IP limiting entirely.
pub fn strip_port(s: &str) -> &str {
    let s = s.trim();
    if let Some(rest) = s.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            return &rest[..end];
        }
    }
    match s.rfind(':') {
        // IPv6 without brackets contains multiple colons; treat the
        // whole thing as the host.
        Some(idx) if !s[..idx].contains(':') => &s[..idx],
        _ => s,
    }
}

/// Returns `true` when `host` is a loopback address or the `localhost`
/// hostname. Used to decide whether distributed peers stay on the local
/// machine (plaintext is fine) or reach over a network (TLS required).
///
/// A bare IP literal is parsed and checked with `IpAddr::is_loopback`
/// (covers 127.0.0.0/8 and `::1`). Anything that is not a parseable IP is
/// compared case-insensitively against `localhost`; all other hostnames are
/// treated as non-loopback because we cannot resolve them safely at config
/// time.
fn host_is_loopback(host: &str) -> bool {
    let host = host.trim();
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    // Strip IPv6 brackets if present (e.g. "[::1]").
    let bare = host.strip_prefix('[').and_then(|r| r.strip_suffix(']')).unwrap_or(host);
    matches!(bare.parse::<std::net::IpAddr>(), Ok(ip) if ip.is_loopback())
}

/// Parse a URL-shaped string and return `true` when its host is a loopback
/// address or `localhost`. A URL that does not parse, or has no host, is
/// treated as NON-loopback (fail safe: assume it reaches the network).
fn url_host_is_loopback(url: &str) -> bool {
    match url::Url::parse(url) {
        Ok(parsed) => match parsed.host_str() {
            Some(h) => host_is_loopback(h),
            None => false,
        },
        Err(_) => false,
    }
}

/// Returns `true` when distributed mode reaches at least one NON-loopback
/// peer. Used by transport validation: a coordinator with non-loopback
/// `worker_urls`, or a worker with a non-loopback `coordinator_url`, is
/// talking over a network and must use TLS unless the operator opts out.
fn distributed_reaches_network(config: &SqeConfig) -> bool {
    let worker_to_coordinator = !config.worker.coordinator_url.is_empty()
        && !url_host_is_loopback(&config.worker.coordinator_url);
    let coordinator_to_workers = config
        .coordinator
        .worker_urls
        .iter()
        .any(|u| !url_host_is_loopback(u));
    worker_to_coordinator || coordinator_to_workers
}

#[derive(Debug, Deserialize, Clone)]
pub struct SessionConfig {
    /// Idle timeout in seconds. Sessions with no activity for this duration are expired.
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_secs: u64,
    /// Absolute timeout in seconds. Sessions older than this are expired regardless of activity.
    #[serde(default = "default_absolute_timeout")]
    pub absolute_timeout_secs: u64,
    /// Session persistence backend. Options: "memory" (default), "file".
    /// "memory" = in-process only (lost on restart).
    /// "file" = periodic snapshot to disk (survives restart, best-effort).
    #[serde(default = "default_session_persistence")]
    pub persistence: String,
    /// Path for file-based session persistence. Default: "/tmp/sqe-sessions.json"
    #[serde(default = "default_session_persistence_path")]
    pub persistence_path: String,
    /// How often to snapshot sessions to disk (seconds). Default: 60.
    #[serde(default = "default_session_snapshot_interval")]
    pub snapshot_interval_secs: u64,
    /// How often the session-expiry sweeper runs (seconds). Default: 60.
    #[serde(default = "default_session_expiry_sweep_interval")]
    pub expiry_sweep_interval_secs: u64,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            idle_timeout_secs: default_idle_timeout(),
            absolute_timeout_secs: default_absolute_timeout(),
            persistence: default_session_persistence(),
            persistence_path: default_session_persistence_path(),
            snapshot_interval_secs: default_session_snapshot_interval(),
            expiry_sweep_interval_secs: default_session_expiry_sweep_interval(),
        }
    }
}

fn default_idle_timeout() -> u64 { 900 }       // 15 minutes
fn default_absolute_timeout() -> u64 { 28800 }  // 8 hours
fn default_session_persistence() -> String { "memory".to_string() }
fn default_session_persistence_path() -> String { "/tmp/sqe-sessions.json".to_string() }
fn default_session_snapshot_interval() -> u64 { 60 }
fn default_session_expiry_sweep_interval() -> u64 { 60 }
fn default_auth_handshake_timeout_secs() -> u64 { 30 }
fn default_health_check_interval_secs() -> u64 { 5 }
fn default_health_check_max_failures() -> u32 { 3 }
fn default_credential_refresh_interval_secs() -> u64 { 60 }
fn default_credential_push_connect_timeout_secs() -> u64 { 5 }
fn default_credential_push_request_timeout_secs() -> u64 { 10 }
fn default_shutdown_drain_secs() -> u64 { 25 }  // < helm terminationGracePeriodSeconds
fn default_query_timeout() -> u64 { 300 }       // 5 minutes
fn default_max_result_rows() -> usize { 1_000_000 }
fn default_max_concurrent_queries() -> usize { 100 }
fn default_max_concurrent_per_user() -> usize { 20 }
fn default_per_user_memory_budget() -> String { "1GB".to_string() }
fn default_slow_query_threshold() -> u64 { 30 }
fn default_query_profile() -> String { "off".to_string() }
fn default_runtime_filter_inlist_max_values() -> usize { 65536 }
fn default_runtime_filter_inlist_max_size() -> String { "4MB".to_string() }
fn default_stream_idle_timeout() -> u64 { 300 }
fn default_max_query_memory() -> String { "256MB".to_string() }
fn default_distribution_threshold() -> String { "128MB".to_string() }
fn default_distribution_file_threshold() -> usize { 4 }
fn default_target_task_size() -> String { "256MB".to_string() }
fn default_fanout_buffer_budget() -> String { "0".to_string() }
fn default_sort_mode() -> String { "adaptive".to_string() }
fn default_late_mat_min_projection_cols() -> usize { 1 }
fn default_star_schema_min_ratio() -> usize { 10 }

fn default_coordinator_memory() -> String { "8GB".to_string() }
fn default_memory_pool() -> String { "greedy".to_string() }
fn default_coordinator_spill_dir() -> String { "/tmp/sqe-coordinator-spill".to_string() }
fn default_spill_compression() -> String { "lz4".to_string() }
fn default_flight_compression() -> String { "lz4".to_string() }
fn default_shuffle_compression() -> String { "zstd".to_string() }
fn default_max_workers() -> usize { 1024 }
fn default_worker_connect_timeout() -> u64 { 5 }
fn default_worker_rpc_timeout() -> u64 { 630 }

fn default_flight_port() -> u16 { 50051 }
fn default_trino_port() -> u16 { 8080 }
fn default_mode() -> String { "hybrid".to_string() }
fn default_worker_flight_port() -> u16 { 50052 }
fn default_heartbeat() -> u64 { 5 }
fn default_memory() -> String { "8GB".to_string() }
fn default_spill_dir() -> String { "/tmp/sqe-spill".to_string() }
fn default_scan_timeout() -> u64 { 600 }         // 10 minutes
fn default_refresh_buffer() -> u64 { 60 }
fn default_true() -> bool { true }
fn default_cache_ttl() -> u64 { 30 }
fn default_table_format_version() -> u8 { 2 }
fn default_small_file_threshold_mb() -> u64 { 3 }
fn default_parquet_compression() -> String { "zstd".to_string() }
fn default_manifest_concurrency() -> usize { 64 }
fn default_prometheus_port() -> u16 { 9090 }
fn default_per_user_rpm() -> u32 { 60 }
fn default_global_rpm() -> u32 { 1000 }
fn default_auth_rpm() -> u32 { 10 }
fn default_metadata_rpm() -> u32 { 120 }

impl SqeConfig {
    /// Default name for the legacy `[catalog]` block when the new
    /// `[catalogs.*]` map is also populated. Picked to match what
    /// embedded mode (`sqe-cli --warehouse <path>`) calls its
    /// single catalog so users moving between embedded and cluster
    /// see the same SQL identifier.
    pub const LEGACY_CATALOG_NAME: &'static str = "iceberg";

    /// Flatten the legacy `[catalog]` block plus the `[catalogs.*]`
    /// map into a single ordered list of named catalogs. Order is
    /// stable: legacy block first if it's the only one, otherwise
    /// `[catalogs.*]` entries in alphabetical order, with the legacy
    /// block joining only when explicitly named via
    /// `query.default_catalog == Self::LEGACY_CATALOG_NAME` or when
    /// `[catalogs.*]` is empty.
    ///
    /// Practical outcomes:
    /// - Legacy single-catalog config: returns one entry,
    ///   `("iceberg", &self.catalog)`.
    /// - Pure new-style config (only `[catalogs.*]` set, no
    ///   `default_catalog`): returns the named catalogs sorted by
    ///   name. The legacy `[catalog]` block exists in the struct but
    ///   is dropped — operators set it to a placeholder REST URL to
    ///   satisfy the deserializer and the runtime ignores it.
    /// - Mixed (`[catalogs.*]` populated AND `default_catalog =
    ///   "iceberg"`): legacy block joins under name "iceberg",
    ///   alongside the named entries.
    ///
    /// The returned `&CatalogConfig` references borrow from `self`,
    /// so the caller doesn't pay clone cost during dispatch.
    pub fn flattened_catalogs(&self) -> Vec<(String, &CatalogConfig)> {
        if self.catalogs.is_empty() {
            return vec![(Self::LEGACY_CATALOG_NAME.to_string(), &self.catalog)];
        }
        let mut out: Vec<(String, &CatalogConfig)> = self
            .catalogs
            .iter()
            .map(|(k, v)| (k.clone(), v))
            .collect();
        // Stable order so DataFusion catalog registration produces a
        // deterministic information_schema ordering and the welcome
        // banner is reproducible.
        out.sort_by(|a, b| a.0.cmp(&b.0));

        // If the operator explicitly named the legacy block, fold it
        // in (or replace if the name collides).
        if let Some(name) = self
            .query
            .default_catalog
            .as_deref()
            .filter(|n| !n.is_empty())
        {
            // Remove any existing entry with the legacy name so the
            // legacy block wins (operator set it explicitly; that's
            // an opt-in to use the legacy block as that name).
            out.retain(|(n, _)| n != name);
            out.push((name.to_string(), &self.catalog));
            out.sort_by(|a, b| a.0.cmp(&b.0));
        }
        out
    }

    /// The name of the catalog that DataFusion treats as the
    /// "default" for unqualified table names. Resolution order:
    /// 1. `query.default_catalog` if set and non-empty.
    /// 2. The first entry from `flattened_catalogs()`.
    /// 3. `Self::LEGACY_CATALOG_NAME` as a last resort (no
    ///    catalogs flattened, which is a config error caught
    ///    elsewhere).
    pub fn resolve_default_catalog(&self) -> String {
        if let Some(name) = self
            .query
            .default_catalog
            .as_deref()
            .filter(|n| !n.is_empty())
        {
            return name.to_string();
        }
        self.flattened_catalogs()
            .first()
            .map(|(n, _)| n.clone())
            .unwrap_or_else(|| Self::LEGACY_CATALOG_NAME.to_string())
    }
}

impl SqeConfig {
    /// Validate configuration: required fields and port conflicts.
    pub fn validate(&self) -> crate::error::Result<()> {
        let mut errors = Vec::new();

        // Required fields
        //
        // When `auth.providers` is configured, the legacy fields (client_id,
        // keycloak_url, token_endpoint) are not required.
        let has_providers = !self.auth.providers.is_empty();

        if !has_providers && self.auth.client_id.trim().is_empty() {
            errors.push("auth.client_id is required".to_string());
        }
        if self.catalog.catalog_url.trim().is_empty() {
            errors.push(
                "catalog.catalog_url is required (TOML field; or env \
                 SQE_CATALOG__CATALOG_URL). Legacy names `polaris_url` / \
                 SQE_CATALOG__POLARIS_URL also work via serde alias."
                    .to_string(),
            );
        }
        if !has_providers
            && self.auth.keycloak_url.trim().is_empty()
            && self.auth.token_endpoint.trim().is_empty()
        {
            errors.push(
                "at least one of auth.keycloak_url or auth.token_endpoint must be set, or configure auth.providers"
                    .to_string(),
            );
        }

        // Port conflicts
        if self.coordinator.flight_sql_port == self.coordinator.trino_http_port
            && self.coordinator.trino_http_port > 0
        {
            errors.push(format!(
                "port conflict: coordinator.flight_sql_port and coordinator.trino_http_port are both {}",
                self.coordinator.flight_sql_port
            ));
        }
        if self.coordinator.flight_sql_port == self.metrics.prometheus_port {
            errors.push(format!(
                "port conflict: coordinator.flight_sql_port and metrics.prometheus_port are both {}",
                self.coordinator.flight_sql_port
            ));
        }
        if self.coordinator.quack_port > 0 {
            let qp = self.coordinator.quack_port;
            if qp == self.coordinator.flight_sql_port {
                errors.push(format!(
                    "port conflict: coordinator.quack_port and coordinator.flight_sql_port are both {qp}"
                ));
            }
            if qp == self.coordinator.trino_http_port {
                errors.push(format!(
                    "port conflict: coordinator.quack_port and coordinator.trino_http_port are both {qp}"
                ));
            }
            if qp == self.metrics.prometheus_port {
                errors.push(format!(
                    "port conflict: coordinator.quack_port and metrics.prometheus_port are both {qp}"
                ));
            }
        }

        // TLS validation: if one of cert/key is set, both must be set
        let tls = &self.coordinator.tls;
        if !tls.cert_file.is_empty() && tls.key_file.is_empty() {
            errors.push("tls.cert_file is set but tls.key_file is missing".to_string());
        }
        if tls.cert_file.is_empty() && !tls.key_file.is_empty() {
            errors.push("tls.key_file is set but tls.cert_file is missing".to_string());
        }
        if tls.is_enabled() {
            if !std::path::Path::new(&tls.cert_file).exists() {
                errors.push(format!("tls.cert_file '{}' not found", tls.cert_file));
            }
            if !std::path::Path::new(&tls.key_file).exists() {
                errors.push(format!("tls.key_file '{}' not found", tls.key_file));
            }
            if !tls.ca_file.is_empty() && !std::path::Path::new(&tls.ca_file).exists() {
                errors.push(format!("tls.ca_file '{}' not found", tls.ca_file));
            }
        }

        // Parquet compression validation
        let valid_compressions = ["zstd", "lz4", "snappy", "none"];
        if !valid_compressions.contains(&self.catalog.parquet_compression.to_lowercase().as_str()) {
            errors.push(format!(
                "catalog.parquet_compression '{}' is not supported; valid options: {}",
                self.catalog.parquet_compression,
                valid_compressions.join(", ")
            ));
        }

        // Ranger policy engine: numeric fields that silently break the store at
        // zero. A zero timeout yields an HTTP client that fails every fetch
        // (deny-all); a zero breaker threshold opens the circuit on the first
        // failure and denies permanently. Both are misconfigurations, not
        // tuning choices, so fail fast rather than fail closed forever.
        if self.policy.engine == PolicyEngine::Ranger {
            if self.policy.ranger.timeout_secs == 0 {
                errors.push(
                    "policy.ranger.timeout_secs must be >= 1 (0 yields a zero-timeout \
                     HTTP client that fails every Ranger fetch and denies all queries)"
                        .to_string(),
                );
            }
            if self.policy.ranger.breaker_failure_threshold == 0 {
                errors.push(
                    "policy.ranger.breaker_failure_threshold must be >= 1 (0 opens the \
                     circuit breaker on the first failure and denies permanently)"
                        .to_string(),
                );
            }
        }

        // Table metadata cache vs. fine-grained policy. The column->tags map
        // that drives tag masks and tag row-filters is read from the table
        // metadata cache. With the cache disabled (ttl_secs = 0 ->
        // max_capacity(0)), every tag lookup misses, and the rewriter now
        // fails CLOSED on an unknown tag state (see TagSource::column_tags).
        // The result under a policy engine is that every query is denied. Reject
        // the misconfig at load time rather than denying every query at runtime.
        if self.policy.engine != PolicyEngine::Passthrough
            && self.catalog.metadata_cache_ttl_secs == 0
        {
            errors.push(
                "catalog.metadata_cache_ttl_secs must be >= 1 when policy.engine \
                 enforces fine-grained policy (0 disables the table metadata cache, \
                 so tag lookups always miss and deny)"
                    .to_string(),
            );
        }

        // Distributed-mode worker secret is required unless explicitly waived.
        if !self.coordinator.worker_urls.is_empty()
            && self.coordinator.worker_secret.is_empty()
            && !self.coordinator.allow_unauthenticated_workers
        {
            errors.push(
                "coordinator.worker_urls is set but coordinator.worker_secret is empty. \
                 Any TCP-reachable client could register as a worker and receive query \
                 fragments with user bearer tokens. Set worker_secret (recommended), or \
                 explicitly set coordinator.allow_unauthenticated_workers = true to opt out."
                    .to_string(),
            );
        }

        // Worker-side mirror: refuse to boot a worker with an empty secret
        // unless the operator explicitly opts in. A worker that accepts
        // unauthenticated Flight calls hands out the user's S3 credentials
        // and refreshed STS tokens to any TCP-reachable peer.
        if !self.worker.coordinator_url.is_empty()
            && self.worker.worker_secret.is_empty()
            && !self.worker.allow_unauthenticated
        {
            errors.push(
                "worker.coordinator_url is set but worker.worker_secret is empty. \
                 Any TCP-reachable client could send scan tickets or refresh \
                 credentials on this worker, leaking user S3 credentials. Set \
                 worker.worker_secret to match coordinator.worker_secret \
                 (recommended), or explicitly set worker.allow_unauthenticated \
                 = true to opt out."
                    .to_string(),
            );
        }

        // Plaintext-on-the-wire fail-closed (issue #211). When distributed
        // mode reaches a non-loopback peer, every hop carries user OIDC
        // bearer tokens, the shared worker secret, and vended S3 credentials.
        // On a plaintext channel an on-path observer harvests all of these.
        // Refuse to boot without TLS unless the operator explicitly waives via
        // `security.allow_insecure_transport = true`. Loopback-only setups
        // (127.0.0.1 / ::1 / localhost) stay usable without TLS for dev.
        if distributed_reaches_network(self)
            && !self.coordinator.tls.is_enabled()
            && !self.security.allow_insecure_transport
        {
            errors.push(
                "distributed mode targets a non-loopback peer (coordinator.worker_urls \
                 or worker.coordinator_url) but TLS is not configured. User bearer \
                 tokens, the worker secret, and vended S3 credentials would travel in \
                 cleartext and can be captured and replayed by an on-path observer. \
                 Set [coordinator.tls] cert_file/key_file to enable TLS (recommended), \
                 or explicitly set security.allow_insecure_transport = true to opt out."
                    .to_string(),
            );
        }

        // tokio::time::interval panics if the period is zero. Reject the
        // misconfig at load time with a message that names the field.
        if self.session.snapshot_interval_secs == 0 {
            errors.push(
                "session.snapshot_interval_secs must be > 0 (tokio::time::interval rejects zero periods)".to_string(),
            );
        }
        if self.worker.heartbeat_interval_secs == 0 {
            errors.push(
                "worker.heartbeat_interval_secs must be > 0 (tokio::time::interval rejects zero periods)".to_string(),
            );
        }
        if self.metrics.openlineage.replay_interval_secs == 0 {
            errors.push(
                "metrics.openlineage.replay_interval_secs must be > 0 (tokio::time::interval rejects zero periods)".to_string(),
            );
        }

        validate_urls(self, &mut errors);
        validate_byte_sizes(self, &mut errors);
        validate_memory_budget_invariants(self, &mut errors);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(crate::error::SqeError::Config(
                format!("config error: {}", errors.join("; ")),
            ))
        }
    }

    pub fn load(path: &str) -> crate::error::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| crate::error::SqeError::Config(format!("Failed to read {path}: {e}")))?;
        let mut config: Self = toml::from_str(&content)
            .map_err(|e| crate::error::SqeError::Config(format!("Failed to parse config: {e}")))?;
        config.apply_env_overrides();
        config.log_deprecation_warnings();
        Ok(config)
    }

    /// Log warnings for deprecated config keys that still work but will be removed.
    fn log_deprecation_warnings(&self) {
        if !self.auth.keycloak_url.is_empty() {
            tracing::warn!("config key 'auth.keycloak_url' is deprecated — the OIDC password grant provider works with any OIDC-compliant endpoint, not just Keycloak. This key will continue to work but may be renamed in a future release.");
        }
        if let Some(w) = self.legacy_auth_ignored_warning() {
            tracing::warn!("{w}");
        }
    }

    /// #276: when both the legacy flat endpoint config (`auth.keycloak_url` /
    /// `auth.token_endpoint`) and `[[auth.providers]]` are set, the provider
    /// chain takes precedence and the legacy endpoint fields are ignored. This
    /// used to be silent; surface it as a warning so operators notice the dead
    /// config. Returns `None` when there is no ambiguity. The shared
    /// `auth.client_id`/`client_secret` are intentionally NOT flagged — the
    /// `oidc_password` provider may inherit `client_secret` from `[auth]`.
    pub(crate) fn legacy_auth_ignored_warning(&self) -> Option<String> {
        let has_providers = !self.auth.providers.is_empty();
        let has_legacy_endpoint = !self.auth.keycloak_url.trim().is_empty()
            || !self.auth.token_endpoint.trim().is_empty();
        if has_providers && has_legacy_endpoint {
            Some(
                "auth.keycloak_url / auth.token_endpoint (legacy) are set alongside \
                 [[auth.providers]]; the provider chain takes precedence and the legacy \
                 endpoint fields are IGNORED. Remove them to avoid confusion."
                    .to_string(),
            )
        } else {
            None
        }
    }

    /// Apply environment variable overrides. Convention: `SQE_<SECTION>__<FIELD>`.
    /// E.g. `SQE_AUTH__KEYCLOAK_URL=https://...` overrides `auth.keycloak_url`.
    fn apply_env_overrides(&mut self) {
        // Coordinator
        env_override_u16("SQE_COORDINATOR__FLIGHT_SQL_PORT", &mut self.coordinator.flight_sql_port);
        env_override_u16("SQE_COORDINATOR__TRINO_HTTP_PORT", &mut self.coordinator.trino_http_port);
        env_override_str("SQE_COORDINATOR__MODE", &mut self.coordinator.mode);
        env_override_bool("SQE_COORDINATOR__DEBUG", &mut self.coordinator.debug);
        env_override_secret("SQE_COORDINATOR__WORKER_SECRET", &mut self.coordinator.worker_secret);
        env_override_bool(
            "SQE_COORDINATOR__ALLOW_UNAUTHENTICATED_WORKERS",
            &mut self.coordinator.allow_unauthenticated_workers,
        );
        env_override_str("SQE_TLS__CERT_FILE", &mut self.coordinator.tls.cert_file);
        env_override_str("SQE_TLS__KEY_FILE", &mut self.coordinator.tls.key_file);
        env_override_str("SQE_TLS__CA_FILE", &mut self.coordinator.tls.ca_file);
        env_override_u64(
            "SQE_COORDINATOR__AUTH_HANDSHAKE_TIMEOUT_SECS",
            &mut self.coordinator.auth_handshake_timeout_secs,
        );
        env_override_u64(
            "SQE_COORDINATOR__HEALTH_CHECK_INTERVAL_SECS",
            &mut self.coordinator.health_check_interval_secs,
        );
        env_override_u32(
            "SQE_COORDINATOR__HEALTH_CHECK_MAX_FAILURES",
            &mut self.coordinator.health_check_max_failures,
        );
        env_override_u64(
            "SQE_COORDINATOR__CREDENTIAL_REFRESH_INTERVAL_SECS",
            &mut self.coordinator.credential_refresh_interval_secs,
        );
        env_override_u64(
            "SQE_COORDINATOR__CREDENTIAL_PUSH_CONNECT_TIMEOUT_SECS",
            &mut self.coordinator.credential_push_connect_timeout_secs,
        );
        env_override_u64(
            "SQE_COORDINATOR__CREDENTIAL_PUSH_REQUEST_TIMEOUT_SECS",
            &mut self.coordinator.credential_push_request_timeout_secs,
        );
        // Coordinator memory knobs use short, un-namespaced env names because
        // operators reach for them under OOM pressure and a mistyped
        // `SQE_COORDINATOR__MEMORY_LIMIT` silently keeps the file value (the bug
        // this fixes: an 8GB intent ran with a 64GB file limit and OOM-killed a
        // 31GB box). Applied overrides log at INFO with the value; neither is a
        // secret.
        // `SQE_MEMORY_LIMIT` overrides `coordinator.memory_limit` ("8GB", "512MB").
        env_override_str_logged("SQE_MEMORY_LIMIT", &mut self.coordinator.memory_limit);
        // `SQE_MEMORY_POOL` overrides `coordinator.memory_pool` ("greedy" | "fair").
        env_override_str_logged("SQE_MEMORY_POOL", &mut self.coordinator.memory_pool);

        // Worker
        env_override_str("SQE_WORKER__COORDINATOR_URL", &mut self.worker.coordinator_url);
        env_override_u16("SQE_WORKER__FLIGHT_PORT", &mut self.worker.flight_port);
        env_override_str("SQE_WORKER__ADVERTISE_URL", &mut self.worker.advertise_url);
        env_override_u64("SQE_WORKER__HEARTBEAT_INTERVAL_SECS", &mut self.worker.heartbeat_interval_secs);
        env_override_str("SQE_WORKER__MEMORY_LIMIT", &mut self.worker.memory_limit);
        env_override_bool("SQE_WORKER__SPILL_TO_DISK", &mut self.worker.spill_to_disk);
        env_override_str("SQE_WORKER__SPILL_DIR", &mut self.worker.spill_dir);
        env_override_u64("SQE_WORKER__SCAN_TIMEOUT_SECS", &mut self.worker.scan_timeout_secs);
        env_override_str("SQE_WORKER__WORKER_SECRET", &mut self.worker.worker_secret);
        env_override_bool(
            "SQE_WORKER__ALLOW_UNAUTHENTICATED",
            &mut self.worker.allow_unauthenticated,
        );

        // Security
        env_override_bool(
            "SQE_SECURITY__ALLOW_INSECURE_TRANSPORT",
            &mut self.security.allow_insecure_transport,
        );

        // Auth
        env_override_str("SQE_AUTH__KEYCLOAK_URL", &mut self.auth.keycloak_url);
        env_override_str("SQE_AUTH__REALM", &mut self.auth.realm);
        env_override_str("SQE_AUTH__CLIENT_ID", &mut self.auth.client_id);
        env_override_secret("SQE_AUTH__CLIENT_SECRET", &mut self.auth.client_secret);
        env_override_str("SQE_AUTH__TOKEN_ENDPOINT", &mut self.auth.token_endpoint);
        env_override_u64("SQE_AUTH__TOKEN_REFRESH_BUFFER_SECS", &mut self.auth.token_refresh_buffer_secs);
        env_override_bool("SQE_AUTH__SSL_VERIFICATION", &mut self.auth.ssl_verification);
        env_override_str("SQE_AUTH__ROLES_CLAIM", &mut self.auth.roles_claim);

        // Auth providers: secrets must be injectable via env so Kubernetes
        // Secret mounts, Vault Agent, and External Secrets Operator can rotate
        // without rewriting TOML. Convention: `SQE_AUTH__PROVIDERS__<N>__<FIELD>`
        // where `<N>` is the zero-based index in the `[[auth.providers]]`
        // array. Today only `client_secret` is wired through, matching the
        // fields where the bug bit (issue #14). Token endpoints and
        // discovery URLs stay in TOML.
        for (idx, provider) in self.auth.providers.iter_mut().enumerate() {
            let env_name = format!("SQE_AUTH__PROVIDERS__{idx}__CLIENT_SECRET");
            match provider {
                AuthProviderConfig::OidcPassword { client_secret, .. } => {
                    env_override_str(&env_name, client_secret);
                }
                AuthProviderConfig::ClientCredentials { client_secret, .. } => {
                    env_override_str(&env_name, client_secret);
                }
                AuthProviderConfig::TokenExchange { client_secret, .. } => {
                    if let Some(secret) = client_secret.as_mut() {
                        env_override_str(&env_name, secret);
                    } else if let Ok(v) = std::env::var(&env_name) {
                        if !v.is_empty() {
                            *client_secret = Some(v);
                        }
                    }
                }
                // Variants without a client_secret field: nothing to override.
                AuthProviderConfig::BearerToken { .. }
                | AuthProviderConfig::AwsIam { .. }
                | AuthProviderConfig::ApiKey { .. }
                | AuthProviderConfig::Mtls { .. }
                | AuthProviderConfig::Anonymous { .. }
                | AuthProviderConfig::ClientCredentialsPassthrough { .. }
                | AuthProviderConfig::BearerPassthrough { .. } => {}
            }
        }

        // [auth].client_secret -> oidc_password provider inheritance. The
        // shared `[auth]` client_id/client_secret is SQE's own OIDC client, the
        // same one the `oidc_password` (ROPC) provider uses. A provider with an
        // empty client_secret inherits `[auth].client_secret` (which
        // `SQE_AUTH__CLIENT_SECRET` fills), so the common "one shared secret via
        // env" setup keeps working after migrating to `[[auth.providers]]`.
        // Only empty fields are filled, so an explicit per-provider secret
        // (TOML or `SQE_AUTH__PROVIDERS__<N>__CLIENT_SECRET`) still wins. Scoped
        // to `oidc_password` on purpose: `client_credentials` / `token_exchange`
        // are distinct external clients with their own secrets.
        let shared_secret = self.auth.client_secret.expose().to_string();
        if !shared_secret.is_empty() {
            for provider in self.auth.providers.iter_mut() {
                if let AuthProviderConfig::OidcPassword { client_secret, .. } = provider {
                    if client_secret.is_empty() {
                        *client_secret = shared_secret.clone();
                    }
                }
            }
        }

        // Catalog
        // SQE_CATALOG__CATALOG_URL is the canonical env var; the legacy
        // SQE_CATALOG__POLARIS_URL is honoured first for backwards-compat
        // and overridden by the new name when both are set.
        env_override_str("SQE_CATALOG__POLARIS_URL", &mut self.catalog.catalog_url);
        env_override_str("SQE_CATALOG__CATALOG_URL", &mut self.catalog.catalog_url);
        env_override_str("SQE_CATALOG__WAREHOUSE", &mut self.catalog.warehouse);
        env_override_u64("SQE_CATALOG__METADATA_CACHE_TTL_SECS", &mut self.catalog.metadata_cache_ttl_secs);
        env_override_u8("SQE_CATALOG__DEFAULT_TABLE_FORMAT_VERSION", &mut self.catalog.default_table_format_version);
        env_override_usize("SQE_CATALOG__MANIFEST_CONCURRENCY", &mut self.catalog.manifest_concurrency);

        // Storage
        env_override_str("SQE_STORAGE__S3_ENDPOINT", &mut self.storage.s3_endpoint);
        env_override_str("SQE_STORAGE__S3_REGION", &mut self.storage.s3_region);
        env_override_str("SQE_STORAGE__S3_ACCESS_KEY", &mut self.storage.s3_access_key);
        env_override_secret("SQE_STORAGE__S3_SECRET_KEY", &mut self.storage.s3_secret_key);
        env_override_bool("SQE_STORAGE__S3_PATH_STYLE", &mut self.storage.s3_path_style);
        env_override_bool("SQE_STORAGE__S3_ALLOW_HTTP", &mut self.storage.s3_allow_http);
        env_override_usize("SQE_STORAGE__PREFETCH_CONCURRENCY", &mut self.storage.prefetch_concurrency);
        env_override_str("SQE_STORAGE__AZURE_ACCOUNT", &mut self.storage.azure_account);
        env_override_str("SQE_STORAGE__AZURE_ACCESS_KEY", &mut self.storage.azure_access_key);
        env_override_str("SQE_STORAGE__AZURE_SAS_TOKEN", &mut self.storage.azure_sas_token);
        env_override_bool("SQE_STORAGE__AZURE_USE_EMULATOR", &mut self.storage.azure_use_emulator);
        env_override_str("SQE_STORAGE__GCS_SERVICE_ACCOUNT_PATH", &mut self.storage.gcs_service_account_path);
        env_override_str("SQE_STORAGE__GCS_SERVICE_ACCOUNT_KEY", &mut self.storage.gcs_service_account_key);

        // Policy
        env_override_parse("SQE_POLICY__ENGINE", &mut self.policy.engine);
        env_override_str("SQE_POLICY__MASK_KEY", &mut self.policy.mask_key);
        env_override_secret(
            "SQE_POLICY__RANGER__ADMIN_PASSWORD",
            &mut self.policy.ranger.admin_password,
        );

        // Access control
        env_override_parse("SQE_ACCESS_CONTROL__BACKEND", &mut self.access_control.backend);
        env_override_str("SQE_ACCESS_CONTROL__URL", &mut self.access_control.url);
        env_override_secret(
            "SQE_ACCESS_CONTROL__RANGER__ADMIN_PASSWORD",
            &mut self.access_control.ranger.admin_password,
        );

        // Metrics
        env_override_u16("SQE_METRICS__PROMETHEUS_PORT", &mut self.metrics.prometheus_port);
        env_override_str("SQE_METRICS__OTLP_ENDPOINT", &mut self.metrics.otlp_endpoint);
        env_override_str("SQE_METRICS__AUDIT_LOG_PATH", &mut self.metrics.audit_log_path);
        env_override_str("SQE_METRICS__AUDIT__FORMAT", &mut self.metrics.audit.format);
        env_override_bool(
            "SQE_METRICS__AUDIT__SUPERDEBUG_LOG_RESULTS",
            &mut self.metrics.audit.superdebug_log_results,
        );
        env_override_u64(
            "SQE_METRICS__AUDIT__DASHBOARD_ACCESS_AUDIT_WINDOW_SECS",
            &mut self.metrics.audit.dashboard_access_audit_window_secs,
        );

        // Metrics: AuditExport
        env_override_bool(
            "SQE_METRICS__AUDIT_EXPORT__ENABLED",
            &mut self.metrics.audit_export.enabled,
        );
        env_override_str(
            "SQE_METRICS__AUDIT_EXPORT__OTLP_ENDPOINT",
            &mut self.metrics.audit_export.otlp_endpoint,
        );
        env_override_str(
            "SQE_METRICS__AUDIT_EXPORT__SPOOL_PATH",
            &mut self.metrics.audit_export.spool_path,
        );

        // Metrics: OpenLineage
        env_override_bool(
            "SQE_METRICS__OPENLINEAGE__ENABLED",
            &mut self.metrics.openlineage.enabled,
        );
        env_override_str(
            "SQE_METRICS__OPENLINEAGE__JOB_NAMESPACE",
            &mut self.metrics.openlineage.job_namespace,
        );
        env_override_str(
            "SQE_METRICS__OPENLINEAGE__PRODUCER",
            &mut self.metrics.openlineage.producer,
        );
        env_override_bool(
            "SQE_METRICS__OPENLINEAGE__EMIT_SELECTS",
            &mut self.metrics.openlineage.emit_selects,
        );
        env_override_str(
            "SQE_METRICS__OPENLINEAGE__FILE_PATH",
            &mut self.metrics.openlineage.file_path,
        );
        env_override_str(
            "SQE_METRICS__OPENLINEAGE__HTTP_ENDPOINT",
            &mut self.metrics.openlineage.http_endpoint,
        );
        env_override_str(
            "SQE_METRICS__OPENLINEAGE__AUTH_MODE",
            &mut self.metrics.openlineage.auth_mode,
        );
        env_override_str(
            "SQE_METRICS__OPENLINEAGE__API_KEY",
            &mut self.metrics.openlineage.api_key,
        );
        env_override_u64(
            "SQE_METRICS__OPENLINEAGE__HTTP_TIMEOUT_MS",
            &mut self.metrics.openlineage.http_timeout_ms,
        );
        env_override_u32(
            "SQE_METRICS__OPENLINEAGE__HTTP_RETRY_ATTEMPTS",
            &mut self.metrics.openlineage.http_retry_attempts,
        );
        env_override_str(
            "SQE_METRICS__OPENLINEAGE__SPOOL_PATH",
            &mut self.metrics.openlineage.spool_path,
        );
        env_override_u64(
            "SQE_METRICS__OPENLINEAGE__SPOOL_MAX_BYTES",
            &mut self.metrics.openlineage.spool_max_bytes,
        );
        env_override_u64(
            "SQE_METRICS__OPENLINEAGE__REPLAY_INTERVAL_SECS",
            &mut self.metrics.openlineage.replay_interval_secs,
        );
        env_override_usize(
            "SQE_METRICS__OPENLINEAGE__CHANNEL_CAPACITY",
            &mut self.metrics.openlineage.channel_capacity,
        );

        // Rate limit
        env_override_bool("SQE_RATE_LIMIT__ENABLED", &mut self.rate_limit.enabled);
        env_override_u32("SQE_RATE_LIMIT__PER_USER_QUERIES_PER_MINUTE", &mut self.rate_limit.per_user_queries_per_minute);
        env_override_u32("SQE_RATE_LIMIT__GLOBAL_QUERIES_PER_MINUTE", &mut self.rate_limit.global_queries_per_minute);

        // Session
        env_override_u64("SQE_SESSION__IDLE_TIMEOUT_SECS", &mut self.session.idle_timeout_secs);
        env_override_u64("SQE_SESSION__ABSOLUTE_TIMEOUT_SECS", &mut self.session.absolute_timeout_secs);
        env_override_u64(
            "SQE_SESSION__EXPIRY_SWEEP_INTERVAL_SECS",
            &mut self.session.expiry_sweep_interval_secs,
        );

        // Query
        env_override_u64("SQE_QUERY__TIMEOUT_SECS", &mut self.query.timeout_secs);
    }
}

/// Validate the relationship between `query.per_user_memory_budget` and
/// `query.max_query_memory`.
///
/// When the per-user budget is enabled (non-zero) and is smaller than the
/// per-query reservation, every single query is rejected on admission
/// with `Per-user memory budget exceeded for '<user>': 0 bytes reserved,
/// limit <N> bytes` — the message reads as if the user is over budget
/// when in fact the *request* exceeds the budget on the very first
/// query. We hit this with the SQE 1GB default + the 32GB
/// `max_query_memory` shipped in `tests/sqe-test.toml` for SF100 sweeps
/// (issue #131): integration tests fired the rejection on every
/// statement until the per-user gate was disabled with
/// `per_user_memory_budget = "0"`.
///
/// Fail loudly at config-load time so operators see the misconfiguration
/// before the first query reaches the engine. `per_user_memory_budget =
/// "0"` (gate disabled) is always accepted.
fn validate_memory_budget_invariants(config: &SqeConfig, errors: &mut Vec<String>) {
    let budget_str = config.query.per_user_memory_budget.trim();
    let per_query_str = config.query.max_query_memory.trim();
    if budget_str.is_empty() || per_query_str.is_empty() {
        return;
    }
    if budget_str == "0" {
        // Operator deliberately disabled the per-user gate.
        return;
    }
    let budget = match parse_memory_limit(budget_str) {
        Ok(v) => v,
        Err(_) => return, // surfaced by validate_byte_sizes
    };
    let per_query = match parse_memory_limit(per_query_str) {
        Ok(v) => v,
        Err(_) => return,
    };
    if per_query > 0 && budget < per_query {
        errors.push(format!(
            "query.per_user_memory_budget ({budget_str}) must be >= \
             query.max_query_memory ({per_query_str}), or set \
             per_user_memory_budget = \"0\" to disable the gate. With \
             the current values every single query would be rejected on \
             admission with `Per-user memory budget exceeded for \
             '<user>': 0 bytes reserved, limit {budget} bytes` because \
             the first reservation alone ({per_query} bytes) exceeds \
             the per-user cap"
        ));
    }
}

/// Validate every byte-size string in the config. Moves the parse-error
/// surface for memory-limit / cache-size strings from query time to
/// startup so `coordinator.memory_limit = "8X"` fails at config-load
/// instead of two seconds into the first query (issue #116).
fn validate_byte_sizes(config: &SqeConfig, errors: &mut Vec<String>) {
    let mut check = |field: &str, value: &str| {
        if value.is_empty() {
            return;
        }
        if let Err(e) = parse_memory_limit(value) {
            errors.push(format!("{field} = {value:?}: {e}"));
        }
    };

    check("coordinator.memory_limit", &config.coordinator.memory_limit);
    check("worker.memory_limit", &config.worker.memory_limit);
    check("storage.coalesce_threshold", &config.storage.coalesce_threshold);
    check("storage.footer_cache_size", &config.storage.footer_cache_size);
    check("storage.prefetch_buffer", &config.storage.prefetch_buffer);
}

/// Validate every URL-shaped string in the config. Empty values are
/// skipped (they're either optional or caught by required-field checks
/// elsewhere). Each failure produces a precise field-name error so the
/// operator does not have to chase a parse error two seconds into the
/// first query (issue #108).
fn validate_urls(config: &SqeConfig, errors: &mut Vec<String>) {
    let mut check = |field: &str, value: &str| {
        if value.is_empty() {
            return;
        }
        if let Err(e) = url::Url::parse(value) {
            errors.push(format!("{field} = {value:?} is not a valid URL: {e}"));
        }
    };

    check("catalog.catalog_url", &config.catalog.catalog_url);
    for (name, cat) in &config.catalogs {
        let label = format!("catalogs.{name}.catalog_url");
        check(&label, &cat.catalog_url);
    }

    check("worker.coordinator_url", &config.worker.coordinator_url);
    for (i, url) in config.coordinator.worker_urls.iter().enumerate() {
        let label = format!("coordinator.worker_urls[{i}]");
        check(&label, url);
    }

    check("storage.s3_endpoint", &config.storage.s3_endpoint);
    check("metrics.otlp_endpoint", &config.metrics.otlp_endpoint);
    check("metrics.openlineage.http_endpoint", &config.metrics.openlineage.http_endpoint);

    check("auth.keycloak_url", &config.auth.keycloak_url);
    check("auth.token_endpoint", &config.auth.token_endpoint);

    if let Some(ext) = &config.auth.external {
        check("auth.external.issuer", &ext.issuer);
        if let Some(v) = ext.authorization_endpoint.as_deref() {
            check("auth.external.authorization_endpoint", v);
        }
        if let Some(v) = ext.token_endpoint.as_deref() {
            check("auth.external.token_endpoint", v);
        }
        if let Some(v) = ext.device_authorization_endpoint.as_deref() {
            check("auth.external.device_authorization_endpoint", v);
        }
    }

    check("access_control.url", &config.access_control.url);
    check("policy.ranger.url", &config.policy.ranger.url);
}

fn env_override_str(key: &str, target: &mut String) {
    if let Ok(val) = std::env::var(key) {
        *target = val;
    }
}

/// Like [`env_override_str`], but logs at INFO when the override fires. The
/// value is included: these knobs (coordinator memory limit / pool) are not
/// secrets, and an operator diagnosing an OOM needs to see the effective value
/// rather than guess whether the env var took effect.
fn env_override_str_logged(key: &str, target: &mut String) {
    if let Ok(val) = std::env::var(key) {
        tracing::info!(env = key, value = %val, "config override applied from environment");
        *target = val;
    }
}

fn env_override_parse<T>(key: &str, target: &mut T)
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    if let Ok(val) = std::env::var(key) {
        match val.parse::<T>() {
            Ok(parsed) => *target = parsed,
            Err(e) => tracing::warn!("{key}={val:?} rejected: {e}; keeping previous value"),
        }
    }
}

fn env_override_secret(key: &str, target: &mut SecretString) {
    if let Ok(val) = std::env::var(key) {
        *target = SecretString::new(val);
    }
}

fn env_override_u8(key: &str, target: &mut u8) {
    if let Ok(val) = std::env::var(key) {
        if let Ok(parsed) = val.parse() {
            *target = parsed;
        } else {
            tracing::warn!("{key}={val:?} is not a valid u8, ignoring");
        }
    }
}

fn env_override_u32(key: &str, target: &mut u32) {
    if let Ok(val) = std::env::var(key) {
        if let Ok(parsed) = val.parse() {
            *target = parsed;
        } else {
            tracing::warn!("{key}={val:?} is not a valid u32, ignoring");
        }
    }
}

fn env_override_u16(key: &str, target: &mut u16) {
    if let Ok(val) = std::env::var(key) {
        if let Ok(parsed) = val.parse() {
            *target = parsed;
        } else {
            tracing::warn!("{key}={val:?} is not a valid u16, ignoring");
        }
    }
}

fn env_override_u64(key: &str, target: &mut u64) {
    if let Ok(val) = std::env::var(key) {
        if let Ok(parsed) = val.parse() {
            *target = parsed;
        } else {
            tracing::warn!("{key}={val:?} is not a valid u64, ignoring");
        }
    }
}

fn env_override_bool(key: &str, target: &mut bool) {
    if let Ok(val) = std::env::var(key) {
        match val.to_lowercase().as_str() {
            "true" | "1" | "yes" => *target = true,
            "false" | "0" | "no" => *target = false,
            _ => tracing::warn!("{key}={val:?} is not a valid bool, ignoring"),
        }
    }
}

fn env_override_usize(key: &str, target: &mut usize) {
    if let Ok(val) = std::env::var(key) {
        if let Ok(parsed) = val.parse() {
            *target = parsed;
        } else {
            tracing::warn!("{key}={val:?} is not a valid usize, ignoring");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_memory_limit_bytes() {
        assert_eq!(parse_memory_limit("1024").unwrap(), 1024);
        assert_eq!(parse_memory_limit("1024B").unwrap(), 1024);
        assert_eq!(parse_memory_limit("1024b").unwrap(), 1024);
    }

    #[test]
    fn catalog_discovery_parses_and_defaults() {
        assert_eq!(CatalogDiscovery::parse("polaris-auto"), CatalogDiscovery::PolarisAuto);
        assert_eq!(CatalogDiscovery::parse("static"), CatalogDiscovery::Static);
        assert_eq!(CatalogDiscovery::parse("PoLaRiS-AuTo"), CatalogDiscovery::PolarisAuto);
        assert_eq!(CatalogDiscovery::parse("nonsense"), CatalogDiscovery::Static);
        assert_eq!(CatalogDiscovery::default(), CatalogDiscovery::Static);
    }

    #[test]
    fn test_parse_memory_limit_kilobytes() {
        assert_eq!(parse_memory_limit("1K").unwrap(), 1024);
        assert_eq!(parse_memory_limit("1KB").unwrap(), 1024);
        assert_eq!(parse_memory_limit("2kb").unwrap(), 2048);
    }

    #[test]
    fn test_parse_memory_limit_megabytes() {
        assert_eq!(parse_memory_limit("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_memory_limit("1MB").unwrap(), 1024 * 1024);
        assert_eq!(parse_memory_limit("512MB").unwrap(), 512 * 1024 * 1024);
    }

    #[test]
    fn test_parse_memory_limit_gigabytes() {
        assert_eq!(parse_memory_limit("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_limit("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_limit("8GB").unwrap(), 8 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_memory_limit_terabytes() {
        assert_eq!(
            parse_memory_limit("1TB").unwrap(),
            1024 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn test_parse_memory_limit_whitespace() {
        assert_eq!(parse_memory_limit("  1GB  ").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_memory_limit_empty() {
        assert!(parse_memory_limit("").is_err());
        assert!(parse_memory_limit("   ").is_err());
    }

    #[test]
    fn test_parse_memory_limit_invalid() {
        assert!(parse_memory_limit("abc").is_err());
        assert!(parse_memory_limit("GB").is_err());
    }

    #[test]
    fn test_parse_memory_limit_unknown_suffix() {
        assert!(parse_memory_limit("100XB").is_err());
    }

    #[test]
    fn test_parse_memory_limit_fractional_still_supported() {
        // CORE-02 fix must not regress fractional sizes.
        assert_eq!(
            parse_memory_limit("1.5GB").unwrap(),
            (1.5 * 1024.0 * 1024.0 * 1024.0) as usize
        );
    }

    #[test]
    fn test_parse_memory_limit_overflow_errors_not_saturates() {
        // CORE-02: an oversized value must error, not saturate to usize::MAX.
        let result = parse_memory_limit("99999999TB");
        assert!(result.is_err(), "oversized memory limit must be rejected");
        // Confirm the previous saturating behaviour is gone.
        assert_ne!(result.unwrap_or(0), usize::MAX);
    }

    // CORE-01: credential fields must never appear in Debug output.
    #[test]
    fn auth_provider_config_debug_redacts_client_secret() {
        let cfg = AuthProviderConfig::OidcPassword {
            token_url: "https://idp/token".to_string(),
            client_id: "sqe".to_string(),
            client_secret: "super-secret-value".to_string(),
            roles_claim: "realm_access.roles".to_string(),
            subject_claim: "sub".to_string(),
            email_claim: String::new(),
            groups_claim: String::new(),
            fallthrough_on_reject: false,
        };
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("super-secret-value"), "leaked secret: {dbg}");
        assert!(dbg.contains("[REDACTED]"), "expected redaction marker: {dbg}");

        let cc = AuthProviderConfig::ClientCredentials {
            token_endpoint: "https://idp/token".to_string(),
            client_id: "sqe".to_string(),
            client_secret: "cc-secret-xyz".to_string(),
        };
        assert!(!format!("{cc:?}").contains("cc-secret-xyz"));

        let te = AuthProviderConfig::TokenExchange {
            token_url: "https://idp/token".to_string(),
            client_id: "sqe".to_string(),
            client_secret: Some("te-secret-abc".to_string()),
            audience: None,
            user_claim: "sub".to_string(),
            roles_claim: "realm_access.roles".to_string(),
        };
        assert!(!format!("{te:?}").contains("te-secret-abc"));
    }

    #[test]
    fn worker_config_debug_redacts_worker_secret() {
        let cfg = WorkerConfig {
            worker_secret: "worker-secret-12345".to_string(),
            ..Default::default()
        };
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("worker-secret-12345"), "leaked secret: {dbg}");
        assert!(dbg.contains("[REDACTED]"), "expected redaction marker: {dbg}");
    }

    #[test]
    fn openlineage_config_debug_redacts_api_key() {
        let cfg = OpenLineageConfig {
            api_key: "ol-api-key-secret".to_string(),
            ..Default::default()
        };
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("ol-api-key-secret"), "leaked secret: {dbg}");
        // Reachable via MetricsConfig's derived Debug too.
        let metrics = MetricsConfig {
            openlineage: cfg,
            ..Default::default()
        };
        assert!(!format!("{metrics:?}").contains("ol-api-key-secret"));
    }

    #[test]
    fn test_distribution_threshold_config() {
        let config = QueryConfig::default();
        assert_eq!(config.distribution_threshold, "128MB");
        assert_eq!(config.distribution_file_threshold, 4);
    }

    #[test]
    fn test_worker_config_defaults() {
        let config = WorkerConfig::default();
        assert_eq!(config.memory_limit, "8GB");
        assert!(config.spill_to_disk);
        assert_eq!(config.spill_dir, "/tmp/sqe-spill");
        assert_eq!(config.flight_port, 50052);
        assert_eq!(config.heartbeat_interval_secs, 5);
    }

    /// Helper to build a valid config for validation tests.
    fn valid_config() -> SqeConfig {
        SqeConfig {
            coordinator: CoordinatorConfig {
                flight_sql_port: 50051,
                trino_http_port: 8080,
                quack_port: 0,
                mode: "hybrid".to_string(),
                worker_urls: vec![],
                debug: false,
                tls: TlsConfig::default(),
                worker_secret: SecretString::default(),
                allow_unauthenticated_workers: false,
                memory_limit: default_coordinator_memory(),
                memory_pool: default_memory_pool(),
                spill_to_disk: true,
                spill_dir: default_coordinator_spill_dir(),
                spill_compression: default_spill_compression(),
                flight_compression: default_flight_compression(),
                shuffle_compression: default_shuffle_compression(),
                max_workers: default_max_workers(),
                transport: GrpcTransportConfig::default(),
                worker_connect_timeout_secs: default_worker_connect_timeout(),
                worker_rpc_timeout_secs: default_worker_rpc_timeout(),
                auth_handshake_timeout_secs: default_auth_handshake_timeout_secs(),
                health_check_interval_secs: default_health_check_interval_secs(),
                health_check_max_failures: default_health_check_max_failures(),
                credential_refresh_interval_secs: default_credential_refresh_interval_secs(),
                credential_push_connect_timeout_secs: default_credential_push_connect_timeout_secs(),
                credential_push_request_timeout_secs: default_credential_push_request_timeout_secs(),
                shutdown_drain_secs: default_shutdown_drain_secs(),
            },
            worker: WorkerConfig::default(),
            auth: AuthConfig {
                keycloak_url: "https://keycloak.example.com".to_string(),
                realm: "sqe".to_string(),
                client_id: "sqe-client".to_string(),
                client_secret: SecretString::default(),
                token_endpoint: String::new(),
                token_refresh_buffer_secs: 60,
                ssl_verification: true,
                tls_skip_verify: false,
                roles_claim: default_roles_claim(),
                providers: Vec::new(),
                role_mappings: std::collections::HashMap::new(),
                external: None,
                admin_roles: default_admin_roles(),
            },
            catalog: CatalogConfig {
                catalog_url: "https://polaris.example.com".to_string(),
                warehouse: "wh".to_string(),
                backend: CatalogBackend::default(),
                metadata_cache_ttl_secs: 30,
                default_table_format_version: 2,
                trust_sort_order: false,
                namespace_visibility_filter: true,
                small_file_threshold_mb: 3,
                parquet_compression: "zstd".to_string(),
                manifest_concurrency: 64,
                runtime_filters: RuntimeFiltersConfig::default(),
                auth: None,
                storage: None,
            },
            catalogs: HashMap::new(),
            storage: StorageConfig::default(),
            policy: PolicyConfig::default(),
            access_control: AccessControlConfig::default(),
            metrics: MetricsConfig::default(),
            rate_limit: RateLimitConfig::default(),
            security: SecurityConfig::default(),
            session: SessionConfig::default(),
            query: QueryConfig::default(),
            query_cache: QueryCacheConfig::default(),
            query_history: QueryHistoryConfig::default(),
        }
    }

    #[test]
    fn test_validate_valid_config() {
        let config = valid_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_missing_client_id() {
        let mut config = valid_config();
        config.auth.client_id = String::new();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("auth.client_id is required"),
            "Expected client_id error, got: {err}"
        );
    }

    #[test]
    fn test_validate_missing_catalog_url() {
        let mut config = valid_config();
        config.catalog.catalog_url = String::new();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("catalog.catalog_url is required"),
            "Expected catalog_url error, got: {err}"
        );
    }

    #[test]
    fn test_validate_no_auth_backend() {
        let mut config = valid_config();
        config.auth.keycloak_url = String::new();
        config.auth.token_endpoint = String::new();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("auth.keycloak_url or auth.token_endpoint"),
            "Expected auth backend error, got: {err}"
        );
    }

    #[test]
    fn test_validate_token_endpoint_suffices() {
        let mut config = valid_config();
        config.auth.keycloak_url = String::new();
        config.auth.token_endpoint = "https://token.example.com/token".to_string();
        assert!(config.validate().is_ok());
    }

    // #276: legacy flat endpoint config + [[auth.providers]] is ambiguous.
    fn oidc_provider_for_validate() -> AuthProviderConfig {
        AuthProviderConfig::OidcPassword {
            token_url: "https://idp.example.com/token".to_string(),
            client_id: "sqe".to_string(),
            client_secret: String::new(),
            roles_claim: "realm_access.roles".to_string(),
            subject_claim: "sub".to_string(),
            email_claim: String::new(),
            groups_claim: String::new(),
            fallthrough_on_reject: false,
        }
    }

    #[test]
    fn warns_when_legacy_keycloak_url_set_with_providers() {
        let mut config = valid_config(); // valid_config sets keycloak_url
        config.auth.providers = vec![oidc_provider_for_validate()];
        // Non-fatal (3 quickstarts intentionally set both), but warned.
        assert!(config.validate().is_ok());
        let w = config.legacy_auth_ignored_warning().expect("warning");
        assert!(w.contains("IGNORED"), "got: {w}");
    }

    #[test]
    fn warns_when_legacy_token_endpoint_set_with_providers() {
        let mut config = valid_config();
        config.auth.keycloak_url = String::new();
        config.auth.token_endpoint = "https://token.example.com/token".to_string();
        config.auth.providers = vec![oidc_provider_for_validate()];
        assert!(config.legacy_auth_ignored_warning().is_some());
    }

    #[test]
    fn no_warning_for_providers_without_legacy_endpoints() {
        let mut config = valid_config();
        config.auth.keycloak_url = String::new();
        config.auth.token_endpoint = String::new();
        config.auth.providers = vec![oidc_provider_for_validate()];
        assert!(config.validate().is_ok(), "{:?}", config.validate());
        assert!(config.legacy_auth_ignored_warning().is_none());
    }

    #[test]
    fn no_warning_for_shared_client_secret_with_providers() {
        // The shared [auth].client_secret is fine alongside providers (the
        // oidc_password provider may inherit it); only legacy ENDPOINT fields
        // trigger the precedence warning.
        let mut config = valid_config();
        config.auth.keycloak_url = String::new();
        config.auth.token_endpoint = String::new();
        config.auth.client_secret = SecretString::new("shared".to_string());
        config.auth.providers = vec![oidc_provider_for_validate()];
        assert!(config.legacy_auth_ignored_warning().is_none());
    }

    #[test]
    fn test_validate_rejects_malformed_catalog_url() {
        let mut config = valid_config();
        config.catalog.catalog_url = "not a url".to_string();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("catalog.catalog_url") && err.contains("not a valid URL"),
            "Expected URL parse error, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_malformed_memory_limit() {
        let mut config = valid_config();
        config.coordinator.memory_limit = "12XYZ".to_string();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("coordinator.memory_limit"),
            "Expected memory_limit parse error, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_malformed_worker_url() {
        let mut config = valid_config();
        config.coordinator.worker_urls = vec!["http://good.example".to_string(), "://broken".to_string()];
        config.coordinator.worker_secret = SecretString::new("s".to_string());
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("coordinator.worker_urls[1]") && err.contains("not a valid URL"),
            "Expected URL parse error, got: {err}"
        );
    }

    // ── Insecure-transport fail-closed (issue #211) ─────────────

    #[test]
    fn test_validate_rejects_distributed_non_loopback_without_tls() {
        // Coordinator side: a non-loopback worker URL with no TLS and no
        // opt-in must be rejected.
        let mut config = valid_config();
        config.coordinator.worker_urls = vec!["http://worker-1.svc.cluster.local:50052".to_string()];
        config.coordinator.worker_secret = SecretString::new("shared".to_string());
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("non-loopback peer") && err.contains("allow_insecure_transport"),
            "Expected insecure-transport error, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_distributed_worker_non_loopback_without_tls() {
        // Worker side: a non-loopback coordinator URL with no TLS and no
        // opt-in must be rejected.
        let mut config = valid_config();
        config.worker.coordinator_url = "http://coordinator.svc.cluster.local:50051".to_string();
        config.worker.worker_secret = "shared".to_string();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("non-loopback peer"),
            "Expected insecure-transport error, got: {err}"
        );
    }

    #[test]
    fn test_validate_allows_distributed_non_loopback_with_opt_in() {
        let mut config = valid_config();
        config.coordinator.worker_urls = vec!["http://worker-1.svc.cluster.local:50052".to_string()];
        config.coordinator.worker_secret = SecretString::new("shared".to_string());
        config.security.allow_insecure_transport = true;
        assert!(
            config.validate().is_ok(),
            "opt-in should permit plaintext distributed: {:?}",
            config.validate().err()
        );
    }

    #[test]
    fn test_validate_allows_distributed_non_loopback_with_tls() {
        let mut config = valid_config();
        config.coordinator.worker_urls = vec!["http://worker-1.svc.cluster.local:50052".to_string()];
        config.coordinator.worker_secret = SecretString::new("shared".to_string());
        // is_enabled() only checks the strings are non-empty; the paths are
        // validated elsewhere and the existing valid_config skips that check.
        config.coordinator.tls.cert_file = "/etc/sqe/tls.crt".to_string();
        config.coordinator.tls.key_file = "/etc/sqe/tls.key".to_string();
        let err = config.validate().err().map(|e| e.to_string()).unwrap_or_default();
        assert!(
            !err.contains("non-loopback peer"),
            "TLS-enabled distributed should not raise the transport error, got: {err}"
        );
    }

    #[test]
    fn test_validate_allows_loopback_distributed_without_tls() {
        // All-loopback dev setup stays usable without TLS.
        let mut config = valid_config();
        config.coordinator.worker_urls = vec!["http://127.0.0.1:50052".to_string()];
        config.worker.coordinator_url = "http://localhost:50051".to_string();
        config.coordinator.worker_secret = SecretString::new("shared".to_string());
        config.worker.worker_secret = "shared".to_string();
        assert!(
            config.validate().is_ok(),
            "loopback distributed should be ok without TLS: {:?}",
            config.validate().err()
        );
    }

    #[test]
    fn test_host_is_loopback_helper() {
        assert!(host_is_loopback("127.0.0.1"));
        assert!(host_is_loopback("127.5.6.7"));
        assert!(host_is_loopback("::1"));
        assert!(host_is_loopback("[::1]"));
        assert!(host_is_loopback("localhost"));
        assert!(host_is_loopback("LOCALHOST"));
        assert!(!host_is_loopback("0.0.0.0"));
        assert!(!host_is_loopback("10.0.0.5"));
        assert!(!host_is_loopback("worker.svc.cluster.local"));
    }

    #[test]
    fn test_validate_rejects_zero_snapshot_interval() {
        let mut config = valid_config();
        config.session.snapshot_interval_secs = 0;
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("session.snapshot_interval_secs must be > 0"),
            "Expected zero snapshot interval error, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_zero_heartbeat_interval() {
        let mut config = valid_config();
        config.worker.heartbeat_interval_secs = 0;
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("worker.heartbeat_interval_secs must be > 0"),
            "Expected zero heartbeat interval error, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_zero_replay_interval() {
        let mut config = valid_config();
        config.metrics.openlineage.replay_interval_secs = 0;
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("metrics.openlineage.replay_interval_secs must be > 0"),
            "Expected zero replay interval error, got: {err}"
        );
    }

    #[test]
    fn test_validate_flight_trino_port_conflict() {
        let mut config = valid_config();
        config.coordinator.flight_sql_port = 8080;
        config.coordinator.trino_http_port = 8080;
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("flight_sql_port") && err.contains("trino_http_port"),
            "Expected port conflict error, got: {err}"
        );
    }

    #[test]
    fn test_validate_flight_metrics_port_conflict() {
        let mut config = valid_config();
        config.coordinator.flight_sql_port = 9090;
        config.metrics.prometheus_port = 9090;
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("flight_sql_port") && err.contains("prometheus_port"),
            "Expected port conflict error, got: {err}"
        );
    }

    #[test]
    fn test_validate_multiple_errors() {
        let mut config = valid_config();
        config.auth.client_id = String::new();
        config.catalog.catalog_url = "  ".to_string();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("auth.client_id") && err.contains("catalog.catalog_url"),
            "Expected multiple errors, got: {err}"
        );
    }

    /// Regression for issue #6: distributed mode with no worker_secret used
    /// to log a SECURITY error and continue booting, leaving the heartbeat
    /// handler open to any reachable client. validate() must now refuse.
    #[test]
    fn validate_rejects_distributed_without_worker_secret() {
        let mut config = valid_config();
        config.coordinator.worker_urls = vec!["http://worker-1:50051".to_string()];
        config.coordinator.worker_secret = SecretString::default();
        config.coordinator.allow_unauthenticated_workers = false;
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("worker_urls") && err.contains("worker_secret"),
            "Expected worker_secret guard error, got: {err}"
        );
    }

    #[test]
    fn validate_accepts_distributed_when_explicitly_unauthenticated() {
        let mut config = valid_config();
        config.coordinator.worker_urls = vec!["http://worker-1:50051".to_string()];
        config.coordinator.worker_secret = SecretString::default();
        config.coordinator.allow_unauthenticated_workers = true;
        // Non-loopback peer without TLS: waive the transport rule so this
        // test stays focused on the worker_secret guard.
        config.security.allow_insecure_transport = true;
        // The explicit opt-in is allowed, visible in config diffs.
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_accepts_distributed_with_worker_secret() {
        let mut config = valid_config();
        config.coordinator.worker_urls = vec!["http://worker-1:50051".to_string()];
        config.coordinator.worker_secret = SecretString::new("shared-secret-value".to_string());
        config.security.allow_insecure_transport = true;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_accepts_single_node_without_worker_secret() {
        // No workers configured -> secret irrelevant, no error.
        let mut config = valid_config();
        config.coordinator.worker_urls.clear();
        config.coordinator.worker_secret = SecretString::default();
        assert!(config.validate().is_ok());
    }

    /// Regression for issues #22 + #35: a worker that registers with a
    /// coordinator but has an empty `worker_secret` would accept
    /// unauthenticated scan tickets and credential refresh actions from
    /// anyone with network reach. validate() must refuse unless the
    /// operator explicitly waives.
    #[test]
    fn validate_rejects_worker_without_secret() {
        let mut config = valid_config();
        config.worker.coordinator_url = "http://coordinator:50051".to_string();
        config.worker.worker_secret = String::new();
        config.worker.allow_unauthenticated = false;
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("worker.coordinator_url") && err.contains("worker.worker_secret"),
            "Expected worker-side secret guard error, got: {err}"
        );
    }

    #[test]
    fn validate_accepts_worker_when_explicitly_unauthenticated() {
        let mut config = valid_config();
        config.worker.coordinator_url = "http://coordinator:50051".to_string();
        config.worker.worker_secret = String::new();
        config.worker.allow_unauthenticated = true;
        config.security.allow_insecure_transport = true;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_accepts_worker_with_secret() {
        let mut config = valid_config();
        config.worker.coordinator_url = "http://coordinator:50051".to_string();
        config.worker.worker_secret = "shared-secret-value".to_string();
        config.security.allow_insecure_transport = true;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_accepts_standalone_worker_without_secret() {
        // A worker not pointed at any coordinator is not exposed to the
        // distributed-mode threat model; no error.
        let mut config = valid_config();
        config.worker.coordinator_url = String::new();
        config.worker.worker_secret = String::new();
        assert!(config.validate().is_ok());
    }

    /// Old configs that used `polaris_url` continue to deserialize via the
    /// serde alias. New configs use `catalog_url`. Both populate the same
    /// in-memory field, and a config built from a legacy TOML still passes
    /// validate() — guarding against any future addition of
    /// `#[serde(deny_unknown_fields)]` on `CatalogConfig` or its parent
    /// that would silently drop the alias path.
    #[test]
    fn legacy_polaris_url_alias_deserializes() {
        let new_toml = "[catalog]\ncatalog_url = \"http://new.example.com\"\nwarehouse = \"wh\"\n";
        let old_toml =
            "[catalog]\npolaris_url = \"http://old.example.com\"\nwarehouse = \"wh\"\n";

        #[derive(serde::Deserialize)]
        struct Wrap {
            catalog: CatalogConfig,
        }
        let new_w: Wrap = toml::from_str(new_toml).expect("new TOML deserializes");
        let old_w: Wrap = toml::from_str(old_toml).expect("legacy TOML deserializes");
        assert_eq!(new_w.catalog.catalog_url, "http://new.example.com");
        assert_eq!(old_w.catalog.catalog_url, "http://old.example.com");

        // Round-trip into a full SqeConfig and validate. Both must pass.
        let mut full = valid_config();
        full.catalog = old_w.catalog;
        assert!(
            full.validate().is_ok(),
            "legacy polaris_url config should pass validate()"
        );
    }

    /// Legacy single-catalog config: flattening yields one entry
    /// named `iceberg` (the canonical embedded-mode name) and
    /// `resolve_default_catalog()` returns it.
    #[test]
    fn flatten_with_only_legacy_catalog() {
        let cfg = valid_config();
        let flat = cfg.flattened_catalogs();
        assert_eq!(flat.len(), 1);
        assert_eq!(flat[0].0, SqeConfig::LEGACY_CATALOG_NAME);
        assert_eq!(cfg.resolve_default_catalog(), "iceberg");
    }

    /// New-style config with a single named catalog: flattening
    /// uses that name and the legacy block is dropped.
    #[test]
    fn flatten_with_named_catalog_drops_legacy() {
        let mut cfg = valid_config();
        cfg.catalogs.insert(
            "polaris".to_string(),
            CatalogConfig {
                catalog_url: "http://polaris:8181".to_string(),
                warehouse: "main".to_string(),
                backend: CatalogBackend::default(),
                metadata_cache_ttl_secs: 30,
                default_table_format_version: 2,
                trust_sort_order: false,
                namespace_visibility_filter: true,
                small_file_threshold_mb: 3,
                parquet_compression: "zstd".to_string(),
                manifest_concurrency: 64,
                runtime_filters: RuntimeFiltersConfig::default(),
                auth: None,
                storage: None,
            },
        );
        let flat = cfg.flattened_catalogs();
        assert_eq!(flat.len(), 1);
        assert_eq!(flat[0].0, "polaris");
        assert_eq!(cfg.resolve_default_catalog(), "polaris");
    }

    /// Two named catalogs: alphabetical order, first becomes default
    /// unless the operator names one explicitly.
    #[test]
    fn flatten_sorts_named_catalogs() {
        let mut cfg = valid_config();
        for name in ["zeta", "alpha", "mid"] {
            cfg.catalogs.insert(
                name.to_string(),
                CatalogConfig {
                    catalog_url: "http://x".to_string(),
                    warehouse: name.to_string(),
                    backend: CatalogBackend::default(),
                    metadata_cache_ttl_secs: 30,
                    default_table_format_version: 2,
                    trust_sort_order: false,
                namespace_visibility_filter: true,
                    small_file_threshold_mb: 3,
                    parquet_compression: "zstd".to_string(),
                    manifest_concurrency: 64,
                    runtime_filters: RuntimeFiltersConfig::default(),
                    auth: None,
                    storage: None,
                },
            );
        }
        let names: Vec<String> = cfg
            .flattened_catalogs()
            .iter()
            .map(|(n, _)| n.clone())
            .collect();
        assert_eq!(names, vec!["alpha", "mid", "zeta"]);
        assert_eq!(cfg.resolve_default_catalog(), "alpha");
    }

    /// Operator-set `default_catalog` overrides alphabetical pick
    /// AND folds the legacy `[catalog]` block in under that name.
    /// The mixed-mode use case: one Polaris cluster `[catalog]`
    /// kept for backwards-compat, plus a Glue `[catalogs.glue]`
    /// for new tables, with Polaris remaining the default.
    #[test]
    fn default_catalog_can_promote_legacy_block() {
        let mut cfg = valid_config();
        cfg.query.default_catalog = Some("legacy_polaris".to_string());
        cfg.catalogs.insert(
            "glue".to_string(),
            CatalogConfig {
                catalog_url: "".to_string(),
                warehouse: "s3://wh/".to_string(),
                backend: CatalogBackend::default(),
                metadata_cache_ttl_secs: 30,
                default_table_format_version: 2,
                trust_sort_order: false,
                namespace_visibility_filter: true,
                small_file_threshold_mb: 3,
                parquet_compression: "zstd".to_string(),
                manifest_concurrency: 64,
                runtime_filters: RuntimeFiltersConfig::default(),
                auth: None,
                storage: None,
            },
        );
        let flat = cfg.flattened_catalogs();
        let names: Vec<String> = flat.iter().map(|(n, _)| n.clone()).collect();
        assert_eq!(names, vec!["glue", "legacy_polaris"]);
        // glue wins the alphabetic tiebreak; that's intentional.
        // The operator can pick legacy_polaris explicitly:
        assert_eq!(cfg.resolve_default_catalog(), "legacy_polaris");
    }

    /// Empty `default_catalog = ""` is treated as unset (operator
    /// may have left an empty placeholder in TOML during a rollout).
    #[test]
    fn empty_default_catalog_string_is_treated_as_unset() {
        let mut cfg = valid_config();
        cfg.query.default_catalog = Some("".to_string());
        // No named catalogs, only the legacy block: should still
        // resolve to the canonical legacy name.
        assert_eq!(cfg.resolve_default_catalog(), "iceberg");
    }

    /// End-to-end TOML deserialization: the documented shape with
    /// Polaris + Nessie + AWS Glue + S3 Tables in one config
    /// round-trips through serde and `flattened_catalogs()` returns
    /// the four named entries.
    #[test]
    fn multi_backend_toml_round_trips() {
        let toml = r#"
[coordinator]
flight_sql_port = 50051
trino_http_port = 8080
mode = "hybrid"

[auth]
keycloak_url = "https://kc.example.com"
client_id = "sqe-client"

# Legacy single-catalog block kept as a backwards-compat placeholder.
# Operators leaving this minimal must populate `[catalogs.*]` below.
[catalog]
catalog_url = ""

[catalogs.polaris]
catalog_url = "http://polaris:8181/api/catalog"
warehouse = "main"
[catalogs.polaris.backend]
type = "rest"

[catalogs.nessie]
catalog_url = "http://nessie:19120/iceberg"
warehouse = "lake"
[catalogs.nessie.backend]
type = "rest"

[catalogs.aws_glue]
catalog_url = ""
[catalogs.aws_glue.backend]
type = "glue"
region = "eu-central-1"
warehouse = "s3://my-bucket/wh"

[catalogs.aws_s3tables]
catalog_url = ""
[catalogs.aws_s3tables.backend]
type = "s3tables"
table_bucket_arn = "arn:aws:s3tables:eu-west-1:123456789012:bucket/my-bucket"
"#;
        let cfg: SqeConfig = toml::from_str(toml).expect("multi-backend TOML deserializes");
        let names: Vec<String> = cfg
            .flattened_catalogs()
            .iter()
            .map(|(n, _)| n.clone())
            .collect();
        assert_eq!(
            names,
            vec!["aws_glue", "aws_s3tables", "nessie", "polaris"],
            "named catalogs sorted alphabetically",
        );
        // The legacy [catalog] block has an empty catalog_url; with
        // no `default_catalog` set, it is dropped.
        assert!(!names.contains(&"iceberg".to_string()));
        // Each backend variant deserialised correctly:
        assert!(matches!(
            cfg.catalogs["polaris"].backend,
            CatalogBackend::Rest
        ));
        assert!(matches!(
            cfg.catalogs["aws_glue"].backend,
            CatalogBackend::Glue { .. }
        ));
        assert!(matches!(
            cfg.catalogs["aws_s3tables"].backend,
            CatalogBackend::S3tables { .. }
        ));
    }

    /// V7: per-catalog auth + storage overrides round-trip through
    /// the TOML deserializer, including all four `CatalogAuthConfig`
    /// variants and a per-catalog `[storage]` block that points at
    /// a different S3 endpoint.
    #[test]
    fn per_catalog_auth_and_storage_overrides_deserialize() {
        let toml = r#"
[coordinator]
flight_sql_port = 50051
trino_http_port = 8080
mode = "hybrid"

[auth]
keycloak_url = "https://kc.example.com"
client_id = "sqe-client"

[catalog]
catalog_url = ""

[catalogs.polaris]
catalog_url = "http://polaris:8181/api/catalog"
warehouse = "main"
[catalogs.polaris.backend]
type = "rest"
# auth omitted: defaults to SessionBearer

[catalogs.partner_polaris]
catalog_url = "https://partner.com/iceberg"
warehouse = "shared"
[catalogs.partner_polaris.backend]
type = "rest"
[catalogs.partner_polaris.auth]
type = "client_credentials"
token_endpoint = "https://partner.com/oauth/tokens"
client_id = "sqe-partner"
client_secret = "secret-from-env"
[catalogs.partner_polaris.storage]
s3_endpoint = "https://partner-s3.example.com"
s3_region = "us-east-1"
s3_access_key = "partner-key"
s3_secret_key = "partner-secret"

[catalogs.public_nessie]
catalog_url = "https://nessie.public.example.com/iceberg"
warehouse = "public"
[catalogs.public_nessie.backend]
type = "rest"
[catalogs.public_nessie.auth]
type = "anonymous"

[catalogs.aws_glue]
catalog_url = ""
[catalogs.aws_glue.backend]
type = "glue"
region = "eu-central-1"
warehouse = "s3://wh/"
[catalogs.aws_glue.auth]
type = "aws"
"#;
        let cfg: SqeConfig = toml::from_str(toml).expect("V7 TOML deserializes");

        // Default (omitted) is SessionBearer (None on the field).
        assert!(cfg.catalogs["polaris"].auth.is_none());
        assert!(cfg.catalogs["polaris"].storage.is_none());

        // Partner has both overrides.
        match &cfg.catalogs["partner_polaris"].auth {
            Some(CatalogAuthConfig::ClientCredentials {
                token_endpoint,
                client_id,
                client_secret,
                scope,
            }) => {
                assert_eq!(token_endpoint, "https://partner.com/oauth/tokens");
                assert_eq!(client_id, "sqe-partner");
                assert_eq!(client_secret, "secret-from-env");
                assert!(scope.is_none(), "scope is optional, defaults to None");
            }
            other => panic!("partner_polaris auth wrong variant: {other:?}"),
        }
        let partner_storage = cfg.catalogs["partner_polaris"]
            .storage
            .as_ref()
            .expect("partner has storage override");
        assert_eq!(partner_storage.s3_endpoint, "https://partner-s3.example.com");
        assert_eq!(partner_storage.s3_region, "us-east-1");
        assert_eq!(partner_storage.s3_access_key, "partner-key");

        // Anonymous + AWS variants:
        assert!(matches!(
            cfg.catalogs["public_nessie"].auth,
            Some(CatalogAuthConfig::Anonymous)
        ));
        assert!(matches!(
            cfg.catalogs["aws_glue"].auth,
            Some(CatalogAuthConfig::Aws)
        ));
    }

    #[test]
    fn test_tls_config_disabled_by_default() {
        let tls = TlsConfig::default();
        assert!(!tls.is_enabled());
    }

    #[test]
    fn test_tls_config_enabled_with_cert_and_key() {
        let tls = TlsConfig {
            cert_file: "/tmp/cert.pem".to_string(),
            key_file: "/tmp/key.pem".to_string(),
            ca_file: String::new(),
        };
        assert!(tls.is_enabled());
    }

    #[test]
    fn test_validate_tls_cert_without_key() {
        let mut config = valid_config();
        config.coordinator.tls.cert_file = "/tmp/cert.pem".to_string();
        // key_file is empty
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("tls.key_file is missing"),
            "Expected cert-without-key error, got: {err}"
        );
    }

    #[test]
    fn test_validate_tls_key_without_cert() {
        let mut config = valid_config();
        config.coordinator.tls.key_file = "/tmp/key.pem".to_string();
        // cert_file is empty
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("tls.cert_file is missing"),
            "Expected key-without-cert error, got: {err}"
        );
    }

    #[test]
    fn test_validate_tls_missing_files() {
        let mut config = valid_config();
        config.coordinator.tls.cert_file = "/nonexistent/cert.pem".to_string();
        config.coordinator.tls.key_file = "/nonexistent/key.pem".to_string();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("tls.cert_file") && err.contains("not found"),
            "Expected missing file error, got: {err}"
        );
    }

    #[test]
    fn test_storage_config_s3_allow_http_defaults_false() {
        let config = StorageConfig::default();
        assert!(!config.s3_allow_http, "s3_allow_http should default to false (secure by default)");
    }

    #[test]
    fn test_query_cache_defaults() {
        let config = QueryCacheConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_memory_mb, 256);
        assert_eq!(config.max_entry_mb, 5);
        assert_eq!(config.ttl_secs, 300);
    }

    #[test]
    fn test_query_history_defaults() {
        let config = QueryHistoryConfig::default();
        assert_eq!(config.max_entries, 10000);
        assert_eq!(config.ttl_secs, 1800);
    }

    // -----------------------------------------------------------------------
    // AuthProviderConfig parsing
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_oidc_password_provider() {
        let toml_str = r#"
            type = "oidc_password"
            token_url = "https://idp.example.com/token"
            client_id = "sqe"
            client_secret = "changeme"
            roles_claim = "custom.roles"
        "#;

        let config: AuthProviderConfig = toml::from_str(toml_str).unwrap();
        match config {
            AuthProviderConfig::OidcPassword {
                token_url,
                client_id,
                client_secret,
                roles_claim,
                ..
            } => {
                assert_eq!(token_url, "https://idp.example.com/token");
                assert_eq!(client_id, "sqe");
                assert_eq!(client_secret, "changeme");
                assert_eq!(roles_claim, "custom.roles");
            }
            other => panic!("Expected OidcPassword, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_oidc_password_provider_defaults() {
        let toml_str = r#"
            type = "oidc_password"
            token_url = "https://idp.example.com/token"
            client_id = "sqe"
        "#;

        let config: AuthProviderConfig = toml::from_str(toml_str).unwrap();
        match config {
            AuthProviderConfig::OidcPassword {
                client_secret,
                roles_claim,
                ..
            } => {
                assert_eq!(client_secret, "");
                assert_eq!(roles_claim, "realm_access.roles");
            }
            other => panic!("Expected OidcPassword, got: {other:?}"),
        }
    }

    /// Task 9: subject_claim/email_claim/groups_claim must round-trip through
    /// TOML for both `oidc_password` and `bearer_token` variants.
    /// When omitted, email_claim and groups_claim default to empty string;
    /// subject_claim defaults to "sub".
    #[test]
    fn auth_provider_config_claim_fields_toml_round_trip() {
        // Case 1: explicit values in oidc_password
        let with_claims = r#"
            type = "oidc_password"
            token_url = "https://idp.example.com/token"
            client_id = "sqe"
            email_claim = "email"
            groups_claim = "groups"
        "#;
        let config: AuthProviderConfig = toml::from_str(with_claims).unwrap();
        match config {
            AuthProviderConfig::OidcPassword {
                email_claim,
                groups_claim,
                subject_claim,
                ..
            } => {
                assert_eq!(email_claim, "email", "email_claim should be 'email'");
                assert_eq!(groups_claim, "groups", "groups_claim should be 'groups'");
                assert_eq!(subject_claim, "sub", "subject_claim should default to 'sub'");
            }
            other => panic!("Expected OidcPassword, got: {other:?}"),
        }

        // Case 2: omitted claim fields default to empty (oidc_password)
        let omitted_claims = r#"
            type = "oidc_password"
            token_url = "https://idp.example.com/token"
            client_id = "sqe"
        "#;
        let config: AuthProviderConfig = toml::from_str(omitted_claims).unwrap();
        match config {
            AuthProviderConfig::OidcPassword {
                email_claim,
                groups_claim,
                subject_claim,
                ..
            } => {
                assert_eq!(email_claim, "", "email_claim should default to empty");
                assert_eq!(groups_claim, "", "groups_claim should default to empty");
                assert_eq!(subject_claim, "sub", "subject_claim should default to 'sub'");
            }
            other => panic!("Expected OidcPassword, got: {other:?}"),
        }

        // Case 3: explicit values in bearer_token
        let bearer_with_claims = r#"
            type = "bearer_token"
            jwks_url = "https://idp.example.com/.well-known/jwks.json"
            allow_insecure_jwks = true
            allow_unbounded_audience = true
            email_claim = "email"
            groups_claim = "groups"
        "#;
        let config: AuthProviderConfig = toml::from_str(bearer_with_claims).unwrap();
        match config {
            AuthProviderConfig::BearerToken {
                email_claim,
                groups_claim,
                subject_claim,
                ..
            } => {
                assert_eq!(email_claim, "email", "bearer email_claim should be 'email'");
                assert_eq!(
                    groups_claim, "groups",
                    "bearer groups_claim should be 'groups'"
                );
                assert_eq!(
                    subject_claim, "sub",
                    "bearer subject_claim should default to 'sub'"
                );
            }
            other => panic!("Expected BearerToken, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_client_credentials_provider() {
        let toml_str = r#"
            type = "client_credentials"
            token_endpoint = "https://polaris.example.com/oauth/tokens"
            client_id = "polaris-client"
            client_secret = "polaris-secret"
        "#;

        let config: AuthProviderConfig = toml::from_str(toml_str).unwrap();
        match config {
            AuthProviderConfig::ClientCredentials {
                token_endpoint,
                client_id,
                client_secret,
            } => {
                assert_eq!(token_endpoint, "https://polaris.example.com/oauth/tokens");
                assert_eq!(client_id, "polaris-client");
                assert_eq!(client_secret, "polaris-secret");
            }
            other => panic!("Expected ClientCredentials, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_client_credentials_passthrough_provider_defaults() {
        // Only token_url is required; the rest default. Crucially, no
        // client_id / client_secret fields exist (they arrive per connection).
        let toml_str = r#"
            type = "client_credentials_passthrough"
            token_url = "http://keycloak:8080/realms/iceberg-sp/protocol/openid-connect/token"
        "#;

        let config: AuthProviderConfig = toml::from_str(toml_str).unwrap();
        match config {
            AuthProviderConfig::ClientCredentialsPassthrough {
                token_url,
                roles_claim,
                subject_claim,
                scope,
                fallthrough_on_reject: _,
            } => {
                assert_eq!(
                    token_url,
                    "http://keycloak:8080/realms/iceberg-sp/protocol/openid-connect/token"
                );
                assert_eq!(roles_claim, "realm_access.roles");
                assert_eq!(subject_claim, "sub");
                assert!(scope.is_none());
            }
            other => panic!("Expected ClientCredentialsPassthrough, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_client_credentials_passthrough_provider_explicit() {
        let toml_str = r#"
            type = "client_credentials_passthrough"
            token_url = "http://idp/token"
            roles_claim = "groups"
            subject_claim = "client_id"
            scope = "catalog"
        "#;

        let config: AuthProviderConfig = toml::from_str(toml_str).unwrap();
        match config {
            AuthProviderConfig::ClientCredentialsPassthrough {
                roles_claim,
                subject_claim,
                scope,
                ..
            } => {
                assert_eq!(roles_claim, "groups");
                assert_eq!(subject_claim, "client_id");
                assert_eq!(scope.as_deref(), Some("catalog"));
            }
            other => panic!("Expected ClientCredentialsPassthrough, got: {other:?}"),
        }
    }

    #[test]
    fn client_credentials_passthrough_debug_names_variant() {
        // The variant holds no secret, so it falls through to the name-only
        // summary. Assert the name is correct (not the generic fallback).
        let cfg = AuthProviderConfig::ClientCredentialsPassthrough {
            token_url: "http://idp/token".to_string(),
            roles_claim: "realm_access.roles".to_string(),
            subject_claim: "sub".to_string(),
            scope: None,
            fallthrough_on_reject: false,
        };
        let dbg = format!("{cfg:?}");
        assert!(
            dbg.contains("ClientCredentialsPassthrough"),
            "debug should name the variant: {dbg}"
        );
    }

    #[test]
    fn test_parse_anonymous_provider() {
        let toml_str = r#"
            type = "anonymous"
            user = "dev-user"
            roles = ["admin", "reader"]
        "#;

        let config: AuthProviderConfig = toml::from_str(toml_str).unwrap();
        match config {
            AuthProviderConfig::Anonymous { user, roles } => {
                assert_eq!(user, "dev-user");
                assert_eq!(roles, vec!["admin", "reader"]);
            }
            other => panic!("Expected Anonymous, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_anonymous_provider_defaults() {
        let toml_str = r#"
            type = "anonymous"
        "#;

        let config: AuthProviderConfig = toml::from_str(toml_str).unwrap();
        match config {
            AuthProviderConfig::Anonymous { user, roles } => {
                assert_eq!(user, "anonymous");
                assert!(roles.is_empty());
            }
            other => panic!("Expected Anonymous, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_providers_array_in_auth_config() {
        let toml_str = r#"
            client_id = "sqe"

            [[providers]]
            type = "oidc_password"
            token_url = "https://idp.example.com/token"
            client_id = "sqe"

            [[providers]]
            type = "anonymous"
            user = "fallback"
        "#;

        let config: AuthConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.providers.len(), 2);
        assert!(matches!(config.providers[0], AuthProviderConfig::OidcPassword { .. }));
        assert!(matches!(config.providers[1], AuthProviderConfig::Anonymous { .. }));
    }

    #[test]
    fn test_parse_auth_config_no_providers_backward_compat() {
        let toml_str = r#"
            keycloak_url = "https://keycloak.example.com"
            realm = "sqe"
            client_id = "sqe-client"
            client_secret = "secret"
        "#;

        let config: AuthConfig = toml::from_str(toml_str).unwrap();
        assert!(config.providers.is_empty());
        assert_eq!(config.keycloak_url, "https://keycloak.example.com");
        assert_eq!(config.client_id, "sqe-client");
    }

    #[test]
    fn test_validate_with_providers_no_legacy_fields_needed() {
        let mut config = valid_config();
        config.auth.keycloak_url = String::new();
        config.auth.token_endpoint = String::new();
        config.auth.client_id = String::new();
        config.auth.providers = vec![AuthProviderConfig::Anonymous {
            user: "test".to_string(),
            roles: vec![],
        }];
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_parse_external_auth_config() {
        let toml_str = r#"
            issuer = "https://idp.example.com/realms/sqe"
            client_id = "sqe"
            client_secret = "secret"
            scopes = ["openid", "profile"]

            [device]
            client_id = "sqe-cli"
            scopes = ["openid", "profile", "offline_access"]
        "#;
        let config: ExternalAuthConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.issuer, "https://idp.example.com/realms/sqe");
        assert_eq!(config.client_id, "sqe");
        assert_eq!(config.client_secret, Some("secret".to_string()));
        assert_eq!(config.challenge_timeout_secs, 900);
        assert!(config.device.is_some());
        let device = config.device.unwrap();
        assert_eq!(device.client_id, "sqe-cli");
        assert_eq!(device.scopes, vec!["openid", "profile", "offline_access"]);
    }

    #[test]
    fn test_parse_external_auth_config_minimal() {
        let toml_str = r#"
            issuer = "https://idp.example.com"
            client_id = "sqe"
        "#;
        let config: ExternalAuthConfig = toml::from_str(toml_str).unwrap();
        assert!(config.client_secret.is_none());
        assert!(config.device.is_none());
        assert_eq!(config.scopes, vec!["openid", "profile"]);
    }

    // -----------------------------------------------------------------------
    // QueryConfig: defaults and custom values
    // -----------------------------------------------------------------------

    #[test]
    fn test_query_config_defaults() {
        let config = QueryConfig::default();
        assert_eq!(config.timeout_secs, 300);
        assert_eq!(config.max_result_rows, 1_000_000);
        assert_eq!(config.max_concurrent_queries, 100);
        assert_eq!(config.slow_query_threshold_secs, 30);
        assert_eq!(config.max_query_memory, "256MB");
        assert_eq!(config.query_profile, "off");
    }

    #[test]
    fn test_profile_mode_parse() {
        assert_eq!(ProfileMode::parse("off"), ProfileMode::Off);
        assert_eq!(ProfileMode::parse("slow"), ProfileMode::Slow);
        assert_eq!(ProfileMode::parse("all"), ProfileMode::All);
        // Case-insensitive and whitespace-tolerant.
        assert_eq!(ProfileMode::parse(" ALL "), ProfileMode::All);
        assert_eq!(ProfileMode::parse("Slow"), ProfileMode::Slow);
        // Unknown values fall back to Off (profiling is opt-in).
        assert_eq!(ProfileMode::parse("verbose"), ProfileMode::Off);
        assert_eq!(ProfileMode::parse(""), ProfileMode::Off);
    }

    #[test]
    fn test_query_profile_from_toml() {
        let config: QueryConfig = toml::from_str(r#"query_profile = "slow""#).unwrap();
        assert_eq!(config.query_profile, "slow");
        assert_eq!(ProfileMode::parse(&config.query_profile), ProfileMode::Slow);
    }

    #[test]
    fn test_runtime_filter_knobs_defaults_and_overrides() {
        // Defaults: IN-list materialization sized for star-schema dimension
        // filters, 100ms scan-open wait.
        let q: QueryConfig = toml::from_str("").unwrap();
        assert_eq!(q.runtime_filter_inlist_max_values, 65536);
        assert_eq!(q.runtime_filter_inlist_max_size, "4MB");
        let rf: RuntimeFiltersConfig = toml::from_str("").unwrap();
        assert_eq!(rf.wait_ms, 100);

        let q: QueryConfig = toml::from_str(
            r#"
            runtime_filter_inlist_max_values = 1000
            runtime_filter_inlist_max_size = "256KB"
            "#,
        )
        .unwrap();
        assert_eq!(q.runtime_filter_inlist_max_values, 1000);
        assert_eq!(q.runtime_filter_inlist_max_size, "256KB");
        let rf: RuntimeFiltersConfig = toml::from_str("wait_ms = 0").unwrap();
        assert_eq!(rf.wait_ms, 0);
    }

    #[test]
    fn test_query_config_custom_values() {
        let toml_str = r#"
            timeout_secs = 60
            max_result_rows = 500
            max_concurrent_queries = 50
            slow_query_threshold_secs = 10
            max_query_memory = "512MB"
        "#;
        let config: QueryConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.timeout_secs, 60);
        assert_eq!(config.max_result_rows, 500);
        assert_eq!(config.max_concurrent_queries, 50);
        assert_eq!(config.slow_query_threshold_secs, 10);
        assert_eq!(config.max_query_memory, "512MB");
    }

    // -----------------------------------------------------------------------
    // SessionConfig: defaults and file persistence
    // -----------------------------------------------------------------------

    #[test]
    fn test_session_config_defaults() {
        let config = SessionConfig::default();
        assert_eq!(config.idle_timeout_secs, 900);
        assert_eq!(config.absolute_timeout_secs, 28800);
        assert_eq!(config.persistence, "memory");
        assert_eq!(config.persistence_path, "/tmp/sqe-sessions.json");
        assert_eq!(config.snapshot_interval_secs, 60);
    }

    #[test]
    fn test_session_config_file_persistence() {
        let toml_str = r#"
            persistence = "file"
            persistence_path = "/var/data/sessions.json"
            snapshot_interval_secs = 30
        "#;
        let config: SessionConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.persistence, "file");
        assert_eq!(config.persistence_path, "/var/data/sessions.json");
        assert_eq!(config.snapshot_interval_secs, 30);
    }

    // -----------------------------------------------------------------------
    // OpenLineage config tests
    // -----------------------------------------------------------------------

    /// Minimal skeleton TOML (coordinator/auth/catalog blocks) so a full
    /// `SqeConfig` deserialises in OpenLineage tests. Only `catalog.catalog_url`
    /// is structurally required; everything else has serde defaults.
    const OL_TEST_SKELETON: &str = r#"
[coordinator]
[auth]
[catalog]
catalog_url = "http://polaris.example/api/catalog"
"#;

    #[test]
    fn openlineage_config_parses_from_toml() {
        let body = r#"
[metrics]
prometheus_port = 9090
otlp_endpoint = ""

[metrics.openlineage]
enabled = true
job_namespace = "sqe-prod"
emit_selects = true
file_path = "/var/log/ol.jsonl"
http_endpoint = "https://marquez.example/api/v1/lineage"
auth_mode = "bearer"
api_key = "secret"
spool_path = "/var/spool/sqe-ol"
spool_max_bytes = 209715200
"#;
        let toml = format!("{OL_TEST_SKELETON}{body}");
        let cfg: SqeConfig = toml::from_str(&toml).unwrap();
        let ol = &cfg.metrics.openlineage;
        assert!(ol.enabled);
        assert_eq!(ol.job_namespace, "sqe-prod");
        assert!(ol.emit_selects);
        assert_eq!(ol.spool_max_bytes, 209715200);
    }

    #[test]
    fn openlineage_config_uses_defaults_when_omitted() {
        let body = r#"
[metrics]
prometheus_port = 9090
otlp_endpoint = ""
"#;
        let toml = format!("{OL_TEST_SKELETON}{body}");
        let cfg: SqeConfig = toml::from_str(&toml).unwrap();
        let ol = &cfg.metrics.openlineage;
        assert!(!ol.enabled);
        assert_eq!(ol.job_namespace, "sqe");
        assert_eq!(ol.spool_max_bytes, 100 * 1024 * 1024);
        assert_eq!(ol.channel_capacity, 10000);
    }

    #[test]
    fn env_overrides_apply_to_openlineage() {
        // std::env::set_var is process-wide; serialise against every other
        // env-touching test in this module so cargo's parallel thread-pool
        // cannot interleave set_var / remove_var calls.
        let _g = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        std::env::set_var("SQE_METRICS__OPENLINEAGE__ENABLED", "true");
        std::env::set_var("SQE_METRICS__OPENLINEAGE__SPOOL_MAX_BYTES", "999");
        std::env::set_var("SQE_METRICS__OPENLINEAGE__CHANNEL_CAPACITY", "42");

        let mut cfg = valid_config();
        cfg.apply_env_overrides();

        assert!(cfg.metrics.openlineage.enabled);
        assert_eq!(cfg.metrics.openlineage.spool_max_bytes, 999);
        assert_eq!(cfg.metrics.openlineage.channel_capacity, 42);

        // Cleanup so other tests in this process aren't affected.
        std::env::remove_var("SQE_METRICS__OPENLINEAGE__ENABLED");
        std::env::remove_var("SQE_METRICS__OPENLINEAGE__SPOOL_MAX_BYTES");
        std::env::remove_var("SQE_METRICS__OPENLINEAGE__CHANNEL_CAPACITY");
    }

    #[test]
    fn env_overrides_apply_to_coordinator_memory() {
        // std::env::set_var is process-wide; serialise against every other
        // env-touching test in this module (see ENV_LOCK docs below).
        let _g = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        std::env::set_var("SQE_MEMORY_LIMIT", "12GB");
        std::env::set_var("SQE_MEMORY_POOL", "fair");

        let mut cfg = valid_config();
        // Precondition: the short env names must actually change the value, i.e.
        // start from something other than what we set (guards the regression
        // where nothing read SQE_MEMORY_LIMIT and the file value silently won).
        assert_ne!(cfg.coordinator.memory_limit, "12GB");
        cfg.apply_env_overrides();

        assert_eq!(cfg.coordinator.memory_limit, "12GB");
        assert_eq!(cfg.coordinator.memory_pool, "fair");

        std::env::remove_var("SQE_MEMORY_LIMIT");
        std::env::remove_var("SQE_MEMORY_POOL");
    }

    #[test]
    fn validate_rejects_enabled_without_sinks() {
        let cfg = OpenLineageConfig {
            enabled: true,
            ..OpenLineageConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.contains("at least one of file_path or http_endpoint"));
    }

    #[test]
    fn validate_rejects_bearer_without_api_key() {
        let cfg = OpenLineageConfig {
            enabled: true,
            file_path: "/tmp/ol.jsonl".into(),
            auth_mode: "bearer".into(),
            ..OpenLineageConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.contains("requires api_key"));
    }

    #[test]
    fn validate_rejects_spool_without_http() {
        let cfg = OpenLineageConfig {
            enabled: true,
            file_path: "/tmp/ol.jsonl".into(),
            spool_path: "/tmp/spool".into(),
            ..OpenLineageConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.contains("spool_path requires http_endpoint"));
    }

    #[test]
    fn validate_rejects_tiny_spool_cap() {
        let cfg = OpenLineageConfig {
            enabled: true,
            file_path: "/tmp/ol.jsonl".into(),
            spool_max_bytes: 1024, // 1 KiB, well under 1 MiB
            ..OpenLineageConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.contains("at least 1 MiB"));
    }

    #[test]
    fn validate_passes_with_valid_config() {
        let cfg = OpenLineageConfig {
            enabled: true,
            file_path: "/tmp/ol.jsonl".into(),
            ..OpenLineageConfig::default()
        };
        assert!(cfg.validate().is_ok());
    }

    // --- Provider env-var override (issue #14 regression test) ---

    /// Lock used to serialise env-var test mutation since std::env::set_var
    /// has process-wide effect. Every test in this module that calls
    /// std::env::set_var or std::env::remove_var MUST acquire this lock
    /// at its top with `let _g = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());`
    /// before touching the environment.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn provider_client_secret_env_override_beats_toml() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        let mut cfg = valid_config();
        cfg.auth.providers = vec![
            AuthProviderConfig::OidcPassword {
                token_url: "http://idp.example.com/token".to_string(),
                client_id: "sqe".to_string(),
                client_secret: "toml-secret".to_string(),
                roles_claim: "realm_access.roles".to_string(),
                subject_claim: "sub".to_string(),
                email_claim: String::new(),
                groups_claim: String::new(),
                fallthrough_on_reject: false,
            },
            AuthProviderConfig::ClientCredentials {
                token_endpoint: "http://polaris:8181/oauth/tokens".to_string(),
                client_id: "polaris-sa".to_string(),
                client_secret: "toml-polaris".to_string(),
            },
        ];

        std::env::set_var(
            "SQE_AUTH__PROVIDERS__0__CLIENT_SECRET",
            "from-env-oidc",
        );
        std::env::set_var(
            "SQE_AUTH__PROVIDERS__1__CLIENT_SECRET",
            "from-env-ccg",
        );

        cfg.apply_env_overrides();

        match &cfg.auth.providers[0] {
            AuthProviderConfig::OidcPassword { client_secret, .. } => {
                assert_eq!(client_secret, "from-env-oidc");
            }
            other => panic!("expected OidcPassword, got {other:?}"),
        }
        match &cfg.auth.providers[1] {
            AuthProviderConfig::ClientCredentials { client_secret, .. } => {
                assert_eq!(client_secret, "from-env-ccg");
            }
            other => panic!("expected ClientCredentials, got {other:?}"),
        }

        std::env::remove_var("SQE_AUTH__PROVIDERS__0__CLIENT_SECRET");
        std::env::remove_var("SQE_AUTH__PROVIDERS__1__CLIENT_SECRET");
    }

    #[test]
    fn oidc_password_inherits_auth_client_secret_when_empty() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        std::env::remove_var("SQE_AUTH__CLIENT_SECRET");
        std::env::remove_var("SQE_AUTH__PROVIDERS__0__CLIENT_SECRET");

        // The common deployment: shared secret on [auth] (often via
        // SQE_AUTH__CLIENT_SECRET), oidc_password provider left empty.
        let mut cfg = valid_config();
        cfg.auth.client_secret = SecretString::new("shared-from-auth".to_string());
        cfg.auth.providers = vec![AuthProviderConfig::OidcPassword {
            token_url: "http://idp.example.com/token".to_string(),
            client_id: "sqe".to_string(),
            client_secret: String::new(),
            roles_claim: "realm_access.roles".to_string(),
            subject_claim: "sub".to_string(),
            email_claim: String::new(),
            groups_claim: String::new(),
            fallthrough_on_reject: false,
        }];

        cfg.apply_env_overrides();

        match &cfg.auth.providers[0] {
            AuthProviderConfig::OidcPassword { client_secret, .. } => {
                assert_eq!(client_secret, "shared-from-auth");
            }
            other => panic!("expected OidcPassword, got {other:?}"),
        }
    }

    #[test]
    fn explicit_oidc_password_secret_beats_auth_inheritance() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        std::env::remove_var("SQE_AUTH__CLIENT_SECRET");
        std::env::remove_var("SQE_AUTH__PROVIDERS__0__CLIENT_SECRET");

        let mut cfg = valid_config();
        cfg.auth.client_secret = SecretString::new("shared-from-auth".to_string());
        cfg.auth.providers = vec![AuthProviderConfig::OidcPassword {
            token_url: "http://idp.example.com/token".to_string(),
            client_id: "sqe".to_string(),
            client_secret: "explicit-provider-secret".to_string(),
            roles_claim: "realm_access.roles".to_string(),
            subject_claim: "sub".to_string(),
            email_claim: String::new(),
            groups_claim: String::new(),
            fallthrough_on_reject: false,
        }];

        cfg.apply_env_overrides();

        match &cfg.auth.providers[0] {
            AuthProviderConfig::OidcPassword { client_secret, .. } => {
                assert_eq!(client_secret, "explicit-provider-secret");
            }
            other => panic!("expected OidcPassword, got {other:?}"),
        }
    }

    #[test]
    fn provider_client_secret_env_override_token_exchange_optional() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        let mut cfg = valid_config();
        cfg.auth.providers = vec![AuthProviderConfig::TokenExchange {
            token_url: "http://idp.example.com/token".to_string(),
            client_id: "sqe".to_string(),
            client_secret: None,
            audience: Some("polaris".to_string()),
            user_claim: "sub".to_string(),
            roles_claim: "realm_access.roles".to_string(),
        }];

        std::env::set_var(
            "SQE_AUTH__PROVIDERS__0__CLIENT_SECRET",
            "exchange-secret",
        );

        cfg.apply_env_overrides();

        match &cfg.auth.providers[0] {
            AuthProviderConfig::TokenExchange { client_secret, .. } => {
                assert_eq!(client_secret.as_deref(), Some("exchange-secret"));
            }
            other => panic!("expected TokenExchange, got {other:?}"),
        }

        std::env::remove_var("SQE_AUTH__PROVIDERS__0__CLIENT_SECRET");
    }

    // --- TvfPolicy (issue #10 regression tests) ---

    #[test]
    fn tvf_object_store_config_deserializes_from_toml() {
        #[derive(Deserialize)]
        struct Wrap {
            storage: StorageConfig,
        }
        let toml = r#"
            [storage]
            s3_endpoint = "http://s3:9000"

            [storage.tvf]
            allowed_object_store_prefixes = [
                "s3://data-platform-staging/_table-load-staging/",
                "s3://scratch/{user}/",
            ]
            object_store_admin_roles = ["sqe-storage-admin"]
        "#;
        let w: Wrap = toml::from_str(toml).expect("[storage.tvf] TOML deserializes");
        assert_eq!(w.storage.tvf.allowed_object_store_prefixes.len(), 2);
        assert_eq!(
            w.storage.tvf.object_store_admin_roles,
            vec!["sqe-storage-admin".to_string()]
        );
        // Untouched defaults stay fail-closed.
        assert!(!w.storage.tvf.allow_local_paths);
        assert!(!w.storage.tvf.allow_http);
    }

    /// Plain authenticated remote caller (no roles).
    fn caller(name: &str) -> TvfCaller {
        TvfCaller::for_user(name.to_string(), Vec::new())
    }

    /// Shorthand: check `path` as a default remote caller without inline
    /// credentials.
    fn check_remote(policy: &TvfPolicy, path: &str) -> Result<(), String> {
        policy.check_path(path, &caller("alice"), false)
    }

    #[test]
    fn tvf_default_denies_object_store_schemes_for_remote_callers() {
        // Identity-aware gate: with no `allowed_object_store_prefixes`,
        // engine-credentialed object-store reads are denied out of the box.
        let policy = TvfPolicy::default();
        for path in [
            "s3://my-bucket/data.parquet",
            "s3a://my-bucket/data.parquet",
            "abfss://c@a.dfs.core.windows.net/x",
            "abfs://c@a.dfs.core.windows.net/x",
            "azure://container/x",
            "az://container/x",
            "gs://bucket/x",
            "gcs://bucket/x",
        ] {
            let err = check_remote(&policy, path).unwrap_err();
            assert!(
                err.contains("allowed_object_store_prefixes"),
                "expected deny naming the config for {path}, got: {err}"
            );
        }
        // hf:// stays open — no engine storage credentials involved.
        assert!(check_remote(&policy, "hf://datasets/foo/bar/x").is_ok());
    }

    #[test]
    fn tvf_object_store_prefix_allows_exact_subtree() {
        let policy = TvfPolicy {
            allowed_object_store_prefixes: vec![
                "s3://data-platform-staging/_table-load-staging/".to_string(),
            ],
            ..Default::default()
        };
        assert!(check_remote(
            &policy,
            "s3://data-platform-staging/_table-load-staging/abc-123/data.csv"
        )
        .is_ok());
        // s3a:// alias is covered by an s3:// prefix.
        assert!(check_remote(
            &policy,
            "s3a://data-platform-staging/_table-load-staging/abc-123/data.csv"
        )
        .is_ok());
        // Same bucket outside the prefix: denied.
        assert!(check_remote(&policy, "s3://data-platform-staging/secret.parquet").is_err());
        // Other buckets: denied.
        assert!(check_remote(&policy, "s3://prod-data/customers.parquet").is_err());
    }

    #[test]
    fn tvf_object_store_prefix_requires_segment_boundary() {
        // `s3://staging-bucket` must not match `s3://staging-bucket-evil`.
        let policy = TvfPolicy {
            allowed_object_store_prefixes: vec!["s3://staging-bucket".to_string()],
            ..Default::default()
        };
        assert!(check_remote(&policy, "s3://staging-bucket/x.csv").is_ok());
        assert!(check_remote(&policy, "s3://staging-bucket").is_ok());
        assert!(check_remote(&policy, "s3://staging-bucket-evil/x.csv").is_err());
        // Same trickery against a prefix with a key part.
        let policy = TvfPolicy {
            allowed_object_store_prefixes: vec!["s3://b/staging".to_string()],
            ..Default::default()
        };
        assert!(check_remote(&policy, "s3://b/staging/x.csv").is_ok());
        assert!(check_remote(&policy, "s3://b/staging-evil/x.csv").is_err());
    }

    #[test]
    fn tvf_object_store_rejects_dot_segment_traversal() {
        let policy = TvfPolicy {
            allowed_object_store_prefixes: vec!["s3://staging/uploads/".to_string()],
            ..Default::default()
        };
        // Literal dot segments would be collapsed by url/HTTP layers AFTER
        // the prefix check — reject outright.
        let err = check_remote(&policy, "s3://staging/uploads/../../etc/x.csv").unwrap_err();
        assert!(err.contains("dot path segment"), "got: {err}");
        // Percent-encoded variants too.
        let err =
            check_remote(&policy, "s3://staging/uploads/%2e%2e/secret.csv").unwrap_err();
        assert!(err.contains("dot path segment"), "got: {err}");
        let err =
            check_remote(&policy, "s3://staging/uploads/.%2E/secret.csv").unwrap_err();
        assert!(err.contains("dot path segment"), "got: {err}");
        // Percent-encoded slash could smuggle a hidden segment boundary.
        let err =
            check_remote(&policy, "s3://staging/uploads%2f..%2fsecret.csv").unwrap_err();
        assert!(
            err.contains("percent-encoded slash") || err.contains("dot path segment"),
            "got: {err}"
        );
        // Double-encoding (`%252e` -> `%2e` -> `.`).
        let err =
            check_remote(&policy, "s3://staging/uploads/%252e%252e/secret.csv").unwrap_err();
        assert!(err.contains("double-encoding") || err.contains("dot path segment"), "got: {err}");
    }

    #[test]
    fn tvf_object_store_user_placeholder_substitution() {
        let policy = TvfPolicy {
            allowed_object_store_prefixes: vec!["s3://scratch/{user}/".to_string()],
            ..Default::default()
        };
        // Own prefix: allowed.
        assert!(policy
            .check_path("s3://scratch/alice/data.csv", &caller("alice"), false)
            .is_ok());
        // Another user's prefix: denied.
        assert!(policy
            .check_path("s3://scratch/alice/data.csv", &caller("bob"), false)
            .is_err());
        // No username available -> `{user}` prefixes never match.
        assert!(policy
            .check_path("s3://scratch/alice/data.csv", &TvfCaller::default(), false)
            .is_err());
        // A username with URL-shape-rewriting characters is not substituted.
        assert!(policy
            .check_path("s3://scratch/a/b/data.csv", &caller("a/b"), false)
            .is_err());
        assert!(policy
            .check_path("s3://scratch/../x/data.csv", &caller(".."), false)
            .is_err());
    }

    #[test]
    fn tvf_object_store_admin_role_override() {
        let policy = TvfPolicy {
            object_store_admin_roles: vec!["sqe-storage-admin".to_string()],
            ..Default::default()
        };
        let admin = TvfCaller::for_user(
            "root".to_string(),
            vec!["uma_authorization".to_string(), "SQE-Storage-Admin".to_string()],
        );
        assert!(policy
            .check_path("s3://any-bucket/any.parquet", &admin, false)
            .is_ok());
        // Without the role: denied.
        assert!(check_remote(&policy, "s3://any-bucket/any.parquet").is_err());
        // Empty role list never matches anything.
        let no_roles = TvfPolicy::default();
        assert!(no_roles
            .check_path("s3://any-bucket/any.parquet", &admin, false)
            .is_err());
    }

    #[test]
    fn tvf_object_store_inline_credentials_bypass() {
        // Complete inline credentials = the engine's storage key is not
        // used; the object store enforces access itself.
        let policy = TvfPolicy::default();
        assert!(policy
            .check_path("s3://their-own-bucket/x.csv", &caller("alice"), true)
            .is_ok());
    }

    #[test]
    fn tvf_object_store_trusted_local_bypass() {
        let policy = TvfPolicy::default();
        assert!(policy
            .check_path("s3://bucket/x.csv", &TvfCaller::trusted_local(), false)
            .is_ok());
    }

    #[test]
    fn tvf_default_rejects_local_absolute_paths() {
        let policy = TvfPolicy::default();
        let err = check_remote(&policy, "/etc/shadow").unwrap_err();
        assert!(err.contains("local filesystem paths are disabled"));
        let err = check_remote(&policy, "/proc/self/environ").unwrap_err();
        assert!(err.contains("local filesystem"));
        let err = check_remote(&policy, "file:///root/.aws/credentials").unwrap_err();
        assert!(err.contains("local filesystem"));
    }

    #[test]
    fn tvf_default_rejects_arbitrary_http_hosts() {
        let policy = TvfPolicy::default();
        // The IMDS scenario from the issue.
        let err = check_remote(
            &policy,
            "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
        )
        .unwrap_err();
        assert!(err.contains("not in `[storage.tvf] allowed_http_hosts`"));
        assert!(err.contains("169.254.169.254"));
    }

    #[test]
    fn tvf_allowed_http_host_is_accepted_exact_match() {
        let policy = TvfPolicy {
            allowed_http_hosts: vec![
                "data.example.com".to_string(),
                "huggingface.co".to_string(),
            ],
            ..Default::default()
        };
        assert!(check_remote(&policy, "https://data.example.com/file.parquet").is_ok());
        // Case-insensitive host comparison.
        assert!(check_remote(&policy, "https://DATA.EXAMPLE.COM/file.parquet").is_ok());
        // Different port is still allowed (host match only).
        assert!(check_remote(&policy, "https://data.example.com:8080/file.parquet").is_ok());
        // Subdomain that isn't allowlisted is rejected (no wildcards).
        assert!(check_remote(&policy, "https://api.data.example.com/file.parquet").is_err());
    }

    #[test]
    fn tvf_allow_http_true_bypasses_allowlist() {
        let policy = TvfPolicy {
            allow_http: true,
            ..Default::default()
        };
        assert!(check_remote(&policy, "http://169.254.169.254/").is_ok());
        assert!(check_remote(&policy, "https://anything.example/x").is_ok());
    }

    #[test]
    fn tvf_allow_local_paths_true_permits_filesystem() {
        let policy = TvfPolicy {
            allow_local_paths: true,
            ..Default::default()
        };
        assert!(check_remote(&policy, "/var/data/foo.parquet").is_ok());
        assert!(check_remote(&policy, "file:///var/data/foo.parquet").is_ok());
    }

    #[test]
    fn tvf_malformed_http_url_returns_error() {
        let policy = TvfPolicy {
            allowed_http_hosts: vec!["example.com".to_string()],
            ..Default::default()
        };
        let err = check_remote(&policy, "http:///just-a-path").unwrap_err();
        assert!(err.contains("malformed URL") || err.contains("missing host"));
    }

    // --- TvfPolicy::check_endpoint (issue #46 regression tests) ---

    #[test]
    fn tvf_endpoint_empty_is_allowed() {
        let policy = TvfPolicy::default();
        assert!(policy.check_endpoint("").is_ok());
    }

    #[test]
    fn tvf_endpoint_imds_url_is_rejected_by_default() {
        let policy = TvfPolicy::default();
        let err = policy
            .check_endpoint("http://169.254.169.254/latest/meta-data/")
            .unwrap_err();
        assert!(err.contains("not in `[storage.tvf] allowed_http_hosts`"));
        assert!(err.contains("169.254.169.254"));
    }

    #[test]
    fn tvf_endpoint_bare_host_is_allowed() {
        // MinIO / Ceph deployments use bare host:port endpoints. Allowed
        // because they cannot be the IMDS http://169.254.169.254 URL.
        let policy = TvfPolicy::default();
        assert!(policy.check_endpoint("minio.local:9000").is_ok());
        assert!(policy.check_endpoint("s3.example:9000").is_ok());
    }

    #[test]
    fn tvf_endpoint_allowed_http_host_passes() {
        let policy = TvfPolicy {
            allowed_http_hosts: vec!["s3.us-east-1.amazonaws.com".to_string()],
            ..Default::default()
        };
        assert!(policy
            .check_endpoint("https://s3.us-east-1.amazonaws.com")
            .is_ok());
        let err = policy
            .check_endpoint("https://other-region.amazonaws.com")
            .unwrap_err();
        assert!(err.contains("not in `[storage.tvf] allowed_http_hosts`"));
    }

    #[test]
    fn tvf_endpoint_allow_http_true_bypasses_allowlist() {
        let policy = TvfPolicy {
            allow_http: true,
            ..Default::default()
        };
        // Defense-in-depth surrenders when allow_http = true.
        assert!(policy.check_endpoint("http://169.254.169.254").is_ok());
    }

    // --- SecurityConfig::resolve_client_ip (issue #74 regression tests) ---

    #[test]
    fn security_default_ignores_x_forwarded_for() {
        let security = SecurityConfig::default();
        let resolved = security
            .resolve_client_ip(Some("10.0.0.1:55555"), Some("1.2.3.4"));
        assert_eq!(resolved, "10.0.0.1:55555");
    }

    #[test]
    fn security_trusted_proxy_returns_forwarded_for() {
        let security = SecurityConfig {
            trusted_proxies: vec!["10.0.0.1".to_string()],
            ..Default::default()
        };
        let resolved = security
            .resolve_client_ip(Some("10.0.0.1:33333"), Some("203.0.113.7"));
        assert_eq!(resolved, "203.0.113.7");
    }

    #[test]
    fn security_chain_walks_right_to_left_skipping_trusted() {
        let security = SecurityConfig {
            trusted_proxies: vec![
                "10.0.0.1".to_string(),
                "10.0.0.2".to_string(),
            ],
            ..Default::default()
        };
        let resolved = security.resolve_client_ip(
            Some("10.0.0.1"),
            Some("203.0.113.7, 10.0.0.2, 10.0.0.1"),
        );
        assert_eq!(resolved, "203.0.113.7");
    }

    #[test]
    fn security_untrusted_peer_keeps_peer_addr() {
        let security = SecurityConfig {
            trusted_proxies: vec!["10.0.0.99".to_string()],
            ..Default::default()
        };
        let resolved = security
            .resolve_client_ip(Some("198.51.100.5"), Some("1.2.3.4"));
        assert_eq!(resolved, "198.51.100.5");
    }

    #[test]
    fn security_handles_missing_forwarded_for() {
        let security = SecurityConfig {
            trusted_proxies: vec!["10.0.0.1".to_string()],
            ..Default::default()
        };
        let resolved = security.resolve_client_ip(Some("10.0.0.1"), None);
        assert_eq!(resolved, "10.0.0.1");
    }

    // --- AuthConfig::has_admin_role (issue #3 regression tests) ---

    fn auth_with_admin_roles(roles: Vec<&str>) -> AuthConfig {
        let mut cfg = valid_config().auth;
        cfg.admin_roles = roles.into_iter().map(String::from).collect();
        cfg
    }

    #[test]
    fn admin_role_matches_when_caller_in_allowlist() {
        let auth = auth_with_admin_roles(vec!["service_admin", "catalog_admin"]);
        let caller_roles = vec!["analyst".to_string(), "catalog_admin".to_string()];
        assert!(auth.has_admin_role(&caller_roles));
    }

    #[test]
    fn admin_role_misses_when_caller_lacks_admin() {
        let auth = auth_with_admin_roles(vec!["service_admin", "catalog_admin"]);
        let caller_roles = vec!["analyst".to_string(), "viewer".to_string()];
        assert!(!auth.has_admin_role(&caller_roles));
    }

    #[test]
    fn admin_role_misses_when_caller_has_no_roles() {
        let auth = auth_with_admin_roles(vec!["service_admin"]);
        assert!(!auth.has_admin_role(&[]));
    }

    #[test]
    fn admin_role_fails_closed_when_allowlist_empty() {
        // Operator explicitly cleared the allowlist - every admin
        // statement is rejected, even for users who happen to hold
        // a role named "service_admin".
        let auth = auth_with_admin_roles(vec![]);
        let caller_roles = vec!["service_admin".to_string()];
        assert!(!auth.has_admin_role(&caller_roles));
    }

    #[test]
    fn admin_role_match_is_case_sensitive() {
        // Role names are exact-match. SERVICE_ADMIN != service_admin.
        let auth = auth_with_admin_roles(vec!["service_admin"]);
        let caller_roles = vec!["SERVICE_ADMIN".to_string()];
        assert!(!auth.has_admin_role(&caller_roles));
    }

    fn coord_with_secret(secret: &str) -> CoordinatorConfig {
        let toml_src = format!(
            r#"
            mode = "single"
            worker_secret = "{secret}"
            "#
        );
        toml::from_str(&toml_src).expect("valid coordinator config")
    }

    #[test]
    fn coordinator_debug_does_not_leak_worker_secret() {
        let cfg = coord_with_secret("super-secret-cluster-root-AAA");
        let dbg = format!("{:?}", cfg);
        assert!(
            !dbg.contains("super-secret-cluster-root-AAA"),
            "worker_secret leaked to Debug output: {dbg}"
        );
        assert!(dbg.contains("<set>"), "presence sentinel missing: {dbg}");
        assert!(dbg.contains("CoordinatorConfig"), "struct tag missing: {dbg}");
    }

    #[test]
    fn coordinator_debug_distinguishes_unset_worker_secret() {
        let cfg = coord_with_secret("");
        let dbg = format!("{:?}", cfg);
        assert!(
            dbg.contains("worker_secret: <unset>"),
            "expected unset sentinel: {dbg}"
        );
    }

    /// Regression for #131: when `per_user_memory_budget` is smaller than
    /// `max_query_memory`, every query is rejected on admission. Validate
    /// at config-load instead.
    #[test]
    fn validate_rejects_per_user_budget_below_max_query_memory() {
        let mut config = valid_config();
        config.query.max_query_memory = "32GB".to_string();
        config.query.per_user_memory_budget = "1GB".to_string();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("per_user_memory_budget") && err.contains("max_query_memory"),
            "expected budget-vs-per-query guard, got: {err}"
        );
    }

    #[test]
    fn validate_accepts_per_user_budget_equal_to_max_query_memory() {
        let mut config = valid_config();
        config.query.max_query_memory = "32GB".to_string();
        config.query.per_user_memory_budget = "32GB".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_accepts_per_user_budget_disabled() {
        let mut config = valid_config();
        config.query.max_query_memory = "32GB".to_string();
        config.query.per_user_memory_budget = "0".to_string();
        // "0" disables the gate, so the size relationship is irrelevant.
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_rejects_ranger_zero_timeout() {
        let mut config = valid_config();
        config.policy.engine = PolicyEngine::Ranger;
        config.policy.ranger.url = "http://ranger:6080".to_string();
        config.policy.ranger.service_name = "hive".to_string();
        config.policy.ranger.timeout_secs = 0;
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("policy.ranger.timeout_secs"),
            "expected zero-timeout guard, got: {err}"
        );
    }

    #[test]
    fn validate_rejects_ranger_zero_breaker_threshold() {
        let mut config = valid_config();
        config.policy.engine = PolicyEngine::Ranger;
        config.policy.ranger.url = "http://ranger:6080".to_string();
        config.policy.ranger.service_name = "hive".to_string();
        config.policy.ranger.breaker_failure_threshold = 0;
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("policy.ranger.breaker_failure_threshold"),
            "expected zero-threshold guard, got: {err}"
        );
    }

    #[test]
    fn validate_ranger_guards_do_not_fire_for_other_engines() {
        // The Ranger numeric guards must not affect non-Ranger deployments.
        let mut config = valid_config();
        config.policy.ranger.timeout_secs = 0;
        config.policy.ranger.breaker_failure_threshold = 0;
        // engine stays at its default (not Ranger), so these zeros are ignored.
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_metadata_cache_ttl_under_policy_engine() {
        // A disabled table metadata cache (ttl=0) makes every tag lookup miss,
        // and the rewriter now fails closed on an unknown tag state, denying
        // every query. Reject the misconfig at load time under a policy engine.
        let mut config = valid_config();
        config.policy.engine = PolicyEngine::Ranger;
        config.policy.ranger.url = "http://ranger:6080".to_string();
        config.policy.ranger.service_name = "hive".to_string();
        config.catalog.metadata_cache_ttl_secs = 0;
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("catalog.metadata_cache_ttl_secs"),
            "expected metadata-cache-ttl guard, got: {err}"
        );
    }

    #[test]
    fn validate_zero_metadata_cache_ttl_allowed_under_passthrough() {
        // Passthrough applies no policy, so a disabled metadata cache is a
        // legitimate tuning choice (no tag lookups happen).
        let mut config = valid_config();
        config.policy.engine = PolicyEngine::Passthrough;
        config.catalog.metadata_cache_ttl_secs = 0;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_metadata_cache_ttl_under_inmemory_engine() {
        // The guard fires for any non-passthrough engine, not just Ranger.
        let mut config = valid_config();
        config.policy.engine = PolicyEngine::InMemory;
        config.catalog.metadata_cache_ttl_secs = 0;
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("catalog.metadata_cache_ttl_secs"),
            "expected metadata-cache-ttl guard under in-memory engine, got: {err}"
        );
    }
}

#[cfg(test)]
mod ranger_config_tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn parse_ranger_backend_from_str() {
        assert_eq!(
            AccessControlBackend::from_str("ranger").unwrap(),
            AccessControlBackend::Ranger
        );
    }

    #[test]
    fn unknown_backend_lists_ranger() {
        let err = AccessControlBackend::from_str("bogus").unwrap_err();
        assert!(err.contains("ranger"), "error should mention ranger: {err}");
    }

    #[test]
    fn ranger_config_defaults() {
        let c = RangerConfig::default();
        assert_eq!(c.service_name, "polaris");
        assert_eq!(c.admin_user, "admin");
        assert_eq!(c.timeout_secs, 30);
        assert!(!c.accept_invalid_certs);
        assert!(c.realm.is_empty());
    }

    #[test]
    fn access_control_config_default_includes_ranger() {
        let c = AccessControlConfig::default();
        assert_eq!(c.ranger.service_name, "polaris");
    }

    #[test]
    fn ranger_config_deserializes_from_toml() {
        let toml = r#"
            backend = "ranger"
            url = "http://ranger-admin:6080"
            [ranger]
            service-name = "dev_polaris"
            admin-user = "admin"
            admin-password = "secret"
            realm = "POLARIS"
        "#;
        let c: AccessControlConfig = toml::from_str(toml).unwrap();
        assert_eq!(c.backend, AccessControlBackend::Ranger);
        assert_eq!(c.ranger.service_name, "dev_polaris");
        assert_eq!(c.ranger.admin_password.expose(), "secret");
        assert_eq!(c.ranger.realm, "POLARIS");
    }

    #[test]
    fn policy_engine_parses_ranger() {
        use std::str::FromStr;
        assert_eq!(
            crate::config::PolicyEngine::from_str("ranger").unwrap(),
            crate::config::PolicyEngine::Ranger
        );
    }

    #[test]
    fn policy_engine_unknown_lists_ranger() {
        use std::str::FromStr;
        let err = crate::config::PolicyEngine::from_str("nope").unwrap_err();
        assert!(err.contains("ranger"), "error must list ranger: {err}");
    }

    #[test]
    fn ranger_policy_config_defaults() {
        let c = crate::config::RangerPolicyConfig::default();
        assert_eq!(c.service_name, "hive");
        assert_eq!(c.admin_user, "admin");
        assert_eq!(c.cache_ttl_secs, 30);
    }

    #[test]
    fn audit_config_defaults_are_back_compatible() {
        let c = AuditConfig::default();
        assert_eq!(c.format, "native");
        assert!(c.gdpr_tags.is_empty());
        assert_eq!(c.gdpr_identifier_mode, "tokenize");
        assert!(!c.superdebug_log_results);
    }

    #[test]
    fn audit_export_config_defaults() {
        let c = AuditExportConfig::default();
        assert!(!c.enabled);
        assert_eq!(c.target, "otlp");
        assert_eq!(c.batch_max, 512);
        assert_eq!(c.flush_interval_ms, 2000);
        assert_eq!(c.start_at, "now");
        assert_eq!(c.max_spool_bytes, 1_073_741_824);
    }

    #[test]
    fn write_memory_safety_defaults() {
        let c = QueryConfig::default();
        assert_eq!(c.fanout_max_open_writers, 0, "0 = auto");
        assert_eq!(c.fanout_buffer_budget, "0", "\"0\" = auto");
        assert!(c.write_buffer_tracking, "tracking on by default");
        assert!(!c.merge_target_streaming, "B2 off by default");
    }

    /// A config file predating the write-memory-safety knobs still parses, and
    /// the new fields fall back to their defaults (`#[serde(default)]`).
    #[test]
    fn write_memory_safety_knobs_are_back_compatible() {
        let toml = r#"
timeout_secs = 120
max_query_memory = "512MB"
"#;
        let c: QueryConfig = toml::from_str(toml).expect("old config still parses");
        assert_eq!(c.fanout_max_open_writers, 0);
        assert_eq!(c.fanout_buffer_budget, "0");
        assert!(c.write_buffer_tracking);
        // Explicit overrides round-trip.
        let toml = r#"
fanout_max_open_writers = 32
fanout_buffer_budget = "256MB"
write_buffer_tracking = false
"#;
        let c: QueryConfig = toml::from_str(toml).expect("overrides parse");
        assert_eq!(c.fanout_max_open_writers, 32);
        assert_eq!(c.fanout_buffer_budget, "256MB");
        assert!(!c.write_buffer_tracking);
    }
}
