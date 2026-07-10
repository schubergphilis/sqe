//! Read-only web UI: serde DTOs and pure mapping functions from the
//! coordinator's in-memory `QueryTracker` / `WorkerRegistry` snapshots into the
//! dashboard's JSON wire format. The axum handlers in `bin/sqe_server.rs` are
//! thin wrappers over these functions. Ops-only, network-gated; see
//! `docs/superpowers/specs/2026-06-01-sqe-web-ui-design.md`.

use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use uuid::Uuid;

use crate::query_tracker::{QueryRecord, QueryState, QueryTracker};
use sqe_metrics::audit::redact_pii;

// ── Overview endpoint ──────────────────────────────────────────

/// Static node identity and capability info, populated once at startup from
/// config and passed into `overview()`. Keeping this struct separate from
/// `HealthState` means the mapping function stays a pure fn (no async, no Arc).
#[derive(Clone)]
pub struct NodeInfo {
    pub name: &'static str,
    pub version: &'static str,
    pub role: &'static str,
    /// DataFusion version string (hardcoded "51" to match cluster_status).
    pub datafusion_version: &'static str,
    // ── capabilities ──────────────────────────────────────────────
    pub flight_sql_port: u16,
    /// None when trino_http_port == 0 (disabled).
    pub trino_http_port: Option<u16>,
    /// None when quack_port == 0 (disabled).
    pub quack_port: Option<u16>,
    /// Human-readable catalog backend name (e.g. "rest", "hms", "glue").
    pub catalog_backend: String,
    pub catalog_url: String,
    /// S3 endpoint URL if set, otherwise "local".
    pub storage: String,
    /// Human-readable max query memory string (e.g. "256MB"), or None if unlimited/unset.
    pub memory_limit: Option<String>,
    pub max_concurrent_queries: usize,
}

#[derive(Serialize, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OverviewNodeDto {
    pub name: &'static str,
    pub version: &'static str,
    pub role: &'static str,
    pub datafusion_version: &'static str,
    pub uptime_seconds: u64,
}

#[derive(Serialize, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OverviewCapabilitiesDto {
    pub flight_sql_port: u16,
    pub trino_http_port: Option<u16>,
    pub quack_port: Option<u16>,
    pub catalog_backend: String,
    pub catalog_url: String,
    pub storage: String,
    pub memory_limit: Option<String>,
    pub max_concurrent_queries: usize,
}

#[derive(Serialize, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OverviewResourcesDto {
    pub cpu_cores: usize,
    pub memory_limit_bytes: u64,
    pub active_queries: usize,
    pub peak_query_memory_bytes: u64,
    pub total_spill_bytes: u64,
}

#[derive(Serialize, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OverviewMetricsDto {
    pub total_queries: usize,
    pub running: usize,
    pub finished: usize,
    pub failed: usize,
    pub canceled: usize,
    pub total_output_rows: u64,
    pub total_bytes_scanned: u64,
    /// Mean execution_ms over FINISHED queries; 0 when there are none.
    pub avg_execution_ms: u64,
}

#[derive(Serialize, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OverviewDto {
    pub node: OverviewNodeDto,
    pub capabilities: OverviewCapabilitiesDto,
    pub resources: OverviewResourcesDto,
    pub metrics: OverviewMetricsDto,
}

/// Build the overview response. `node` carries static config info; `uptime_seconds`
/// is computed by the caller from `Instant::elapsed`. `cpu_cores` is
/// `std::thread::available_parallelism()` (fallback 1), resolved by the caller.
/// The tracker provides all runtime counters.
pub fn overview(node: &NodeInfo, uptime_seconds: u64, cpu_cores: usize, tracker: &QueryTracker) -> OverviewDto {
    let records = tracker.records();

    let mut running = 0usize;
    let mut finished = 0usize;
    let mut failed = 0usize;
    let mut canceled = 0usize;
    let mut total_output_rows: u64 = 0;
    let mut total_bytes_scanned: u64 = 0;
    let mut finished_execution_ms_sum: u64 = 0;
    let mut peak_query_memory_bytes: u64 = 0;
    let mut total_spill_bytes: u64 = 0;

    for rec in &records {
        match rec.state {
            QueryState::Running | QueryState::Queued => running += 1,
            QueryState::Finished => {
                finished += 1;
                finished_execution_ms_sum += rec.execution_ms;
            }
            QueryState::Failed => failed += 1,
            QueryState::Canceled => canceled += 1,
        }
        total_output_rows += rec.output_rows as u64;
        total_bytes_scanned += rec.bytes_scanned;
        if rec.peak_memory_bytes > peak_query_memory_bytes {
            peak_query_memory_bytes = rec.peak_memory_bytes;
        }
        total_spill_bytes += rec.spill_bytes;
    }

    let avg_execution_ms = if finished > 0 { finished_execution_ms_sum / finished as u64 } else { 0 };
    let total_queries = records.len();

    let memory_limit_bytes = node
        .memory_limit
        .as_deref()
        .and_then(|s| sqe_core::parse_memory_limit(s).ok())
        .unwrap_or(0) as u64;

    OverviewDto {
        node: OverviewNodeDto {
            name: node.name,
            version: node.version,
            role: node.role,
            datafusion_version: node.datafusion_version,
            uptime_seconds,
        },
        capabilities: OverviewCapabilitiesDto {
            flight_sql_port: node.flight_sql_port,
            trino_http_port: node.trino_http_port,
            quack_port: node.quack_port,
            catalog_backend: node.catalog_backend.clone(),
            catalog_url: node.catalog_url.clone(),
            storage: node.storage.clone(),
            memory_limit: node.memory_limit.clone(),
            max_concurrent_queries: node.max_concurrent_queries,
        },
        resources: OverviewResourcesDto {
            cpu_cores,
            memory_limit_bytes,
            active_queries: tracker.active_count(),
            peak_query_memory_bytes,
            total_spill_bytes,
        },
        metrics: OverviewMetricsDto {
            total_queries,
            running,
            finished,
            failed,
            canceled,
            total_output_rows,
            total_bytes_scanned,
            avg_execution_ms,
        },
    }
}

/// The dashboard single-page app, embedded at compile time.
pub const DASHBOARD_HTML: &str = include_str!("web_ui/dashboard.html");

/// WEB-02 (updated C1): username, roles, client_ip, and SQL text are now
/// exposed on these routes because all dashboard endpoints are admin-gated
/// (bearer token + a role from `auth.admin_roles` required, default:
/// `service_admin` / `catalog_admin`, sub-project C task 1). The raw SQL is
/// NEVER placed in the DTO. The `sql` field always contains the output of
/// `redact_pii`, which replaces known PII patterns (email, phone, SSN,
/// card numbers, secret keywords) with bracketed placeholders before the
/// value leaves this layer.
fn sql_digest(sql: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(sql.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[derive(Serialize, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryListItem {
    pub query_id: String,
    pub state: String,
    pub source: Option<String>,
    /// SHA-256 hex digest of the raw SQL text. Kept for correlation and
    /// grouping of identical statements. See also `sql`.
    pub sql_hash: String,
    /// SQL text after `redact_pii` masking. Raw SQL is never placed here.
    /// Exposed because the route is admin-gated (WEB-02 / C1).
    pub sql: String,
    /// Identity of the submitting user. Exposed because the route is
    /// admin-gated (WEB-02 / C1).
    pub username: String,
    /// Roles held by the submitting user at query time.
    pub roles: Vec<String>,
    /// Source IP of the submitting client, if available.
    pub client_ip: Option<String>,
    pub created: String,
    pub started: Option<String>,
    pub ended: Option<String>,
    pub queued_ms: u64,
    pub planning_ms: u64,
    pub execution_ms: u64,
    pub output_rows: usize,
    pub rows_scanned: u64,
    pub bytes_scanned: u64,
    pub spill_bytes: u64,
    pub peak_memory_bytes: u64,
    pub error_type: Option<String>,
    pub error_message: Option<String>,
}

fn to_list_item(r: &QueryRecord) -> QueryListItem {
    QueryListItem {
        query_id: r.query_id.to_string(),
        state: r.state.to_string(),
        source: r.source.clone(),
        sql_hash: sql_digest(&r.sql),
        sql: redact_pii(&r.sql),
        username: r.user.clone(),
        roles: r.roles.clone(),
        client_ip: r.client_ip.clone(),
        created: r.created.to_rfc3339(),
        started: r.started.map(|t| t.to_rfc3339()),
        ended: r.ended.map(|t| t.to_rfc3339()),
        queued_ms: r.queued_ms,
        planning_ms: r.planning_ms,
        execution_ms: r.execution_ms,
        output_rows: r.output_rows,
        rows_scanned: r.rows_scanned,
        bytes_scanned: r.bytes_scanned,
        spill_bytes: r.spill_bytes,
        peak_memory_bytes: r.peak_memory_bytes,
        error_type: r.error_type.clone(),
        error_message: r.error_message.as_deref().map(redact_pii),
    }
}

/// Most-recent-first list of queries, optionally filtered by state, capped at
/// `limit`. `state_filter` matches `QueryState::to_string()` case-insensitively
/// ("running", "finished", ...); `None` or "all" returns every state.
pub fn query_list(
    tracker: &QueryTracker,
    state_filter: Option<&str>,
    limit: usize,
) -> Vec<QueryListItem> {
    let want = state_filter
        .map(|s| s.to_ascii_lowercase())
        .filter(|s| s != "all" && !s.is_empty());

    let mut records = tracker.records();
    records.sort_by(|a, b| b.created.cmp(&a.created));

    records
        .into_iter()
        .filter(|r| match &want {
            Some(w) => r.state.to_string().eq_ignore_ascii_case(w),
            None => true,
        })
        .take(limit)
        .map(|r| to_list_item(&r))
        .collect()
}

#[derive(Serialize, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FragmentDto {
    pub task_id: String,
    pub worker_url: String,
    pub state: String,
    pub elapsed_ms: u64,
    pub input_rows: usize,
    pub output_rows: usize,
}

#[derive(Serialize, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryDetail {
    #[serde(flatten)]
    pub summary: QueryListItem,
    // username, roles, client_ip, and redacted SQL are carried in `summary`
    // (QueryListItem) because the routes are admin-gated (WEB-02 / C1).
    // session_id is not exposed (internal correlation identifier only).
    pub tables_touched: Vec<String>,
    pub error_code: Option<String>,
    pub fragments: Vec<FragmentDto>,
    /// Passive per-operator profile (see `[query] query_profile`). Detail
    /// response only: the rendered plan tree can reach 64 KiB and would
    /// bloat the list endpoint.
    pub profile: Option<String>,
}

/// Full record for one query plus its fragment list. `None` if the id is
/// unknown or has aged out of the history window.
pub fn query_detail(tracker: &QueryTracker, id: &Uuid) -> Option<QueryDetail> {
    let rec = tracker.records().into_iter().find(|r| &r.query_id == id)?;
    let fragments = rec
        .fragments_snapshot()
        .into_iter()
        .map(|f| FragmentDto {
            task_id: f.task_id,
            worker_url: f.worker_url,
            state: f.state.to_string(),
            elapsed_ms: f.elapsed_ms,
            input_rows: f.input_rows,
            output_rows: f.output_rows,
        })
        .collect();
    Some(QueryDetail {
        summary: to_list_item(&rec),
        tables_touched: rec.tables_touched.clone(),
        error_code: rec.error_code.clone(),
        fragments,
        profile: rec.profile.clone(),
    })
}

#[derive(Serialize, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct WorkerDto {
    pub url: String,
    pub healthy: bool,
    pub in_flight: u32,
}

#[derive(Serialize, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct WorkersDto {
    pub total: usize,
    pub healthy_count: usize,
    pub active_queries: usize,
    pub workers: Vec<WorkerDto>,
}

/// Build the cluster/workers view. `total` and `healthy_urls` come from the
/// `WorkerRegistry` (awaited by the caller); `active_queries` from
/// `QueryTracker::active_count()`. Per-worker `in_flight` counts Running
/// fragments of Running queries grouped by worker URL. Only healthy workers are
/// listed by URL; the `total` vs `healthy_count` gap reveals any that are down.
///
/// When there are NO remote workers (`total == 0` and `healthy_urls` is empty),
/// returns a single synthetic worker representing the local node (coordinator
/// acting as both coordinator and worker). This makes single-node deployments
/// show one healthy worker instead of an empty list.
pub fn workers_view(
    total: usize,
    healthy_urls: Vec<String>,
    active_queries: usize,
    tracker: &QueryTracker,
) -> WorkersDto {
    // Single-node mode: no remote workers registered. Return one synthetic
    // worker entry that represents the local node doing both roles.
    if total == 0 && healthy_urls.is_empty() {
        return WorkersDto {
            total: 1,
            healthy_count: 1,
            active_queries,
            workers: vec![WorkerDto {
                url: "local (coordinator + worker)".to_string(),
                healthy: true,
                in_flight: active_queries as u32,
            }],
        };
    }

    let mut in_flight: HashMap<String, u32> = HashMap::new();
    for rec in tracker.records() {
        if rec.state == QueryState::Running {
            for f in rec.fragments_snapshot() {
                if matches!(f.state, crate::query_tracker::FragmentState::Running) {
                    *in_flight.entry(f.worker_url.clone()).or_default() += 1;
                }
            }
        }
    }
    let workers = healthy_urls
        .iter()
        .map(|url| WorkerDto {
            url: url.clone(),
            healthy: true,
            in_flight: in_flight.get(url).copied().unwrap_or(0),
        })
        .collect();
    WorkersDto {
        total,
        healthy_count: healthy_urls.len(),
        active_queries,
        workers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_tracker::QueryTracker;
    use sqe_core::QueryHistoryConfig;

    fn tracker() -> QueryTracker {
        QueryTracker::new(&QueryHistoryConfig { max_entries: 100, ttl_secs: 60 })
    }

    #[tokio::test]
    async fn query_list_returns_started_records_recent_first() {
        let t = tracker();
        let id1 = Uuid::now_v7();
        t.start(id1, "alice", Some("cli"), "SELECT 1", "s1", None, vec![]);
        let id2 = Uuid::now_v7();
        t.start(id2, "bob", Some("flight_sql"), "SELECT 2", "s2", None, vec![]);

        let list = query_list(&t, None, 100);
        assert_eq!(list.len(), 2);
        // id2 was created after id1, so it sorts first.
        assert_eq!(list[0].query_id, id2.to_string());
        assert_eq!(list[0].state, "QUEUED");
        assert_eq!(list[0].sql_hash, sql_digest("SELECT 2"));
        // Admin-gated routes expose username (WEB-02 / C1).
        assert_eq!(list[0].username, "bob");
    }

    #[tokio::test]
    async fn query_list_filters_by_state_and_hashes_sql() {
        let t = tracker();
        let id = Uuid::now_v7();
        let sql = "SELECT secret FROM t WHERE email = 'jane@x.com'";
        t.start(id, "alice", None, sql, "s1", None, vec![]);
        t.running(&id, 5);

        let running = query_list(&t, Some("running"), 100);
        assert_eq!(running.len(), 1);
        // Raw SQL must not appear in the DTO; redact_pii replaces PII patterns.
        let json = serde_json::to_string(&running[0]).unwrap();
        assert!(!json.contains("jane@x.com"), "raw email must not appear in serialized DTO: {json}");
        assert_eq!(running[0].sql_hash, sql_digest(sql));

        let finished = query_list(&t, Some("finished"), 100);
        assert!(finished.is_empty());

        // "all" behaves like no filter.
        assert_eq!(query_list(&t, Some("all"), 100).len(), 1);
    }

    #[tokio::test]
    async fn query_list_respects_limit() {
        let t = tracker();
        for i in 0..10 {
            t.start(Uuid::now_v7(), &format!("u{i}"), None, "SELECT 1", "s", None, vec![]);
        }
        assert_eq!(query_list(&t, None, 3).len(), 3);
    }

    #[tokio::test]
    async fn query_list_hashes_multibyte_sql_without_panic() {
        let t = tracker();
        let id = Uuid::now_v7();
        // Multibyte SQL must hash cleanly (the old byte-slice truncation could
        // split a char; hashing operates on bytes and never panics).
        let sql = "λ".repeat(600);
        t.start(id, "alice", None, &sql, "s1", None, vec![]);
        let list = query_list(&t, None, 100);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].sql_hash, sql_digest(&sql));
    }

    #[tokio::test]
    async fn query_detail_includes_fragments() {
        use crate::query_tracker::{FragmentInfo, FragmentState};
        let t = tracker();
        let id = Uuid::now_v7();
        t.start(id, "alice", None, "SELECT *", "s1", None, vec!["analyst".to_string()]);
        t.set_fragments(&id, vec![FragmentInfo {
            task_id: "frag-0".into(),
            worker_url: "http://w1:50052".into(),
            state: FragmentState::Running,
            elapsed_ms: 12,
            input_rows: 5,
            output_rows: 3,
        }]);

        let detail = query_detail(&t, &id).expect("detail present");
        assert_eq!(detail.summary.query_id, id.to_string());
        // Admin-gated: username is now in the summary (WEB-02 / C1).
        assert_eq!(detail.summary.username, "alice");
        assert_eq!(detail.summary.sql_hash, sql_digest("SELECT *"));
        assert_eq!(detail.fragments.len(), 1);
        assert_eq!(detail.fragments[0].worker_url, "http://w1:50052");
        assert_eq!(detail.fragments[0].state, "RUNNING");
    }

    #[tokio::test]
    async fn query_detail_unknown_id_is_none() {
        let t = tracker();
        assert!(query_detail(&t, &Uuid::now_v7()).is_none());
    }

    #[tokio::test]
    async fn workers_view_counts_in_flight_fragments() {
        use crate::query_tracker::{FragmentInfo, FragmentState};
        let t = tracker();
        let id = Uuid::now_v7();
        t.start(id, "alice", None, "SELECT *", "s1", None, vec![]);
        t.running(&id, 1);
        t.set_fragments(&id, vec![
            FragmentInfo { task_id: "f0".into(), worker_url: "http://w1:50052".into(),
                state: FragmentState::Running, elapsed_ms: 0, input_rows: 0, output_rows: 0 },
            FragmentInfo { task_id: "f1".into(), worker_url: "http://w1:50052".into(),
                state: FragmentState::Running, elapsed_ms: 0, input_rows: 0, output_rows: 0 },
            FragmentInfo { task_id: "f2".into(), worker_url: "http://w2:50052".into(),
                state: FragmentState::Finished, elapsed_ms: 9, input_rows: 0, output_rows: 0 },
        ]);

        let view = workers_view(
            2,
            vec!["http://w1:50052".to_string(), "http://w2:50052".to_string()],
            t.active_count(),
            &t,
        );
        assert_eq!(view.total, 2);
        assert_eq!(view.healthy_count, 2);
        assert_eq!(view.active_queries, 1);
        let w1 = view.workers.iter().find(|w| w.url == "http://w1:50052").unwrap();
        assert_eq!(w1.in_flight, 2);
        let w2 = view.workers.iter().find(|w| w.url == "http://w2:50052").unwrap();
        assert_eq!(w2.in_flight, 0); // its only fragment is Finished
    }

    #[tokio::test]
    async fn workers_view_empty_cluster() {
        let t = tracker();
        let view = workers_view(0, vec![], 0, &t);
        // Single-node mode: expect exactly one synthetic local worker entry.
        assert_eq!(view.total, 1);
        assert_eq!(view.healthy_count, 1);
        assert_eq!(view.workers.len(), 1);
        assert!(view.workers[0].url.contains("local"));
        assert!(view.workers[0].healthy);
    }

    #[test]
    fn dashboard_html_is_present_and_html() {
        assert!(DASHBOARD_HTML.contains("<title>SQE</title>"));
        assert!(DASHBOARD_HTML.contains("/api/v1/queries"));
    }

    fn default_node_info() -> NodeInfo {
        NodeInfo {
            name: "SQE",
            version: "0.1.0",
            role: "coordinator",
            datafusion_version: "51",
            flight_sql_port: 50051,
            trino_http_port: Some(8080),
            quack_port: None,
            catalog_backend: "rest".to_string(),
            catalog_url: "http://polaris:8181".to_string(),
            storage: "http://minio:9000".to_string(),
            memory_limit: Some("256MB".to_string()),
            max_concurrent_queries: 100,
        }
    }

    #[tokio::test]
    async fn overview_empty_tracker() {
        let t = tracker();
        let node = default_node_info();
        let dto = overview(&node, 42, 4, &t);
        assert_eq!(dto.node.role, "coordinator");
        assert_eq!(dto.node.uptime_seconds, 42);
        assert_eq!(dto.resources.cpu_cores, 4);
        assert_eq!(dto.metrics.total_queries, 0);
        assert_eq!(dto.metrics.running, 0);
        assert_eq!(dto.metrics.finished, 0);
        assert_eq!(dto.metrics.avg_execution_ms, 0);
        assert_eq!(dto.capabilities.flight_sql_port, 50051);
        assert_eq!(dto.capabilities.trino_http_port, Some(8080));
        assert_eq!(dto.capabilities.quack_port, None);
    }

    #[tokio::test]
    async fn overview_metrics_aggregation() {
        let t = tracker();

        // Two finished queries with different execution times.
        let id1 = Uuid::now_v7();
        t.start(id1, "alice", None, "SELECT 1", "s1", None, vec![]);
        t.running(&id1, 5);
        t.complete(&id1, 100, 200, vec!["t1".to_string()], 1024, 100, 0, 512 * 1024 * 1024);

        let id2 = Uuid::now_v7();
        t.start(id2, "bob", None, "SELECT 2", "s2", None, vec![]);
        t.running(&id2, 3);
        t.complete(&id2, 50, 400, vec![], 2048, 50, 64, 256 * 1024 * 1024);

        // One failed query.
        let id3 = Uuid::now_v7();
        t.start(id3, "carol", None, "BAD SQL", "s3", None, vec![]);
        t.running(&id3, 1);
        t.failed(&id3, &sqe_core::SqeError::Execution("syntax error".to_string()));

        let node = default_node_info();
        let dto = overview(&node, 0, 2, &t);

        assert_eq!(dto.metrics.total_queries, 3);
        assert_eq!(dto.metrics.finished, 2);
        assert_eq!(dto.metrics.failed, 1);
        assert_eq!(dto.metrics.running, 0);
        assert_eq!(dto.metrics.canceled, 0);
        // avg of 200 and 400 = 300
        assert_eq!(dto.metrics.avg_execution_ms, 300);
        assert_eq!(dto.metrics.total_output_rows, 150); // 100 + 50
        assert_eq!(dto.metrics.total_bytes_scanned, 3072); // 1024 + 2048
        // peak memory = max(512MB, 256MB) = 512MB
        assert_eq!(dto.resources.peak_query_memory_bytes, 512 * 1024 * 1024);
        assert_eq!(dto.resources.total_spill_bytes, 64);
    }

    #[tokio::test]
    async fn overview_memory_limit_bytes_parsed() {
        let t = tracker();
        let mut node = default_node_info();
        node.memory_limit = Some("1GB".to_string());
        let dto = overview(&node, 0, 1, &t);
        assert_eq!(dto.resources.memory_limit_bytes, 1024 * 1024 * 1024);
    }

    #[tokio::test]
    async fn overview_no_memory_limit_yields_zero() {
        let t = tracker();
        let mut node = default_node_info();
        node.memory_limit = None;
        let dto = overview(&node, 0, 1, &t);
        assert_eq!(dto.resources.memory_limit_bytes, 0);
    }

    /// TDD: admin-gated DTOs expose username, roles, client_ip, and redacted SQL.
    /// The `sql` field must contain the redact_pii placeholder for an email and
    /// must NOT contain the raw email literal.
    #[tokio::test]
    async fn to_list_item_exposes_admin_fields_with_redacted_sql() {
        let t = tracker();
        let id = Uuid::now_v7();
        let sql = "SELECT * FROM users WHERE email = 'a@b.com'";
        t.start(id, "alice", None, sql, "s1", Some("10.0.0.7"), vec!["admin".to_string()]);

        let list = query_list(&t, None, 100);
        assert_eq!(list.len(), 1);
        let item = &list[0];

        assert_eq!(item.username, "alice");
        assert_eq!(item.roles, vec!["admin".to_string()]);
        assert_eq!(item.client_ip, Some("10.0.0.7".to_string()));
        // sql must be redact_pii output: email replaced, raw literal absent.
        assert!(item.sql.contains("[EMAIL]"), "expected [EMAIL] in sql, got: {}", item.sql);
        assert!(!item.sql.contains("a@b.com"), "raw email must not appear in sql, got: {}", item.sql);
    }

    /// error_message is passed through redact_pii so PII in engine error
    /// text does not leak to the dashboard caller.
    #[tokio::test]
    async fn error_message_is_redacted_in_dto() {
        let t = tracker();
        let id = Uuid::now_v7();
        t.start(id, "bob", None, "SELECT 1", "s1", None, vec![]);
        // Simulate a failed query whose error message contains an email address.
        let err = sqe_core::SqeError::Execution(
            "constraint violation for user 'a@b.com'".to_string(),
        );
        t.failed(&id, &err);

        let list = query_list(&t, None, 100);
        assert_eq!(list.len(), 1);
        let item = &list[0];

        // The DTO must contain the redacted placeholder, not the raw address.
        let msg = item.error_message.as_deref().expect("error_message present");
        assert!(
            msg.contains("[EMAIL]"),
            "expected [EMAIL] in error_message, got: {msg}"
        );
        assert!(
            !msg.contains("a@b.com"),
            "raw email must not appear in error_message, got: {msg}"
        );
    }
}
