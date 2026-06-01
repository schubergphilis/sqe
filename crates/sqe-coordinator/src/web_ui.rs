//! Read-only web UI: serde DTOs and pure mapping functions from the
//! coordinator's in-memory `QueryTracker` / `WorkerRegistry` snapshots into the
//! dashboard's JSON wire format. The axum handlers in `bin/sqe_server.rs` are
//! thin wrappers over these functions. Ops-only, network-gated; see
//! `docs/superpowers/specs/2026-06-01-sqe-web-ui-design.md`.

use serde::Serialize;
use std::collections::HashMap;
use uuid::Uuid;

use crate::query_tracker::{QueryRecord, QueryState, QueryTracker};

/// SQL is truncated server-side; the full statement is not needed for an ops
/// glance and bounds the payload.
const SQL_MAX: usize = 512;

fn truncate_sql(sql: &str) -> String {
    if sql.len() <= SQL_MAX {
        sql.to_string()
    } else {
        format!("{}...", &sql[..SQL_MAX])
    }
}

#[derive(Serialize, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryListItem {
    pub query_id: String,
    pub state: String,
    pub user: String,
    pub source: Option<String>,
    pub sql: String,
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
        user: r.user.clone(),
        source: r.source.clone(),
        sql: truncate_sql(&r.sql),
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
        error_message: r.error_message.clone(),
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
    pub session_id: String,
    pub client_ip: Option<String>,
    pub roles: Vec<String>,
    pub tables_touched: Vec<String>,
    pub error_code: Option<String>,
    pub fragments: Vec<FragmentDto>,
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
        session_id: rec.session_id.clone(),
        client_ip: rec.client_ip.clone(),
        roles: rec.roles.clone(),
        tables_touched: rec.tables_touched.clone(),
        error_code: rec.error_code.clone(),
        fragments,
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
pub fn workers_view(
    total: usize,
    healthy_urls: Vec<String>,
    active_queries: usize,
    tracker: &QueryTracker,
) -> WorkersDto {
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
        assert_eq!(list[0].user, "bob");
        assert_eq!(list[0].state, "QUEUED");
    }

    #[tokio::test]
    async fn query_list_filters_by_state_and_truncates_sql() {
        let t = tracker();
        let id = Uuid::now_v7();
        let long_sql = "X".repeat(SQL_MAX + 50);
        t.start(id, "alice", None, &long_sql, "s1", None, vec![]);
        t.running(&id, 5);

        let running = query_list(&t, Some("running"), 100);
        assert_eq!(running.len(), 1);
        assert!(running[0].sql.ends_with("..."));
        assert_eq!(running[0].sql.len(), SQL_MAX + 3);

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
        assert_eq!(detail.roles, vec!["analyst".to_string()]);
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
        assert_eq!(view.total, 0);
        assert!(view.workers.is_empty());
    }
}
