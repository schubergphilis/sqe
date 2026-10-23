use chrono::{DateTime, Utc};
use dashmap::DashMap;
use moka::sync::Cache;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use sqe_core::QueryHistoryConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryState {
    Queued,
    Running,
    Finished,
    Failed,
    Canceled,
}

impl std::fmt::Display for QueryState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Queued => write!(f, "QUEUED"),
            Self::Running => write!(f, "RUNNING"),
            Self::Finished => write!(f, "FINISHED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Canceled => write!(f, "CANCELED"),
        }
    }
}

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

#[derive(Debug, Clone)]
pub struct QueryRecord {
    pub query_id: Uuid,
    pub state: QueryState,
    pub user: String,
    /// Stable owner identity ([`sqe_core::SessionUser::principal_id`]) for
    /// ownership checks; `user` is display only.
    pub principal: String,
    pub source: Option<String>,
    pub sql: String,
    pub session_id: String,
    pub client_ip: Option<String>,
    pub roles: Vec<String>,
    pub created: DateTime<Utc>,
    pub started: Option<DateTime<Utc>>,
    pub ended: Option<DateTime<Utc>>,
    pub queued_ms: u64,
    pub planning_ms: u64,
    pub execution_ms: u64,
    pub output_rows: usize,
    /// Rows written/affected by a DML statement (INSERT/CTAS/UPDATE/DELETE/
    /// MERGE). `None` for reads and for statements with no write semantics.
    /// Distinct from `output_rows`, which counts result-set rows (a write's
    /// result set is a 1-row `rows_affected` batch or empty, so `output_rows`
    /// alone cannot report how many rows a write changed).
    pub rows_written: Option<u64>,
    pub error_type: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub tables_touched: Vec<String>,
    /// Bytes the scans decoded (their output).
    pub bytes_scanned: u64,
    /// Bytes the scans fetched from storage.
    pub physical_input_bytes: u64,
    pub rows_scanned: u64,
    pub spill_bytes: u64,
    pub peak_memory_bytes: u64,
    /// Passive per-operator profile rendered when `[query] query_profile`
    /// requires one (mode "all", or "slow"/failure). Capped at 64 KiB by
    /// the renderer.
    pub profile: Option<String>,
    pub trace_id: Option<String>,
    pub fragments: Arc<Mutex<Vec<FragmentInfo>>>,
}

impl QueryRecord {
    pub fn fragments_snapshot(&self) -> Vec<FragmentInfo> {
        self.fragments
            .lock()
            .expect("fragments mutex poisoned")
            .clone()
    }
}

/// An in-flight query: its current record plus the token that aborts it.
struct ActiveQuery {
    record: Arc<QueryRecord>,
    token: CancellationToken,
}

/// Tracks queries through their lifecycle.
///
/// Finding 403: in-flight records live in `active`, a plain map with no
/// TTL, so a long-running query can never be evicted while it runs (the
/// old single moka map evicted a `Running` record after `ttl_secs` of no
/// state change, after which `owner_of` returned `None` and cancel/complete
/// became no-ops). A record moves to the TTL'd `history` cache only when it
/// reaches a terminal state (Finished / Failed / Canceled).
pub struct QueryTracker {
    history: Cache<Uuid, Arc<QueryRecord>>,
    active: DashMap<Uuid, ActiveQuery>,
}

impl QueryTracker {
    /// Look up a record whether it is in flight or already in history.
    pub fn get(&self, query_id: &Uuid) -> Option<Arc<QueryRecord>> {
        if let Some(a) = self.active.get(query_id) {
            return Some(Arc::clone(&a.record));
        }
        self.history.get(query_id)
    }

    /// Copy-on-write update of a record in whichever map holds it.
    fn update(&self, query_id: &Uuid, f: impl FnOnce(&mut QueryRecord)) {
        if let Some(mut a) = self.active.get_mut(query_id) {
            let mut record = (*a.record).clone();
            f(&mut record);
            a.record = Arc::new(record);
            return;
        }
        if let Some(old) = self.history.get(query_id) {
            let mut record = (*old).clone();
            f(&mut record);
            self.history.insert(*query_id, Arc::new(record));
        }
    }

    /// Move a record from `active` to `history`, applying the terminal
    /// transition. Falls back to updating a history record in place when
    /// the query already finished (e.g. the stream finalizer running after
    /// an explicit cancel).
    fn finish(&self, query_id: &Uuid, f: impl FnOnce(&mut QueryRecord)) {
        if let Some((_, a)) = self.active.remove(query_id) {
            let mut record = (*a.record).clone();
            f(&mut record);
            self.history.insert(*query_id, Arc::new(record));
        } else if let Some(old) = self.history.get(query_id) {
            // Already terminal: a second `failed` for the same error (the
            // timeout arm marks the query, then the shared error path marks
            // it again) must not log and count the failure twice. Only a
            // cancel that landed first is refined by the later verdict.
            if old.state != QueryState::Canceled {
                return;
            }
            let mut record = (*old).clone();
            f(&mut record);
            self.history.insert(*query_id, Arc::new(record));
        }
    }

    /// The caller of an in-flight query went away without waiting for a
    /// verdict: the future driving it was dropped (an HTTP client that gave
    /// up, an MCP deadline). Moves the record to history as `Canceled` with
    /// the reason, so `system.runtime.queries` and the history do not show
    /// a query that is still "running" hours after nothing runs it. A no-op
    /// once the query is terminal.
    pub fn abandoned(&self, query_id: &Uuid, reason: &str) {
        if let Some((_, active)) = self.active.remove(query_id) {
            active.token.cancel();
            let mut record = (*active.record).clone();
            let now = Utc::now();
            record.state = QueryState::Canceled;
            record.ended = Some(now);
            record.error_code = Some("QUERY_ABANDONED".to_string());
            record.error_message = Some(reason.to_string());
            let elapsed_ms = wall_elapsed_ms(&record, now);
            tracing::warn!(
                query_id = %record.query_id,
                username = %record.user,
                elapsed_ms,
                reason,
                "Query abandoned by its caller"
            );
            self.history.insert(*query_id, Arc::new(record));
        }
    }
}

/// Milliseconds from the query's creation (admission) to `now`. Includes
/// queueing and planning, unlike `execution_ms`, so it matches what a
/// client experienced.
fn wall_elapsed_ms(record: &QueryRecord, now: DateTime<Utc>) -> u64 {
    (now - record.created).num_milliseconds().max(0) as u64
}

impl QueryTracker {
    pub fn new(config: &QueryHistoryConfig) -> Self {
        let history = Cache::builder()
            .max_capacity(config.max_entries)
            .time_to_live(std::time::Duration::from_secs(config.ttl_secs))
            .build();
        Self {
            history,
            active: DashMap::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn start(
        &self,
        query_id: Uuid,
        user: &str,
        source: Option<&str>,
        sql: &str,
        session_id: &str,
        client_ip: Option<&str>,
        roles: Vec<String>,
        trace_id: Option<String>,
    ) -> CancellationToken {
        self.start_with_principal(
            query_id, user, user, source, sql, session_id, client_ip, roles, trace_id,
        )
    }

    /// [`Self::start`] with a stable owner identity distinct from the
    /// display `user` (the IdP subject when one is known).
    #[allow(clippy::too_many_arguments)]
    pub fn start_with_principal(
        &self,
        query_id: Uuid,
        user: &str,
        principal: &str,
        source: Option<&str>,
        sql: &str,
        session_id: &str,
        client_ip: Option<&str>,
        roles: Vec<String>,
        trace_id: Option<String>,
    ) -> CancellationToken {
        let token = CancellationToken::new();
        let record = QueryRecord {
            query_id,
            state: QueryState::Queued,
            user: user.to_string(),
            principal: principal.to_string(),
            source: source.map(|s| s.to_string()),
            sql: sql.to_string(),
            session_id: session_id.to_string(),
            client_ip: client_ip.map(|s| s.to_string()),
            roles,
            created: Utc::now(),
            started: None,
            ended: None,
            queued_ms: 0,
            planning_ms: 0,
            execution_ms: 0,
            output_rows: 0,
            rows_written: None,
            error_type: None,
            error_code: None,
            error_message: None,
            tables_touched: Vec::new(),
            bytes_scanned: 0,
            physical_input_bytes: 0,
            rows_scanned: 0,
            spill_bytes: 0,
            peak_memory_bytes: 0,
            profile: None,
            trace_id,
            fragments: Arc::new(Mutex::new(Vec::new())),
        };
        self.active.insert(
            query_id,
            ActiveQuery {
                record: Arc::new(record),
                token: token.clone(),
            },
        );
        token
    }

    pub fn running(&self, query_id: &Uuid, planning_ms: u64) {
        self.update(query_id, |record| {
            let now = Utc::now();
            record.state = QueryState::Running;
            record.started = Some(now);
            let elapsed = (now - record.created).num_milliseconds();
            record.queued_ms = elapsed.max(0) as u64;
            record.planning_ms = planning_ms;
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub fn complete(
        &self,
        query_id: &Uuid,
        rows: usize,
        execution_ms: u64,
        tables_touched: Vec<String>,
        bytes_scanned: u64,
        rows_scanned: u64,
        spill_bytes: u64,
        peak_memory_bytes: u64,
    ) {
        self.finish(query_id, |record| {
            let now = Utc::now();
            record.state = QueryState::Finished;
            record.ended = Some(now);
            record.output_rows = rows;
            record.execution_ms = execution_ms;
            record.tables_touched = tables_touched;
            record.bytes_scanned = bytes_scanned;
            record.rows_scanned = rows_scanned;
            record.spill_bytes = spill_bytes;
            record.peak_memory_bytes = peak_memory_bytes;
            tracing::info!(
                query_id = %record.query_id,
                username = %record.user,
                rows = rows,
                execution_ms = execution_ms,
                elapsed_ms = wall_elapsed_ms(record, now),
                bytes_scanned = bytes_scanned,
                "Query completed"
            );
        });
    }

    /// Terminal failure record. Every failure path (buffered execution,
    /// the query timeout, streaming plan failures, the stream finalizer)
    /// ends here, so this is the one place that writes the failure to the
    /// server log (issue #668). Before this, a failed query left only an
    /// "Executing query" line with no completion and no error: the error
    /// went back to the client and nowhere else. User errors (syntax, missing
    /// table, denied) are WARN; internal, resource and external failures
    /// are ERROR. The message is the full internal one, not the redacted
    /// client message, because the log is the operator's only copy.
    pub fn failed(&self, query_id: &Uuid, error: &sqe_core::SqeError) {
        let code = error.error_code();
        self.finish(query_id, |record| {
            let now = Utc::now();
            record.state = QueryState::Failed;
            record.ended = Some(now);
            record.error_type = Some(code.trino_error_type().to_string());
            record.error_code = Some(code.name().to_string());
            record.error_message = Some(error.client_message());
            let elapsed_ms = wall_elapsed_ms(record, now);
            if code.is_user_error() {
                tracing::warn!(
                    query_id = %record.query_id,
                    username = %record.user,
                    error_code = code.name(),
                    error_type = code.trino_error_type(),
                    elapsed_ms,
                    error = %error,
                    "Query failed"
                );
            } else {
                tracing::error!(
                    query_id = %record.query_id,
                    username = %record.user,
                    error_code = code.name(),
                    error_type = code.trino_error_type(),
                    elapsed_ms,
                    error = %error,
                    "Query failed"
                );
            }
        });
    }

    pub fn canceled(&self, query_id: &Uuid) {
        self.finish(query_id, |record| {
            record.state = QueryState::Canceled;
            record.ended = Some(Utc::now());
        });
    }

    /// Attach a rendered per-operator profile to a query record. Called by
    /// the stream finalizer after `complete` / `failed`, so the terminal
    /// record carries the profile.
    pub fn set_profile(&self, query_id: &Uuid, profile: String) {
        self.update(query_id, |record| record.profile = Some(profile));
    }

    /// Record the rows written/affected by a DML statement. Called from the
    /// completion path once the affected count is recovered from the write
    /// result. Mirrors [`Self::set_profile`]: a copy-on-write update of the
    /// terminal record so it carries the count alongside the other stats.
    pub fn set_rows_written(&self, query_id: &Uuid, rows_written: u64) {
        self.update(query_id, |record| record.rows_written = Some(rows_written));
    }

    /// Record the bytes the statement's scans fetched from storage.
    pub fn set_physical_input_bytes(&self, query_id: &Uuid, bytes: u64) {
        self.update(query_id, |record| record.physical_input_bytes = bytes);
    }

    /// Cancel an in-flight query: fire its token and move the record to
    /// history as `Canceled`. Returns `false` when the query is not active
    /// (unknown, or already terminal).
    pub fn cancel(&self, query_id: &Uuid) -> bool {
        if let Some((_, active)) = self.active.remove(query_id) {
            active.token.cancel();
            let mut record = (*active.record).clone();
            record.state = QueryState::Canceled;
            record.ended = Some(Utc::now());
            self.history.insert(*query_id, Arc::new(record));
            true
        } else {
            false
        }
    }

    /// Look up the submitting username for a query, if known.
    /// Returned for ownership checks on cancel/inspect operations.
    /// In-flight queries are found regardless of age (finding 403).
    pub fn owner_of(&self, query_id: &Uuid) -> Option<String> {
        self.get(query_id).map(|r| r.user.clone())
    }

    /// Stable owner identity of a query, for ownership checks: compare it
    /// with [`sqe_core::SessionUser::principal_id`], never the username.
    pub fn principal_of(&self, query_id: &Uuid) -> Option<String> {
        self.get(query_id).map(|r| r.principal.clone())
    }

    pub fn set_fragments(&self, query_id: &Uuid, fragments: Vec<FragmentInfo>) {
        if let Some(record) = self.get(query_id) {
            let mut guard = record.fragments.lock().expect("fragments mutex poisoned");
            *guard = fragments;
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
        if let Some(record) = self.get(query_id) {
            let mut guard = record.fragments.lock().expect("fragments mutex poisoned");
            if let Some(frag) = guard.iter_mut().find(|f| f.task_id == task_id) {
                frag.state = state;
                frag.elapsed_ms = elapsed_ms;
                frag.output_rows = output_rows;
            }
        }
    }

    /// Check whether all fragments have completed and return per-fragment timing
    /// for straggler analysis.
    ///
    /// Returns `Some(Vec<(task_id, worker_url, elapsed_ms)>)` when every fragment
    /// is in a terminal state (`Finished` or `Failed`).  Returns `None` if any
    /// fragment is still `Running` or `Retried`, or if the query is unknown.
    pub fn all_fragments_done(&self, query_id: &Uuid) -> Option<Vec<(String, String, u64)>> {
        let record = self.get(query_id)?;
        let guard = record.fragments.lock().expect("fragments mutex poisoned");
        if guard.is_empty() {
            return None;
        }
        let all_done = guard
            .iter()
            .all(|f| matches!(f.state, FragmentState::Finished | FragmentState::Failed));
        if all_done {
            Some(
                guard
                    .iter()
                    .map(|f| (f.task_id.clone(), f.worker_url.clone(), f.elapsed_ms))
                    .collect(),
            )
        } else {
            None
        }
    }

    /// All known records: in-flight first, then history. A record that is
    /// moving between the maps while we iterate is de-duplicated by id.
    pub fn records(&self) -> Vec<Arc<QueryRecord>> {
        let mut seen = std::collections::HashSet::new();
        let mut records = Vec::new();
        for entry in self.active.iter() {
            seen.insert(*entry.key());
            records.push(Arc::clone(&entry.value().record));
        }
        for (k, v) in &self.history {
            if seen.insert(*k) {
                records.push(v);
            }
        }
        records
    }

    pub fn active_count(&self) -> usize {
        self.active.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> QueryHistoryConfig {
        QueryHistoryConfig {
            max_entries: 100,
            ttl_secs: 60,
        }
    }

    #[tokio::test]
    async fn start_creates_queued_record() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        let _token = tracker.start(
            id,
            "alice",
            Some("cli"),
            "SELECT 1",
            "s1",
            None,
            vec![],
            None,
        );
        let records = tracker.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].state, QueryState::Queued);
        assert_eq!(records[0].user, "alice");
        assert_eq!(tracker.active_count(), 1);
    }

    #[tokio::test]
    async fn full_lifecycle_queued_running_finished() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        tracker.start(id, "bob", None, "SELECT *", "s2", None, vec![], None);
        tracker.running(&id, 10);
        tracker.complete(&id, 42, 150, vec!["ns.table1".to_string()], 0, 0, 0, 0);
        let rec = tracker.get(&id).unwrap();
        assert_eq!(rec.state, QueryState::Finished);
        assert_eq!(rec.output_rows, 42);
        assert_eq!(rec.execution_ms, 150);
        assert_eq!(rec.planning_ms, 10);
        assert!(rec.tables_touched.contains(&"ns.table1".to_string()));
        assert_eq!(tracker.active_count(), 0);
    }

    #[tokio::test]
    async fn failed_records_error() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        tracker.start(id, "carol", None, "BAD SQL", "s3", None, vec![], None);
        tracker.running(&id, 0);
        let err = sqe_core::SqeError::with_code(
            sqe_core::SqeErrorCode::ExecutionFailed,
            "syntax error near BAD".to_string(),
        );
        tracker.failed(&id, &err);
        let rec = tracker.get(&id).unwrap();
        assert_eq!(rec.state, QueryState::Failed);
        assert!(rec.error_type.is_some());
        assert!(rec.error_code.is_some());
        assert!(rec.error_message.is_some());
        assert_eq!(tracker.active_count(), 0);
    }

    /// The timeout arm marks the query failed and the shared error path
    /// marks it again with the same error; the second verdict is dropped so
    /// the failure is logged and counted once.
    #[tokio::test]
    async fn a_second_failure_verdict_on_a_terminal_query_is_dropped() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        tracker.start(id, "carol", None, "SELECT 1", "s3", None, vec![], None);
        tracker.running(&id, 0);
        let timeout = sqe_core::SqeError::with_code(
            sqe_core::SqeErrorCode::ExecutionFailed,
            "Query timed out after 300s".to_string(),
        );
        tracker.failed(&id, &timeout);
        let first = tracker.get(&id).unwrap();
        let other = sqe_core::SqeError::with_code(
            sqe_core::SqeErrorCode::ExecutionFailed,
            "a different verdict".to_string(),
        );
        tracker.failed(&id, &other);
        let second = tracker.get(&id).unwrap();
        assert_eq!(second.state, QueryState::Failed);
        assert_eq!(second.error_message, first.error_message, "first verdict stands");
        assert_eq!(second.ended, first.ended);
    }

    /// A dropped caller moves the query to `Canceled` with the reason; the
    /// history never shows a query as running when nothing runs it, and a
    /// verdict arriving afterwards refines the record rather than being lost.
    #[tokio::test]
    async fn abandoned_cancels_an_active_query_and_is_a_no_op_once_terminal() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        let token = tracker.start(id, "dave", None, "SELECT 1", "s3", None, vec![], None);
        tracker.running(&id, 0);
        tracker.abandoned(&id, "caller went away");
        assert!(token.is_cancelled(), "the in-flight work is told to stop");
        let rec = tracker.get(&id).unwrap();
        assert_eq!(rec.state, QueryState::Canceled);
        assert_eq!(rec.error_code.as_deref(), Some("QUERY_ABANDONED"));
        assert_eq!(rec.error_message.as_deref(), Some("caller went away"));
        assert_eq!(tracker.active_count(), 0);

        // Already terminal: nothing changes.
        tracker.abandoned(&id, "again");
        assert_eq!(
            tracker.get(&id).unwrap().error_message.as_deref(),
            Some("caller went away")
        );
        // A finished query is not abandoned by a late guard drop.
        let done = Uuid::now_v7();
        tracker.start(done, "dave", None, "SELECT 2", "s3", None, vec![], None);
        tracker.complete(&done, 1, 1, vec![], 0, 0, 0, 0);
        tracker.abandoned(&done, "late");
        assert_eq!(tracker.get(&done).unwrap().state, QueryState::Finished);
    }

    #[tokio::test]
    async fn cancel_fires_token_and_records() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        let token = tracker.start(id, "dave", None, "SELECT 1", "s4", None, vec![], None);
        assert!(!token.is_cancelled());
        let cancelled = tracker.cancel(&id);
        assert!(cancelled);
        assert!(token.is_cancelled());
        let rec = tracker.get(&id).unwrap();
        assert_eq!(rec.state, QueryState::Canceled);
    }

    #[tokio::test]
    async fn set_profile_attaches_to_terminal_record() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        tracker.start(id, "alice", None, "SELECT 1", "s1", None, vec![], None);
        tracker.running(&id, 1);
        tracker.complete(&id, 5, 10, vec![], 0, 0, 0, 0);
        tracker.set_profile(&id, "elapsed_ms=10\nProjectionExec".to_string());
        let rec = tracker.get(&id).unwrap();
        assert_eq!(rec.state, QueryState::Finished);
        assert!(rec.profile.as_deref().unwrap().contains("ProjectionExec"));

        // Unknown id is a no-op.
        tracker.set_profile(&Uuid::now_v7(), "x".to_string());
    }

    #[tokio::test]
    async fn cancel_unknown_returns_false() {
        let tracker = QueryTracker::new(&test_config());
        assert!(!tracker.cancel(&Uuid::now_v7()));
    }

    #[tokio::test]
    async fn records_returns_all_entries() {
        let tracker = QueryTracker::new(&test_config());
        for i in 0..5 {
            let id = Uuid::now_v7();
            tracker.start(
                id,
                &format!("user{i}"),
                None,
                "SELECT 1",
                "s",
                None,
                vec![],
                None,
            );
        }
        assert_eq!(tracker.records().len(), 5);
    }

    #[tokio::test]
    async fn concurrent_query_lifecycle() {
        // 20 concurrent queries going through full lifecycle
        // (start -> running -> complete)
        let tracker = Arc::new(QueryTracker::new(&test_config()));
        let mut handles = tokio::task::JoinSet::new();

        for i in 0..20 {
            let tracker = tracker.clone();
            handles.spawn(async move {
                let id = Uuid::now_v7();
                let user = format!("user{i}");
                let sql = format!("SELECT {i}");
                tracker.start(id, &user, None, &sql, "session", None, vec![], None);
                tracker.running(&id, (i * 5) as u64);
                // Simulate some work
                tokio::task::yield_now().await;
                tracker.complete(
                    &id,
                    i as usize,
                    (i * 10) as u64,
                    vec![format!("table{i}")],
                    0,
                    0,
                    0,
                    0,
                );
                id
            });
        }

        let mut query_ids = Vec::new();
        while let Some(result) = handles.join_next().await {
            query_ids.push(result.expect("task should not panic"));
        }

        // All queries should be finished and no longer active
        assert_eq!(tracker.active_count(), 0, "all queries should be complete");

        // All 20 records should exist in history
        let records = tracker.records();
        assert_eq!(records.len(), 20);

        // Every record should be in Finished state
        for record in &records {
            assert_eq!(
                record.state,
                QueryState::Finished,
                "query {} should be Finished",
                record.query_id
            );
        }
    }

    #[tokio::test]
    async fn concurrent_mixed_lifecycle() {
        // 20 queries with a mix of finish, fail, cancel to test
        // concurrent state transitions.
        let tracker = Arc::new(QueryTracker::new(&test_config()));
        let mut handles = tokio::task::JoinSet::new();

        for i in 0..20 {
            let tracker = tracker.clone();
            handles.spawn(async move {
                let id = Uuid::now_v7();
                tracker.start(id, "user", None, "SELECT 1", "s", None, vec![], None);
                tracker.running(&id, 5);
                tokio::task::yield_now().await;

                match i % 3 {
                    0 => tracker.complete(&id, 10, 100, vec![], 0, 0, 0, 0),
                    1 => tracker.failed(
                        &id,
                        &sqe_core::SqeError::Execution("test error".to_string()),
                    ),
                    _ => {
                        tracker.cancel(&id);
                    }
                }
                (id, i % 3)
            });
        }

        let mut outcomes = Vec::new();
        while let Some(result) = handles.join_next().await {
            outcomes.push(result.expect("task should not panic"));
        }

        assert_eq!(tracker.active_count(), 0, "all queries should be inactive");
        let records = tracker.records();
        assert_eq!(records.len(), 20);

        // Verify states match the modulo pattern
        for (id, remainder) in &outcomes {
            let rec = tracker.get(id).unwrap();
            match remainder {
                0 => assert_eq!(rec.state, QueryState::Finished),
                1 => assert_eq!(rec.state, QueryState::Failed),
                _ => assert_eq!(rec.state, QueryState::Canceled),
            }
        }
    }

    #[tokio::test]
    async fn set_and_update_fragments() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        tracker.start(id, "alice", None, "SELECT *", "s1", None, vec![], None);

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

        let rec = tracker.get(&id).unwrap();
        assert_eq!(rec.fragments_snapshot().len(), 2);

        tracker.update_fragment(&id, "frag-0", FragmentState::Finished, 42, 100);
        let rec = tracker.get(&id).unwrap();
        let snap = rec.fragments_snapshot();
        assert_eq!(snap[0].state, FragmentState::Finished);
        assert_eq!(snap[0].elapsed_ms, 42);
        assert_eq!(snap[1].state, FragmentState::Running);
    }

    #[tokio::test]
    async fn update_fragment_unknown_is_noop() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        tracker.start(id, "bob", None, "SELECT 1", "s2", None, vec![], None);
        tracker.update_fragment(&id, "nonexistent", FragmentState::Failed, 0, 0);
        // Should not panic
    }

    #[tokio::test]
    async fn all_fragments_done_returns_none_while_running() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        tracker.start(id, "alice", None, "SELECT *", "s1", None, vec![], None);

        let frags = vec![
            FragmentInfo {
                task_id: "frag-0".into(),
                worker_url: "http://w1:50052".into(),
                state: FragmentState::Running,
                elapsed_ms: 0,
                input_rows: 0,
                output_rows: 0,
            },
            FragmentInfo {
                task_id: "frag-1".into(),
                worker_url: "http://w2:50052".into(),
                state: FragmentState::Running,
                elapsed_ms: 0,
                input_rows: 0,
                output_rows: 0,
            },
        ];
        tracker.set_fragments(&id, frags);

        // Neither fragment done yet
        assert!(tracker.all_fragments_done(&id).is_none());

        // Finish only frag-0
        tracker.update_fragment(&id, "frag-0", FragmentState::Finished, 100, 50);
        assert!(
            tracker.all_fragments_done(&id).is_none(),
            "frag-1 still running"
        );

        // Finish frag-1 too
        tracker.update_fragment(&id, "frag-1", FragmentState::Finished, 500, 200);
        let timings = tracker.all_fragments_done(&id);
        assert!(timings.is_some(), "all done now");
        let timings = timings.unwrap();
        assert_eq!(timings.len(), 2);
        assert_eq!(timings[0].2, 100);
        assert_eq!(timings[1].2, 500);
    }

    #[tokio::test]
    async fn all_fragments_done_returns_none_for_unknown_query() {
        let tracker = QueryTracker::new(&test_config());
        assert!(tracker.all_fragments_done(&Uuid::now_v7()).is_none());
    }

    #[tokio::test]
    async fn all_fragments_done_returns_none_for_empty_fragments() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        tracker.start(id, "alice", None, "SELECT 1", "s1", None, vec![], None);
        // No fragments set — treated as "not a distributed query"
        assert!(tracker.all_fragments_done(&id).is_none());
    }

    #[tokio::test]
    async fn all_fragments_done_works_with_failed_fragments() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        tracker.start(id, "alice", None, "SELECT *", "s1", None, vec![], None);

        let frags = vec![
            FragmentInfo {
                task_id: "frag-0".into(),
                worker_url: "http://w1:50052".into(),
                state: FragmentState::Running,
                elapsed_ms: 0,
                input_rows: 0,
                output_rows: 0,
            },
            FragmentInfo {
                task_id: "frag-1".into(),
                worker_url: "http://w2:50052".into(),
                state: FragmentState::Running,
                elapsed_ms: 0,
                input_rows: 0,
                output_rows: 0,
            },
        ];
        tracker.set_fragments(&id, frags);

        tracker.update_fragment(&id, "frag-0", FragmentState::Finished, 200, 10);
        tracker.update_fragment(&id, "frag-1", FragmentState::Failed, 50, 0);
        // Both are terminal (Finished / Failed) — should be "done"
        assert!(tracker.all_fragments_done(&id).is_some());
    }

    /// Finding 403: an in-flight record must survive `ttl_secs`. The old
    /// single moka map evicted a `Running` record with no state change after
    /// the TTL, so `owner_of` returned `None` and cancel reported "not found"
    /// for exactly the long queries that need cancelling.
    #[test]
    fn principal_of_is_the_stable_id_not_the_display_user() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        tracker.start_with_principal(
            id, "alice", "sub-1", None, "SELECT 1", "s1", None, vec![], None,
        );
        assert_eq!(tracker.owner_of(&id).as_deref(), Some("alice"));
        assert_eq!(tracker.principal_of(&id).as_deref(), Some("sub-1"));

        let plain = Uuid::now_v7();
        tracker.start(plain, "bob", None, "SELECT 1", "s2", None, vec![], None);
        assert_eq!(tracker.principal_of(&plain).as_deref(), Some("bob"));
    }

    #[tokio::test]
    async fn running_record_outlives_history_ttl() {
        let config = QueryHistoryConfig {
            max_entries: 100,
            ttl_secs: 1,
        };
        let tracker = QueryTracker::new(&config);
        let id = Uuid::now_v7();
        let token = tracker.start(id, "alice", None, "SELECT 1", "s1", None, vec![], None);
        tracker.running(&id, 1);

        // Well past the TTL; flush moka's deferred eviction too.
        tokio::time::sleep(std::time::Duration::from_millis(1300)).await;
        tracker.history.run_pending_tasks();

        assert_eq!(tracker.owner_of(&id).as_deref(), Some("alice"));
        assert_eq!(tracker.get(&id).unwrap().state, QueryState::Running);
        assert!(tracker.records().iter().any(|r| r.query_id == id));
        assert!(tracker.cancel(&id), "cancel must find the in-flight record");
        assert!(token.is_cancelled());
        assert_eq!(tracker.get(&id).unwrap().state, QueryState::Canceled);
        assert_eq!(tracker.active_count(), 0);
    }

    /// Terminal transitions move the record out of `active`; a late
    /// finalizer running after an explicit cancel updates the history copy
    /// instead of resurrecting the query.
    #[tokio::test]
    async fn terminal_state_moves_record_to_history() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        tracker.start(id, "bob", None, "SELECT 1", "s", None, vec![], None);
        assert!(
            tracker.history.get(&id).is_none(),
            "in-flight is not history"
        );
        assert!(tracker.cancel(&id));
        assert!(tracker.history.get(&id).is_some());
        assert_eq!(tracker.active_count(), 0);
        // Late finalizer after cancel: no-op on active, in-place on history.
        tracker.canceled(&id);
        tracker.set_profile(&id, "p".to_string());
        assert_eq!(
            tracker.history.get(&id).unwrap().profile.as_deref(),
            Some("p")
        );
        assert_eq!(tracker.records().len(), 1);
    }

    #[tokio::test]
    async fn history_cache_eviction() {
        // Create more queries than max_entries; verify old ones are evicted.
        let config = QueryHistoryConfig {
            max_entries: 5,
            ttl_secs: 60,
        };
        let tracker = QueryTracker::new(&config);

        let mut ids = Vec::new();
        for i in 0..10 {
            let id = Uuid::now_v7();
            tracker.start(
                id,
                &format!("user{i}"),
                None,
                "SELECT 1",
                "s",
                None,
                vec![],
                None,
            );
            tracker.complete(&id, 0, 0, vec![], 0, 0, 0, 0);
            ids.push(id);
        }

        // moka uses async eviction — run pending tasks to trigger it
        tracker.history.run_pending_tasks();

        let records = tracker.records();
        // With max_capacity=5, the cache should have evicted some entries
        assert!(
            records.len() <= 5,
            "cache should evict old entries: found {} records, expected <= 5",
            records.len()
        );
    }

    /// Captures everything a `tracing` subscriber writes while `f` runs.
    fn capture_log(f: impl FnOnce()) -> String {
        use std::io::Write;
        use tracing_subscriber::fmt::MakeWriter;

        #[derive(Clone, Default)]
        struct Buf(Arc<Mutex<Vec<u8>>>);
        impl Write for Buf {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> MakeWriter<'a> for Buf {
            type Writer = Buf;
            fn make_writer(&'a self) -> Buf {
                self.clone()
            }
        }

        let buf = Buf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        let bytes = buf.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    // Issue #668: a failed query used to leave only "Executing query" in the
    // server log. Every failure path ends in `failed`, so this is the one
    // event that guarantees an operator can grep the query_id and see why.
    #[test]
    fn failed_writes_one_query_failed_line_with_query_id() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        let log = capture_log(|| {
            tracker.start(id, "carol", None, "SELECT * FROM nope", "s3", None, vec![], None);
            let err = sqe_core::SqeError::Execution("table 'nope' not found in catalog".to_string());
            assert!(err.error_code().is_user_error());
            tracker.failed(&id, &err);
        });
        let failed_lines: Vec<&str> = log.lines().filter(|l| l.contains("Query failed")).collect();
        assert_eq!(failed_lines.len(), 1, "log was:\n{log}");
        let line = failed_lines[0];
        assert!(line.contains(&id.to_string()), "{line}");
        assert!(line.contains("WARN"), "user errors are WARN: {line}");
        assert!(line.contains("error_code=\"TABLE_NOT_FOUND\""), "{line}");
        assert!(line.contains("table 'nope' not found"), "{line}");
        assert!(line.contains("elapsed_ms="), "{line}");
        assert_eq!(tracker.get(&id).unwrap().state, QueryState::Failed);
    }

    #[test]
    fn failed_internal_error_is_logged_at_error_level() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        let log = capture_log(|| {
            tracker.start(id, "dave", None, "SELECT 1", "s4", None, vec![], None);
            let err = sqe_core::SqeError::Internal(anyhow::anyhow!("worker vanished"));
            assert!(!err.error_code().is_user_error());
            tracker.failed(&id, &err);
        });
        let line = log
            .lines()
            .find(|l| l.contains("Query failed"))
            .unwrap_or_else(|| panic!("no Query failed line in:\n{log}"));
        assert!(line.contains("ERROR"), "{line}");
        assert!(line.contains("worker vanished"), "{line}");
    }

    #[test]
    fn complete_writes_query_completed_with_elapsed() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        let log = capture_log(|| {
            tracker.start(id, "erin", None, "SELECT 1", "s5", None, vec![], None);
            tracker.complete(&id, 7, 12, vec![], 0, 0, 0, 0);
        });
        let line = log
            .lines()
            .find(|l| l.contains("Query completed"))
            .unwrap_or_else(|| panic!("no Query completed line in:\n{log}"));
        assert!(line.contains(&id.to_string()), "{line}");
        assert!(line.contains("rows=7"), "{line}");
        assert!(line.contains("elapsed_ms="), "{line}");
    }
}
