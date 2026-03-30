use std::sync::Arc;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use moka::sync::Cache;
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
    pub error_type: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub tables_touched: Vec<String>,
    pub fragments: Vec<FragmentInfo>,
}

pub struct QueryTracker {
    history: Cache<Uuid, Arc<QueryRecord>>,
    active: DashMap<Uuid, CancellationToken>,
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
    ) -> CancellationToken {
        let token = CancellationToken::new();
        let record = QueryRecord {
            query_id,
            state: QueryState::Queued,
            user: user.to_string(),
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
            error_type: None,
            error_code: None,
            error_message: None,
            tables_touched: Vec::new(),
            fragments: Vec::new(),
        };
        self.history.insert(query_id, Arc::new(record));
        self.active.insert(query_id, token.clone());
        token
    }

    pub fn running(&self, query_id: &Uuid, planning_ms: u64) {
        if let Some(old) = self.history.get(query_id) {
            let mut record = (*old).clone();
            let now = Utc::now();
            record.state = QueryState::Running;
            record.started = Some(now);
            let elapsed = (now - record.created).num_milliseconds();
            record.queued_ms = elapsed.max(0) as u64;
            record.planning_ms = planning_ms;
            self.history.insert(*query_id, Arc::new(record));
        }
    }

    pub fn complete(
        &self,
        query_id: &Uuid,
        rows: usize,
        execution_ms: u64,
        tables_touched: Vec<String>,
    ) {
        if let Some(old) = self.history.get(query_id) {
            let mut record = (*old).clone();
            record.state = QueryState::Finished;
            record.ended = Some(Utc::now());
            record.output_rows = rows;
            record.execution_ms = execution_ms;
            record.tables_touched = tables_touched;
            self.history.insert(*query_id, Arc::new(record));
        }
        self.active.remove(query_id);
    }

    pub fn failed(&self, query_id: &Uuid, error: &sqe_core::SqeError) {
        if let Some(old) = self.history.get(query_id) {
            let mut record = (*old).clone();
            let code = error.error_code();
            record.state = QueryState::Failed;
            record.ended = Some(Utc::now());
            record.error_type = Some(code.trino_error_type().to_string());
            record.error_code = Some(code.name().to_string());
            record.error_message = Some(error.client_message());
            self.history.insert(*query_id, Arc::new(record));
        }
        self.active.remove(query_id);
    }

    pub fn canceled(&self, query_id: &Uuid) {
        if let Some(old) = self.history.get(query_id) {
            let mut record = (*old).clone();
            record.state = QueryState::Canceled;
            record.ended = Some(Utc::now());
            self.history.insert(*query_id, Arc::new(record));
        }
        self.active.remove(query_id);
    }

    pub fn cancel(&self, query_id: &Uuid) -> bool {
        if let Some((_, token)) = self.active.remove(query_id) {
            token.cancel();
            self.canceled(query_id);
            true
        } else {
            false
        }
    }

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

    pub fn records(&self) -> Vec<Arc<QueryRecord>> {
        let mut records = Vec::new();
        for (_, v) in &self.history {
            records.push(v);
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
        QueryHistoryConfig { max_entries: 100, ttl_secs: 60 }
    }

    #[tokio::test]
    async fn start_creates_queued_record() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        let _token = tracker.start(id, "alice", Some("cli"), "SELECT 1", "s1", None, vec![]);
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
        tracker.start(id, "bob", None, "SELECT *", "s2", None, vec![]);
        tracker.running(&id, 10);
        tracker.complete(&id, 42, 150, vec!["ns.table1".to_string()]);
        let rec = tracker.history.get(&id).unwrap();
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
        tracker.start(id, "carol", None, "BAD SQL", "s3", None, vec![]);
        tracker.running(&id, 0);
        let err = sqe_core::SqeError::Execution("syntax error near BAD".to_string());
        tracker.failed(&id, &err);
        let rec = tracker.history.get(&id).unwrap();
        assert_eq!(rec.state, QueryState::Failed);
        assert!(rec.error_type.is_some());
        assert!(rec.error_code.is_some());
        assert!(rec.error_message.is_some());
        assert_eq!(tracker.active_count(), 0);
    }

    #[tokio::test]
    async fn cancel_fires_token_and_records() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        let token = tracker.start(id, "dave", None, "SELECT 1", "s4", None, vec![]);
        assert!(!token.is_cancelled());
        let cancelled = tracker.cancel(&id);
        assert!(cancelled);
        assert!(token.is_cancelled());
        let rec = tracker.history.get(&id).unwrap();
        assert_eq!(rec.state, QueryState::Canceled);
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
            tracker.start(id, &format!("user{i}"), None, "SELECT 1", "s", None, vec![]);
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
                tracker.start(id, &user, None, &sql, "session", None, vec![]);
                tracker.running(&id, (i * 5) as u64);
                // Simulate some work
                tokio::task::yield_now().await;
                tracker.complete(&id, i as usize, (i * 10) as u64, vec![format!("table{i}")]);
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
                tracker.start(id, "user", None, "SELECT 1", "s", None, vec![]);
                tracker.running(&id, 5);
                tokio::task::yield_now().await;

                match i % 3 {
                    0 => tracker.complete(&id, 10, 100, vec![]),
                    1 => tracker.failed(&id, &sqe_core::SqeError::Execution("test error".to_string())),
                    _ => { tracker.cancel(&id); }
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
            let rec = tracker.history.get(id).unwrap();
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

        tracker.update_fragment(&id, "frag-0", FragmentState::Finished, 42, 100);
        let rec = tracker.history.get(&id).unwrap();
        assert_eq!(rec.fragments[0].state, FragmentState::Finished);
        assert_eq!(rec.fragments[0].elapsed_ms, 42);
        assert_eq!(rec.fragments[1].state, FragmentState::Running);
    }

    #[tokio::test]
    async fn update_fragment_unknown_is_noop() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        tracker.start(id, "bob", None, "SELECT 1", "s2", None, vec![]);
        tracker.update_fragment(&id, "nonexistent", FragmentState::Failed, 0, 0);
        // Should not panic
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
            tracker.start(id, &format!("user{i}"), None, "SELECT 1", "s", None, vec![]);
            tracker.complete(&id, 0, 0, vec![]);
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
}
