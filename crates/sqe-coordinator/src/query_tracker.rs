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
    pub tables_touched: Vec<String>,
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
            tables_touched: Vec::new(),
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

    pub fn failed(&self, query_id: &Uuid, error_type: &str, error_code: Option<&str>) {
        if let Some(old) = self.history.get(query_id) {
            let mut record = (*old).clone();
            record.state = QueryState::Failed;
            record.ended = Some(Utc::now());
            record.error_type = Some(error_type.to_string());
            record.error_code = error_code.map(|s| s.to_string());
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
        tracker.failed(&id, "SyntaxError", Some("42000"));
        let rec = tracker.history.get(&id).unwrap();
        assert_eq!(rec.state, QueryState::Failed);
        assert_eq!(rec.error_type.as_deref(), Some("SyntaxError"));
        assert_eq!(rec.error_code.as_deref(), Some("42000"));
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
}
