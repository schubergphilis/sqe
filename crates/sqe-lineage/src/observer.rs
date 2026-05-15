//! Lineage observer trait + ChannelObserver implementation.
//!
//! See `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md` §3.2-3.3.

use chrono::{DateTime, Utc};
use datafusion::logical_expr::LogicalPlan;
use sqe_core::SecretString;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct UserCtx {
    pub username: String,
    pub bearer: Option<SecretString>,
}

pub enum LineageHint {
    DdlSchema {
        catalog: String,
        schema: String,
        table: String,
        columns: Vec<(String, String)>,
    },
}

pub enum PlanOrHint {
    Plan(Box<LogicalPlan>),
    Hint(LineageHint),
}

pub struct QueryStartCtx {
    pub run_id: Uuid,
    pub job_namespace: String,
    pub sql: String,
    pub user: UserCtx,
    pub session_id: String,
    pub started_at: DateTime<Utc>,
    pub statement_kind: String,
}

pub struct QueryCompleteCtx {
    pub run_id: Uuid,
    pub job_namespace: String,
    pub sql: String,
    pub user: UserCtx,
    pub session_id: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub duration: Duration,
    pub statement_kind: String,
    pub rows_returned: usize,
    pub plan: Option<PlanOrHint>,
}

pub struct QueryFailCtx {
    pub run_id: Uuid,
    pub job_namespace: String,
    pub sql: String,
    pub user: UserCtx,
    pub session_id: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub duration: Duration,
    pub statement_kind: String,
    pub error_message: String,
    pub plan: Option<PlanOrHint>,
}

/// Test helper. Constructs a minimally-valid context for unit tests in this
/// crate and downstream crates (e.g. `sqe-coordinator`).
impl QueryStartCtx {
    pub fn dummy() -> Self {
        Self {
            run_id: Uuid::nil(),
            job_namespace: "sqe".into(),
            sql: "SELECT 1".into(),
            user: UserCtx {
                username: "test".into(),
                bearer: None,
            },
            session_id: "sess-1".into(),
            started_at: Utc::now(),
            statement_kind: "query".into(),
        }
    }
}

/// Test helper. Constructs a minimally-valid context for unit tests in this
/// crate and downstream crates (e.g. `sqe-coordinator`).
impl QueryCompleteCtx {
    pub fn dummy() -> Self {
        Self {
            run_id: Uuid::nil(),
            job_namespace: "sqe".into(),
            sql: "SELECT 1".into(),
            user: UserCtx {
                username: "test".into(),
                bearer: None,
            },
            session_id: "sess-1".into(),
            started_at: Utc::now(),
            ended_at: Utc::now(),
            duration: Duration::from_millis(1),
            statement_kind: "query".into(),
            rows_returned: 0,
            plan: None,
        }
    }
}

/// Test helper. Constructs a minimally-valid context for unit tests in this
/// crate and downstream crates (e.g. `sqe-coordinator`).
impl QueryFailCtx {
    pub fn dummy() -> Self {
        Self {
            run_id: Uuid::nil(),
            job_namespace: "sqe".into(),
            sql: "SELECT 1".into(),
            user: UserCtx {
                username: "test".into(),
                bearer: None,
            },
            session_id: "sess-1".into(),
            started_at: Utc::now(),
            ended_at: Utc::now(),
            duration: Duration::from_millis(1),
            statement_kind: "query".into(),
            error_message: "boom".into(),
            plan: None,
        }
    }
}

pub trait LineageObserver: Send + Sync {
    fn on_query_start(&self, ctx: QueryStartCtx);
    fn on_query_complete(&self, ctx: QueryCompleteCtx);
    fn on_query_fail(&self, ctx: QueryFailCtx);
}

pub enum LineageMsg {
    Start(QueryStartCtx),
    Complete(QueryCompleteCtx),
    Fail(QueryFailCtx),
}

pub struct ChannelObserver {
    tx: mpsc::Sender<LineageMsg>,
    dropped: prometheus::IntCounter,
}

impl ChannelObserver {
    pub fn new(tx: mpsc::Sender<LineageMsg>, dropped: prometheus::IntCounter) -> Self {
        Self { tx, dropped }
    }

    fn try_send(&self, msg: LineageMsg) {
        if self.tx.try_send(msg).is_err() {
            self.dropped.inc();
            tracing::warn!("sqe-lineage channel full; dropping event");
        }
    }
}

impl LineageObserver for ChannelObserver {
    fn on_query_start(&self, ctx: QueryStartCtx) {
        self.try_send(LineageMsg::Start(ctx));
    }
    fn on_query_complete(&self, ctx: QueryCompleteCtx) {
        self.try_send(LineageMsg::Complete(ctx));
    }
    fn on_query_fail(&self, ctx: QueryFailCtx) {
        self.try_send(LineageMsg::Fail(ctx));
    }
}
