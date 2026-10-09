//! Lineage observer trait + ChannelObserver implementation.
//!
//! See `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md` §3.2-3.3.

use chrono::{DateTime, Utc};
use datafusion::logical_expr::LogicalPlan;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

/// Lineage identifies the user without retaining session credentials.
#[derive(Clone, Debug)]
pub struct UserCtx {
    pub username: String,
}

pub enum LineageHint {
    DdlSchema {
        catalog: String,
        schema: String,
        table: String,
        columns: Vec<(String, String)>,
    },
    /// A `CALL system.<proc>(target)` maintenance procedure. The target table
    /// is both the input and output dataset (the procedure reads and rewrites
    /// its own metadata/data), so lineage records it on both sides. No column
    /// lineage: maintenance does not transform column values.
    MaintenanceTable {
        catalog: String,
        schema: String,
        table: String,
    },
    /// A statement that changes a table's lifecycle without a source plan:
    /// `DROP TABLE` (`change = "DROP"`) or a plain `CREATE TABLE`
    /// (`"CREATE"`). The table is the one output dataset and carries the OL
    /// `lifecycleStateChange` facet; there is no schema or column lineage.
    /// Values are the [`crate::event::lifecycle`] constants.
    TableLifecycle {
        catalog: String,
        schema: String,
        table: String,
        change: String,
    },
}

/// The session scope a statement was planned in: the catalog and schema
/// DataFusion resolved unqualified and 2-part table names against. Lineage
/// uses it to name a scan of `customers` or `sales.customers` after the
/// catalog the table actually lives in, instead of a literal `default`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LineageScope {
    pub catalog: String,
    pub schema: String,
}

impl Default for LineageScope {
    /// `default.default`, the fallback this crate used before the scope was
    /// threaded through (and still the DataFusion default).
    fn default() -> Self {
        Self {
            catalog: "default".into(),
            schema: "default".into(),
        }
    }
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
    /// Masked and restricted scan columns as the policy enforcer recorded
    /// them, `(table, column)` with `table` the scan's `TableReference`
    /// display; column lineage reports them as `MASKED` (review 610). Empty
    /// when no policy engine is wired or nothing was masked.
    pub masked_fields: Vec<(String, String)>,
    /// Scope unqualified table names resolved in.
    pub scope: LineageScope,
    /// OL lifecycle change the statement made to its output dataset(s), e.g.
    /// `CREATE` for a CTAS. `None` for plain writes.
    pub output_lifecycle: Option<String>,
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
    /// Masked and restricted scan columns as the policy enforcer recorded
    /// them, `(table, column)` with `table` the scan's `TableReference`
    /// display; column lineage reports them as `MASKED` (review 610). Empty
    /// when no policy engine is wired or nothing was masked.
    pub masked_fields: Vec<(String, String)>,
    /// Scope unqualified table names resolved in.
    pub scope: LineageScope,
    /// OL lifecycle change the statement attempted on its output dataset(s).
    pub output_lifecycle: Option<String>,
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
            },
            session_id: "sess-1".into(),
            started_at: Utc::now(),
            ended_at: Utc::now(),
            duration: Duration::from_millis(1),
            statement_kind: "query".into(),
            rows_returned: 0,
            plan: None,
            masked_fields: Vec::new(),
            scope: LineageScope::default(),
            output_lifecycle: None,
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
            },
            session_id: "sess-1".into(),
            started_at: Utc::now(),
            ended_at: Utc::now(),
            duration: Duration::from_millis(1),
            statement_kind: "query".into(),
            error_message: "boom".into(),
            plan: None,
            masked_fields: Vec::new(),
            scope: LineageScope::default(),
            output_lifecycle: None,
        }
    }
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

impl ChannelObserver {
    pub fn on_query_start(&self, ctx: QueryStartCtx) {
        self.try_send(LineageMsg::Start(ctx));
    }
    pub fn on_query_complete(&self, ctx: QueryCompleteCtx) {
        self.try_send(LineageMsg::Complete(ctx));
    }
    pub fn on_query_fail(&self, ctx: QueryFailCtx) {
        self.try_send(LineageMsg::Fail(ctx));
    }
}
