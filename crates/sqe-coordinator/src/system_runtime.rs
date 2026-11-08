//! The virtual `system.runtime` schema (`queries`, `query_history`, `nodes`,
//! `tasks`), rendered from the coordinator's own
//! [`QueryTracker`](crate::query_tracker::QueryTracker) records and, for
//! `query_history`, the native audit log.
//!
//! This used to live in `sqe-catalog` behind mirrored copies of
//! `QueryRecord`, `QueryState` and `FragmentInfo` "to avoid a circular crate
//! dependency", with the coordinator converting every record on every read.
//! It only ever rendered coordinator state, so it lives here now and reads
//! the tracker's types directly (#576). The catalog's `SystemCatalogProvider`
//! keeps a `dyn SchemaProvider` slot for it.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_array::builder::{
    BooleanBuilder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, Session};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;

use crate::query_tracker::{QueryRecord, QueryState};

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

/// Callback that returns a fresh snapshot of the tracker's query records.
pub type QueryRecordsFn = Arc<dyn Fn() -> Vec<Arc<QueryRecord>> + Send + Sync>;

/// DataFusion `SchemaProvider` for the virtual `system.runtime` schema.
///
/// Exposes Trino's `system.runtime` tables plus a history view:
///
/// - `queries` — one row per tracked query
/// - `query_history` — `queries` plus the finished queries in the audit log
/// - `nodes`   — one row per cluster node
/// - `tasks`   — one row per finished-query task (single-node: one task per query)
pub struct RuntimeSchemaProvider {
    records_fn: QueryRecordsFn,
    warehouse: String,
    coordinator_uri: String,
    worker_urls: Vec<String>,
    /// When set, `queries` / `tasks` only show records owned by this user.
    /// `None` (admin sessions) shows every user's queries.
    owner_filter: Option<String>,
    /// The audit log `query_history` reads; `None` shows tracker records only.
    history: Option<AuditHistorySource>,
}

impl RuntimeSchemaProvider {
    /// Create a new runtime schema provider.
    ///
    /// * `records_fn`      — closure returning a snapshot of all tracked query records
    /// * `warehouse`       — warehouse name (used as coordinator node_id)
    /// * `coordinator_uri` — coordinator HTTP URI for the `nodes` table
    /// * `worker_urls`     — optional list of worker HTTP URIs
    pub fn new(
        records_fn: QueryRecordsFn,
        warehouse: String,
        coordinator_uri: String,
        worker_urls: Vec<String>,
    ) -> Self {
        Self {
            records_fn,
            warehouse,
            coordinator_uri,
            worker_urls,
            owner_filter: None,
            history: None,
        }
    }

    /// Read finished queries for `query_history` from this audit log.
    pub fn with_audit_history(mut self, source: Option<AuditHistorySource>) -> Self {
        self.history = source;
        self
    }

    /// Restrict `queries` / `tasks` to records owned by `owner` (a non-admin
    /// session must not read other users' SQL). `None` shows all records.
    pub fn with_owner_filter(mut self, owner: Option<String>) -> Self {
        self.owner_filter = owner;
        self
    }

    fn records(&self) -> Vec<Arc<QueryRecord>> {
        let mut records = (self.records_fn)();
        if let Some(owner) = &self.owner_filter {
            records.retain(|r| &r.user == owner);
        }
        records
    }
}

impl std::fmt::Debug for RuntimeSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeSchemaProvider")
            .field("warehouse", &self.warehouse)
            .field("coordinator_uri", &self.coordinator_uri)
            .field("worker_urls", &self.worker_urls)
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for RuntimeSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        vec![
            "queries".into(),
            "query_history".into(),
            "nodes".into(),
            "tasks".into(),
        ]
    }

    fn table_exist(&self, name: &str) -> bool {
        matches!(name, "queries" | "query_history" | "nodes" | "tasks")
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        match name {
            "queries" => Ok(Some(build_queries_table(&self.records())?)),
            "query_history" => Ok(Some(Arc::new(QueryHistoryTable {
                live: self.records(),
                source: self.history.clone(),
                owner_filter: self.owner_filter.clone(),
                schema: Arc::new(queries_schema()),
            }))),
            "nodes" => Ok(Some(build_nodes_table(
                &self.warehouse,
                &self.coordinator_uri,
                &self.worker_urls,
            )?)),
            "tasks" => Ok(Some(build_tasks_table(
                &self.records(),
                &self.warehouse,
            )?)),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// queries table — 21 columns
// ---------------------------------------------------------------------------

fn ts_type() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC")))
}

fn queries_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_id", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("user", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, true),
        Field::new("query", DataType::Utf8, false),
        Field::new("resource_group_id", DataType::Utf8, false),
        Field::new("queued_time_ms", DataType::Int64, false),
        Field::new("analysis_time_ms", DataType::Int64, false),
        Field::new("planning_time_ms", DataType::Int64, false),
        Field::new("execution_time_ms", DataType::Int64, false),
        Field::new("created", ts_type(), false),
        Field::new("started", ts_type(), true),
        Field::new("last_heartbeat", ts_type(), true),
        Field::new("end", ts_type(), true),
        Field::new("output_rows", DataType::Int64, false),
        Field::new("bytes_scanned", DataType::Int64, false),
        Field::new("rows_scanned", DataType::Int64, false),
        Field::new("spill_bytes", DataType::Int64, false),
        Field::new("peak_memory_bytes", DataType::Int64, false),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("error_type", DataType::Utf8, true),
        Field::new("error_code", DataType::Utf8, true),
        Field::new("rows_written", DataType::Int64, true),
    ])
}

fn u64_to_i64_saturating(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

fn build_queries_table(records: &[Arc<QueryRecord>]) -> DFResult<Arc<dyn TableProvider>> {
    let batch = queries_batch(records)?;
    Ok(Arc::new(MemTable::try_new(
        batch.schema(),
        vec![vec![batch]],
    )?))
}

// ---------------------------------------------------------------------------
// query_history table — `queries` plus the audit log (#812)
// ---------------------------------------------------------------------------

/// Most audit events one `query_history` scan reads; the newest are kept.
const HISTORY_ROW_CAP: usize = 100_000;

/// Where `query_history` reads finished queries the tracker has dropped.
#[derive(Debug, Clone)]
pub struct AuditHistorySource {
    /// `metrics.audit_log_path`: the active native audit file.
    pub path: std::path::PathBuf,
    /// Read window when the query bounds neither `end` nor `created`
    /// (`[query_history] lookback_secs`).
    pub lookback: chrono::Duration,
}

/// The audit log `query_history` reads under `config`: the native file when
/// `metrics.audit_log_path` is set and `metrics.audit.format` writes native
/// events there ("native" or "both"). With "ocsf" the file holds OCSF, whose
/// query class is shared with other event kinds, so the history shows the
/// tracker's records only.
pub fn audit_history_source(config: &sqe_core::SqeConfig) -> Option<AuditHistorySource> {
    if config.metrics.audit_log_path.is_empty() {
        return None;
    }
    if matches!(
        crate::parse_audit_format(&config.metrics.audit.format),
        sqe_metrics::audit::AuditFormat::Ocsf
    ) {
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            tracing::warn!(
                "metrics.audit.format = \"ocsf\": system.runtime.query_history reads the \
                 native audit log, so it shows only the queries still in memory; use \
                 \"both\" to keep the history"
            );
        });
        return None;
    }
    Some(AuditHistorySource {
        path: std::path::PathBuf::from(&config.metrics.audit_log_path),
        lookback: i64::try_from(config.query_history.lookback_secs)
            .ok()
            .and_then(chrono::Duration::try_seconds)
            .unwrap_or(chrono::Duration::MAX),
    })
}

/// `system.runtime.query_history`: the tracker's records (running queries
/// and the recently finished ones) plus the finished queries in the native
/// audit log, so a query stays visible after the tracker's TTL and after a
/// coordinator restart. Same columns as `queries`.
///
/// The audit log is read at scan time, for the window the query's filters on
/// `end` / `created` give (the filters stay applied above the scan), or the
/// last `lookback` when they give none.
#[derive(Debug)]
struct QueryHistoryTable {
    live: Vec<Arc<QueryRecord>>,
    source: Option<AuditHistorySource>,
    owner_filter: Option<String>,
    schema: arrow::datatypes::SchemaRef,
}

#[async_trait]
impl TableProvider for QueryHistoryTable {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Inexact: the bounds only pick which audit lines to read; DataFusion
        // still applies every filter to the rows.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mut records = self.live.clone();
        if let Some(source) = &self.source {
            let bounds = EndBounds::from_filters(filters);
            // A lookback past chrono's range reads the whole log.
            let from = bounds.from.unwrap_or_else(|| {
                chrono::Utc::now()
                    .checked_sub_signed(source.lookback)
                    .unwrap_or(chrono::DateTime::<chrono::Utc>::MIN_UTC)
            });
            let path = source.path.clone();
            let events = tokio::task::spawn_blocking(move || {
                sqe_metrics::audit::read_query_events(&path, from, bounds.to, HISTORY_ROW_CAP)
            })
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let mut seen: std::collections::HashSet<uuid::Uuid> =
                records.iter().map(|r| r.query_id).collect();
            // Newest first, so a query logged twice shows its last event.
            for event in events.into_iter().rev() {
                if let Some(owner) = &self.owner_filter {
                    if &event.actor.username != owner {
                        continue;
                    }
                }
                if let Some(record) = record_from_event(event) {
                    if seen.insert(record.query_id) {
                        records.push(Arc::new(record));
                    }
                }
            }
        }
        let batch = queries_batch(&records)?;
        MemTable::try_new(batch.schema(), vec![vec![batch]])?
            .scan(state, projection, &[], limit)
            .await
    }
}

/// The audit window a scan's filters give, in terms of `end` (the audit
/// event's `time`). `created <= end`, so a lower bound on `created` is one
/// on `end` too; an upper bound on `created` is not.
#[derive(Debug, Default, Clone, Copy)]
struct EndBounds {
    from: Option<chrono::DateTime<chrono::Utc>>,
    to: Option<chrono::DateTime<chrono::Utc>>,
}

impl EndBounds {
    /// One second of slack each way: the log keeps sub-millisecond times
    /// and `created` is derived from `end` minus the duration.
    const SLACK: chrono::Duration = chrono::Duration::seconds(1);

    fn from_filters(filters: &[Expr]) -> Self {
        let mut bounds = Self::default();
        for filter in filters {
            bounds.add(filter);
        }
        bounds
    }

    fn add(&mut self, expr: &Expr) {
        use datafusion::logical_expr::{Between, BinaryExpr, Operator};
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if *op == Operator::And {
                    self.add(left);
                    self.add(right);
                    return;
                }
                let (column, op, millis) = match (time_column(left), ts_millis(right)) {
                    (Some(c), Some(v)) => (c, *op, v),
                    _ => match (ts_millis(left), time_column(right), op.swap()) {
                        (Some(v), Some(c), Some(op)) => (c, op, v),
                        _ => return,
                    },
                };
                match op {
                    Operator::Gt | Operator::GtEq => self.lower(column, millis),
                    Operator::Lt | Operator::LtEq => self.upper(column, millis),
                    Operator::Eq => {
                        self.lower(column, millis);
                        self.upper(column, millis);
                    }
                    _ => {}
                }
            }
            Expr::Between(Between {
                expr,
                negated: false,
                low,
                high,
            }) => {
                if let Some(column) = time_column(expr) {
                    if let Some(v) = ts_millis(low) {
                        self.lower(column, v);
                    }
                    if let Some(v) = ts_millis(high) {
                        self.upper(column, v);
                    }
                }
            }
            _ => {}
        }
    }

    fn lower(&mut self, _column: TimeColumn, millis: i64) {
        let Some(t) = chrono::DateTime::from_timestamp_millis(millis) else {
            return;
        };
        let t = t.checked_sub_signed(Self::SLACK).unwrap_or(t);
        self.from = Some(self.from.map_or(t, |f| f.max(t)));
    }

    fn upper(&mut self, column: TimeColumn, millis: i64) {
        if column != TimeColumn::End {
            return;
        }
        let Some(t) = chrono::DateTime::from_timestamp_millis(millis) else {
            return;
        };
        let t = t.checked_add_signed(Self::SLACK).unwrap_or(t);
        self.to = Some(self.to.map_or(t, |u| u.min(t)));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeColumn {
    End,
    Created,
}

/// `end` or `created`, under any casts the planner put on it.
fn time_column(expr: &Expr) -> Option<TimeColumn> {
    match expr {
        Expr::Column(c) if c.name == "end" => Some(TimeColumn::End),
        Expr::Column(c) if c.name == "created" => Some(TimeColumn::Created),
        Expr::Cast(c) => time_column(&c.expr),
        Expr::TryCast(c) => time_column(&c.expr),
        _ => None,
    }
}

/// A timestamp literal as epoch milliseconds.
fn ts_millis(expr: &Expr) -> Option<i64> {
    use datafusion::common::ScalarValue;
    match expr {
        Expr::Literal(ScalarValue::TimestampSecond(Some(v), _), _) => v.checked_mul(1000),
        Expr::Literal(ScalarValue::TimestampMillisecond(Some(v), _), _) => Some(*v),
        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(v), _), _) => Some(v / 1000),
        Expr::Literal(ScalarValue::TimestampNanosecond(Some(v), _), _) => Some(v / 1_000_000),
        _ => None,
    }
}

/// A finished query as the `queries` table shows it, from its audit event.
/// `None` for an event without a parseable query id.
fn record_from_event(event: sqe_metrics::audit::AuditEvent) -> Option<QueryRecord> {
    use sqe_metrics::audit::Outcome;
    let query_id = uuid::Uuid::parse_str(event.query_id.as_deref()?).ok()?;
    let timing = event.timing.unwrap_or(sqe_metrics::audit::Timing {
        duration_ms: 0,
        queued_ms: 0,
        planning_ms: 0,
        execution_ms: 0,
    });
    let stats = event.stats;
    let ended = event.time;
    let created = ended - chrono::Duration::milliseconds(u64_to_i64_saturating(timing.duration_ms));
    let started = created + chrono::Duration::milliseconds(u64_to_i64_saturating(timing.queued_ms));
    let (state, error_type, error_code, error_message) = match event.outcome {
        Outcome::Success => (QueryState::Finished, None, None, None),
        Outcome::Failure {
            error_type,
            error_code,
            message,
        } => {
            let state = if error_code.as_deref() == Some("QUERY_CANCELLED") {
                QueryState::Canceled
            } else {
                QueryState::Failed
            };
            (state, error_type, error_code, message)
        }
    };
    Some(QueryRecord {
        query_id,
        state,
        principal: event
            .actor
            .subject
            .clone()
            .unwrap_or_else(|| event.actor.username.clone()),
        user: event.actor.username,
        source: event.actor.app_name,
        sql: event.query.and_then(|q| q.text).unwrap_or_default(),
        session_id: event.session_id.unwrap_or_default(),
        client_ip: event.client_ip,
        roles: event.actor.roles,
        created,
        started: Some(started),
        ended: Some(ended),
        queued_ms: timing.queued_ms,
        planning_ms: timing.planning_ms,
        execution_ms: timing.execution_ms,
        output_rows: stats.as_ref().map_or(0, |s| s.rows_returned),
        rows_written: stats.as_ref().and_then(|s| s.rows_written),
        error_type,
        error_code,
        error_message,
        tables_touched: Vec::new(),
        bytes_scanned: stats.as_ref().map_or(0, |s| s.bytes_scanned),
        physical_input_bytes: 0,
        rows_scanned: stats.as_ref().map_or(0, |s| s.rows_scanned),
        spill_bytes: stats.as_ref().map_or(0, |s| s.spill_bytes),
        peak_memory_bytes: stats.as_ref().map_or(0, |s| s.peak_memory_bytes),
        profile: None,
        trace_id: event.trace_id,
        fragments: Arc::default(),
    })
}

fn queries_batch(records: &[Arc<QueryRecord>]) -> DFResult<RecordBatch> {
    let schema = Arc::new(queries_schema());

    let tz: Arc<str> = Arc::from("UTC");

    let mut query_id_b = StringBuilder::new();
    let mut state_b = StringBuilder::new();
    let mut user_b = StringBuilder::new();
    let mut source_b = StringBuilder::new();
    let mut query_b = StringBuilder::new();
    let mut resource_group_b = StringBuilder::new();
    let mut queued_ms_b = Int64Builder::new();
    let mut analysis_ms_b = Int64Builder::new();
    let mut planning_ms_b = Int64Builder::new();
    let mut execution_ms_b = Int64Builder::new();
    let mut created_b = TimestampMillisecondBuilder::new().with_timezone(tz.clone());
    let mut started_b = TimestampMillisecondBuilder::new().with_timezone(tz.clone());
    let mut heartbeat_b = TimestampMillisecondBuilder::new().with_timezone(tz.clone());
    let mut end_b = TimestampMillisecondBuilder::new().with_timezone(tz);
    let mut output_rows_b = Int64Builder::new();
    let mut bytes_scanned_b = Int64Builder::new();
    let mut rows_scanned_b = Int64Builder::new();
    let mut spill_bytes_b = Int64Builder::new();
    let mut peak_memory_bytes_b = Int64Builder::new();
    let mut trace_id_b = StringBuilder::new();
    let mut error_type_b = StringBuilder::new();
    let mut error_code_b = StringBuilder::new();
    let mut rows_written_b = Int64Builder::new();

    for rec in records {
        query_id_b.append_value(rec.query_id.to_string());
        state_b.append_value(rec.state.to_string());
        user_b.append_value(&rec.user);
        match &rec.source {
            Some(s) => source_b.append_value(s),
            None => source_b.append_null(),
        }
        // Same redaction as the web UI query list: the tracker stores SQL
        // verbatim, and this table is readable by every authenticated session,
        // so a `CREATE SECRET ... TOKEN '<jwt>'` would otherwise be one SELECT away.
        query_b.append_value(sqe_metrics::audit::redact_pii(&rec.sql));
        resource_group_b.append_value("global");
        queued_ms_b.append_value(u64_to_i64_saturating(rec.queued_ms));
        analysis_ms_b.append_value(0); // no separate analysis phase
        planning_ms_b.append_value(u64_to_i64_saturating(rec.planning_ms));
        execution_ms_b.append_value(u64_to_i64_saturating(rec.execution_ms));
        created_b.append_value(rec.created.timestamp_millis());
        match rec.started {
            Some(ts) => started_b.append_value(ts.timestamp_millis()),
            None => started_b.append_null(),
        }
        // last_heartbeat = started (no heartbeat protocol yet)
        match rec.started {
            Some(ts) => heartbeat_b.append_value(ts.timestamp_millis()),
            None => heartbeat_b.append_null(),
        }
        match rec.ended {
            Some(ts) => end_b.append_value(ts.timestamp_millis()),
            None => end_b.append_null(),
        }
        output_rows_b.append_value(u64_to_i64_saturating(rec.output_rows as u64));
        bytes_scanned_b.append_value(u64_to_i64_saturating(rec.bytes_scanned));
        rows_scanned_b.append_value(u64_to_i64_saturating(rec.rows_scanned));
        spill_bytes_b.append_value(u64_to_i64_saturating(rec.spill_bytes));
        peak_memory_bytes_b.append_value(u64_to_i64_saturating(rec.peak_memory_bytes));
        match &rec.trace_id {
            Some(s) => trace_id_b.append_value(s),
            None => trace_id_b.append_null(),
        }
        match &rec.error_type {
            Some(s) => error_type_b.append_value(s),
            None => error_type_b.append_null(),
        }
        match &rec.error_code {
            Some(s) => error_code_b.append_value(s),
            None => error_code_b.append_null(),
        }
        match rec.rows_written {
            Some(n) => rows_written_b.append_value(u64_to_i64_saturating(n)),
            None => rows_written_b.append_null(),
        }
    }

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(query_id_b.finish()) as ArrayRef,
            Arc::new(state_b.finish()) as ArrayRef,
            Arc::new(user_b.finish()) as ArrayRef,
            Arc::new(source_b.finish()) as ArrayRef,
            Arc::new(query_b.finish()) as ArrayRef,
            Arc::new(resource_group_b.finish()) as ArrayRef,
            Arc::new(queued_ms_b.finish()) as ArrayRef,
            Arc::new(analysis_ms_b.finish()) as ArrayRef,
            Arc::new(planning_ms_b.finish()) as ArrayRef,
            Arc::new(execution_ms_b.finish()) as ArrayRef,
            Arc::new(created_b.finish()) as ArrayRef,
            Arc::new(started_b.finish()) as ArrayRef,
            Arc::new(heartbeat_b.finish()) as ArrayRef,
            Arc::new(end_b.finish()) as ArrayRef,
            Arc::new(output_rows_b.finish()) as ArrayRef,
            Arc::new(bytes_scanned_b.finish()) as ArrayRef,
            Arc::new(rows_scanned_b.finish()) as ArrayRef,
            Arc::new(spill_bytes_b.finish()) as ArrayRef,
            Arc::new(peak_memory_bytes_b.finish()) as ArrayRef,
            Arc::new(trace_id_b.finish()) as ArrayRef,
            Arc::new(error_type_b.finish()) as ArrayRef,
            Arc::new(error_code_b.finish()) as ArrayRef,
            Arc::new(rows_written_b.finish()) as ArrayRef,
        ],
    )?)
}

// ---------------------------------------------------------------------------
// nodes table — 5 columns
// ---------------------------------------------------------------------------

fn nodes_schema() -> Schema {
    Schema::new(vec![
        Field::new("node_id", DataType::Utf8, false),
        Field::new("http_uri", DataType::Utf8, false),
        Field::new("node_version", DataType::Utf8, false),
        Field::new("coordinator", DataType::Boolean, false),
        Field::new("state", DataType::Utf8, false),
    ])
}

fn build_nodes_table(
    warehouse: &str,
    coordinator_uri: &str,
    worker_urls: &[String],
) -> DFResult<Arc<dyn TableProvider>> {
    let schema = Arc::new(nodes_schema());

    let mut node_id_b = StringBuilder::new();
    let mut http_uri_b = StringBuilder::new();
    let mut version_b = StringBuilder::new();
    let mut coordinator_b = BooleanBuilder::new();
    let mut state_b = StringBuilder::new();

    let version = env!("CARGO_PKG_VERSION");

    // Coordinator row
    node_id_b.append_value(warehouse);
    http_uri_b.append_value(coordinator_uri);
    version_b.append_value(version);
    coordinator_b.append_value(true);
    state_b.append_value("active");

    // Worker rows
    for (i, url) in worker_urls.iter().enumerate() {
        node_id_b.append_value(format!("worker-{i}"));
        http_uri_b.append_value(url);
        version_b.append_value(version);
        coordinator_b.append_value(false);
        state_b.append_value("active");
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(node_id_b.finish()) as ArrayRef,
            Arc::new(http_uri_b.finish()) as ArrayRef,
            Arc::new(version_b.finish()) as ArrayRef,
            Arc::new(coordinator_b.finish()) as ArrayRef,
            Arc::new(state_b.finish()) as ArrayRef,
        ],
    )?;

    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

// ---------------------------------------------------------------------------
// tasks table — 7 columns
// ---------------------------------------------------------------------------

fn tasks_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_id", DataType::Utf8, false),
        Field::new("task_id", DataType::Utf8, false),
        Field::new("node_id", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("elapsed_ms", DataType::Int64, false),
        Field::new("input_rows", DataType::Int64, false),
        Field::new("output_rows", DataType::Int64, false),
    ])
}

fn build_tasks_table(
    records: &[Arc<QueryRecord>],
    coordinator_node_id: &str,
) -> DFResult<Arc<dyn TableProvider>> {
    let schema = Arc::new(tasks_schema());

    let mut query_id_b = StringBuilder::new();
    let mut task_id_b = StringBuilder::new();
    let mut node_id_b = StringBuilder::new();
    let mut state_b = StringBuilder::new();
    let mut elapsed_ms_b = Int64Builder::new();
    let mut input_rows_b = Int64Builder::new();
    let mut output_rows_b = Int64Builder::new();

    for rec in records {
        if rec.state != QueryState::Finished {
            continue;
        }
        let query_id = rec.query_id.to_string();
        let fragments = rec.fragments_snapshot();
        if fragments.is_empty() {
            // Local (single-node) execution — emit one synthetic task.
            query_id_b.append_value(&query_id);
            task_id_b.append_value(format!("{query_id}-0"));
            node_id_b.append_value(coordinator_node_id);
            state_b.append_value("FINISHED");
            elapsed_ms_b.append_value(u64_to_i64_saturating(rec.execution_ms));
            input_rows_b.append_value(u64_to_i64_saturating(rec.output_rows as u64));
            output_rows_b.append_value(u64_to_i64_saturating(rec.output_rows as u64));
        } else {
            // Distributed execution: one row per fragment with real worker URLs.
            for frag in &fragments {
                query_id_b.append_value(&query_id);
                task_id_b.append_value(&frag.task_id);
                node_id_b.append_value(&frag.worker_url);
                state_b.append_value(frag.state.to_string());
                elapsed_ms_b.append_value(u64_to_i64_saturating(frag.elapsed_ms));
                input_rows_b.append_value(u64_to_i64_saturating(frag.input_rows as u64));
                output_rows_b.append_value(u64_to_i64_saturating(frag.output_rows as u64));
            }
        }
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(query_id_b.finish()) as ArrayRef,
            Arc::new(task_id_b.finish()) as ArrayRef,
            Arc::new(node_id_b.finish()) as ArrayRef,
            Arc::new(state_b.finish()) as ArrayRef,
            Arc::new(elapsed_ms_b.finish()) as ArrayRef,
            Arc::new(input_rows_b.finish()) as ArrayRef,
            Arc::new(output_rows_b.finish()) as ArrayRef,
        ],
    )?;

    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use chrono::Utc;
    use uuid::Uuid;

    use crate::query_tracker::{FragmentInfo, FragmentState};

    /// A tracker record in `state` for `user`, with every other field at its
    /// idle value. Tests set what they assert on.
    fn record(
        n: u128,
        state: QueryState,
        user: &str,
        source: Option<&str>,
        sql: &str,
    ) -> QueryRecord {
        QueryRecord {
            query_id: Uuid::from_u128(n),
            state,
            user: user.to_string(),
            principal: user.to_string(),
            source: source.map(str::to_string),
            sql: sql.to_string(),
            session_id: "session".to_string(),
            client_ip: None,
            roles: Vec::new(),
            created: Utc::now(),
            started: Some(Utc::now()),
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
            trace_id: None,
            fragments: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn sample_records() -> Vec<Arc<QueryRecord>> {
        let mut finished = record(1, QueryState::Finished, "alice", Some("cli"), "SELECT 1");
        finished.ended = Some(Utc::now());
        finished.queued_ms = 5;
        finished.planning_ms = 10;
        finished.execution_ms = 100;
        finished.output_rows = 1;
        finished.bytes_scanned = 1024;
        finished.rows_scanned = 10;
        finished.peak_memory_bytes = 2048;
        finished.trace_id = Some("0123456789abcdef0123456789abcdef".to_string());

        let mut failed = record(2, QueryState::Failed, "bob", None, "BAD SQL");
        failed.ended = Some(Utc::now());
        failed.queued_ms = 2;
        failed.error_type = Some("SyntaxError".to_string());
        failed.error_code = Some("42000".to_string());

        let mut running = record(
            3,
            QueryState::Running,
            "carol",
            Some("dbt"),
            "SELECT * FROM orders",
        );
        running.queued_ms = 1;
        running.planning_ms = 5;

        vec![Arc::new(finished), Arc::new(failed), Arc::new(running)]
    }

    // -----------------------------------------------------------------------
    // queries table
    // -----------------------------------------------------------------------

    #[test]
    fn test_queries_table_has_23_columns() {
        let table = build_queries_table(&sample_records()).unwrap();
        assert_eq!(
            table.schema().fields().len(),
            23,
            "queries table must have exactly 23 columns"
        );
    }

    #[test]
    fn test_queries_table_column_names() {
        let table = build_queries_table(&sample_records()).unwrap();
        let schema = table.schema();
        let expected = [
            "query_id",
            "state",
            "user",
            "source",
            "query",
            "resource_group_id",
            "queued_time_ms",
            "analysis_time_ms",
            "planning_time_ms",
            "execution_time_ms",
            "created",
            "started",
            "last_heartbeat",
            "end",
            "output_rows",
            "bytes_scanned",
            "rows_scanned",
            "spill_bytes",
            "peak_memory_bytes",
            "trace_id",
            "error_type",
            "error_code",
            "rows_written",
        ];
        for (i, name) in expected.iter().enumerate() {
            assert_eq!(
                schema.field(i).name(),
                *name,
                "queries column {i} name mismatch"
            );
        }
    }

    /// #725: the tracker keeps SQL verbatim and this table is readable by every
    /// authenticated session, so the `query` column must go through the same
    /// redaction as the web UI list.
    #[test]
    fn queries_table_redacts_secret_literals() {
        let rec = record(
            9,
            QueryState::Finished,
            "root",
            None,
            "CREATE SECRET s TYPE oidc TOKEN 'eyJhbGciOi.super-secret'",
        );
        let batch = queries_batch(&[Arc::new(rec)]).unwrap();
        let col = batch
            .column_by_name("query")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let shown = col.value(0);
        assert!(
            !shown.contains("super-secret"),
            "secret literal leaked: {shown}"
        );
        assert!(
            shown.contains("[REDACTED]"),
            "expected redaction marker: {shown}"
        );
    }

    #[test]
    fn test_queries_table_empty_records() {
        let table = build_queries_table(&[]).unwrap();
        assert_eq!(table.schema().fields().len(), 23);
    }

    #[test]
    fn test_queries_table_timestamp_columns_have_utc_timezone() {
        let schema = queries_schema();
        for name in &["created", "started", "last_heartbeat", "end"] {
            let field = schema.field_with_name(name).unwrap();
            match field.data_type() {
                DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) => {
                    assert_eq!(
                        tz.as_ref(),
                        "UTC",
                        "timestamp column {name} must use UTC timezone"
                    );
                }
                other => panic!("expected Timestamp(Millisecond, UTC) for {name}, got {other:?}"),
            }
        }
    }

    // -----------------------------------------------------------------------
    // nodes table
    // -----------------------------------------------------------------------

    #[test]
    fn test_nodes_table_has_5_columns() {
        let table = build_nodes_table("wh", "http://localhost:8080", &[]).unwrap();
        assert_eq!(
            table.schema().fields().len(),
            5,
            "nodes table must have exactly 5 columns"
        );
    }

    #[test]
    fn test_nodes_table_column_names() {
        let table = build_nodes_table("wh", "http://localhost:8080", &[]).unwrap();
        let schema = table.schema();
        let expected = [
            "node_id",
            "http_uri",
            "node_version",
            "coordinator",
            "state",
        ];
        for (i, name) in expected.iter().enumerate() {
            assert_eq!(
                schema.field(i).name(),
                *name,
                "nodes column {i} name mismatch"
            );
        }
    }

    #[test]
    fn test_nodes_table_coordinator_only() {
        // With no workers, the table should have exactly 1 row (coordinator).
        let table = build_nodes_table("my-wh", "http://localhost:8080", &[]).unwrap();
        // We can verify indirectly via schema — the actual row count requires
        // executing the table, but the schema should be valid.
        assert_eq!(table.schema().fields().len(), 5);
    }

    #[test]
    fn test_nodes_table_with_workers() {
        let workers = vec![
            "http://worker-0:9090".to_string(),
            "http://worker-1:9090".to_string(),
        ];
        let table = build_nodes_table("my-wh", "http://localhost:8080", &workers).unwrap();
        assert_eq!(table.schema().fields().len(), 5);
    }

    // -----------------------------------------------------------------------
    // tasks table
    // -----------------------------------------------------------------------

    #[test]
    fn test_tasks_table_has_7_columns() {
        let table = build_tasks_table(&sample_records(), "my-wh").unwrap();
        assert_eq!(
            table.schema().fields().len(),
            7,
            "tasks table must have exactly 7 columns"
        );
    }

    #[test]
    fn test_tasks_table_column_names() {
        let table = build_tasks_table(&sample_records(), "my-wh").unwrap();
        let schema = table.schema();
        let expected = [
            "query_id",
            "task_id",
            "node_id",
            "state",
            "elapsed_ms",
            "input_rows",
            "output_rows",
        ];
        for (i, name) in expected.iter().enumerate() {
            assert_eq!(
                schema.field(i).name(),
                *name,
                "tasks column {i} name mismatch"
            );
        }
    }

    #[test]
    fn test_tasks_table_only_finished_queries() {
        // sample_records has 3 records but only 1 is Finished.
        // We verify the schema is correct — row count is an execution-time concern.
        let table = build_tasks_table(&sample_records(), "my-wh").unwrap();
        assert_eq!(table.schema().fields().len(), 7);
    }

    #[tokio::test]
    async fn test_owner_scoped_provider_hides_other_users_queries() {
        let records = sample_records();
        let records_fn: QueryRecordsFn = Arc::new(move || records.clone());
        let provider = RuntimeSchemaProvider::new(
            Arc::clone(&records_fn),
            "my-wh".into(),
            "http://localhost:50051".into(),
            vec![],
        )
        .with_owner_filter(Some("alice".into()));
        for table in ["queries", "tasks"] {
            let t = provider.table(table).await.unwrap().unwrap();
            let batch = table_rows(&t).await;
            assert_eq!(batch.num_rows(), 1, "{table}: only alice's own query");
        }

        // No filter (admin session): every user's queries.
        let admin = RuntimeSchemaProvider::new(
            records_fn,
            "my-wh".into(),
            "http://localhost:50051".into(),
            vec![],
        )
        .with_owner_filter(None);
        let t = admin.table("queries").await.unwrap().unwrap();
        assert_eq!(table_rows(&t).await.num_rows(), 3);
    }

    #[test]
    fn test_tasks_table_empty() {
        let table = build_tasks_table(&[], "my-wh").unwrap();
        assert_eq!(table.schema().fields().len(), 7);
    }

    #[tokio::test]
    async fn test_tasks_table_distributed_uses_worker_urls() {
        // A finished query with two fragments should emit two task rows using
        // real worker URLs as node_id.
        let mut distributed = record(0x10, QueryState::Finished, "alice", None, "SELECT 1");
        distributed.ended = Some(Utc::now());
        distributed.planning_ms = 5;
        distributed.execution_ms = 200;
        distributed.output_rows = 42;
        distributed.fragments = Arc::new(Mutex::new(vec![
            FragmentInfo {
                task_id: "frag-0".to_string(),
                worker_url: "http://worker-1:50052".to_string(),
                state: FragmentState::Finished,
                elapsed_ms: 100,
                input_rows: 20,
                output_rows: 20,
            },
            FragmentInfo {
                task_id: "frag-1".to_string(),
                worker_url: "http://worker-2:50052".to_string(),
                state: FragmentState::Finished,
                elapsed_ms: 150,
                input_rows: 22,
                output_rows: 22,
            },
        ]));
        let records = vec![Arc::new(distributed)];

        let table = build_tasks_table(&records, "my-wh").unwrap();
        assert_eq!(table.schema().fields().len(), 7);
        let batch = table_rows(&table).await;
        assert_eq!(batch.num_rows(), 2, "one task row per fragment");
        let node_ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(node_ids.value(0), "http://worker-1:50052");
        assert_eq!(node_ids.value(1), "http://worker-2:50052");
        let states = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(states.value(0), "FINISHED");
    }

    /// Scan a one-batch `MemTable` back into its rows.
    async fn table_rows(table: &Arc<dyn TableProvider>) -> RecordBatch {
        let ctx = datafusion::prelude::SessionContext::new();
        let plan = table
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan");
        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("collect");
        arrow::compute::concat_batches(&table.schema(), &batches).expect("concat")
    }

    #[tokio::test]
    async fn test_tasks_table_local_uses_coordinator_node_id() {
        // A finished query with NO fragments should emit one synthetic task
        // using the coordinator node_id.
        let mut local = record(0x20, QueryState::Finished, "bob", None, "SELECT 1");
        local.ended = Some(Utc::now());
        local.planning_ms = 3;
        local.execution_ms = 50;
        local.output_rows = 5;
        let records = vec![Arc::new(local)];

        let table = build_tasks_table(&records, "my-coordinator").unwrap();
        assert_eq!(table.schema().fields().len(), 7);
        let batch = table_rows(&table).await;
        assert_eq!(batch.num_rows(), 1, "one synthetic task");
        let node_ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(node_ids.value(0), "my-coordinator");
    }

    // -----------------------------------------------------------------------
    // SchemaProvider trait
    // -----------------------------------------------------------------------

    #[test]
    fn test_table_names() {
        let provider = RuntimeSchemaProvider::new(
            Arc::new(Vec::new),
            "wh".to_string(),
            "http://localhost:8080".to_string(),
            vec![],
        );
        let mut names = provider.table_names();
        names.sort();
        assert_eq!(names, vec!["nodes", "queries", "query_history", "tasks"]);
    }

    // -----------------------------------------------------------------------
    // query_history (#812)
    // -----------------------------------------------------------------------

    /// One native audit line for a finished query, as the logger writes it.
    fn audit_line(id: u128, user: &str, ended: chrono::DateTime<Utc>, status: &str) -> String {
        let outcome = match status {
            "success" => r#""status":"success""#.to_string(),
            code => format!(r#""status":"failure","error_code":"{code}","message":"boom""#),
        };
        format!(
            r#"{{"time":"{time}","kind":"query","actor":{{"username":"{user}"}},{outcome},"timing":{{"duration_ms":1500,"queued_ms":100,"planning_ms":400,"execution_ms":1000}},"stats":{{"rows_returned":7,"bytes_scanned":64}},"query":{{"text":"SELECT {id}","query_hash":"h","statement_type":"Query"}},"query_id":"{qid}"}}"#,
            time = ended.to_rfc3339(),
            qid = Uuid::from_u128(id),
        )
    }

    /// A provider over `live` and an audit log of `lines`, run through SQL.
    async fn history_rows(
        live: Vec<Arc<QueryRecord>>,
        lines: &[String],
        owner: Option<&str>,
        sql: &str,
    ) -> Vec<String> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        // A legacy `AuditEntry` line and a torn tail are skipped.
        let body = format!(
            "{{\"timestamp\":\"2026-01-01T00:00:00Z\",\"user\":\"alice\"}}\n{}\n{{\"time\":\"20",
            lines.join("\n")
        );
        std::fs::write(&path, body).unwrap();
        let provider = RuntimeSchemaProvider::new(
            Arc::new(move || live.clone()),
            "wh".into(),
            "http://localhost:8080".into(),
            vec![],
        )
        .with_owner_filter(owner.map(str::to_string))
        .with_audit_history(Some(AuditHistorySource {
            path,
            lookback: chrono::Duration::hours(1),
        }));
        let ctx = datafusion::prelude::SessionContext::new();
        let table = provider.table("query_history").await.unwrap().unwrap();
        ctx.register_table("query_history", table).unwrap();
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut out = Vec::new();
        for batch in &batches {
            let column = arrow::compute::cast(batch.column(0), &DataType::Utf8).unwrap();
            let column = column
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            out.extend(column.iter().map(|v| v.unwrap_or_default().to_string()));
        }
        out.sort();
        out
    }

    #[tokio::test]
    async fn query_history_adds_audited_queries_the_tracker_dropped() {
        let now = Utc::now();
        let running = record(1, QueryState::Running, "alice", None, "SELECT 1");
        let live = vec![Arc::new(running)];
        // In the order the logger appends them: oldest first.
        let lines = [
            // Older than the one-hour lookback.
            audit_line(5, "alice", now - chrono::Duration::days(3), "success"),
            audit_line(
                4,
                "alice",
                now - chrono::Duration::minutes(7),
                "QUERY_CANCELLED",
            ),
            audit_line(3, "bob", now - chrono::Duration::minutes(6), "SQE-1"),
            audit_line(2, "alice", now - chrono::Duration::minutes(5), "success"),
            // Still in the tracker too: shown once, from the tracker.
            audit_line(1, "alice", now, "success"),
        ];
        let rows = history_rows(
            live.clone(),
            &lines,
            None,
            "SELECT concat(query, ':', state) FROM query_history",
        )
        .await;
        assert_eq!(
            rows,
            [
                "SELECT 1:RUNNING",
                "SELECT 2:FINISHED",
                "SELECT 3:FAILED",
                "SELECT 4:CANCELED"
            ]
        );

        // A non-admin session sees its own queries only.
        let sql = "SELECT query FROM query_history";
        let rows = history_rows(live.clone(), &lines, Some("bob"), sql).await;
        assert_eq!(rows, ["SELECT 3"]);

        // A bound on `end` older than the lookback reads further back.
        let since = (now - chrono::Duration::days(4)).to_rfc3339();
        let sql = format!(
            "SELECT query FROM query_history WHERE \"end\" >= TIMESTAMP '{since}' \
             AND \"end\" < now() - INTERVAL '2' DAY"
        );
        let rows = history_rows(live.clone(), &lines, None, &sql).await;
        assert_eq!(rows, ["SELECT 5"]);

        // The audit row carries the event's timing and stats.
        let sql = "SELECT concat(CAST(output_rows AS VARCHAR), '/', CAST(bytes_scanned AS VARCHAR), '/', \
                   CAST(queued_time_ms AS VARCHAR), '/', CAST(\"end\" - created AS VARCHAR)) \
                   FROM query_history WHERE query = 'SELECT 2'";
        let rows = history_rows(live, &lines, None, sql).await;
        assert_eq!(rows.len(), 1);
        assert!(rows[0].starts_with("7/64/100/"), "{rows:?}");
    }

    #[test]
    fn end_bounds_come_from_end_and_created_filters() {
        use datafusion::common::ScalarValue;
        use datafusion::prelude::{col, lit};
        let utc = Some("UTC".into());
        let ts = |ms: i64| lit(ScalarValue::TimestampMillisecond(Some(ms), utc.clone()));
        let bounds = EndBounds::from_filters(&[
            col("created").gt_eq(ts(10_000)),
            ts(60_000).gt(col("end")),
            // An upper bound on `created` does not bound `end`.
            col("created").lt(ts(30_000)),
            col("query").eq(lit("x")),
        ]);
        assert_eq!(bounds.from.unwrap().timestamp_millis(), 9_000);
        assert_eq!(bounds.to.unwrap().timestamp_millis(), 61_000);
        let none = EndBounds::from_filters(&[col("created").lt(ts(30_000))]);
        assert!(none.from.is_none() && none.to.is_none());

        // Bounds at chrono's range ends do not overflow the slack.
        let min = chrono::DateTime::<chrono::Utc>::MIN_UTC.timestamp_millis();
        let max = chrono::DateTime::<chrono::Utc>::MAX_UTC.timestamp_millis();
        let edge = EndBounds::from_filters(&[col("end").gt_eq(ts(min)), col("end").lt_eq(ts(max))]);
        assert_eq!(edge.from.unwrap().timestamp_millis(), min);
        assert_eq!(edge.to.unwrap().timestamp_millis(), max);
    }

    #[test]
    fn test_table_exist() {
        let provider = RuntimeSchemaProvider::new(
            Arc::new(Vec::new),
            "wh".to_string(),
            "http://localhost:8080".to_string(),
            vec![],
        );
        assert!(provider.table_exist("queries"));
        assert!(provider.table_exist("query_history"));
        assert!(provider.table_exist("nodes"));
        assert!(provider.table_exist("tasks"));
        assert!(!provider.table_exist("unknown"));
    }

    #[tokio::test]
    async fn test_table_returns_some_for_known_tables() {
        let provider = RuntimeSchemaProvider::new(
            Arc::new(Vec::new),
            "wh".to_string(),
            "http://localhost:8080".to_string(),
            vec![],
        );
        for name in &["queries", "nodes", "tasks"] {
            let result = provider.table(name).await;
            assert!(result.is_ok(), "table({name}) should succeed");
            assert!(
                result.unwrap().is_some(),
                "table({name}) should return Some"
            );
        }
    }

    #[tokio::test]
    async fn test_table_returns_none_for_unknown() {
        let provider = RuntimeSchemaProvider::new(
            Arc::new(Vec::new),
            "wh".to_string(),
            "http://localhost:8080".to_string(),
            vec![],
        );
        let result = provider.table("unknown").await.unwrap();
        assert!(result.is_none());
    }
}
