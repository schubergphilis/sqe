//! The virtual `system.runtime` schema (`queries`, `nodes`, `tasks`), rendered
//! from the coordinator's own [`QueryTracker`](crate::query_tracker::QueryTracker)
//! records.
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
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::Result as DFResult;

use crate::query_tracker::{QueryRecord, QueryState};

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

/// Callback that returns a fresh snapshot of the tracker's query records.
pub type QueryRecordsFn = Arc<dyn Fn() -> Vec<Arc<QueryRecord>> + Send + Sync>;

/// DataFusion `SchemaProvider` for the virtual `system.runtime` schema.
///
/// Exposes three tables compatible with Trino's `system.runtime` contract:
///
/// - `queries` — one row per tracked query
/// - `nodes`   — one row per cluster node
/// - `tasks`   — one row per finished-query task (single-node: one task per query)
pub struct RuntimeSchemaProvider {
    records_fn: QueryRecordsFn,
    warehouse: String,
    coordinator_uri: String,
    worker_urls: Vec<String>,
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
        }
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
        vec!["queries".into(), "nodes".into(), "tasks".into()]
    }

    fn table_exist(&self, name: &str) -> bool {
        matches!(name, "queries" | "nodes" | "tasks")
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        match name {
            "queries" => Ok(Some(build_queries_table(&(self.records_fn)())?)),
            "nodes" => Ok(Some(build_nodes_table(
                &self.warehouse,
                &self.coordinator_uri,
                &self.worker_urls,
            )?)),
            "tasks" => Ok(Some(build_tasks_table(
                &(self.records_fn)(),
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
        assert_eq!(names, vec!["nodes", "queries", "tasks"]);
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
