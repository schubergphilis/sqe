//! Server-side prepared statements for Flight SQL (finding 406, #586).
//!
//! `CreatePreparedStatement` used to hand the SQL text back as the "handle",
//! plan it again on every `GetFlightInfo` and `DoGet`, and splice bound
//! parameters into the text as literals. The store here is the server-side
//! object that was missing: one entry per `(session, statement id)` holding
//! the SQL, the [`LogicalPlan`] planned once with `$1..$N` placeholders, the
//! parameter types DataFusion inferred, the output schema, and the current
//! parameter bind.
//!
//! Binding is typed: values arrive as Arrow scalars from the parameter
//! `RecordBatch`, are checked against the inferred placeholder types before
//! anything executes, and are substituted into the plan with
//! [`LogicalPlan::with_param_values`]. The bind survives until `Close` or the
//! idle TTL, so a JDBC `executeQuery()` repeated without a new `setX` (what
//! connection pools do) runs with the previous values instead of a bare `?`.
//!
//! Policy is not part of the stored plan on purpose. The store keeps the
//! *unenforced* plan; every execution binds the parameters and then hands the
//! plan to the normal streaming path, where the policy enforcer rewrites it
//! for the executing user. Reusing the plan saves the parse, catalog and
//! placeholder-inference work, never the policy pass.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::logical_expr::LogicalPlan;
use datafusion::scalar::ScalarValue;
use uuid::Uuid;

/// Maximum prepared statements held across all sessions.
pub const PREPARED_STATEMENTS_MAX_ENTRIES: u64 = 10_000;
/// How long a prepared statement survives without being fetched, bound or
/// executed. Every access refreshes the clock.
pub const PREPARED_STATEMENTS_IDLE_TTL: Duration = Duration::from_secs(1800);

/// `(authenticated session id, statement id)`. Binding the key to the
/// server-assigned session id means a peer cannot reach another session's
/// statement or bind by guessing the id (WEB-03).
type PreparedKey = (String, Uuid);

/// One prepared statement.
pub struct PreparedStatement {
    /// The SQL as the client sent it (`?` or `$n` placeholders). Used for the
    /// audit record and for the text path of non-query statements.
    pub sql: String,
    /// `sql` with `?` placeholders rewritten to `$1..$N`; what was planned.
    pub numbered_sql: String,
    /// The plan built once at prepare time, still carrying placeholders.
    /// `None` for statements the streaming path does not run (DML, DDL,
    /// SHOW, ...), which execute through the buffered text path instead.
    pub plan: Option<StoredPlan>,
    /// Inferred type of each positional parameter; `None` where DataFusion
    /// had no context to infer one (accepts any bind).
    pub param_types: Vec<Option<DataType>>,
    /// Output schema advertised to the client.
    pub dataset_schema: SchemaRef,
    /// The current bind, kept until `Close`, a re-bind, or eviction.
    params: RwLock<Option<Vec<ScalarValue>>>,
    /// The stored plan's tables carry catalog-vended keys; past this instant
    /// (refresh margin before the earliest expiry) the plan must not run
    /// and the statement is re-planned from its text, which reloads the
    /// tables and gets new keys. `None`: no expiring credentials.
    plan_credentials_usable_until: Option<std::time::SystemTime>,
    /// The client-held transaction the statement was prepared in (#824):
    /// the execute requests that follow carry no transaction id of their own.
    transaction_id: Option<String>,
}

/// Earliest refresh deadline of the vended credentials held by the Iceberg
/// tables `plan` scans (subqueries included).
fn plan_credentials_usable_until(plan: &LogicalPlan) -> Option<std::time::SystemTime> {
    use datafusion::common::tree_node::TreeNodeRecursion;
    use sqe_catalog::vended_credentials::{refresh_deadline, table_credential_expiry};
    let now = std::time::SystemTime::now();
    let mut earliest: Option<std::time::SystemTime> = None;
    let _ = plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            let expiry = datafusion::datasource::source_as_provider(&scan.source)
                .ok()
                .and_then(|provider| {
                    let any: &dyn std::any::Any = provider.as_ref();
                    any.downcast_ref::<sqe_catalog::table_provider::SqeTableProvider>()
                        .and_then(|p| table_credential_expiry(p.table()))
                });
            if let Some(expires_at) = expiry {
                let deadline = refresh_deadline(expires_at, now);
                earliest = Some(earliest.map_or(deadline, |e| e.min(deadline)));
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    earliest
}

/// A [`LogicalPlan`] behind a trait object, so that the auto-trait solver
/// stops here.
///
/// This exists for compile time, not behaviour. `PreparedStatement` sits in a
/// moka cache inside `SqeFlightSqlService`, and every `async_trait` method of
/// that service (20-odd of them) has to prove `SqeFlightSqlService: Sync` for
/// its own `Send` future. With a bare `LogicalPlan` field that proof walked
/// DataFusion's entire plan and expression type graph once per method, about
/// 1.1 s each (`-Zself-profile`, 2026-09-07). `dyn PlanHolder` is `Send +
/// Sync` by declaration, so the walk ends at this field.
#[derive(Clone)]
pub struct StoredPlan(Arc<dyn PlanHolder>);

trait PlanHolder: Send + Sync + 'static {
    fn plan(&self) -> &LogicalPlan;
}

impl PlanHolder for LogicalPlan {
    fn plan(&self) -> &LogicalPlan {
        self
    }
}

impl StoredPlan {
    pub fn new(plan: LogicalPlan) -> Self {
        Self(Arc::new(plan))
    }

    pub fn plan(&self) -> &LogicalPlan {
        self.0.plan()
    }
}

impl std::fmt::Debug for StoredPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StoredPlan(..)")
    }
}

impl std::fmt::Debug for PreparedStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedStatement")
            .field("sql", &self.sql)
            .field("param_types", &self.param_types)
            .field("has_plan", &self.plan.is_some())
            .field("bound", &self.is_bound())
            .finish()
    }
}

impl PreparedStatement {
    pub fn new(
        sql: String,
        numbered_sql: String,
        plan: Option<LogicalPlan>,
        param_types: Vec<Option<DataType>>,
        dataset_schema: SchemaRef,
    ) -> Self {
        let plan_credentials_usable_until = plan.as_ref().and_then(plan_credentials_usable_until);
        Self {
            sql,
            numbered_sql,
            plan: plan.map(StoredPlan::new),
            param_types,
            dataset_schema,
            params: RwLock::new(None),
            plan_credentials_usable_until,
            transaction_id: None,
        }
    }

    /// The statement runs in the client-held transaction `transaction_id`
    /// (`ActionCreatePreparedStatementRequest.transaction_id`, #824).
    #[must_use]
    pub fn with_transaction_id(mut self, transaction_id: Option<String>) -> Self {
        self.transaction_id = transaction_id;
        self
    }

    /// The client-held transaction this statement runs in, as the handle
    /// bytes a request carries.
    pub fn transaction_handle(&self) -> Option<&[u8]> {
        self.transaction_id.as_deref().map(str::as_bytes)
    }

    /// True when the stored plan holds vended credentials that are due for
    /// refresh: run the statement through the text path instead, so the
    /// tables are reloaded (fresh keys) rather than scanned with dead ones.
    pub fn stored_plan_credentials_expired(&self) -> bool {
        self.plan_credentials_usable_until
            .is_some_and(|until| std::time::SystemTime::now() >= until)
    }

    /// Positional parameter types read off a planned statement:
    /// `$1..$N` in order, `N` being the highest placeholder number seen in
    /// either the plan or the `?` rewrite.
    pub fn param_types_from_plan(
        plan: &LogicalPlan,
        numbered_count: usize,
    ) -> Result<Vec<Option<DataType>>, String> {
        let types = plan
            .get_parameter_types()
            .map_err(|e| format!("parameter type inference failed: {e}"))?;
        let highest = types
            .keys()
            .filter_map(|k| k.strip_prefix('$').and_then(|n| n.parse::<usize>().ok()))
            .max()
            .unwrap_or(0);
        Ok((1..=highest.max(numbered_count))
            .map(|i| types.get(&format!("${i}")).cloned().flatten())
            .collect())
    }

    pub fn param_count(&self) -> usize {
        self.param_types.len()
    }

    /// The `parameter_schema` for `ActionCreatePreparedStatementResult`:
    /// one nullable field per placeholder, named `$n`. A parameter whose type
    /// could not be inferred is advertised as `Utf8`, the most permissive
    /// type a driver can build a bind vector for.
    pub fn parameter_schema(&self) -> Schema {
        Schema::new(
            self.param_types
                .iter()
                .enumerate()
                .map(|(i, dt)| {
                    Field::new(
                        format!("${}", i + 1),
                        dt.clone().unwrap_or(DataType::Utf8),
                        true,
                    )
                })
                .collect::<Vec<_>>(),
        )
    }

    pub fn is_bound(&self) -> bool {
        self.params.read().map(|g| g.is_some()).unwrap_or(false)
    }

    /// Validate and store a bind. Rejects a wrong arity or a value whose Arrow
    /// type cannot stand in for the inferred placeholder type; see
    /// [`coerce_param`]. Replaces any earlier bind.
    pub fn bind(&self, values: Vec<ScalarValue>) -> Result<(), String> {
        if values.len() != self.param_types.len() {
            return Err(format!(
                "prepared statement expects {} parameters but bind supplied {}",
                self.param_types.len(),
                values.len()
            ));
        }
        let coerced = values
            .into_iter()
            .zip(self.param_types.iter())
            .enumerate()
            .map(|(i, (value, expected))| match expected {
                Some(expected) => coerce_param(value, expected, i + 1),
                None => Ok(value),
            })
            .collect::<Result<Vec<_>, _>>()?;
        *self
            .params
            .write()
            .map_err(|_| "bind lock poisoned".to_string())? = Some(coerced);
        Ok(())
    }

    /// The current bind, if any.
    pub fn bound_values(&self) -> Option<Vec<ScalarValue>> {
        self.params.read().ok().and_then(|g| g.clone())
    }

    /// The stored plan with the current bind substituted for its
    /// placeholders. Errors when the statement has parameters and nothing is
    /// bound, or when it has no plan (non-query statement).
    pub fn bound_plan(&self) -> Result<LogicalPlan, String> {
        let plan = self
            .plan
            .as_ref()
            .map(|p| p.plan().clone())
            .ok_or_else(|| "prepared statement has no plan (not a query)".to_string())?;
        if self.param_types.is_empty() {
            return Ok(plan);
        }
        let values = self.bound_values().ok_or_else(|| {
            format!(
                "prepared statement has {} parameters but no values are bound",
                self.param_types.len()
            )
        })?;
        plan.with_param_values(values)
            .map_err(|e| format!("binding prepared statement parameters failed: {e}"))
    }
}

/// Check one bound value against the placeholder type DataFusion inferred.
///
/// Exact matches pass. Nulls become a typed null. Values in the same type
/// family (any integer or float for a numeric placeholder, any string
/// encoding for a string placeholder, ...) are cast with DataFusion's strict
/// cast, so an out-of-range integer is an error rather than a silent wrap.
/// A string bound to a date, time or timestamp placeholder is parsed; a
/// string bound to a numeric or boolean placeholder is rejected outright, as
/// is any other cross-family bind. The rejection happens here, before the
/// plan is bound or executed.
pub fn coerce_param(
    value: ScalarValue,
    expected: &DataType,
    position: usize,
) -> Result<ScalarValue, String> {
    let actual = value.data_type();
    if &actual == expected {
        return Ok(value);
    }
    if value.is_null() {
        return ScalarValue::try_from(expected)
            .map_err(|e| format!("parameter ${position}: cannot build null of {expected}: {e}"));
    }
    let compatible = match (family(&actual), family(expected)) {
        (Some(a), Some(e)) if a == e => true,
        (Some(Family::String), Some(Family::Date | Family::Time | Family::Timestamp)) => true,
        _ => false,
    };
    if !compatible {
        return Err(format!(
            "parameter ${position}: expected {expected}, got {actual}"
        ));
    }
    value
        .cast_to(expected)
        .map_err(|e| format!("parameter ${position}: cannot convert {actual} to {expected}: {e}"))
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Family {
    Boolean,
    Numeric,
    String,
    Binary,
    Date,
    Time,
    Timestamp,
    Duration,
    Interval,
}

fn family(dt: &DataType) -> Option<Family> {
    use DataType::*;
    Some(match dt {
        Boolean => Family::Boolean,
        Int8
        | Int16
        | Int32
        | Int64
        | UInt8
        | UInt16
        | UInt32
        | UInt64
        | Float16
        | Float32
        | Float64
        | Decimal128(_, _)
        | Decimal256(_, _) => Family::Numeric,
        Utf8 | LargeUtf8 | Utf8View => Family::String,
        Binary | LargeBinary | BinaryView | FixedSizeBinary(_) => Family::Binary,
        Date32 | Date64 => Family::Date,
        Time32(_) | Time64(_) => Family::Time,
        Timestamp(_, _) => Family::Timestamp,
        Duration(_) => Family::Duration,
        Interval(_) => Family::Interval,
        _ => return None,
    })
}

/// Bounded, idle-expiring store of prepared statements.
#[derive(Clone)]
pub struct PreparedStatementStore {
    cache: Arc<moka::future::Cache<PreparedKey, Arc<PreparedStatement>>>,
}

impl Default for PreparedStatementStore {
    fn default() -> Self {
        Self::new(
            PREPARED_STATEMENTS_MAX_ENTRIES,
            PREPARED_STATEMENTS_IDLE_TTL,
        )
    }
}

impl PreparedStatementStore {
    pub fn new(max_entries: u64, idle_ttl: Duration) -> Self {
        Self {
            cache: Arc::new(
                moka::future::Cache::builder()
                    .max_capacity(max_entries)
                    .time_to_idle(idle_ttl)
                    .build(),
            ),
        }
    }

    /// Store a statement for `session_id` and return its id, which is the
    /// opaque handle handed to the client.
    pub async fn insert(&self, session_id: &str, statement: PreparedStatement) -> Uuid {
        let id = Uuid::now_v7();
        self.cache
            .insert((session_id.to_string(), id), Arc::new(statement))
            .await;
        id
    }

    pub async fn get(&self, session_id: &str, id: Uuid) -> Option<Arc<PreparedStatement>> {
        self.cache.get(&(session_id.to_string(), id)).await
    }

    pub async fn remove(&self, session_id: &str, id: Uuid) {
        self.cache.invalidate(&(session_id.to_string(), id)).await;
    }

    /// The wire form of a statement id: its hyphenated UUID text.
    pub fn handle_bytes(id: Uuid) -> Vec<u8> {
        id.to_string().into_bytes()
    }

    /// Parse a client-supplied handle back into a statement id.
    pub fn parse_handle(handle: &[u8]) -> Result<Uuid, String> {
        std::str::from_utf8(handle)
            .ok()
            .and_then(|s| Uuid::parse_str(s.trim()).ok())
            .ok_or_else(|| {
                "prepared statement handle is not a statement id issued by this server; \
                 re-prepare the statement"
                    .to_string()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    fn plan(sql: &str) -> LogicalPlan {
        let ctx = SessionContext::new();
        futures::executor::block_on(ctx.state().create_logical_plan(sql)).expect("plans")
    }

    fn prepared(sql: &str) -> PreparedStatement {
        let (numbered, n) = sqe_core::number_placeholders(sql);
        let plan = plan(&numbered);
        let types = PreparedStatement::param_types_from_plan(&plan, n).expect("types");
        let schema = Arc::new(plan.schema().as_arrow().clone());
        PreparedStatement::new(sql.to_string(), numbered, Some(plan), types, schema)
    }

    #[test]
    fn infers_placeholder_types_and_advertises_them() {
        let stmt = prepared("SELECT ? + 1");
        assert_eq!(stmt.param_types, vec![Some(DataType::Int64)]);
        let schema = stmt.parameter_schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "$1");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn numbered_placeholders_in_source_are_counted() {
        let stmt = prepared("SELECT $2 + 1, $1 + 1");
        assert_eq!(stmt.param_count(), 2);
    }

    #[test]
    fn bind_survives_multiple_bound_plan_calls() {
        let stmt = prepared("SELECT ? + 1");
        assert!(
            stmt.bound_plan().is_err(),
            "unbound parameters must not plan"
        );
        stmt.bind(vec![ScalarValue::Int64(Some(41))]).unwrap();
        let first = stmt.bound_plan().expect("bound");
        let second = stmt.bound_plan().expect("still bound");
        assert_eq!(format!("{first}"), format!("{second}"));
        assert!(format!("{first}").contains("Int64(41)"), "{first}");
        assert!(stmt.is_bound());
    }

    #[test]
    fn bind_widens_within_family_and_rejects_across() {
        let stmt = prepared("SELECT ? + 1");
        // Int32 for an Int64 placeholder: JDBC setInt on a BIGINT parameter.
        stmt.bind(vec![ScalarValue::Int32(Some(7))]).unwrap();
        assert_eq!(stmt.bound_values().unwrap()[0], ScalarValue::Int64(Some(7)));
        // A string for a numeric placeholder is a type error, before execution.
        let err = stmt
            .bind(vec![ScalarValue::Utf8(Some("abc".into()))])
            .expect_err("Utf8 must not bind to Int64");
        assert!(err.contains("expected Int64, got Utf8"), "{err}");
        // The failed bind did not clobber the good one.
        assert_eq!(stmt.bound_values().unwrap()[0], ScalarValue::Int64(Some(7)));
        // Arity mismatch.
        assert!(stmt.bind(vec![]).is_err());
        // Null takes the placeholder type.
        stmt.bind(vec![ScalarValue::Null]).unwrap();
        assert_eq!(stmt.bound_values().unwrap()[0], ScalarValue::Int64(None));
    }

    #[test]
    fn string_parses_into_temporal_but_not_numeric() {
        assert!(coerce_param(
            ScalarValue::Utf8(Some("2024-01-02".into())),
            &DataType::Date32,
            1
        )
        .is_ok());
        assert!(coerce_param(
            ScalarValue::Utf8(Some("not a date".into())),
            &DataType::Date32,
            1
        )
        .is_err());
        assert!(coerce_param(ScalarValue::Utf8(Some("5".into())), &DataType::Int64, 1).is_err());
        assert!(coerce_param(ScalarValue::Int64(Some(1)), &DataType::Boolean, 1).is_err());
    }

    #[test]
    fn untyped_placeholder_accepts_anything() {
        let stmt = prepared("SELECT ?");
        assert_eq!(stmt.param_types, vec![None]);
        assert_eq!(
            stmt.parameter_schema().field(0).data_type(),
            &DataType::Utf8
        );
        stmt.bind(vec![ScalarValue::Boolean(Some(true))]).unwrap();
        assert!(stmt.bound_plan().is_ok());
    }

    #[tokio::test]
    async fn store_is_session_scoped_and_handles_round_trip() {
        let store = PreparedStatementStore::default();
        let id = store.insert("s1", prepared("SELECT 1")).await;
        let bytes = PreparedStatementStore::handle_bytes(id);
        assert_eq!(PreparedStatementStore::parse_handle(&bytes).unwrap(), id);
        assert!(PreparedStatementStore::parse_handle(b"SELECT 1").is_err());
        assert!(store.get("s1", id).await.is_some());
        assert!(
            store.get("s2", id).await.is_none(),
            "other session sees nothing"
        );
        store.remove("s1", id).await;
        assert!(store.get("s1", id).await.is_none());
    }

    async fn iceberg_scan_plan(expires_at_ms: Option<i64>) -> LogicalPlan {
        let metadata: iceberg::spec::TableMetadata = serde_json::from_value(serde_json::json!({
            "format-version": 1,
            "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
            "location": "s3://bucket/ns/t",
            "last-updated-ms": 1602638573874_i64,
            "last-column-id": 1,
            "schema": {"type": "struct", "fields": [
                {"id": 1, "name": "x", "required": true, "type": "long"}]},
            "partition-spec": [],
            "properties": {},
            "current-snapshot-id": -1,
            "snapshots": []
        }))
        .unwrap();
        let mut builder =
            iceberg::io::FileIOBuilder::new(Arc::new(iceberg::io::LocalFsStorageFactory));
        if let Some(ms) = expires_at_ms {
            builder = builder.with_prop("s3.session-token-expires-at-ms", ms);
        }
        let table = iceberg::table::Table::builder()
            .runtime(iceberg::Runtime::current())
            .metadata(metadata)
            .identifier(iceberg::TableIdent::from_strs(["ns", "t"]).unwrap())
            .file_io(builder.build())
            .disable_cache()
            .build()
            .unwrap();
        let provider = sqe_catalog::table_provider::SqeTableProvider::try_new(table)
            .await
            .unwrap();
        datafusion::logical_expr::LogicalPlanBuilder::scan(
            "t",
            datafusion::datasource::provider_as_source(Arc::new(provider)),
            None,
        )
        .unwrap()
        .build()
        .unwrap()
    }

    fn stored(plan: LogicalPlan) -> PreparedStatement {
        let schema = Arc::new(plan.schema().as_arrow().clone());
        PreparedStatement::new(
            "SELECT x FROM t".into(),
            "SELECT x FROM t".into(),
            Some(plan),
            vec![],
            schema,
        )
    }

    /// A stored plan holding expired vended keys is reported expired, so
    /// execution re-plans from text (reloading the table) instead of
    /// scanning with dead credentials. Long-lived and static keys keep the
    /// stored plan.
    #[tokio::test]
    async fn stored_plan_with_expired_vended_credentials_is_replanned() {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        assert!(
            stored(iceberg_scan_plan(Some(now_ms - 1_000)).await).stored_plan_credentials_expired()
        );
        assert!(!stored(iceberg_scan_plan(Some(now_ms + 3_600_000)).await)
            .stored_plan_credentials_expired());
        assert!(!stored(iceberg_scan_plan(None).await).stored_plan_credentials_expired());
        assert!(!prepared("SELECT 1").stored_plan_credentials_expired());
    }
}
