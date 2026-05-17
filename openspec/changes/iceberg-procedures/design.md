## Architecture

```
SQL                            sqe-sql            sqe-coordinator                     iceberg-rust                       Backend
---                            -------            ---------------                     ------------                       -------
CALL system                  → ProcedureStmt  → ProcedureRegistry::dispatch     → Catalog::register_table       → Polaris REST POST /register
  .register_table(             AST node           ↓                                                              → Glue CreateTable
    ns, t,                                        Procedure::execute                                             → HMS ALTER TABLE EXTERNAL
    metadata_location)                            ↓                                                              → JDBC INSERT into tables
                                                  Authorization check                                            → S3 Tables POST /register
                                                  ↓                                                              → Hadoop unsupported
                                                  Result row(s)
```

The flow has three architectural layers:

1. **Grammar** — wraps sqlparser-rs's existing `Statement::Call` parsing, adds an SQE-specific post-parse hook that recognizes the `system.*` procedure namespace and normalizes arguments. Lives in `sqe-sql`.
2. **Dispatch** — a `Procedure` trait, a registry of known procedures, and a single entry point invoked from `sqe-coordinator`'s statement router. Each procedure validates its own arguments and translates the call into one or more `Catalog` trait calls.
3. **Backend** — iceberg-rust's `Catalog` trait already abstracts the per-backend differences. SQE writes zero per-backend code; the existing per-backend implementations in `vendor/iceberg-rust/crates/catalog/{rest,glue,hms,sql,s3tables}` do the work.

## Rust trait definitions

```rust
// crates/sqe-coordinator/src/procedures/mod.rs

/// A SQL-callable procedure. One impl per (`system.foo`) procedure name.
#[async_trait]
pub trait Procedure: Send + Sync {
    /// Procedure namespace and name, e.g. `("system", "register_table")`.
    fn identifier(&self) -> (&'static str, &'static str);

    /// Argument schema. Drives both validation and the SHOW PROCEDURES help text.
    fn arg_schema(&self) -> &[ProcedureArg];

    /// Execute the procedure against the session's catalog and session context.
    async fn execute(
        &self,
        args: ProcedureArgs,
        session: &SessionContext,
    ) -> Result<ProcedureOutput>;
}

/// One declared argument: name, type, optional default.
pub struct ProcedureArg {
    pub name: &'static str,
    pub ty: ArgType,
    pub default: Option<ScalarValue>,
}

pub enum ArgType {
    String,
    Int64,
    Timestamp,
    Boolean,
    StringArray,
}

/// Normalized argument bundle passed to execute(). Procedures pull by name.
pub struct ProcedureArgs {
    values: HashMap<&'static str, ScalarValue>,
}

impl ProcedureArgs {
    pub fn string(&self, name: &str) -> Result<&str> { ... }
    pub fn int64(&self, name: &str) -> Result<i64> { ... }
    pub fn optional_string(&self, name: &str) -> Option<&str> { ... }
}

/// What a procedure returns to the client. Most return a single-row RecordBatch
/// describing the outcome (loaded snapshot id, files registered, etc.). Some
/// return multi-row results (rewrite_data_files lists rewritten files); the
/// trait accommodates both.
pub struct ProcedureOutput {
    pub schema: SchemaRef,
    pub rows: Vec<RecordBatch>,
}

/// Registry of all known procedures. Lives on the session context.
pub struct ProcedureRegistry {
    procedures: HashMap<(&'static str, &'static str), Arc<dyn Procedure>>,
}

impl ProcedureRegistry {
    pub fn default() -> Self {
        let mut r = Self::empty();
        r.register(Arc::new(RegisterTableProcedure));
        r.register(Arc::new(DropTableProcedure));
        r.register(Arc::new(SetCurrentSnapshotProcedure));
        r.register(Arc::new(RollbackToSnapshotProcedure));
        r
    }

    pub async fn dispatch(
        &self,
        stmt: &ProcedureStmt,
        session: &SessionContext,
    ) -> Result<ProcedureOutput> {
        let key = (stmt.namespace.as_str(), stmt.procedure.as_str());
        let proc = self.procedures
            .get(&key)
            .ok_or_else(|| DataFusionError::Plan(
                format!("unknown procedure: {}.{}", stmt.namespace, stmt.procedure)
            ))?;
        let args = bind_args(proc.arg_schema(), &stmt.args)?;
        proc.execute(args, session).await
    }
}
```

## AST and parser

```rust
// crates/sqe-sql/src/ast.rs

/// SQE-specific statement variants produced by post-parse transformation
/// of sqlparser-rs output.
pub enum SqeStatement {
    Standard(sqlparser::ast::Statement),
    Policy(PolicyStatement),       // existing Phase 5 grammar
    Procedure(ProcedureStmt),      // new in this change
}

/// `CALL [catalog.]namespace.procedure(arg1 => val1, arg2 => val2)` or
/// positional `CALL [catalog.]namespace.procedure(val1, val2)`.
pub struct ProcedureStmt {
    pub catalog: Option<String>,
    pub namespace: String,
    pub procedure: String,
    pub args: Vec<ProcedureCallArg>,
}

pub enum ProcedureCallArg {
    Positional(Expr),
    Named { name: String, value: Expr },
}
```

The parser path is:

1. sqlparser-rs parses `CALL ns.proc(args)` into a `Statement::Call` containing an `ObjectName` and a `Vec<FunctionArg>`.
2. SQE's post-parse transformation (`sqe-sql/src/lib.rs`) walks the parsed statement, and when it sees `Statement::Call` with an `ObjectName` that ends in `.system.proc_name` or `system.proc_name`, transforms it into `SqeStatement::Procedure(ProcedureStmt { ... })`.
3. The catalog prefix is optional. `CALL iceberg.system.register_table(...)` and `CALL system.register_table(...)` are both accepted; in the second form the catalog defaults to the session's default catalog.
4. Outside the `system.*` namespace, the CALL statement passes through as `SqeStatement::Standard(Statement::Call(...))` to support future user-defined procedures cleanly.

Named-arguments use the `=>` operator that sqlparser-rs already supports (the same operator used in DataFusion functions). Mixed positional and named are not allowed; we follow Spark's rule.

## Procedure implementations (v1)

### `system.register_table`

```rust
pub struct RegisterTableProcedure;

impl Procedure for RegisterTableProcedure {
    fn identifier(&self) -> (&'static str, &'static str) {
        ("system", "register_table")
    }

    fn arg_schema(&self) -> &[ProcedureArg] {
        &[
            ProcedureArg { name: "namespace", ty: ArgType::String, default: None },
            ProcedureArg { name: "table_name", ty: ArgType::String, default: None },
            ProcedureArg { name: "metadata_location", ty: ArgType::String, default: None },
        ]
    }

    async fn execute(&self, args: ProcedureArgs, session: &SessionContext) -> Result<ProcedureOutput> {
        let namespace = args.string("namespace")?;
        let table_name = args.string("table_name")?;
        let metadata_location = args.string("metadata_location")?;

        let catalog = session.iceberg_catalog()?;
        let ident = iceberg::TableIdent::new(
            iceberg::NamespaceIdent::from_strs([namespace])?,
            table_name.to_string(),
        );

        // Privilege check: same model as CREATE TABLE.
        session.policy().require_create_in_namespace(&ident.namespace)?;

        // Iceberg-rust dispatches to the backend (REST POST /register,
        // Glue CreateTable, HMS ALTER TABLE EXTERNAL, etc.).
        let table = catalog.register_table(&ident, metadata_location).await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Invalidate any cached `Table` instance under this identifier so
        // subsequent queries see the registered table immediately.
        session.invalidate_table_cache(&ident);

        Ok(ProcedureOutput::single_row(
            schema_register_result(),
            vec![
                ScalarValue::from(table.identifier().to_string()),
                ScalarValue::from(table.metadata().current_snapshot_id().unwrap_or(0)),
                ScalarValue::from(metadata_location),
            ],
        ))
    }
}

fn schema_register_result() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("table_identifier", DataType::Utf8, false),
        Field::new("snapshot_id", DataType::Int64, true),
        Field::new("metadata_location", DataType::Utf8, false),
    ]))
}
```

### `system.drop_table`

Catalog-only drop. Removes the catalog entry but does NOT delete underlying parquet or metadata files. Used for catalog migration ("drop on source, register on destination") and for cleaning up stale registrations without losing data. Maps to `Catalog::drop_table(&ident)` which, per iceberg-rust's contract, only writes catalog state.

```rust
async fn execute(&self, args: ProcedureArgs, session: &SessionContext) -> Result<ProcedureOutput> {
    let namespace = args.string("namespace")?;
    let table_name = args.string("table_name")?;
    let purge = args.optional_bool("purge").unwrap_or(false);

    let ident = make_ident(namespace, table_name)?;
    session.policy().require_drop_on_table(&ident)?;

    let catalog = session.iceberg_catalog()?;
    if purge {
        catalog.purge_table(&ident).await
    } else {
        catalog.drop_table(&ident).await
    }.map_err(box_err)?;

    session.invalidate_table_cache(&ident);

    Ok(ProcedureOutput::single_row(...))
}
```

`purge => true` is opt-in; default is catalog-only. This protects against accidental data loss.

### `system.set_current_snapshot`

Time-travel anchor: move the table's current pointer to a different snapshot. Used to roll back to a known-good snapshot or to pin the table at a specific moment in time for reproducible tests.

```rust
async fn execute(&self, args: ProcedureArgs, session: &SessionContext) -> Result<ProcedureOutput> {
    let namespace = args.string("namespace")?;
    let table_name = args.string("table_name")?;
    let snapshot_id = args.int64("snapshot_id")?;

    let ident = make_ident(namespace, table_name)?;
    session.policy().require_modify_on_table(&ident)?;

    let catalog = session.iceberg_catalog()?;
    let mut table = catalog.load_table(&ident).await.map_err(box_err)?;
    let tx = iceberg::transaction::Transaction::new(&table)
        .set_current_snapshot(snapshot_id)?;
    let updated = tx.commit(catalog.as_ref()).await.map_err(box_err)?;

    session.invalidate_table_cache(&ident);
    Ok(ProcedureOutput::single_row(...))
}
```

### `system.rollback_to_snapshot`

Sister of `set_current_snapshot`; same semantics but uses the iceberg-rust `rollback_to_snapshot` helper which records the rollback as a new snapshot rather than directly moving the pointer. This preserves audit history; the procedure-only difference is which transaction op gets called.

## Authorization model

Procedures inherit from the standard policy enforcement that already runs at the LogicalPlan layer for CRUD:

- `system.register_table(ns, t, loc)` requires the session principal to have `CREATE TABLE` on namespace `ns`. Implementation: `session.policy().require_create_in_namespace(&ns)`.
- `system.drop_table(ns, t, purge?)` requires `DROP` on table `ns.t`. With `purge => true`, also requires `MODIFY` on the namespace (purge deletes underlying data).
- `system.set_current_snapshot` and `system.rollback_to_snapshot` require `MODIFY` on the table.

These map onto the existing GRANT vocabulary; no new privilege keywords. The policy hook fires before the iceberg-rust `Catalog` call, so an unauthorized session never reaches the backend. This matches the same plan-rewrite-before-execute model the Phase 5 policy work established.

## Statement routing

`crates/sqe-coordinator/src/query_handler.rs` already branches on statement type after parsing. Add a branch for `SqeStatement::Procedure(stmt)`:

```rust
match parsed_stmt {
    SqeStatement::Standard(stmt) => handle_standard(stmt, ...).await,
    SqeStatement::Policy(stmt) => handle_policy(stmt, ...).await,
    SqeStatement::Procedure(stmt) => {
        let registry = session.procedure_registry();
        let output = registry.dispatch(&stmt, session).await?;
        Ok(QueryResult::from_procedure_output(output))
    }
}
```

`QueryResult::from_procedure_output` wraps the procedure's returned `RecordBatch` in the same envelope a SELECT would use, so existing Flight SQL / Trino HTTP serialization works unchanged.

## Backend support matrix (v1)

| Procedure | Polaris (REST) | Glue | HMS | JDBC | S3 Tables | Hadoop |
|-----------|----------------|------|-----|------|-----------|--------|
| `register_table` | yes | yes | yes (sets `TBLPROPERTIES`) | yes | yes | unsupported (FS discovery) |
| `drop_table` (no purge) | yes | yes | yes | yes | yes | unsupported |
| `drop_table` (purge => true) | yes | yes | yes | yes | yes | yes (filesystem rm) |
| `set_current_snapshot` | yes | yes | yes | yes | yes | yes |
| `rollback_to_snapshot` | yes | yes | yes | yes | yes | yes |

"Unsupported" returns a typed error message naming the backend and procedure; the session continues to function and other procedures still work.

## Testing strategy

1. **Unit**: parser tests for `CALL` AST production (named + positional + mixed-rejection); registry dispatch (known procedure → invoked, unknown procedure → typed error).
2. **Integration**: existing `scripts/integration-test.sh` against the Polaris + RustFS docker stack. Add a new test file that writes a managed table via CTAS, captures its `metadata_location`, drops it from the catalog (no purge), and re-registers it. Verify a SELECT after re-register returns the same rows.
3. **Cross-backend smoke**: extend `tests/integration/catalog_backends.rs` (if it exists; create if not) with a single roundtrip register test per backend. Glue and HMS tests gated behind feature flags and credentials; CI runs the Polaris one only.
4. **dbt compat**: a dbt model that uses `{{ run_query("CALL system.register_table(...)") }}` to surface a pre-loaded table. Add to the dbt-sqe test suite (Phase 2c).

## Open questions

- **Should `register_table` accept a `properties` map?** Iceberg's spec supports per-table properties at register time. Spark exposes them as a fifth named argument. For v1 we'll accept the arg but reject non-empty values with "not supported in v1" to leave room for the right plumbing later.
- **Atomic register-with-side-effects?** Some workflows want to register and immediately set table properties or grant privileges. We'll defer compound CALL semantics; users wanting atomicity can wrap multiple statements in a future BEGIN/COMMIT once SQE has multi-statement transactions.
- **Echo to standard out vs result row?** Spark's procedures return a result set; Trino's are SELECT-shape. We'll return result rows (matches the existing query result protocol) and treat the rows as advisory output, not state. Empty result on success is also acceptable but less useful for tooling.
