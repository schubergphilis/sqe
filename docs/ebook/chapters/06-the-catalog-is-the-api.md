# The Catalog Is the API {#sec:catalog}

> Your BI tool doesn't read your code. It reads your catalog.
> Make it worth reading.

The engine worked. You could connect from DBeaver, authenticate via OIDC, and run a query that hit Polaris, fetched credentials, scanned Parquet from S3, and returned Arrow batches over Flight SQL. Chapter 3 through 5, working end to end.

Then someone on the team opened DBeaver's schema browser and saw nothing.

No tables. No schemas. No columns. An empty tree. The connection was live -- queries returned results if you typed them by hand -- but the tool's metadata browser was blank. DBeaver didn't know what existed because we hadn't told it. The engine had no `information_schema`.

This is the gap between "queries work" and "tools work." Every database client, every BI tool, every ORM, every orchestrator discovers your warehouse through the same mechanism: SQL metadata tables. `information_schema.tables`. `information_schema.columns`. `information_schema.schemata`. These aren't optional. They're the API contract between your engine and the entire ecosystem of tools that will talk to it.

The engine that runs queries but can't describe itself is an engine nobody will use.


## What Tools Actually Ask For

Before writing any code, we studied what clients actually query when they connect. We captured SQL from three sources: DBeaver connecting via Flight SQL, a Python ADBC client running `cursor.columns()`, and dbt running `dbt debug`. The patterns converge fast.

DBeaver's first action after the handshake is to query `information_schema.schemata` to populate its schema tree. Then, when you expand a schema, it queries `information_schema.tables` filtered by `table_schema`. Click a table, it queries `information_schema.columns` filtered by `table_schema` and `table_name`. The tool never asks "what can you do?" -- it asks "what do you have?"

The SQL standard defines three views:

| View | Purpose |
|------|---------|
| `information_schema.schemata` | What schemas (namespaces) exist |
| `information_schema.tables` | What tables exist, in which schemas |
| `information_schema.columns` | What columns each table has, their types, nullability |

Every tool expects them. They're the HTTP of database discovery -- a standard interface that doesn't care what the engine is built from.


## Building a Virtual Provider

The trick is that `information_schema` doesn't point at storage. There's no Parquet file holding the list of your tables. The data comes from the catalog -- from Polaris, in our case -- and it's different for every user session, because Polaris scopes the response to what that user's bearer token can access.

This means we can't register a static table at startup. We need a virtual table provider -- something that looks like a regular DataFusion table to the query engine but generates its results dynamically by calling the catalog API.

DataFusion's extensibility model made this straightforward. We implemented `InformationSchemaProvider` as a `SchemaProvider` that returns three virtual tables:

```rust
#[derive(Debug)]
pub struct InformationSchemaProvider {
    session_catalog: Arc<SessionCatalog>,
    warehouse: String,
}

#[async_trait]
impl SchemaProvider for InformationSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        vec![
            "tables".to_string(),
            "columns".to_string(),
            "schemata".to_string(),
        ]
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        match name {
            "tables" => Ok(Some(self.build_tables_table().await?)),
            "columns" => Ok(Some(self.build_columns_table().await?)),
            "schemata" => Ok(Some(self.build_schemata_table().await?)),
            _ => Ok(None),
        }
    }
}
```

Each `build_*` method calls Polaris through the `SessionCatalog` -- which carries the user's bearer token -- lists namespaces and tables, and assembles the results into Arrow `RecordBatch` instances wrapped in a `MemTable`. The table is ephemeral: it's constructed fresh for each query, reflecting the current state of the catalog at that moment.

::: {.datafusion}
**DataFusion deep dive:** DataFusion's `SchemaProvider` trait is async on the `table()` method
but sync on `table_names()`. This forced a design choice: we use `tokio::task::block_in_place`
in `table_names()` to bridge async catalog calls into the synchronous context. For
`information_schema`, we avoided this by hardcoding the three known table names -- no catalog
call needed to know that `tables`, `columns`, and `schemata` exist.
:::


## The Tables Table

The `information_schema.tables` implementation demonstrates the pattern. Walk every namespace, list every table, emit one row per table with the standard columns:

```rust
async fn build_tables_table(&self) -> DFResult<Arc<dyn TableProvider>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("table_catalog", DataType::Utf8, false),
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
    ]));

    let namespaces = self.list_namespaces_safe().await;

    let mut catalog_builder = StringBuilder::new();
    let mut schema_builder = StringBuilder::new();
    let mut name_builder = StringBuilder::new();
    let mut type_builder = StringBuilder::new();

    for ns in &namespaces {
        let ns_ident = NamespaceIdent::new(ns.clone());
        match self.session_catalog.list_tables(&ns_ident).await {
            Ok(tables) => {
                for table in &tables {
                    catalog_builder.append_value(&self.warehouse);
                    schema_builder.append_value(ns);
                    name_builder.append_value(table.name());
                    type_builder.append_value("BASE TABLE");
                }
            }
            Err(e) => {
                warn!(namespace = %ns, error = %e,
                    "Failed to list tables for information_schema");
            }
        }
    }

    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(catalog_builder.finish()) as ArrayRef,
        Arc::new(schema_builder.finish()) as ArrayRef,
        Arc::new(name_builder.finish()) as ArrayRef,
        Arc::new(type_builder.finish()) as ArrayRef,
    ])?;

    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}
```

Two things to notice. First, the `list_namespaces_safe` helper wraps the catalog call in error handling that logs and returns an empty list rather than failing the entire query. A permission error on one namespace shouldn't black out the entire metadata view. Second, `table_type` is hardcoded to `"BASE TABLE"` -- we're not yet distinguishing views from tables here, though the Iceberg catalog tracks them separately.

The columns table follows the same pattern but goes one level deeper: for each table, it loads the Iceberg metadata and walks the schema fields, emitting one row per column with standard columns including `table_catalog`, `table_schema`, `table_name`, `column_name`, `ordinal_position`, `is_nullable`, and `data_type`.

The Iceberg schema becomes SQL metadata through this translation. `field.required` maps to the SQL standard's `is_nullable` -- `"NO"` for required fields, `"YES"` for optional. `field.field_type` renders as a string representation: Iceberg stores types as `long`, `timestamptz`, `decimal(18,2)`, and the columns table presents them as strings that tools parse into their own type models. Getting this translation right -- matching the exact strings that DBeaver, dbt, and JDBC drivers expect -- is the unglamorous work that makes the difference between "the engine works" and "the engine works with my tools."


## Registering the Virtual Schema

Registration into DataFusion's catalog hierarchy is a one-liner in `SqeCatalogProvider`:

```rust
impl CatalogProvider for SqeCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        let mut names = self.cached_namespaces.clone();
        names.push("information_schema".to_string());
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == "information_schema" {
            return Some(Arc::new(
                InformationSchemaProvider::new(
                    self.session_catalog.clone(),
                    self.warehouse.clone(),
                ),
            ));
        }
        // ... resolve real Iceberg namespaces ...
    }
}
```

The `information_schema` appears alongside real namespaces in `schema_names()`. When a query references `information_schema.tables`, DataFusion resolves it through this provider chain. The user doesn't know or care that the data is generated dynamically rather than read from storage.

One subtlety: the `SessionCatalog` inside the provider carries the user's bearer token. When Alice queries `information_schema.tables`, Polaris returns only the tables Alice can access. When Bob queries the same view, he sees his tables. The metadata view is per-user by construction, not by post-filtering. This falls out naturally from the bearer passthrough architecture described in Chapter 4.


### The Caching Layers

The initial catalog implementation made a Polaris REST call for every table load. At scale factor 1, a TPC-DS query touching 15 dimension tables meant 15 network round-trips before execution started. The fix was a multi-layer caching strategy:

**TableMetadataCache.** A global moka cache shared across all sessions, keyed by table identifier and token fingerprint. TTL is 30 seconds — short enough that schema changes propagate within a query cycle, long enough that a 99-query benchmark run does not hammer Polaris 1,500 times.

**ManifestCache.** Iceberg manifest files are immutable by specification. Once written, their content never changes. We cache parsed manifest entries by S3 path with a 512 MB size limit and a 1-hour TTL backstop (for disaster recovery scenarios where a manifest is overwritten at the same path). This eliminates the most expensive I/O in scan planning.

**FooterCache.** Parquet file footers contain schema, row group metadata, and column statistics. We cache them by file path with size-weighted eviction and Prometheus hit/miss counters.

**RestCatalog instance cache.** The iceberg-rust `RestCatalog` is expensive to create (~250 ms). We cache instances by token fingerprint with a 5-minute TTL.

**SessionContext cache.** The DataFusion `SessionContext` with all registered UDFs, TVFs, and catalog providers is cached per token fingerprint (SHA-256). Atomic population via moka's `try_get_with` eliminates the TOCTOU race where concurrent requests build redundant contexts. Invalidated after every DDL operation.

Together, these five layers reduced warm-query overhead from ~540 ms to under 1 ms.


## Beyond information_schema: The system Catalog

Standard metadata tables tell tools what data exists. But an engine operator needs to know what the engine is doing. Which queries are running? How many workers are active? Where is time being spent?

We implemented `system.runtime.queries`, `system.runtime.tasks`, and `system.runtime.nodes` -- not just for Trino compatibility (that's a bonus), but because querying engine state through SQL is the right interface. An operator shouldn't need to scrape Prometheus to answer "what query is taking so long?"

```sql
SELECT query_id, user, state, execution_time_ms, query
FROM system.runtime.queries
WHERE state = 'RUNNING'
ORDER BY execution_time_ms DESC
```

The data comes from the coordinator's `QueryTracker`. But there's a deliberate crate boundary: `sqe-catalog` defines its own snapshot types rather than importing from `sqe-coordinator`, avoiding a circular dependency. The bridge is a closure -- a `QueryRecordsFn` of type `Arc<dyn Fn() -> Vec<RuntimeQueryRecord> + Send + Sync>` that captures the tracker and maps its records into the catalog crate's types. Every time someone queries `system.runtime.queries`, the closure fires, the tracker dumps its records, and the virtual table materializes results as a `MemTable`. Live data, on demand.

The tasks table shows execution at the fragment level. In single-node mode, each finished query gets one synthetic task row. In distributed mode, each fragment dispatched to a worker gets its own row with the real worker URL. This dual-mode behavior means the same monitoring queries work regardless of deployment topology. You don't need different dashboards for single-node dev and distributed production.

The nodes table is simpler: one row for the coordinator, one row per worker. `env!("CARGO_PKG_VERSION")` pulls the version from `Cargo.toml` at compile time. The `coordinator` boolean column lets you filter nodes by role. This is the same information you'd get from a Kubernetes service endpoint list, but queryable from SQL.

::: {.fieldreport}
**Field report:** The tasks table caught a real bug. During the load test with 50 concurrent clients, we noticed some queries showing tasks on `http://worker-1:50052` that should have gone to `worker-2`. The tasks table made it visible: the fragment scheduler was hashing partition IDs to workers using a scheme that collided on certain partition ranges. We fixed the hash function, and the tasks table confirmed even distribution in the next test run. The tool that monitors the engine found the bug in the engine.
:::


## SHOW Commands: The Parser Wrapping Strategy

Some clients don't query `information_schema` at all. They use `SHOW SCHEMAS`, `SHOW TABLES`, `SHOW CATALOGS`. These are convenience commands -- syntactic sugar over metadata queries -- and not all of them parse cleanly through standard SQL parsers.

The problem: `SHOW CATALOGS` is not part of the SQL standard. `sqlparser-rs` handles `SHOW SCHEMAS` and `SHOW TABLES`, but `SHOW CATALOGS` gets parsed as a `SHOW VARIABLE` or fails entirely. Our custom `EXPLAIN FULL` doesn't parse because no standard dialect recognizes it.

Our parser strategy: wrap `sqlparser-rs`, don't fork it. The `sqe-sql` crate's classifier pre-scans for statements the standard parser doesn't recognize:

```rust
pub fn parse_and_classify(sql: &str) -> sqe_core::Result<StatementKind> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    if upper == "SHOW CATALOGS" || upper.starts_with("SHOW CATALOGS ") {
        return Ok(StatementKind::ShowCatalogs);
    }

    if upper.starts_with("EXPLAIN FULL ") {
        let inner = trimmed["EXPLAIN FULL ".len()..].trim().to_string();
        return Ok(StatementKind::ExplainFull(inner));
    }

    // Standard parsing for everything else
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| sqe_core::SqeError::Execution(format!("Parse error: {e}")))?;

    // ... classify the parsed statement ...
}
```

Each SHOW handler calls the catalog directly and returns a `RecordBatch`. No query planning, no optimization, no execution engine involved. These are metadata lookups that bypass DataFusion entirely because they don't need it.

The wrapping strategy extends to custom SQL we added later. `EXPLAIN FULL` is our enhanced explain that shows partition pruning and cost estimates -- it's not standard SQL, so `sqlparser-rs` would choke on it. The pre-scan catches it before the parser sees it, extracts the inner SQL, and routes it to a dedicated handler. Same pattern, no fork.

The `StatementKind` enum grew to 18 variants as features were added. The classifier has 30 test cases covering every variant, including edge cases like `CREATE OR REPLACE TABLE AS SELECT` (classified as CTAS, not CreateTable) and `ALTER TABLE RENAME COLUMN` (classified as Utility, not Rename). One thing the classifier makes explicit: the boundary between statements that go through DataFusion's planner and statements that bypass it. Queries, CTAS, INSERT, MERGE, DELETE, and EXPLAIN go through the full planning pipeline. SHOW commands, DROP, RENAME, CREATE SCHEMA, and DROP SCHEMA go directly to catalog operations. This routing decision happens once, early, and drives the rest of the execution path.

::: {.devto}
**dev.to connection:** The parser extension pattern follows the same principle described in "When Your SQL Engine Understands Meaning": wrap, don't fork. A forked parser is a maintenance burden that compounds over time. A wrapper that intercepts specific patterns before the standard parser runs is cheap to maintain and easy to test.
:::


## Namespace Resolution

A subtle problem sits at the intersection of catalogs and SQL parsing: how do you resolve `my_schema.my_table`?

Iceberg uses a three-level hierarchy: catalog, namespace, table. SQL tools expect the same three levels: catalog, schema, table. Our `parse_table_ref` function handles the resolution:

```rust
pub fn parse_table_ref(name: &ObjectName)
    -> sqe_core::Result<(NamespaceIdent, String)>
{
    let parts: Vec<String> = name.0.iter()
        .map(|ident| ident.value.clone())
        .collect();

    match parts.len() {
        1 => Ok((NamespaceIdent::new("default".to_string()), parts[0].clone())),
        2 => Ok((NamespaceIdent::new(parts[0].clone()), parts[1].clone())),
        3 => Ok((NamespaceIdent::new(parts[1].clone()), parts[2].clone())),
        n => Err(SqeError::Execution(format!(
            "Invalid table reference with {n} parts: {name}"
        ))),
    }
}
```

One part: the table name, namespace defaults to `"default"`. Two parts: namespace and table. Three parts: catalog, namespace, table -- and we drop the catalog prefix because the catalog was already resolved at session creation. This mirrors how Trino and most SQL engines handle qualified names.

The "default" namespace fallback is important for interactive users. Nobody wants to type `production.finance.transactions` when `finance.transactions` works. But it creates an assumption: there must be a namespace called "default" or the single-name form fails silently. We chose this trade-off over requiring fully qualified names everywhere. The cognitive load on users matters.


## The Full Catalog Hierarchy

Putting it all together, the catalog hierarchy registered in every DataFusion session:

```
<warehouse>                          (SqeCatalogProvider)
  |-- <namespace_1>                  (SqeSchemaProvider)
  |     |-- table_a                  (SqeTableProvider -> Iceberg)
  |     |-- table_b
  |     \-- view_1                   (ViewTable -> planned SQL)
  |-- <namespace_2>
  |     \-- ...
  \-- information_schema             (InformationSchemaProvider)
        |-- tables                   (virtual)
        |-- columns                  (virtual)
        \-- schemata                 (virtual)

system                               (SystemCatalogProvider)
  |-- jdbc                           (JdbcSchemaProvider)
  |     |-- types
  |     |-- catalogs
  |     |-- schemas
  |     |-- tables
  |     \-- columns
  |-- metadata                       (MetadataSchemaProvider)
  |     |-- catalogs
  |     |-- table_properties
  |     |-- schema_properties
  |     \-- table_comments
  \-- runtime                        (RuntimeSchemaProvider)
        |-- queries                  (virtual, live)
        |-- nodes                    (virtual, live)
        \-- tasks                    (virtual, live)
```

Two catalogs. The warehouse catalog holds your data and the standard metadata views. The system catalog holds engine introspection. Both are registered per session, both respect the user's identity, and both are queryable with standard SQL.

The registration happens in two lines:

```rust
ctx.register_catalog(&catalog_name, Arc::new(catalog_provider));
ctx.register_catalog("system", Arc::new(system_catalog));
```

The entire metadata surface of the engine -- information_schema, JDBC tables, metadata tables, runtime tables -- available through standard SQL, scoped to the current user, refreshed on every query.


## The Cost of Getting It Wrong

We learned how much metadata matters the hard way. In the first integration test with DBeaver, the schema browser showed tables but no columns. The `information_schema.columns` virtual table was returning empty because the `build_columns_table` method was iterating over namespaces but not loading tables -- a missing `await` on `load_table` meant the loop skipped every table without error. The Rust compiler didn't catch it because the `match` arm for `Err` returned `continue`, and calling the async function without `.await` produced an unused `Future` warning that we'd silenced in a batch suppression.

The fix was one line. The debugging took two hours, because the symptom -- empty columns -- could have been a Polaris permission issue, an Iceberg schema parsing issue, or a dozen other things. We added an integration test that queries `information_schema.columns` and asserts it returns at least one row. We should have written that test first.

These are the bugs that metadata surfaces get. They're never compile errors. They're always runtime, behavioral, tool-specific, and they manifest as "the tool doesn't work" without telling you why.


## What the Catalog Teaches

The catalog implementation taught us something we should have known from the start: your engine's API is not its query protocol. It's not the Flight SQL endpoint, not the Trino compat layer, not the JDBC driver. The API is the catalog. The metadata. The answer to "what do you have and how is it shaped?"

Every tool in the SQL ecosystem starts by reading the catalog. If the catalog is incomplete, every tool is crippled. If the catalog is slow, every tool feels sluggish. If the catalog lies, every tool built on top of it produces wrong results.

Build a catalog worth reading.
