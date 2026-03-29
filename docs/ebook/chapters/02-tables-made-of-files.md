# Tables Made of Files {#sec:iceberg}

> An Iceberg table is not a table. It is a versioned filesystem
> with opinions about schema.

A database table exists inside a database. You move the database, you move the tables. You lose the database, you lose the tables. You want to query the table from a different engine, you either export it or you connect that engine to the database. The table belongs to the database. The database owns the table. This is how it has been since the 1970s.

A data lake file exists on storage. Anyone with access can read it. But nobody knows what the columns mean, whether the schema changed last Tuesday, or which files belong to a logical table called "orders." You have data, but you do not have a table. You have freedom, but you do not have governance.

Iceberg solves this by putting a metadata tree between you and the files. The metadata is the table. The files are just storage. And the metadata lives alongside the files on the same storage layer — not inside a database, not inside a vendor's service. On S3. On your S3.

That shift — from "the table is inside the database" to "the table is metadata over files" — is the single most important change in data infrastructure in the last decade. It is the foundation that makes a sovereign query engine possible.


## The Metadata Tree

Iceberg's structure is a tree with four levels. Understanding these levels is understanding Iceberg.

At the top is the **metadata file** — a JSON document that describes the table. It contains the current schema, the partition spec, sort orders, and a list of snapshots. Each snapshot represents the table at a point in time. When you write to the table, you create a new snapshot. The old snapshots remain. This is how time travel works.

Each snapshot points to a **manifest list** — an Avro file that contains an entry for each manifest in the snapshot. The manifest list is the index of indices. It records which manifests belong to this snapshot, along with partition-level summary statistics.

Each manifest list entry points to a **manifest file** — also Avro. The manifest contains an entry for each data file it tracks, along with column-level statistics: min values, max values, null counts. These statistics are what make predicate pushdown work. When your query says `WHERE order_date > '2024-01-01'`, the engine reads the manifest, checks the max value of `order_date` in each data file, and skips the files that cannot contain matching rows. The engine never opens those Parquet files. It never reads a single byte from them.

At the bottom are the **data files** — Parquet (or ORC, or Avro, but in practice always Parquet). The actual bytes. The actual rows. The actual columns.

```
metadata.json
  └── snapshot (current)
        └── manifest-list.avro
              ├── manifest-1.avro
              │     ├── data-file-001.parquet  (stats: order_date min=2024-01-01, max=2024-01-31)
              │     └── data-file-002.parquet  (stats: order_date min=2024-02-01, max=2024-02-28)
              └── manifest-2.avro
                    └── data-file-003.parquet  (stats: order_date min=2024-03-01, max=2024-03-31)
```

The tree is small. A table with 10,000 data files might have 10 manifests, one manifest list, and one metadata file. The metadata is typically a few megabytes. The data files might be terabytes. You read megabytes of metadata to decide which terabytes of data to skip.

::: {.iceberg}
**Iceberg deep dive:** The manifest-list-to-manifest-to-data-file hierarchy is not decorative.
Each level provides progressive filtering. The manifest list filters by partition summary
(skip entire partitions). The manifest filters by column statistics (skip individual files
within a partition). The Parquet reader filters by row-group statistics (skip sections
within a file). Three levels of pruning before you read a single row.
:::


## Snapshot Isolation Without a Database

Databases achieve snapshot isolation through MVCC — multi-version concurrency control, implemented with transaction logs and lock managers. Iceberg achieves it through metadata pointer swaps.

The metadata file contains a `current-snapshot-id`. A read query resolves that snapshot ID to a manifest list, and from there to manifests and data files. A write creates new data files, a new manifest (or modifies an existing one), a new manifest list, and a new metadata file pointing to the new snapshot. The write then atomically swaps the metadata pointer — in S3, this is a conditional put; in a catalog like Polaris, it is an atomic commit through the REST API.

Readers see a consistent table. Writers do not block readers. Two writers to different partitions do not conflict. Two writers to the same partition go through optimistic concurrency — one succeeds, the other retries. No locks. No transaction log. No WAL.

The simplicity is disarming. The first time you understand this, it feels too simple. It is not. It has been battle-tested at Netflix, Apple, and Airbnb scale. The simplicity is the point — there are fewer things to break.


## Schema Evolution That Does Not Break Readers

A traditional database ALTER TABLE can be destructive. You add a column, and every reader must know about the new column. You rename a column, and every downstream consumer breaks. You change a type, and the data must be rewritten.

Iceberg schemas are versioned and identified by field IDs, not by field names or positions. Every column has a unique integer ID assigned at creation. If you add a column, it gets a new ID. If you rename a column, the ID stays the same. Old data files were written with the old schema. New data files are written with the new schema. The engine reconciles at read time — old files that lack the new column return nulls for it.

This is schema evolution without data migration. You never rewrite files for a schema change. You update the metadata, and the engine handles the rest.

For a sovereign query engine, this property is critical. The engine does not own the data. Multiple engines might read the same table. If adding a column required rewriting files, every reader would need to coordinate. With Iceberg, there is no coordination. The metadata file records the evolution history, and each reader interprets it independently.


## Partition Evolution

Partitioning in Hive was a directory convention. If you partitioned by `year`, your data lived in `year=2024/` directories. If you later needed to partition by `month`, you had two choices: rewrite all historical data or live with two partitioning schemes and a query engine that could not reason across them.

Iceberg partitioning is metadata, not directory structure. The partition spec says "partition by `year(order_date)`," and the engine evaluates that function when writing. The resulting partition value is recorded in the manifest alongside the data file path. The data file itself can live at any path.

When you evolve the partition spec — say, from yearly to monthly — new data files are written with the new spec. Old data files retain their old partition values. The engine reads both. It knows that data file A was partitioned by year and data file B was partitioned by month, and it filters accordingly. No data rewrite. No migration.

Partition evolution is the feature that convinced me Iceberg was the right table format. Delta Lake and Hudi can handle schema evolution. Delta Lake has since introduced liquid clustering, and Hudi has added its own partition evolution support — the ecosystem is catching up. But Iceberg pioneered transparent partition evolution where old and new partition layouts coexist without data rewriting, and it remains the most mature implementation. For a query engine that does not own the storage, it means you can optimise the physical layout without a maintenance window.


## The PyIceberg Experiments

Before writing a single line of Rust for SQE, I spent months working with Iceberg through Python. The experiments are documented on dev.to, and they shaped the design of everything that came after.

::: {.devto}
**dev.to connection:** "Glue Iceberg Rest Api and PyIceberg" and "Duckberg!" — these articles
trace the path from first discovering the Iceberg REST protocol to understanding what it
actually takes to query Iceberg tables from code you control.
:::

The first experiment was connecting PyIceberg to AWS Glue's Iceberg REST API. Glue had added REST catalog support, which meant you could talk to Glue using the standard Iceberg REST protocol instead of the AWS-specific Glue API. This mattered because it meant the code I wrote would work against any catalog that implemented the same protocol.

PyIceberg made it almost trivially easy:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "glue",
    **{
        "type": "rest",
        "uri": "https://glue.us-east-1.amazonaws.com/iceberg",
        "rest.sigv4-enabled": "true",
        "rest.signing-region": "us-east-1",
    }
)

table = catalog.load_table("my_database.my_table")
scan = table.scan(row_filter="order_date >= '2024-01-01'")
df = scan.to_pandas()
```

Five lines to go from a REST catalog to a Pandas DataFrame. The scan handles manifest reading, file pruning, and Parquet reads. PyIceberg's implementation was clean, well-documented, and correct.

The second experiment bridged two catalogs. Unity Catalog (Databricks) had also implemented the Iceberg REST API, which meant you could read Databricks-managed Iceberg tables from outside Databricks. I wrote about connecting PyIceberg to Unity Catalog and reading the same tables that Databricks' Spark cluster read — from a laptop, with no Databricks runtime.

The third experiment was DuckDB with Iceberg. DuckDB's `iceberg` extension could read Iceberg tables from S3, and combined with a REST catalog, you had a zero-infrastructure query engine for Iceberg. No cluster. No coordinator. Just a binary and a catalog URL.

::: {.devto}
**dev.to connection:** "DuckDB S3 Tables with Iceberg using Iceberg Rest API" — the article
where it clicked that the catalog is the only coordination point. If you control the catalog,
you control access. Everything else is just reading files.
:::

These experiments taught three things:

First, the Iceberg REST protocol is the real standard. Not the Java API. Not the Spark integration. The REST protocol is what enables interoperability, and any serious implementation must start there.

Second, Python is adequate for exploration but inadequate for production query engines. PyIceberg's scan planning works. The scan planning itself is Python-level and GIL-bound, though the underlying Parquet I/O through PyArrow releases the GIL during C++ execution. For a single-user notebook, the distinction doesn't matter. For a multi-user query engine handling concurrent requests, the Python-level bottleneck in scan planning and metadata handling is a real constraint.

Third, the path from "can read Iceberg tables" to "is a query engine" is longer than it looks. DuckDB can scan Iceberg, but it cannot push predicates down to manifest pruning in all cases. PyIceberg can do predicate pushdown, but it cannot do distributed execution. The gap is not in any single component — it is in the integration between catalog, scan planning, predicate pushdown, and query execution.


## iceberg-rust: The Good, The Rough, The Missing

When we decided to build SQE in Rust, the Iceberg library choice was straightforward. There is one: iceberg-rust, the official Apache Iceberg implementation for Rust. At the time we started, it was at version 0.8. By the time we reached production integration tests, it was at 0.9. The version delta matters — 0.9 changed the catalog builder API significantly.

::: {.iceberg}
**Iceberg deep dive:** The workspace Cargo.toml pins `iceberg = "0.9"`, `iceberg-catalog-rest = "0.9"`,
`iceberg-storage-opendal = "0.9"`, and `iceberg-datafusion = "0.9"`. The storage layer uses
OpenDAL rather than iceberg-rust's native FileIO for S3 access, because OpenDAL handles
credential refresh and path-style addressing more reliably.
:::

### What works well

**Scan planning.** The core of Iceberg — reading the metadata tree, evaluating partition filters, reading manifests, applying column statistics to prune data files — works correctly. The `table.scan()` builder gives you a fluent API for column selection and predicate pushdown:

```rust
let scan = table.scan()
    .select(["order_id", "order_date", "total"])
    .with_filter(predicate)
    .build()?;

let batches: Vec<RecordBatch> = scan
    .to_arrow()
    .await?
    .try_collect()
    .await?;
```

The scan respects column projection — it only reads the projected columns from Parquet files, which is a significant I/O optimization. It applies the predicate at the manifest level to skip entire data files. This is the bread and butter of Iceberg, and iceberg-rust implements it correctly.

**Schema conversion.** The `schema_to_arrow_schema` function converts Iceberg schemas to Arrow schemas reliably. This sounds trivial, but Iceberg has types that do not map one-to-one to Arrow (fixed-precision decimals, UUIDs, timestamps with and without timezone), and iceberg-rust handles the edge cases.

**REST catalog client.** The `RestCatalog` implements the full Iceberg REST protocol — namespace listing, table loading, table creation, table commits. It handles the OAuth2 token flow for catalog authentication and credential vending for storage access.

### What we had to work around

**The async-sync boundary.** DataFusion's `CatalogProvider` trait has synchronous methods — `schema_names()` returns `Vec<String>`, not `Future<Vec<String>>`. But iceberg-rust's catalog operations are all async. You cannot call an async function from a sync context without a runtime handle.

The workaround is `tokio::task::block_in_place`:

```rust
fn table_names(&self) -> Vec<String> {
    let handle = tokio::runtime::Handle::try_current()?;
    let catalog = self.session_catalog.clone();
    let ns = NamespaceIdent::new(self.namespace.clone());

    tokio::task::block_in_place(|| {
        handle.block_on(catalog.list_tables(&ns))
    })
}
```

This is not elegant. `block_in_place` tells tokio "I am about to block this thread, please move other tasks elsewhere." It works, but it consumes a thread from the runtime pool. For namespace listing, which happens once per session initialization, the cost is acceptable. For table loading, which happens per query, we use the async `table()` method on `SchemaProvider` instead.

We cached namespace lists at catalog provider construction time to avoid hitting this boundary repeatedly. The `SqeCatalogProvider` fetches all namespace names in its async `try_new()` method and stores them in a `Vec<String>`. The sync `schema_names()` method then returns a clone.

**Credential vending differences between Polaris versions.** When the engine loads a table from Polaris, the REST response includes a `config` section that may contain vended S3 credentials — temporary access keys scoped to that specific table. Polaris 0.9 and Polaris 1.0 return these credentials with different property keys and different expiry formats.

::: {.fieldreport}
**Field report:** The day we discovered that Polaris returns different `file-io` properties
depending on whether you're running Polaris 0.9 or 1.0. The fix was three lines.
The debugging was three days. The credential extraction code now tries RFC3339
timestamps first, then epoch milliseconds, then gives up and falls back to static
credentials. Defensive parsing for config that should be standardized but isn't.
:::

The credential vending module ended up as a 150-line cache backed by moka, with a TTL set conservatively to 50 minutes against a typical 60-minute STS credential lifetime. When vended credentials expire or are absent, the system falls back to static S3 credentials from the engine's configuration. The fallback is critical for development environments where Polaris runs in-memory without credential vending.

**The CatalogBuilder API change from 0.8 to 0.9.** In iceberg-rust 0.8, you constructed a `RestCatalog` with a `RestCatalogConfig`. In 0.9, this changed to a builder pattern with `RestCatalogBuilder::default().load(name, props)`. The migration was mechanical but required touching every test and every catalog construction site.

```rust
// iceberg-rust 0.9 pattern
let catalog = RestCatalogBuilder::default()
    .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
        configured_scheme: "s3".to_string(),
        customized_credential_load: None,
    }))
    .load(
        format!("sqe-session-{}", &token_fingerprint),
        props,
    )
    .await?;
```

The `with_storage_factory` call is required for write operations — without it, `CREATE TABLE` and `INSERT INTO` fail because iceberg-rust does not know how to write to S3. This was not documented at the time. We found it by reading the iceberg-rust source.

::: {.deadend}
**Dead end: iceberg-datafusion's built-in IcebergTableProvider.** The `iceberg-datafusion`
crate provides its own `IcebergTableProvider` that bridges Iceberg tables to DataFusion.
We tried using it directly. The problem: it constructs its own catalog session internally,
so there is no way to inject the user's bearer token. Every query would run as the same
catalog identity. We wrote our own `SqeTableProvider` instead, wrapping the per-user
`Table` object with its already-configured credentials.
:::

### What we built ourselves

The gap between "iceberg-rust can read Iceberg tables" and "DataFusion can query Iceberg tables as a user" required three custom components: a table provider, a catalog provider, and a scan executor. Together, they form the bridge.


## Building the Bridge

### SqeTableProvider: The Table as DataFusion Sees It

DataFusion does not know what Iceberg is. DataFusion knows what a `TableProvider` is — a trait with four key methods:

```rust
trait TableProvider {
    fn schema(&self) -> SchemaRef;
    fn table_type(&self) -> TableType;
    fn supports_filters_pushdown(&self, filters: &[&Expr])
        -> Result<Vec<TableProviderFilterPushDown>>;
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}
```

`schema()` returns the Arrow schema. `table_type()` says whether it is a base table or a view. `supports_filters_pushdown()` tells the optimizer which filter expressions the table provider can handle natively. `scan()` returns an execution plan that will produce record batches when executed.

Our `SqeTableProvider` wraps an Iceberg `Table` object. Construction converts the Iceberg schema to Arrow:

```rust
pub async fn try_new(table: Table) -> Result<Self> {
    let schema = schema_to_arrow_schema(
        table.metadata().current_schema()
    )?;
    Ok(Self { table, schema: Arc::new(schema) })
}
```

The critical method is `supports_filters_pushdown`. We return `Inexact` for every filter we can convert to an Iceberg predicate, and `Unsupported` for the rest. `Inexact` means "I will apply this filter during the scan, but I might not filter every row — DataFusion must still evaluate the filter after scanning." This is correct because Iceberg predicate pushdown prunes manifests and Parquet row groups, but it does not guarantee per-row filtering for all expression types.

::: {.datafusion}
**DataFusion deep dive:** The `Inexact` vs `Exact` distinction matters. If you return `Exact`,
DataFusion removes the filter from the plan — it trusts that the table provider handled it
completely. If you return `Inexact`, DataFusion keeps the filter as a post-scan filter node.
Getting this wrong means silent data corruption: returning `Exact` when your pushdown only
does file-level pruning would skip rows that should have been filtered.
:::

### Predicate Translation: DataFusion Expressions to Iceberg Predicates

DataFusion represents filter conditions as `Expr` — an enum with variants for binary expressions, boolean logic, IN lists, IS NULL, LIKE patterns, and more. Iceberg represents filter conditions as `Predicate` — a different enum with a different structure. The `expr_to_predicate` module translates between them.

The translation handles comparisons (`=`, `!=`, `<`, `>`, `<=`, `>=`), null checks, boolean logic (`AND`, `OR`, `NOT`), IN lists, and prefix LIKE patterns (`col LIKE 'foo%'` becomes `col STARTS WITH 'foo'`). Date casts are deliberately not pushed down because the cast truncates the value, and pushing the truncated value would change the filter semantics.

The interesting design decision is in `AND` vs `OR` handling. For an `AND` predicate where only one side can be converted, we push down the convertible side and let DataFusion handle the rest. This is safe because `Inexact` pushdown means DataFusion will re-evaluate the full filter post-scan. For an `OR` predicate, both sides must convert or we push nothing — because pushing only one side of an OR would widen the result set.

```rust
// AND: partial pushdown is safe (Inexact means DataFusion re-evaluates)
fn to_iceberg_and_predicate(left: TransformedResult, right: TransformedResult)
    -> TransformedResult
{
    match (left, right) {
        (Predicate(l), Predicate(r)) => Predicate(l.and(r)),
        (Predicate(l), _) => Predicate(l),  // push down what we can
        (_, Predicate(r)) => Predicate(r),
        _ => NotTransformed,
    }
}

// OR: both sides must convert, or we drop the whole thing
fn to_iceberg_or_predicate(left: TransformedResult, right: TransformedResult)
    -> TransformedResult
{
    match (left, right) {
        (Predicate(l), Predicate(r)) => Predicate(l.or(r)),
        _ => NotTransformed,  // cannot safely push partial OR
    }
}
```

This asymmetry is subtle. It is also the kind of thing that causes silent correctness bugs if you get it wrong. We wrote 25 unit tests for predicate translation — not because we doubted the logic, but because anyone maintaining this code a year from now needs to know what was tested and what was not.

### IcebergScanExec: The Physical Plan

When DataFusion calls `scan()` on our table provider, we return an `IcebergScanExec` — a custom `ExecutionPlan` node that will actually read data from Iceberg tables via S3.

The execution model has a subtlety. DataFusion's `ExecutionPlan::execute()` is synchronous — it must return a `SendableRecordBatchStream` without awaiting. But Iceberg's scan is inherently async — it needs to read manifests from S3, plan files, and open Parquet readers. The solution is a lazily-initialized stream:

```rust
fn execute(&self, partition: usize, _context: Arc<TaskContext>)
    -> Result<SendableRecordBatchStream>
{
    let table = self.table.clone();
    let projection = self.projection.clone();
    let predicates = self.predicates.clone();

    let stream = futures::stream::once(async move {
        let mut scan_builder = table.scan();
        if let Some(ref cols) = projection {
            scan_builder = scan_builder.select(cols.iter().map(|s| s.as_str()));
        }
        if let Some(pred) = predicates {
            scan_builder = scan_builder.with_filter(pred);
        }
        let scan = scan_builder.build()?;
        let arrow_stream = scan.to_arrow().await?;
        Ok(arrow_stream.map_err(|e| DataFusionError::External(Box::new(e))))
    })
    .try_flatten();

    Ok(Box::pin(IcebergRecordBatchStream {
        schema,
        inner: Box::pin(stream),
        baseline,
    }))
}
```

The `stream::once().try_flatten()` pattern defers the async work to the first poll. When DataFusion pulls the first batch from the stream, the scan initializes, reads manifests, opens Parquet files, and begins streaming record batches. Subsequent polls return additional batches without re-initialization.

The `IcebergRecordBatchStream` wrapper records execution metrics — elapsed time and output row counts — so that `EXPLAIN ANALYZE` can report scan performance. This is not optional instrumentation. Without it, diagnosing slow queries is guesswork.

### SqeCatalogProvider: Mapping Namespaces to Schemas

DataFusion's catalog hierarchy is `Catalog > Schema > Table`. Iceberg's hierarchy is `Catalog > Namespace > Table`. The mapping is direct — an Iceberg namespace becomes a DataFusion schema.

The `SqeCatalogProvider` fetches namespace names at construction time and caches them. When DataFusion asks for a schema by name, it creates a `SqeSchemaProvider` for that namespace. The schema provider in turn creates `SqeTableProvider` instances on demand when DataFusion asks for a table.

The chain of construction follows the user's credentials:

```
User authenticates → SessionCatalog (with bearer token)
    → SqeCatalogProvider (cached namespace list)
        → SqeSchemaProvider (lazy table loading)
            → SqeTableProvider (Iceberg table with vended S3 credentials)
                → IcebergScanExec (reads Parquet from S3)
```

Every link in this chain carries the user's identity. The `SessionCatalog` holds the bearer token. When it loads a table, Polaris validates the token and returns metadata only if the user has access. The vended S3 credentials in the table response are scoped to that user's permissions. The scan reads Parquet files using those scoped credentials. At no point does the engine use a service account.

::: {.sovereignty}
**Sovereignty principle:** The catalog-to-scan credential chain is unbroken. The user's bearer
token flows from authentication through catalog resolution to storage I/O. If the user
does not have access to a table in Polaris, they cannot see it in namespace listings.
If they do not have read access to the underlying S3 path, the scan fails with a storage
error, not a data leak. Authorization is not our code — it is Polaris and S3 doing their
jobs.
:::


## The SessionCatalog: One Catalog Per User

The `SessionCatalog` is the fulcrum. It wraps iceberg-rust's `RestCatalog` in an `RwLock` and configures it with the user's bearer token:

```rust
pub async fn new(
    polaris_url: &str,
    warehouse: &str,
    bearer_token: &str,
    storage_config: &StorageConfig,
) -> Result<Self> {
    let mut props = HashMap::new();
    props.insert("token".to_string(), bearer_token.to_string());
    props.insert("uri".to_string(), polaris_url.to_string());
    props.insert("warehouse".to_string(), warehouse.to_string());

    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 { ... }))
        .load(format!("sqe-session-{}", &token_fingerprint), props)
        .await?;
    // ...
}
```

The session name includes a fingerprint of the token — the last 8 characters. This is deliberate. iceberg-rust's `RestCatalog` caches certain responses internally. If a token is refreshed (the user's session gets a new JWT), the cached responses from the old token must be invalidated. Using a different session name for each token ensures that a refreshed token gets a fresh catalog session with no stale cache entries.

We also built a `SessionCatalogBridge` — a thin wrapper that implements iceberg-rust's `Catalog` trait by delegating to our `SessionCatalog`. This bridge exists for one reason: the `iceberg-datafusion` crate expects a `Catalog` trait object, and our `SessionCatalog` wraps the `RestCatalog` behind an `RwLock` rather than implementing `Catalog` directly. The bridge is boilerplate — every method reads the lock and delegates — but it is necessary for interoperability with the broader iceberg-rust ecosystem.


## Why Rust Changes the Game

PyIceberg taught us what Iceberg is. iceberg-rust taught us what Iceberg can be when the language does not get in the way.

The differences are not about syntax. They are about what happens at scale.

**Memory.** Python's Iceberg implementation reads Parquet files into PyArrow arrays, which then get copied or wrapped when passed to Pandas. In Rust, iceberg-rust produces Arrow `RecordBatch` values that DataFusion consumes directly — zero copy. The data is read from S3 into kernel buffers, deserialized from Parquet into Arrow format, and consumed by the query engine without a single extra allocation for format conversion.

**Concurrency.** PyIceberg's scan planning is single-threaded at the Python level, though PyArrow's Parquet reader releases the GIL during I/O. iceberg-rust's scan produces an async stream of Arrow batches. DataFusion pulls from this stream on its executor threads. When you have multiple concurrent queries — which you always do in a multi-user engine — Rust's async runtime interleaves I/O waits naturally. Python can release the GIL for I/O, but the scan planning, metadata parsing, and predicate evaluation stay on one thread. For true parallelism across concurrent queries, you would need multiprocessing, and multiprocessing means serialising data across process boundaries.

**Predicate pushdown completeness.** PyIceberg pushes predicates down to manifest filtering. So does iceberg-rust. But iceberg-rust integrates with DataFusion's filter pushdown protocol, which means the optimizer can reason about pushdown across the entire query plan. A join with a filter can push the filter down through the join and into the Iceberg scan — something that requires explicit coding in Python but happens automatically through DataFusion's optimizer rules.

**Type safety.** The predicate translation module converts DataFusion expressions to Iceberg predicates. In Python, this would be a series of isinstance checks with silent fallback to no pushdown. In Rust, the match is exhaustive — the compiler tells you when you have missed a variant. When iceberg-rust added the `STARTS_WITH` predicate operator, any code that matched on `PredicateOperator` variants would have failed to compile until updated. In Python, the new operator would have been silently ignored.

None of these differences matter for a single query on a laptop. All of them matter for a hundred concurrent queries on a production cluster.


## The View Problem

Iceberg v2 and v3 support views — SQL definitions stored in the catalog alongside tables. A view is metadata: a SQL string, a schema, and a reference to the namespace where the view's tables live.

iceberg-rust 0.9 does not have first-class view support. The `Catalog` trait has no `load_view` or `create_view` method. We implemented views by talking to the Polaris REST API directly, bypassing iceberg-rust entirely.

The `SessionCatalog` makes HTTP calls to the Polaris views endpoints:

- `POST /v1/{prefix}/namespaces/{ns}/views` to create a view
- `GET /v1/{prefix}/namespaces/{ns}/views` to list views
- `GET /v1/{prefix}/namespaces/{ns}/views/{name}` to load a view's SQL
- `DELETE /v1/{prefix}/namespaces/{ns}/views/{name}` to drop a view

When the `SqeSchemaProvider` resolves a table name, it first tries to load it as an Iceberg table through iceberg-rust. If that fails, it tries to load it as a view through the direct REST call. If it finds a view, it takes the SQL string, plans it through a mini DataFusion `SessionContext` with the same catalog registered, and returns the resulting `LogicalPlan` wrapped in a `ViewTable`.

This is a workaround, not a solution. When iceberg-rust adds native view support, we will migrate to it. But the workaround is complete — views work for SELECT, for joins, for nested views — and it ships.

::: {.deadend}
**Dead end: waiting for iceberg-rust view support.** We considered waiting. The iceberg-rust
project had view support on its roadmap. But "on the roadmap" and "in a released version"
are different things. We needed views for dbt compatibility. We built the REST workaround
in a day. The cost of carrying it is low — it is isolated in `SessionCatalog` and touched
nowhere else.
:::


## The Scan Lifecycle

When a user runs `SELECT order_id, total FROM orders WHERE order_date > '2024-01-01'`, the path from SQL to bytes is:

1. **Parse.** DataFusion parses the SQL into an AST.
2. **Plan.** The planner resolves `orders` by asking the `SqeCatalogProvider` for the default schema, then asking the `SqeSchemaProvider` for the table. The schema provider calls `SessionCatalog::load_table`, which makes a REST call to Polaris with the user's bearer token. Polaris returns the table metadata and vended S3 credentials. `SqeTableProvider::try_new` converts the Iceberg schema to Arrow.
3. **Optimize.** DataFusion's optimizer pushes the `order_date > '2024-01-01'` filter down toward the table scan. It calls `supports_filters_pushdown` and learns that this filter is pushdown-capable (`Inexact`). The optimizer also pushes the column projection — only `order_id` and `total` need to be read.
4. **Execute.** DataFusion calls `scan()` on the table provider with the projection `[0, 2]` (the column indices) and the filter expression. The provider creates an `IcebergScanExec` with the converted Iceberg predicate and the projected column names.
5. **Scan.** On first poll, the `IcebergScanExec` builds an Iceberg scan with column selection and predicate filter. The scan reads the manifest list, applies the predicate to manifest-level statistics to skip irrelevant manifests, then reads the remaining manifests and applies column-level statistics to skip irrelevant data files.
6. **Read.** For each surviving data file, the scan opens the Parquet file using the vended S3 credentials, reads only the projected columns, and returns Arrow `RecordBatch` values.
7. **Post-filter.** DataFusion applies the filter expression again on the returned batches (because the pushdown was `Inexact`), ensuring row-level correctness.
8. **Return.** The filtered, projected batches are streamed to the client via Arrow Flight.

The user sees a result set. The engine opened exactly the Parquet files it needed, read exactly the columns it needed, and applied the filter at every level where it could — manifest, file, row group, and row.


## Tables That Travel

A traditional table is bound to its database. Move the database, you move the table. Lose the database, you lose the table. Query it from another engine, you need a bridge, an export, or a compatible wire protocol.

An Iceberg table is a metadata pointer to files on storage. The metadata is JSON and Avro. The data is Parquet. Any engine that reads these formats can read the table. Any catalog that implements the Iceberg REST protocol can serve the metadata. The table does not belong to the engine. The table does not belong to the catalog. The table belongs to whoever has the files.

This is what makes a sovereign query engine possible. SQE does not own your tables. Polaris does not own your tables. If you stop running SQE tomorrow, your tables are still there — on S3, in Parquet, described by Iceberg metadata that any Iceberg-compatible engine can read. DuckDB can read them. Spark can read them. Trino can read them. A PyIceberg script can read them.

The engine is disposable. The data is not.

iceberg-rust made this real in Rust: a scan planner that prunes intelligently, a schema converter that handles the edge cases, and a REST catalog client that passes through user credentials. We built the bridge — table provider, catalog provider, scan executor — and DataFusion became an Iceberg query engine.

The bridge was about 1,200 lines of Rust across six files. That is all it takes to connect a query engine to the entire Iceberg ecosystem. The ratio matters. We wrote 1,200 lines. We got access to every table, in every namespace, in every catalog that speaks the Iceberg REST protocol.

::: {.ailog}
**AI Logbook:** The AI implemented `SqeTableProvider`, `SqeCatalogProvider`, `IcebergScanExec`, and the predicate translation module — including the subtle AND-vs-OR partial pushdown asymmetry — from a design doc that described the DataFusion trait interfaces. The human specified the `Inexact` vs `Exact` pushdown semantics and the security implication of getting them wrong. The `stream::once().try_flatten()` pattern for deferring async scan initialization inside a synchronous `execute()` method was the AI's solution; it worked on the first attempt.
:::
