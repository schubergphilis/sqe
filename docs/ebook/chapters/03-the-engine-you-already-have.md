# The Engine You Already Have {#sec:datafusion}

> Every query engine is a library pretending to be a service.
> DataFusion drops the pretence.

We needed a query engine. Not a toy. Not a prototype. A real SQL engine that could parse complex queries, optimize them, push predicates into Iceberg manifests, stream Arrow record batches back to clients, and do all of it as the authenticated user. The kind of thing that takes a team of twenty engineers three years to build from scratch.

We had three months and two people.

The obvious answers were obvious for a reason. Trino was what we were replacing -- the auth model couldn't be fixed without forking the entire coordinator. Spark is a cluster computing framework that happens to support SQL, which is like using a forklift to move a chair. DuckDB is brilliant but single-process, single-node, and designed for a different problem.

Then someone pointed at DataFusion and said: "What if the engine already exists, and it's a library?"

That question changed the project. It went from "build a query engine" to "build the parts of a query engine that make it ours." The parser, the optimizer, the execution runtime, the Arrow memory model -- someone already built those. What nobody had built was the authentication model, the catalog integration, and the policy enforcement that would make the engine sovereign.

The distinction between a library and a service turns out to be the most important architectural decision in the entire project. Not which language. Not which protocol. Whether you own the process or someone else does.


## The Fifty-Line Query Engine

DataFusion is not a database. It is not a service. It is a Rust crate you add to `Cargo.toml`, and it gives you a complete SQL query engine that runs inside your process. No cluster. No daemon. No configuration server. No JVM. Just a function call.

Here is a query engine:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv("users", "data/users.csv", CsvReadOptions::new()).await?;

    let df = ctx.sql("SELECT name, age FROM users WHERE age > 30").await?;
    df.show().await?;

    Ok(())
}
```

That is a working SQL engine. It parses the SQL. It builds a logical plan. It optimizes the plan (predicate pushdown, projection pruning, constant folding). It creates a physical execution plan. It executes that plan and streams Arrow record batches to your terminal. Twelve lines of code.

The `SessionContext` is the unit of execution. It holds the catalog (what tables exist), the configuration (how to optimize), and the runtime (where to execute). One `SessionContext`, one query environment. You can create as many as you want. They share nothing unless you explicitly connect them.

This is the property that made SQE possible. Every user query gets its own `SessionContext`, configured with that user's catalog view, that user's credentials, that user's policy constraints. There is no shared mutable state between users. There is no "connection pool" that accidentally leaks one user's permissions to another.


## From SQL String to Arrow Batches

A SQL query goes through five stages in DataFusion. Understanding these stages is understanding why DataFusion is extensible in ways that monolithic engines are not.

![DataFusion query pipeline: SQL string through parsing, logical planning, optimization, physical planning, and execution to Arrow batches](diagrams/rendered/03-datafusion-pipeline.svg)

**Stage 1: Parsing.** The SQL string is parsed by `sqlparser-rs` into an abstract syntax tree (AST). DataFusion uses the same SQL parser that dozens of other Rust projects use. It handles standard SQL well. Non-standard extensions (Trino's `SHOW CATALOGS`, our own `EXPLAIN FULL`) need to be handled before or after the parser.

**Stage 2: Logical Planning.** The AST is converted into a `LogicalPlan` -- a tree of relational algebra operators. A `SELECT name FROM users WHERE age > 30` becomes a `Projection` over a `Filter` over a `TableScan`. The logical plan describes *what* data to compute, not *how* to compute it.

**Stage 3: Optimization.** DataFusion's optimizer runs a series of rewrite rules over the logical plan. Predicates get pushed down toward the table scans. Projections get pruned to eliminate unused columns. Common subexpressions get eliminated. The optimizer is a pipeline of `OptimizerRule` implementations, and you can add your own rules or disable built-in ones.

This is where security enforcement happens in SQE. Before the optimizer runs, we inject policy filters into the logical plan -- row filters above the table scan, column masks that replace sensitive expressions. The optimizer then pushes the user's predicates through our security filters, but it cannot push them past masked columns. More on this in Chapter 8.

**Stage 4: Physical Planning.** The optimized logical plan is converted into a `PhysicalPlan` -- a tree of `ExecutionPlan` nodes that describe the actual computation. A logical `Filter` becomes a `FilterExec`. A logical `TableScan` becomes whatever the `TableProvider` returns from its `scan()` method. In SQE, that is an `IcebergScanExec`.

**Stage 5: Execution.** The physical plan is executed using a pull-based model descended from the Volcano iterator model (Graefe, 1994), but adapted for vectorised execution. Where classic Volcano pulls one row at a time, DataFusion pulls one `RecordBatch` at a time, closer to the morsel-driven parallelism model (Leis et al., 2014) than to pure Volcano. Each `ExecutionPlan` node implements an `execute()` method that returns a stream of Arrow `RecordBatch` values. The top-level node pulls from its children, which pull from their children, all the way down to the leaf scan nodes. Data flows upward as a lazy stream -- nothing is computed until someone reads from the output.

A caveat: "pull-based" describes the pipeline, not every operator. Some operators materialise intermediate state by necessity. A `HashJoinExec` eagerly builds the hash table from its build side before streaming the probe side. An `AggregateExec` accumulates state across all input batches before producing output. These are pipeline breakers -- they consume their entire input before producing any output. The pull model means the pipeline *between* these breakers is lazy, and data streams through non-blocking operators (Filter, Projection) without buffering.

The pull model still matters for memory. If the client only reads the first 100 rows, the scan might never read the remaining files. Memory usage is proportional to the pipeline width plus the materialised state of any pipeline breakers -- not the total data volume.

Here is what that looks like in SQE's `execute_query` method:

```rust
async fn execute_query(
    &self,
    session: &Session,
    sql: &str,
    query_id: &uuid::Uuid,
) -> sqe_core::Result<Vec<RecordBatch>> {
    let ctx = self.create_session_context(session).await?;

    // Stage 1+2: Parse SQL and build a logical plan
    let df = ctx
        .sql(sql)
        .await
        .map_err(|e| SqeError::Execution(format!("SQL planning failed: {e}")))?;

    // Security: inject policy filters before optimization
    let plan = df.logical_plan().clone();
    let enforced_plan = self
        .policy_enforcer
        .evaluate(&session.user, plan)
        .await?;

    // Stage 3: Optimize the policy-enforced logical plan
    let enforced_df = ctx
        .execute_logical_plan(enforced_plan)
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to create execution plan: {e}")))?;

    // Stage 4: Create the physical execution plan
    let physical_plan = enforced_df
        .create_physical_plan()
        .await
        .map_err(|e| SqeError::Execution(format!("Physical plan creation failed: {e}")))?;

    // Try to distribute scan work across workers
    let final_plan = self.try_distribute(physical_plan, session, query_id).await;

    // Stage 5: Execute and collect Arrow batches
    let batches = collect(final_plan, ctx.task_ctx())
        .await
        .map_err(|e| SqeError::Execution(format!("Query execution failed: {e}")))?;

    Ok(batches)
}
```

That is the entire query pipeline. SQL string in, Arrow record batches out. Between those two points, DataFusion handles parsing, logical planning, optimization, physical planning, and execution. SQE inserts itself at two points: policy enforcement (between logical planning and optimization) and distribution (between physical planning and execution). Everything else is DataFusion.

::: {.datafusion}
**DataFusion deep dive:** DataFusion 52 ships with over 100 optimizer rules, including
predicate pushdown, projection pushdown, common subexpression elimination, constant folding,
filter rewriting, join reordering, and limit pushdown. Each rule implements `OptimizerRule`
and returns a rewritten `LogicalPlan`. SQE's policy enforcement is implemented as a plan
rewrite that runs *before* these rules -- this is intentional. The optimizer can then push
user predicates through our security filters, which improves performance while maintaining
the security invariant.
:::


## The SessionContext Is the Product

In a monolithic database, everything is coupled. The catalog is bound to the storage layer. The storage layer is bound to the compute engine. The compute engine is bound to the cluster manager. You can't change one without changing all of them.

DataFusion separates these concerns into traits that you implement.

In SQE, the `create_session_context` method builds a fresh `SessionContext` for each query, configured with the user's credentials:

```rust
async fn create_session_context(
    &self,
    session: &Session,
) -> sqe_core::Result<SessionContext> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema(&catalog_name, "default"),
    );

    // Create a per-session catalog connected to Polaris
    // with the user's bearer token
    let session_catalog = Arc::new(
        SessionCatalog::new(
            &self.config.catalog.polaris_url,
            &self.config.catalog.warehouse,
            &session.access_token,
            &self.config.storage,
        )
        .await?,
    );

    let catalog_provider = SqeCatalogProvider::try_new(
        session_catalog,
        self.config.storage.clone(),
        self.config.catalog.warehouse.clone(),
    )
    .await?;

    ctx.register_catalog(&catalog_name, Arc::new(catalog_provider));

    Ok(ctx)
}
```

Every query gets its own `SessionContext`. Every `SessionContext` gets its own `SessionCatalog`, initialized with the user's bearer token. That token flows through to Polaris for catalog operations and to S3 for data access. There is no shared state between users. There is no privilege escalation path.

This is what "library, not service" means in practice. A service gives you its `SessionContext` with its catalog, its credentials, its policies. A library lets you construct the `SessionContext` yourself, with whatever catalog, credentials, and policies you need.


## The Catalog Hierarchy

DataFusion organizes data through a three-level hierarchy: `CatalogProvider` contains `SchemaProvider` instances, which contain `TableProvider` instances. This maps cleanly to Iceberg's organization: a Polaris catalog contains namespaces, which contain tables.

SQE implements all three levels.

The `SqeCatalogProvider` bridges DataFusion's catalog concept to Iceberg namespaces:

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

        if !self.cached_namespaces.contains(&name.to_string()) {
            return None;
        }

        Some(Arc::new(SqeSchemaProvider::new(
            self.session_catalog.clone(),
            name.to_string(),
            self.storage_config.clone(),
            self.warehouse.clone(),
        )))
    }
}
```

The `SqeSchemaProvider` lists tables within a namespace by calling the Iceberg REST catalog:

```rust
#[async_trait]
impl SchemaProvider for SqeSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        let handle = tokio::runtime::Handle::try_current().ok()?;
        let ns_ident = NamespaceIdent::new(self.namespace.clone());

        // NOTE: DataFusion's SchemaProvider::table_names() is synchronous by design
        // (returns Vec<String>, not a Future). Since our catalog is async, we use
        // block_in_place to bridge the gap. This is a known DataFusion limitation --
        // calling block_on inside an async runtime can deadlock under certain executor
        // configurations. We use block_in_place to yield the current thread first.
        let tables = tokio::task::block_in_place(||
            handle.block_on(self.session_catalog.list_tables(&ns_ident))
        );
        // ... collect table names and view names
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Try as Iceberg table first, then as Iceberg view
        match self.session_catalog.load_table(&table_ident).await {
            Ok(table) => {
                Ok(Some(Arc::new(SqeTableProvider::try_new(table).await?)))
            }
            Err(_) => {
                // Try loading as a view...
            }
        }
    }
}
```

And the `SqeTableProvider` wraps an Iceberg table so DataFusion can scan it:

```rust
#[async_trait]
impl TableProvider for SqeTableProvider {
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let projected_columns = projection.map(|indices| {
            indices.iter()
                .map(|&i| self.schema.field(i).name().clone())
                .collect::<Vec<_>>()
        });

        let predicates = expr_to_predicate::convert_filters_to_predicate(filters);

        Ok(Arc::new(IcebergScanExec::new(
            self.table.clone(),
            projected_schema,
            projected_columns,
            predicates,
        )))
    }
}
```

The `scan()` method is where DataFusion meets Iceberg. DataFusion passes down the column projection (only read the columns the query needs) and the filter expressions (only scan the files that might contain matching rows). SQE converts the DataFusion filter expressions into Iceberg predicates that prune manifest files and Parquet row groups. The actual data reading happens in `IcebergScanExec`, which uses iceberg-rust's scan API to read Parquet files from S3 using the user's vended credentials.

Each layer does exactly one thing. The catalog knows what exists. The schema knows what's in a namespace. The table knows how to scan data. DataFusion orchestrates them. SQE provides the implementations. The user's identity flows through all of it.

::: {.datafusion}
**DataFusion deep dive:** The `CatalogProvider` -> `SchemaProvider` -> `TableProvider` hierarchy
is DataFusion's primary extension point for data sources. Implementing these three traits is
how you teach DataFusion to read from anything -- Iceberg, Delta Lake, Hudi, a REST API,
a flat file on disk. The `TableProvider::scan()` method returns an `ExecutionPlan`, which
means you control exactly how data is read. SQE's `IcebergScanExec` uses iceberg-rust's
scan builder to apply column projection, predicate pushdown, and manifest pruning before
reading a single byte from S3.
:::


## The IcebergScanExec: Where Compute Meets Storage

The leaf node of every query plan in SQE is an `IcebergScanExec`. This is the `ExecutionPlan` that actually reads data from Iceberg tables via S3. It implements DataFusion's `ExecutionPlan` trait, which means DataFusion treats it like any other execution node -- it just happens to read from Iceberg instead of CSV or Parquet files on local disk.

```rust
impl ExecutionPlan for IcebergScanExec {
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
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
}
```

The `execute()` method is synchronous (DataFusion's trait requires it), but it returns an async stream. The actual scan is lazy -- iceberg-rust's `to_arrow()` doesn't fetch data until the stream is polled. This means DataFusion's pull-based execution model works naturally: data flows only when the upstream operator asks for the next batch.

The `table.scan()` call uses the `FileIO` instance that was configured with the user's vended S3 credentials when the table was loaded from the catalog. Every byte read from S3 is attributed to the user who initiated the query. This is the bearer passthrough architecture from Chapter 4 working at the storage layer.

Note the `BaselineMetrics` field. Every `IcebergScanExec` tracks wall-clock time and output row counts. This feeds into `EXPLAIN ANALYZE` output, so you can see exactly how long each scan took and how many rows it produced. Observability is not bolted on. It is part of the execution plan interface.

This is also where distributed execution hooks in. In Chapter 13, we replace the local `IcebergScanExec` with a `DistributedScanExec` that sends the scan work to remote workers. The rest of the plan -- the filters, projections, aggregations above the scan -- stays on the coordinator. The replacement is a tree transformation on the physical plan. DataFusion doesn't know or care that the leaf nodes are now remote. It pulls from them the same way it pulls from local scans.


## Why Rust

The choice of Rust was not ideological. It was practical.

**Arrow is native Rust.** Apache Arrow's canonical implementation is in Rust (`arrow-rs`). DataFusion is written in Rust. iceberg-rust is written in Rust. The entire stack from SQL parsing to S3 byte reading is Rust, which means zero serialization boundaries, zero JNI bridges, zero FFI overhead. A `RecordBatch` returned by iceberg-rust is the same `RecordBatch` that DataFusion processes, in the same memory layout, with zero copies.

**`Send + Sync` gives you parallelism for free.** Rust's type system enforces thread safety at compile time. If your type is `Send + Sync`, it can be shared across threads without data races. DataFusion's `ExecutionPlan` trait requires `Send + Sync`, which means every execution node is safe to execute in parallel. You don't write synchronization code. You don't debug race conditions. The compiler rejects code that would have race conditions.

**Ownership prevents resource leaks.** When a `SessionContext` is dropped, every resource it holds is dropped. The catalog connection closes. The cached metadata is freed. The S3 credentials are zeroed. There is no garbage collector that might hold onto credentials longer than expected. There is no finalizer that might not run.

**The binary is self-contained.** SQE compiles to a single static binary. No JVM to install. No Python interpreter to manage. No shared libraries to version. The Docker image is 47MB. The binary starts in under a second. Deployment is copying a file.

**AI agents write Rust well.** This is an observation, not a hypothesis. Rust code is dense and expressive -- a function signature often tells you what the function does, what it takes, what it returns, and what can go wrong. The borrow checker catches the mistakes that would be runtime bugs in other languages. When an AI generates Rust code that compiles, it is very likely correct. When it generates code that doesn't compile, the error messages are specific enough to guide the fix. We built SQE in 15 days. The AI wrote most of the implementation. The human made the architectural decisions and reviewed every commit. That ratio -- AI implements, human decides -- works because Rust's type system catches the implementation bugs that a human reviewer would miss.

::: {.antipattern}
**Antipattern: choosing Rust for the wrong reasons.** Rust is not the right choice because
it's fast. It's fast, but that's a side effect. Rust is the right choice for SQE because
the Arrow ecosystem is Rust-native, because the type system prevents the class of bugs
that query engines are most prone to (data races, use-after-free, resource leaks), and
because the deployment story (single binary, small container) matches the sovereignty
requirement. If your query engine is a Python wrapper around DuckDB, Rust adds nothing.
Pick the language that matches your constraints.
:::


## The Compile-Time Tax

Rust is not free. The cost is compile time.

A clean build of SQE -- all 12 crates, all dependencies -- takes minutes. Not seconds. Minutes. DataFusion alone pulls in hundreds of transitive dependencies. Add Arrow, iceberg-rust, tonic (for gRPC), and tokio, and the dependency graph is enormous.

Incremental builds save you most of the time. Change a line in `sqe-coordinator`, and only `sqe-coordinator` and its reverse dependencies recompile. That's usually 15-30 seconds. Tolerable.

But certain changes blow up the incremental cache. A macro change in `sqe-core` forces a full rebuild of everything that depends on `sqe-core` -- which is every crate. A dependency version bump in `Cargo.toml` can trigger a cascade. And CI builds from a clean state every time (unless you configure caching carefully), so your pipeline always pays the full price.

We learned this the hard way. Our CI build went from 4 minutes to 18 minutes when we added the distributed execution crates. The fix wasn't faster hardware. It was architectural.

Here is what we did:

**Feature-gated crates.** The `sqe-bench` and `sqe-trino-compat` crates are optional. If you're working on the coordinator, you don't compile the benchmark suite. This shrank the default build graph significantly.

**Dev profile tuning.** In `Cargo.toml`:

```toml
[profile.dev]
debug = 1                       # Line-tables only, not full debug info
split-debuginfo = "unpacked"    # Skip dsymutil on macOS -- saves 20-30% link time

[profile.release]
codegen-units = 4               # More parallelism during codegen
lto = "thin"                    # 80% of full LTO benefit, much faster
strip = true                    # Smaller binary
```

**`cargo check` as the fast feedback loop.** `cargo check` verifies that your code compiles without actually producing binaries. It runs in a fraction of the time. We run `cargo check` after every save and `cargo build` only when we need to run the engine.

**sccache for CI.** Shared compilation cache across CI runs. Changed builds from 18 minutes to 6 minutes for the common case.

The honest trade-off: you pay compile time upfront, and you save debugging time in production. A null pointer in C++ crashes at 3am. A race condition in Java shows up under load. In Rust, these bugs don't exist -- the compiler rejected them at build time. The question is whether you'd rather wait 30 seconds for the compiler or spend four hours debugging a production incident. At scale, this cost compounds in both directions. Plan for it from day one.

::: {.fieldreport}
**Field report:** Our CI build went from 4 minutes to 18 minutes when we added the distributed
execution crates. The fix wasn't faster hardware -- it was splitting the workspace into
feature-gated crates so you only compile what you're changing. `profile.dev` tuning
(line-tables-only debug info, unpacked debuginfo on macOS) saved another 20-30% of link
time. Compile times are a design constraint, not just an annoyance.
:::


## The Competition: DuckDB, Spark, Trino

We evaluated three alternatives before committing to DataFusion. Each one taught us something about what we actually needed.

### DuckDB

DuckDB is an extraordinary piece of engineering. It is the SQLite of analytics -- an embeddable, single-process OLAP engine that is blisteringly fast on data that fits on one machine. If your problem is "run analytical queries on local Parquet files," DuckDB is the answer. Stop reading. Go use DuckDB.

I wrote about DuckDB with Iceberg in 2025 -- querying S3 tables via the Iceberg REST API. It worked beautifully for single-user exploration. The query latency was lower than anything else I'd tested. The developer experience was outstanding.

::: {.devto}
**dev.to connection:** "DuckDB S3 Tables with Iceberg using Iceberg Rest API" (2025) -- this article
explored DuckDB's Iceberg extension for single-user analytics. The performance was
excellent. The limitation was the one we couldn't work around: single-user, single-process.
:::

Our problem was different. We needed multi-user isolation (one engine, many users, each seeing only their data). We needed bearer token passthrough (every S3 access attributed to a specific human). We needed distributed execution across workers. And we needed to control the query plan for policy enforcement -- injecting row filters and column masks before the optimizer runs.

DuckDB is not designed for any of these. It is a single-user, single-process engine. Its extension model allows custom table functions and file readers, but not user-extensible plan rewriting, not custom authentication, not distributed execution. DuckDB has a sophisticated internal optimizer that rewrites plans aggressively -- but you cannot inject your own rewrite rules from outside. You can embed DuckDB in a multi-user service, but you end up building the multi-tenancy, auth, and distribution layers yourself -- which is exactly what DataFusion gives you as traits to implement.

| Dimension | DuckDB | DataFusion |
|-----------|--------|------------|
| Model | Embedded database | Query engine library |
| Multi-user | No (single-process) | Yes (SessionContext per user) |
| User-extensible plan rewriting | No (internal optimizer only) | Yes (LogicalPlan is a tree you can transform) |
| Distribution | No | Yes (PhysicalPlan nodes are serialisable) |
| Extension model | UDF, table functions | CatalogProvider, TableProvider, ExecutionPlan, OptimizerRule |
| Language | C++ | Rust |
| Best for | Single-user analytics, embedded BI | Building custom query engines |

### Spark

Apache Spark is the Swiss army knife of data processing. It does batch. It does streaming. It does ML. It does graph processing. It also does SQL, through Spark SQL, which is a capable query engine with a Hive-compatible catalog and a distributed execution model.

The problem with Spark is that it is Spark. The JVM alone adds hundreds of megabytes to the container image. The driver-executor model requires cluster management (YARN, Kubernetes, or Spark Standalone). Configuration is an art form -- `spark.executor.memory`, `spark.sql.shuffle.partitions`, `spark.dynamicAllocation.enabled` -- and getting it wrong means either wasted resources or OOM errors.

More fundamentally, Spark's auth model is service-account-based. The Spark driver holds credentials, and all executors use those credentials. There is no per-user credential passthrough in the execution layer. You can audit at the application level (who submitted the job), but S3 sees one identity for everything.

We looked at IOMete, which runs Spark on Kubernetes with a cloud-independent model. It is well-executed. But it's still Spark -- still JVM, still service-account auth, still the complexity of a distributed computing framework when what we wanted was a distributed query engine.

There's a subtler problem. Spark's Catalyst optimizer is powerful, but it's a closed system. You can add custom rules, but the internals are tightly coupled. Modifying how Spark handles authentication at the storage layer means modifying Spark itself -- the Hadoop credential provider chain, the FileSystem abstraction, the delegation token mechanism. These are not extension points. They are implementation details that happen to be accessible because the code is open source. Every Spark upgrade risks breaking your modifications.

DataFusion's approach is different. The extension points are the product. `CatalogProvider`, `TableProvider`, `ExecutionPlan` -- these are stable traits with documented contracts. You implement them and DataFusion calls them. Upgrades don't break your implementations unless the trait itself changes, and trait changes are breaking changes that show up in the changelog.

### Trino

Trino is what we were replacing. It is a distributed SQL query engine with a connector architecture, an optimizer, and a coordinator-worker model. It does SQL well. Its connector ecosystem covers dozens of data sources.

The problem was the auth model. Trino authenticates the user at the coordinator, then the coordinator uses its own service account to access data. The coordinator is the security boundary. If the coordinator is compromised, every data source it connects to is exposed. And the CloudTrail trail shows one actor -- the Trino service account -- for every query by every user.

We tried to fix this. We maintained a fork of the Trino Iceberg connector (the "DCAF branch") that passed bearer tokens through to Polaris. Every upstream Trino release required re-merging our changes. Each workaround made the system more complex without making it more secure. After two years of this, the question stopped being "can we fix Trino's auth model" and became "should we."

The answer was no. The auth model is not a bug in Trino. It is a design decision. Trino is designed around the assumption that the engine holds ambient credentials. Changing that assumption would require rewriting the coordinator, the worker communication protocol, the connector interface, and the credential management layer. At that point you are not patching Trino. You are building a new engine with Trino's SQL parser.

Which is approximately what we did. Just with DataFusion's SQL parser instead.

| Dimension | DuckDB | Spark | Trino | DataFusion |
|-----------|--------|-------|-------|------------|
| Model | Embedded DB | Distributed compute framework | Distributed SQL engine | Query engine library |
| Language | C++ | Scala/Java | Java | Rust |
| Auth model | N/A (embedded) | Service account | Service account | You implement it |
| Extension model | UDFs, table functions | Catalyst rules, data sources | Connectors | Traits (Catalog, Table, Plan, Rule) |
| Deployment | In-process | Cluster (YARN/K8s) | Cluster (coordinator + workers) | Your binary, your way |
| Per-user isolation | No | No | Partially (query-level) | Yes (SessionContext) |
| Plan rewriting | No | Yes (Catalyst) | Limited | Yes (LogicalPlan is a tree) |


## The Extensibility Model

DataFusion's power is not in what it does. It is in what it lets you replace.

Every significant component is a trait:

| Trait | What It Controls | SQE Implementation |
|-------|------------------|--------------------|
| `CatalogProvider` | What catalogs/schemas exist | `SqeCatalogProvider` (bridges to Polaris) |
| `SchemaProvider` | What tables exist in a schema | `SqeSchemaProvider` (lists Iceberg namespace) |
| `TableProvider` | How a table is scanned | `SqeTableProvider` (wraps Iceberg table) |
| `ExecutionPlan` | How a physical operation executes | `IcebergScanExec`, `DistributedScanExec` |
| `OptimizerRule` | How the logical plan is rewritten | Policy enforcement (row filters, column masks) |

You don't subclass. You don't monkey-patch. You implement a trait and register it. DataFusion calls your implementation at the right time, in the right context, with the right inputs. The contract is the trait signature. The compiler enforces it.

This is why the "80/20" framing is accurate. DataFusion gives you the SQL parser, the logical planner, the optimizer, the physical planner, the execution runtime, the Arrow memory model, and the pull-based streaming. That's 80% of a query engine. The remaining 20% -- the catalog integration, the storage layer, the auth model, the policy enforcement, the distribution strategy -- is your product. It's the part that makes your engine different from every other engine built on DataFusion.

SQE's `Cargo.toml` tells the story:

```toml
datafusion = "52"
datafusion-common = "52"
datafusion-expr = "52"
datafusion-sql = "52"

arrow = { version = "57", features = ["prettyprint"] }
arrow-flight = { version = "57", features = ["flight-sql-experimental"] }

iceberg = { git = "https://github.com/risingwavelabs/iceberg-rust.git", rev = "1978911ec4" }
iceberg-catalog-rest = { git = "https://github.com/risingwavelabs/iceberg-rust.git", rev = "1978911ec4" }
```

DataFusion 52, Arrow 57, Iceberg 0.9. Three version numbers that represent tens of thousands of hours of engineering by communities we will never have to hire. The SQL parsing, the columnar memory layout, the table format -- all solved problems. SQE's contribution is the glue: how these pieces fit together under a sovereign auth model.

::: {.antipattern}
**Antipattern: reimplementing what DataFusion already does.** The temptation is strong.
You see how DataFusion's CSV reader works and think "I could write a better one for my
use case." Maybe. But now you maintain a CSV reader, and you miss every bug fix and
performance improvement the DataFusion community ships. Implement traits. Don't rewrite
internals. The 20% that's yours is plenty of work.
:::


## The Separation That Changes Everything

The deepest insight from building on DataFusion is not about Rust, or Arrow, or SQL parsing. It is about separation.

In a traditional database, the catalog is the database. The storage is the database. The compute is the database. You cannot use the catalog without the compute engine. You cannot access the storage without the catalog. Everything is one product, one vendor, one bill.

DataFusion separates these concerns into independent layers:

**Catalog: what tables exist, where they are.** In SQE, this is Polaris -- an Apache Iceberg REST catalog. The catalog tells you that table `analytics.events` exists, that its current metadata is at `s3://warehouse/analytics/events/metadata/v42.metadata.json`, and that the user has access to it. The catalog is an API call, not a database operation.

**Storage: how to read the bytes.** The bytes live in S3. They are Parquet files. The FileIO layer (from iceberg-rust) reads them using credentials that Polaris vended for the specific user. The storage layer knows nothing about SQL. It knows about byte ranges and column chunks.

**Compute: how to turn bytes into answers.** DataFusion reads Arrow record batches from the storage layer, applies filters, projections, aggregations, joins, and sorts, and produces result batches. The compute layer knows nothing about where the data came from. It works with Arrow's in-memory columnar format, whether the source is Iceberg, Delta Lake, CSV, or a hand-written `RecordBatch`.

This separation is what makes sovereignty possible. You can swap Polaris for Gravitino. You can swap S3 for R2 or GCS or local disk. You can swap DataFusion for DuckDB (if your requirements change). Each layer is an interface, not an implementation. You are never locked into a vendor at any layer.

And here is the part that matters for multi-tenancy: because the layers are separate, you can configure them differently per user. Alice's catalog view might show different namespaces than Bob's. Alice's storage credentials are scoped to Alice's permissions. Alice's compute might have different memory limits. All within the same engine, the same process, the same binary. The separation is what makes per-user isolation a configuration problem rather than an architecture problem.

::: {.sovereignty}
**Sovereignty principle:** The separation of catalog, storage, and compute is not an
architectural nicety. It is the mechanism of sovereignty. When these three layers are
coupled, you are locked to whatever product couples them. When they are separate, you
can replace any layer without changing the others. Your data stays in your storage. Your
metadata stays in your catalog. Your compute runs where you choose. That is sovereignty.
:::


## The Eighty-Percent Engine

DataFusion gave us a query engine. Not a toy, not a prototype, but a real engine with a real optimizer, real parallel execution, and real Arrow-native performance. The SQL parser handles complex queries. The optimizer produces good plans. The execution engine streams results efficiently.

What DataFusion did not give us: authentication, catalog integration with Polaris, Iceberg table scanning with user-scoped credentials, policy enforcement via plan rewriting, distributed execution across workers, Flight SQL wire protocol, Trino JDBC compatibility, or deployment configuration.

That's the 20%. That's the product.

The lesson is not "use DataFusion." The lesson is that the best infrastructure components are libraries, not services. A library gives you the building blocks and lets you assemble them your way. A service gives you someone else's assembly and charges you for the privilege of not controlling it.

We went from zero to a working single-node query engine -- parsing SQL, authenticating users, querying Iceberg tables via Polaris, streaming Arrow results over Flight SQL -- in three days. Not because we're fast. Because DataFusion did the hard part.

The next three months were the 20%.


## The Upgrade That Required a Fork

DataFusion 53 shipped on April 2, 2026 with features we wanted badly: hash join dynamic filters (5-25x for star-schema joins), LIMIT-aware Parquet pruning (skip entire row groups once the LIMIT is satisfied), and 40x faster query planning. The problem was the dependency chain.

SQE depends on iceberg-rust for Iceberg table reads and writes. Upstream apache/iceberg-rust (v0.9.0) lacks two features SQE needs: `RewriteFilesAction` for Copy-on-Write DELETE/UPDATE, and `PositionDeleteFileWriter` for Merge-on-Read position deletes. The RisingWave Labs fork has both. It is the fork that multiple production systems use.

The RisingWave fork targets DataFusion 52. Upstream merged the DF 53 upgrade on March 25. The fork had not rebased. No timeline.

We forked the fork. Applied the same API migration that upstream PR #2206 documented: `PlanProperties` wrapped in `Arc`, Parquet writer API renames, Arrow 58 type changes. Ten files in the fork, forty-four sites in SQE. Vendored the result into `vendor/iceberg-rust/` (4.6 MB). Single `git clone` gets everything.

The result: TPC-DS dropped from 19.3 seconds to 12.2 seconds. TPC-H from 1.8 to 1.1. Every suite faster. The upgrade was mechanical. The decision to stop waiting and do it ourselves was not.

::: {.sovereignty}
**Sovereignty principle:** When your critical dependency is a fork of a fork, you have two choices: wait for someone else to maintain your supply chain, or own it. We chose to own it. The vendored fork is 4.6 MB. The maintenance burden is one rebase per upstream release. The alternative -- staying on DF 52 indefinitely -- would have left us without hash join dynamic filters, LIMIT-aware pruning, and a year of optimizer improvements. Dependencies are sovereignty decisions.
:::

::: {.fieldreport}
**Field report:** The first integration test -- authenticate via OIDC, query an Iceberg table
through Polaris, receive Arrow batches over Flight SQL -- passed on March 14, 2025. The same
day we scaffolded the crate structure. DataFusion handled the SQL parsing, logical planning,
optimization, and execution. We implemented `CatalogProvider`, `SchemaProvider`,
`TableProvider`, and the Flight SQL handshake. Three files of glue code connected DataFusion
to Polaris to S3 to the user. The test passed. It took less than a day.
That was the moment we knew the library approach would work.
:::

::: {.ailog}
**AI Logbook:** The AI scaffolded all six initial crates and implemented the `execute_query` pipeline (from SQL string to Arrow record batches) in a single session. The human made the decision to use DataFusion as a library rather than Trino, Spark, or DuckDB, after two years of maintaining a Trino fork. The `create_session_context` method. one `SessionContext` per user with per-session credentials. was specified by the human as the architectural constraint; the AI implemented it correctly because Rust's ownership model made the isolation boundaries explicit in the type signatures.
:::
