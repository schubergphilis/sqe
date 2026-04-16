# The Catalog Wars {#sec:catalog-wars}

> Before you can query data, something has to tell you where it is.
> That something is the catalog. And everyone wants to own it.

Every data platform has a centre of gravity. It's not the compute engine. It's not the storage layer. It's the thing that answers two questions: what tables exist, and where are their files?

That thing is the catalog.

For fifteen years, we pretended the catalog didn't matter. It was Hive Metastore running on a MySQL instance that nobody patched, managed by a team that had inherited it from the team before them. It worked. Nobody thought about it. And that was the problem, because when the catalog is invisible, whoever controls it controls your data platform, and you don't even notice until you try to leave.

This chapter is about what happened when the industry noticed. The battle lines that formed. The bets that were placed. And why, after testing every major catalog implementation available in 2024 and 2025, we chose the simplest one.


## The Hive Metastore Era

Hive Metastore was never designed. It was accreted. What started as a metadata store for Apache Hive grew into the default catalog for Spark, Presto, Trino, and every other engine that needed to know where Parquet files lived.

The protocol was Thrift RPC. The backend was a relational database, usually MySQL, sometimes PostgreSQL, occasionally Derby for development clusters that should never have reached production. The schema tracked databases, tables, partitions, and column statistics. It did this adequately.

The problem was coupling. Your catalog spoke Thrift, so your engine had to speak Thrift. Your catalog stored partition locations as absolute paths, so your storage layout was baked into your metadata. Your catalog ran as a single service, so your blast radius was one JVM crash away from every query in the organisation failing simultaneously.

Worse: Hive Metastore was a Java service that ran in the Hadoop ecosystem. When the industry moved to cloud object storage, the Metastore came along, usually as AWS Glue Catalog or a self-managed instance on Kubernetes. The protocol didn't change. The coupling didn't change. The single point of failure didn't change. We just moved the problem to a different data centre.

I ran Hive Metastore instances for years. Every migration was the same: export the metadata, transform the storage paths, import into the new instance, pray that the partition statistics survived. It worked often enough to not get replaced. It failed often enough to generate a steady stream of two-in-the-morning pages.


## The REST Revolution

The Iceberg REST Catalog specification changed everything.

Apache Iceberg needed a catalog, a way to resolve table names to metadata file locations. The earliest Iceberg deployments used Hive Metastore or direct Hadoop filesystem calls. But the Iceberg community made a decision that, in retrospect, was more consequential than any feature in the table format itself: they defined a catalog protocol as HTTP REST.

Not Thrift. Not gRPC. Not a language-specific SDK. HTTP with JSON payloads and a well-defined OpenAPI specification.

This sounds unremarkable. HTTP APIs are everywhere. But for the data catalog space, it was a break from two decades of tight coupling. An HTTP catalog can be consumed by any language, any engine, any cloud. A Python script and a Rust query engine and a Java data pipeline can all talk to the same catalog with nothing more than an HTTP client library.

The specification defines the operations you'd expect:

- `GET /v1/{prefix}/namespaces` -- list namespaces
- `POST /v1/{prefix}/namespaces/{namespace}/tables` -- create a table
- `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}` -- load table metadata
- `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}` -- commit table updates

But the specification also defines something less obvious and far more important: credential vending. When a client loads a table, the catalog can return temporary storage credentials scoped to that specific table. The client doesn't need ambient S3 access. It doesn't need IAM roles pre-provisioned for every table. It asks the catalog for a table, and the catalog gives it both the metadata and the keys to read the files.

This is the mechanism that makes bearer token passthrough possible. The user authenticates to the catalog. The catalog decides what they can access. The catalog vends the storage credentials. The query engine is just a conduit; it passes the user's identity through and receives table-scoped credentials back.

## Glue: The Catalog You Get for Free

AWS Glue Catalog was our first encounter with a production catalog at scale. It ships free with every AWS account. It backs Athena, Redshift Spectrum, EMR, and Lake Formation. If you're on AWS and you have data in S3, Glue is already your catalog whether you chose it or not.

For a long time, Glue only spoke its own API, the `aws-sdk-glue` interface. You called `GetTable`, `GetPartitions`, `CreateTable`. It was AWS-specific, but it worked. Every AWS-native tool supported it.

Then in late 2024, AWS added an Iceberg REST endpoint to Glue. On paper, this was the best of both worlds: the operational simplicity of a managed service with the open protocol of Iceberg REST.

I tested it with PyIceberg as soon as it was available.

::: {.devto}
**From the blog:** "Glue Iceberg Rest Api and PyIceberg" (December 2024). I walked through configuring PyIceberg against Glue's REST endpoint, creating tables, running scans. The API was functional. The limitations surfaced within hours.
:::

The Glue REST endpoint worked for basic operations. You could list namespaces, load tables, read metadata. But the implementation had gaps. Some Iceberg REST operations weren't supported. The credential vending model was tied to IAM; you couldn't pass an OIDC bearer token to Glue's REST endpoint and get back S3 credentials for a specific user. Glue assumed you were already authenticated via IAM. The REST API was a facade over the same Glue internals, not a first-class implementation of the Iceberg REST specification.

The real problem was architectural. Glue's catalog data lives in AWS's managed infrastructure. You can't export it to run on another cloud. You can't run a local instance for development. You can't inspect the underlying storage to debug metadata inconsistencies. The catalog is a black box that happens to speak HTTP.

For a single-cloud deployment where AWS is a permanent commitment, Glue is fine. It's reliable, it scales, it costs nothing extra. But we were building an engine that could run anywhere, on any cloud, on any S3-compatible storage, in any data centre. A catalog that only exists inside AWS doesn't fit that model.

There's also the Collibra angle. We use Collibra for data governance: classification, lineage, access policies. When we explored Collibra Protect with Snowflake and Iceberg tables, the governance layer worked because Snowflake was the enforcement point. But Glue has no equivalent enforcement surface. Lake Formation tries, but it's a separate system with separate concepts and separate permissions. Your governance tool says "mask this column for this role." Then you have to translate that into Lake Formation policies, Glue catalog permissions, and IAM policies. Three translations of one intent. Each translation is a place where the intent gets lost.

::: {.deadend}
**Dead end: Glue as the primary catalog.** We used Glue for nearly two years of development and experimentation. It's how we learned the Iceberg REST protocol. It's how we validated that a query engine could talk to a catalog over HTTP. But it's also how we learned that a managed catalog is a dependency disguised as a convenience. When we tried cross-cloud scenarios (querying the same tables from both AWS and a local development environment) Glue couldn't follow. And when we tried to enforce governance policies across Glue tables, the translation layers between Collibra, Lake Formation, and IAM became their own maintenance burden.
:::


## Unity Catalog: Openness as Strategy

Databricks open-sourced Unity Catalog in mid-2024. This was a significant move. Unity had been Databricks' proprietary catalog for years, the thing that made Databricks workspaces aware of tables, columns, permissions, lineage. Open-sourcing it meant anyone could run a Unity Catalog instance and use it as their Iceberg REST catalog.

I tested it the same week Databricks published the Iceberg REST compatibility layer.

::: {.devto}
**From the blog:** "Unity Catalog Iceberg Rest Api and PyIceberg" (December 2024). Testing Unity's REST compliance with PyIceberg. It worked. Table creation, metadata loading, namespace management, all functional over the standard Iceberg REST API.
:::

Unity Catalog's Iceberg REST support was more complete than Glue's. You could run it as a standalone server, connect from any Iceberg client, and it genuinely implemented the specification. For basic catalog operations, Unity was a credible open-source option.

But Unity Catalog is not just a catalog. It's a governance platform. It tracks permissions, column-level access controls, data lineage, model registrations. These are features, not bugs, but they create a gravity well. Once you adopt Unity's permission model, your security enforcement is tied to Unity. Once you use Unity's lineage tracking, your observability is tied to Unity. The catalog becomes the control plane, and the control plane becomes the platform.

This is exactly Databricks' strategy, and it's not a secret. Open-source the catalog, make it compelling, and the governance layer pulls teams toward the Databricks ecosystem. It's smart business. It's also the opposite of what we needed.

We needed a catalog that did one thing: map table names to metadata locations. No opinions about governance. No opinions about security enforcement. No opinions about which query engine talks to it. A catalog that would be equally happy being called by our Rust engine, a Python script, a dbt model, and a Java Spark job, without any of them needing to understand Unity's permission model.

The open-source version of Unity and the managed Databricks version are different beasts. The open-source version lacks many of the governance features. This is reasonable; the governance layer is the product. But it means evaluating Unity requires deciding which Unity you're evaluating: the ambitious platform, or the stripped-down catalog.

We chose neither. Not because Unity is bad, but because it is too much. A catalog should be a dumb registry that speaks a standard protocol. Unity wants to be smart. Smart catalogs make decisions you didn't ask for.


## The Cross-Cloud Problem

Between the Glue experiments and the Unity evaluation, I spent time on a problem that clarified everything: cross-cloud table access.

The scenario: tables registered in Snowflake, queryable from tools outside Snowflake. Snowflake had added Iceberg table support; you could create tables backed by Parquet files in your own S3 bucket, with metadata managed by Snowflake. This meant the data was nominally open, but the catalog was still Snowflake.

::: {.devto}
**From the blog:** "Bridging Clouds: Access Snowflake Iceberg Tables via Glue and Spark" (October 2024). I built a bridge between Snowflake's Iceberg tables and AWS Glue, making tables visible to Spark EMR jobs. The bridge worked. It also revealed how deeply each catalog assumes it's the only catalog.
:::

The bridging exercise taught me something I should have seen earlier. Every catalog assumes it owns the table. Glue stores the metadata location in its own database. Snowflake stores it in its own metadata layer. Unity stores it in its own persistence backend. When you bridge between them, you're synchronising metadata between two systems that each believe they're the source of truth.

This doesn't scale. Two catalogs means reconciliation logic. Three catalogs means a metadata mesh that nobody wants to maintain. The industry's answer to multi-cloud data access was to synchronise catalogs, to keep Glue and Snowflake and Unity all aware of the same tables. Every synchronisation layer adds latency, introduces consistency windows, and creates a new failure mode.

The right answer is one catalog. One source of truth for table metadata. Every engine, every tool, every cloud, they all talk to the same catalog. The catalog doesn't live inside any engine. It doesn't live inside any cloud provider's managed service. It runs where you put it, speaks HTTP, and has no opinions about what connects to it.

::: {.fieldreport}
**Field report:** The Snowflake-to-Glue bridge worked in production for about six months. During that time, we hit metadata drift three times: tables that were updated in Snowflake but not yet synchronised to Glue, causing Spark jobs to read stale data or fail on schema mismatches. Each incident took about four hours to diagnose because the root cause was always "which catalog has the current version?" Every time, someone asked: "Why do we have two catalogs?" We never had a good answer.
:::


## The DuckDB Detour

Before settling on building our own engine, I spent time with DuckDB, the embedded analytical database that runs anywhere, needs no cluster, and processes Parquet at surprising speed.

::: {.devto}
**From the blog:** "DuckDB S3 Tables with Iceberg using Iceberg Rest API" (January 2025). I connected DuckDB to an Iceberg REST catalog, read tables from S3, and ran analytical queries locally. No Spark. No Trino. No cluster management. Just a binary and a catalog URL.
:::

DuckDB + Iceberg REST was the closest thing to the engine we wanted before we built one. It proved the model: a lightweight engine that talks to a REST catalog and reads Parquet from S3. The experience confirmed that the REST catalog protocol was the right abstraction layer. The catalog answered "what tables exist and where are their files," and DuckDB did the rest.

But DuckDB is embedded, single-node, and, crucially, doesn't support bearer token passthrough for per-user identity. Every DuckDB query runs as whatever credentials are configured at startup. For a personal analytics tool or a CI pipeline, that's fine. For a multi-user query engine where security auditing requires per-user attribution, it's a non-starter.

The DuckDB experiments mattered because they stripped away everything except the catalog interaction. No distributed complexity. No JVM overhead. Just: talk to the catalog, get metadata, read files, return results. That clarity shaped how we designed `sqe-catalog`. The catalog interface should be that simple, even when the engine behind it is distributed.


## Gravitino and Nessie: Interesting but Different

Two other catalogs deserve mention because they solve adjacent problems.

**Apache Gravitino** is a meta-catalog, a federation layer that can front multiple underlying catalogs (Hive Metastore, Iceberg REST, JDBC catalogs) behind a single API. If you have three Hive Metastores and a Glue catalog and you want a unified namespace, Gravitino is the answer. It's technically impressive and solves a real problem for organisations with sprawling catalog infrastructure.

We didn't need federation. We needed to start clean. Gravitino solves the problem of having too many catalogs. Our problem was choosing one catalog to rule them all. Different starting points, different solutions.

**Project Nessie** adds git-like semantics to table metadata: branches, tags, commits, diffs. You can create a branch of your data lake, make changes, and merge them back. For data pipeline development, this is compelling. You can test a transformation on a branch without affecting production tables.

Nessie speaks the Iceberg REST protocol (or close to it), which made it a genuine contender. But the versioning model adds complexity to every catalog operation. A `load_table` call needs to know which branch or tag to resolve. A `commit` needs to handle merge conflicts. These are features we didn't need for a query engine, and features have a carrying cost, even when you don't use them.

**LakeFS** solves a similar versioning problem but at the storage layer rather than the catalog layer. It presents a versioned S3-compatible API, so tools think they're reading regular S3 but get branching and merging for free. We briefly considered it as a complement to a simple catalog. It's a good product solving a different problem than the one we had.


## Polaris: The Catalog That Does Nothing Extra

Apache Polaris started life as Snowflake's internal Iceberg catalog. Snowflake contributed it to the Apache Software Foundation in 2024, and it entered incubation as an Apache project. The pedigree matters: this isn't an academic exercise or a startup's side project. It's a catalog that ran at Snowflake's scale, extracted and open-sourced.

Polaris implements the Iceberg REST specification. Not a subset. Not a superset with proprietary extensions. The specification.

When we first deployed Polaris, the contrast with every other catalog was immediate. There was no governance layer to configure. No permission model to learn beyond the Iceberg REST spec's own token-based auth. No UI to understand. You start the server, point it at a storage backend, and it speaks REST.

The configuration for SQE's catalog connection is four lines in a TOML file:

```toml
[catalog]
polaris_url = "https://polaris.internal:8181/api/catalog"
warehouse = "production"
metadata_cache_ttl_secs = 30
```

The Rust code that creates a per-session catalog connection reflects this simplicity:

```rust
pub struct SessionCatalog {
    inner: Arc<RwLock<RestCatalog>>,
    polaris_url: String,
    warehouse: String,
    bearer_token: String,
    token_fingerprint: String,
    storage_config: StorageConfig,
    http_client: reqwest::Client,
}
```

Each user session gets its own `SessionCatalog` instance, configured with the user's bearer token. The token goes straight to Polaris in the `Authorization` header. Polaris validates it and returns table metadata, including, critically, vended S3 credentials scoped to that user's permissions.

The `SessionCatalog::new` method tells the whole story. It builds a properties map with the token, the URI, and the warehouse, then hands it to `iceberg-rust`'s `RestCatalogBuilder`:

```rust
let mut props = HashMap::new();
props.insert("token".to_string(), bearer_token.to_string());
props.insert("uri".to_string(), polaris_url.to_string());
props.insert("warehouse".to_string(), warehouse.to_string());

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

That's it. No IAM role configuration. No SDK-specific authentication dance. No catalog-specific client library. An HTTP client, a bearer token, and a URL.


## Credential Vending: The Mechanism That Makes It Work

The REST specification's credential vending is the feature that separates a catalog from a metadata registry. When SQE loads a table from Polaris, the response includes not just the metadata location and schema, but also temporary S3 credentials that can read the table's files.

These credentials are scoped. They grant access to the specific S3 prefix where that table's data lives. They expire. They're tied to the identity that requested them.

Our `CredentialCache` extracts these vended credentials from the table config:

```rust
pub fn extract_from_table_config(
    config: &HashMap<String, String>,
) -> Option<VendedCredentials> {
    let access_key = config.get("s3.access-key-id").cloned()?;
    let secret_key = config.get("s3.secret-access-key").cloned()?;
    let session_token = config.get("s3.session-token").cloned();
    // ...
    Some(VendedCredentials {
        access_key,
        secret_key,
        session_token,
        expiry,
    })
}
```

The property names (`s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`) are defined in the Iceberg REST specification. Any conforming catalog returns them. This means our credential extraction code works with Polaris, but it would also work with any other catalog that implements vending correctly.

This is the sovereignty argument in miniature. The code depends on a specification, not on a product. If Polaris ceased to exist tomorrow, any compliant REST catalog would be a drop-in replacement for the credential vending flow.

::: {.sovereignty}
**Sovereignty principle:** Credential vending moves the security boundary from the engine to the catalog. The engine never holds ambient storage credentials. It receives scoped, temporary credentials through a standard protocol. This means you can swap the catalog without touching the engine's security model. The security model is the protocol, not the product.
:::


## The DataFusion Bridge

One practical challenge: DataFusion's `CatalogProvider` trait is synchronous for some operations. You implement `schema_names()` and it returns `Vec<String>`, no async, no futures, no await. But listing namespaces from a REST catalog is inherently asynchronous. You're making an HTTP call.

Our solution is a cached snapshot. When `SqeCatalogProvider` is constructed, it makes one async call to list namespaces and caches the result:

```rust
impl SqeCatalogProvider {
    pub async fn try_new(
        session_catalog: Arc<SessionCatalog>,
        storage_config: StorageConfig,
        warehouse: String,
    ) -> sqe_core::Result<Self> {
        let namespaces = session_catalog.list_namespaces().await?;
        let cached_namespaces: Vec<String> = namespaces
            .iter()
            .map(|ns| /* ... */)
            .collect();
        Ok(Self {
            session_catalog,
            storage_config,
            warehouse,
            cached_namespaces,
        })
    }
}
```

The synchronous `schema_names()` method then returns the cached list. Tables are loaded lazily. `table()` is async, so it can call Polaris on demand.

This matters because it's a pattern you'll see throughout SQE: bridging between DataFusion's trait interfaces and the async reality of a REST catalog. DataFusion was designed with embedded catalogs in mind: Hive Metastore clients or in-memory registries. A remote REST catalog introduces latency and failure modes that the trait signatures don't anticipate. Every bridge we build is a small bet that the trait designers will eventually make async-friendly, and a pragmatic workaround until they do.


## Why "No Opinions" Is the Architecture

Every catalog we evaluated had opinions. Glue has opinions about identity (IAM). Unity has opinions about governance (its own permission model). Nessie has opinions about versioning (branches and tags). Gravitino has opinions about federation (its meta-catalog layer).

Polaris has one primary opinion: tables have names and metadata locations. It does have its own role-based access control (catalog roles, principal roles, and privilege grants) which is a real governance layer. But it's a thin one. It controls who can see which tables and namespaces. It does not control what happens to the data after the table is loaded. It does not inject row filters. It does not mask columns. It does not rewrite queries.

This is a meaningful distinction. A catalog that controls access to table metadata is doing its job. A catalog that controls what happens inside your query engine is doing your job. Polaris stays in its lane. Your governance (row-level security, column masking, audit policies) can be OPA. Or Cedar. Or a custom policy engine built into your query engine (which is what we did in Chapter 8). Your versioning can be handled by Iceberg's own snapshot semantics. Your federation isn't needed because you have one catalog.

![Catalog landscape: comparing REST spec compliance, auth models, and governance opinions across Glue, Unity, Polaris, Gravitino, and Nessie](diagrams/rendered/01-catalog-landscape.svg)

| Catalog | REST Spec | Auth Model | Governance | Opinions |
|---------|-----------|------------|------------|----------|
| Glue | Partial | IAM | Lake Formation | AWS-native, single-cloud |
| Unity (OSS) | Full | Built-in | Limited (OSS) | Databricks ecosystem gravity |
| Unity (Managed) | Full | Built-in | Full | Databricks platform |
| Polaris | Full | OIDC/OAuth2 | None | Table registry only |
| Gravitino | Via proxy | Pluggable | None | Multi-catalog federation |
| Nessie | Near-full | Pluggable | None | Git-like versioning |

The "Opinions" column is the one that matters. When a catalog has opinions about governance, those opinions compete with your own governance design. When a catalog has opinions about auth, those opinions shape your engine's auth model. When a catalog has no opinions, you're free.

Freedom has a cost: you have to make those decisions yourself. You have to implement governance, choose an auth model, design a security enforcement layer. For some teams, that cost is too high, and a catalog with built-in governance (Unity, Glue + Lake Formation) is the right choice.

For us, the cost was the point. We were building a query engine precisely because we wanted to make those decisions ourselves. A catalog with no opinions is the right foundation for an engine with strong opinions.


## Running Polaris in Practice

Polaris runs as a Java application. You can deploy it from a JAR, a Docker container, or a Kubernetes Helm chart. The storage backend for catalog metadata can be an in-memory store (for development), a local file system, or a production database.

For our development and test environment, we run Polaris in memory:

```yaml
polaris:
  image: apache/polaris:latest
  environment:
    POLARIS_BOOTSTRAP_CREDENTIALS: "root:s3cr3t"
  ports:
    - "8181:8181"
```

The entire catalog starts in under two seconds. No database to provision. No persistent volume to mount. For integration testing, this is transformative: every test run gets a clean catalog, and there's no cleanup needed.

For production, Polaris backs its metadata to a relational database (PostgreSQL is the typical choice). The metadata is small: table names, schema definitions, partition specs, snapshot references. The heavy lifting is in the Iceberg metadata files themselves, which live in object storage. The catalog is a pointer to those files, not a copy of them.

This architectural clarity (the catalog points, storage holds) is what makes the system composable. You can inspect the Iceberg metadata files directly with a tool like PyIceberg or `iceberg-rust` without going through the catalog at all. The catalog is the preferred path, but it's not the only path. Your data is never locked behind a catalog API.

One thing we learned quickly: Polaris's in-memory mode is not just a convenience for testing. It became the foundation of our entire integration test suite. Every test starts a fresh Polaris instance, creates namespaces and tables, runs queries through the full SQE stack, and tears everything down. No shared state between tests. No flaky failures from leftover metadata. The entire test stack (Polaris in-memory plus RustFS for S3-compatible storage) starts in under five seconds and requires no cloud credentials. We went from "integration tests need a running AWS environment" to "integration tests run on a laptop at an airport."

That shift in development velocity is hard to overstate. When testing the catalog integration is cheap, you test it more. When you test it more, you find problems earlier. When you find problems earlier, the catalog code is better. The tool shaped the practice.

::: {.fieldreport}
**Field report:** During one debugging session, we needed to inspect a table's manifest list to understand a failed compaction. Instead of building a debug tool for the catalog, we pointed PyIceberg directly at the S3 location from the catalog's `metadata_location` field and read the manifests manually. The table format is the source of truth. The catalog is an index.
:::


## The Centre of the Data Platform

I made a claim at the start of this chapter: the catalog is the centre of the data platform. Let me make it concrete.

When a user runs `SELECT * FROM production.events` in SQE, the catalog answers four questions:

1. **Does this table exist?** The catalog resolves `production.events` to a metadata location, an S3 path pointing to an Iceberg metadata JSON file.

2. **What does it look like?** The metadata file (fetched from S3 using vended credentials) contains the schema, partition spec, sort order, and current snapshot.

3. **Can this user read it?** The catalog's auth layer (OIDC token validation) determines whether the user's identity has permission to load this table's metadata. If not, the table doesn't exist (not an access denied error, just absence, no information leakage).

4. **How does the engine access the files?** The catalog vends temporary S3 credentials scoped to this table's storage location. The engine uses these credentials to read Parquet files.

Four questions. Every one answered by the catalog. If the catalog is wrong about any of them, the query fails. If the catalog is unavailable, no query can run. If the catalog is compromised, table visibility and storage credentials are both compromised.

This is why the catalog is the centre, even though it holds very little data itself. It holds the pointers, the permissions, and the credentials. Everything else (the actual data, the query execution, the result delivery) depends on those three things.

Control the catalog, and you control what data exists (namespace and table management), who can see it (authentication and authorization), and how it's accessed (credential vending). That's the data platform.


## What We Actually Built

The `sqe-catalog` crate is the result of everything in this chapter. It's several thousand lines of Rust across eight core modules:

- **`rest_catalog.rs`**. The `SessionCatalog` struct: one per user session, wraps `iceberg-rust`'s `RestCatalog` with the user's bearer token. Handles tables, views, namespaces.
- **`catalog_provider.rs`**. The DataFusion bridge: implements `CatalogProvider` using cached namespace snapshots. Maps Iceberg namespaces to DataFusion schemas.
- **`schema_provider.rs`**. Implements `SchemaProvider` for a single namespace. Loads tables and views lazily. Handles the sync-to-async bridge for `table_names()`.
- **`table_provider.rs`**. Wraps an Iceberg `Table` as a DataFusion `TableProvider`. Converts schemas, pushes filter predicates down.
- **`credential_vending.rs`**. Extracts and caches vended S3 credentials from catalog responses. TTL-based cache using `moka`.
- **`info_schema.rs`**. Virtual `information_schema` tables (tables, columns, schemata) for SQL standard compliance.
- **`system_catalog.rs`**. Virtual `system` tables for runtime introspection.
- **`iceberg_scan.rs`**. The physical scan operator that reads Iceberg data via the table's `FileIO` and vended credentials.

Since then, the crate has grown to include `manifest_cache`, `footer_cache`, `circuit_breaker`, `s3_io`, `sort_order`, `read_parquet`, `iceberg_metadata_tvf`, `pruning_stats`, and several more, the caching and I/O layers that made SQE competitive with Trino.

The crate depends on `iceberg` and `iceberg-catalog-rest` from iceberg-rust, `datafusion` for the trait implementations, `moka` for caching, and `reqwest` for direct REST calls (views, which iceberg-rust doesn't support natively yet).

None of these modules knows or cares that the catalog is Polaris. They speak the Iceberg REST protocol. If we swapped Polaris for a compliant Nessie instance, or a future version of Unity's REST endpoint, or a catalog that doesn't exist yet, the code wouldn't change. The config file would change. That's it.


## The Lesson

I spent most of 2024 testing catalogs. Glue, Unity, Snowflake's Iceberg support, cross-cloud bridges, Gravitino, Nessie. I wrote about each one. I built prototypes with each one. I hit limitations with each one.

The pattern was always the same. Start with the managed option. It works quickly. Hit a cross-cloud scenario, or a custom auth requirement, or a governance model that doesn't match the built-in one. Realise the catalog has opinions, and those opinions don't match yours. Either live with the mismatch or migrate.

The catalog wars aren't about features. Every major catalog can list tables and return metadata. The wars are about control. Every catalog vendor wants the catalog to be the gravitational centre of their ecosystem. Glue pulls you toward AWS. Unity pulls you toward Databricks. Snowflake's catalog pulls you toward Snowflake. They provide genuine value in exchange for that gravity.

Polaris has no ecosystem to pull you toward. It's a catalog. It stores table metadata. It validates tokens. It vends credentials. It does nothing else. For a project whose entire premise is sovereignty, running independently of any vendor's platform, that absence of ambition is the feature.

The Iceberg REST specification is the real winner. Not Polaris specifically, but the protocol. The protocol means your catalog choice is reversible. It means your engine doesn't know or care which catalog implementation answers its HTTP calls. It means the most important component in your data platform, the one that answers "what tables exist and where are their files," is a standard interface that you can implement, replace, or self-host without touching a line of engine code.

::: {.ailog}
**AI Logbook:** The AI produced the `SessionCatalog`, `CredentialCache`, and all eight `sqe-catalog` modules in a single session from a spec that named each module and its purpose. The human chose Polaris over Unity, Glue, and Nessie after months of hands-on evaluation that the AI never saw. The credential vending extraction code (parsing `s3.access-key-id` from the REST response config map) was correct on the first pass; the three days of debugging Polaris 0.9-vs-1.0 timestamp format differences were entirely human detective work.
:::

The next chapter covers what happens after the catalog tells you where the data is. The data itself: Apache Iceberg's table format, from metadata trees to manifest files to the Parquet files at the bottom. The catalog is the index. Iceberg is the book.
