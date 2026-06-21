# Speaking to Many Catalogs {#sec:multi-catalog}

> The catalog is the API. But which catalog?
> The operator picks. The engine connects.

The previous chapter built a catalog worth reading. Tools could discover what tables existed. Clients could browse columns. The metadata story was solved on the client side.

Then the first real customer asked the obvious question. "Can it talk to Glue?"

Polaris was running on a laptop. The whole stack was wired around the Iceberg REST protocol that Polaris speaks. Glue is not REST. Glue is the AWS API. Different transport, different authentication, different shape entirely.

This chapter is about that detour. Six catalog backends. One dispatch path. And the refactor that took 600 lines of wrapper code out and put 80 lines back in its place.

## The first wrapper

The first non-REST backend we wrote was Hive Metastore. A team running on Hive needed to point SQE at their existing catalog without migrating off Hive. The upstream `iceberg-rust` crate had `iceberg-catalog-hms`. The constructor wanted a Thrift URI and a warehouse path. Easy.

We wrote a wrapper.

```rust
pub struct HmsBackend {
    config: HmsConfig,
}

impl HmsBackend {
    pub async fn build_catalog(
        &self,
        storage_factory: Arc<dyn StorageFactory>,
    ) -> Result<HmsCatalog> {
        // ... thirty lines of config translation
    }
}
```

The wrapper made sense at the time. SQE's config has a typed `CatalogBackend::Hms { uri, warehouse }` enum. The upstream constructor wants strings in a HashMap. Translation between the two needed a place to live, and a per-backend module felt like that place.

Then Glue arrived. Same shape. Different keys. Another wrapper.

```rust
pub struct GlueBackend {
    config: GlueConfig,
}
```

Then JDBC.

```rust
pub struct SqlBackend {
    conn: rusqlite::Connection,
    catalog_name: String,
}
```

By the time someone asked about AWS S3 Tables, the pattern was obvious. Each wrapper was 100 to 250 lines. They all did the same three things. Build a config struct. Translate it to the upstream builder's prop format. Call `build_catalog`. The wrappers were the shape of the typed config trying to match the shape of the upstream API, and we kept writing them by hand.

The dispatch site at the call point was a 90-line match expression with one arm per backend.

```rust
let inner: Arc<dyn iceberg::Catalog> = match backend {
    CatalogBackend::Hms { uri, warehouse } => { /* ... */ }
    CatalogBackend::Glue { region, warehouse, endpoint } => { /* ... */ }
    CatalogBackend::Jdbc { url, warehouse } => { /* ... */ }
    // ...
};
```

Six hundred lines of boilerplate to handle four backends.

## The factory we missed

Upstream had a crate called `iceberg-catalog-loader`. We had not looked at it because the SQE-side wrappers existed before it shipped, and once they existed, nobody went back to ask whether they should.

The loader is exactly what its name suggests. It exposes one function:

```rust
pub fn load(catalog_type: &str) -> Result<Box<dyn BoxedCatalogBuilder>>;
```

Pass in `"glue"` and you get a Glue builder. Pass in `"hms"` and you get an HMS builder. The trait every builder implements looks like this:

```rust
async fn load(
    self: Box<Self>,
    name: String,
    props: HashMap<String, String>,
) -> Result<Arc<dyn Catalog>>;
```

A name and a props map. That is the entire API. Every catalog backend in the iceberg-rust ecosystem already speaks it, because the upstream `CatalogBuilder` trait is defined that way.

The wrappers had been translating SQE's typed config into props maps for years. Each wrapper did exactly that translation. Then it called the upstream builder. Then it returned the catalog. The loader skips the wrapper entirely.

```rust
let inner: Arc<dyn iceberg::Catalog> = iceberg_catalog_loader::load(catalog_type)?
    .load(name.to_string(), props)
    .await?;
```

That is the whole dispatch.

## What stays SQE-specific

Some translation still has to happen on the SQE side. The user writes typed TOML, not raw prop maps. Our config has `CatalogBackend::S3tables { table_bucket_arn, endpoint_url }` because we want compile-time validation that the bucket ARN field exists and is a string. The upstream builder takes a HashMap with magic keys.

The translation is small.

```rust
CatalogBackend::S3tables { table_bucket_arn, endpoint_url } => {
    let mut p = HashMap::new();
    p.insert(
        S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
        table_bucket_arn.clone(),
    );
    if let Some(ep) = endpoint_url {
        p.insert(S3TABLES_CATALOG_PROP_ENDPOINT_URL.to_string(), ep.clone());
    }
    ("s3tables", "sqe-s3tables-session", p)
}
```

Fifteen lines per backend. Five backends in the loader registry plus Hadoop on the SQE side. The whole `for_session_other_backend` function fits in 80 lines. The previous match expression was 90 lines without counting the wrappers.

The constants at the top come from upstream too.
`S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN` is a `&'static str` that the
upstream `iceberg-catalog-s3tables` crate defines as `"table_bucket_arn"`.
We import the constant rather than write the string ourselves so that
when upstream renames it, we get a compile error and notice immediately.

## The upstream patches

Adopting the loader was not free. Two things had to change in the
vendored copy.

The first was feature gating. Upstream's loader pulls every backend
crate as a hard dependency. The static registry hardcodes
`RestCatalogBuilder`, `GlueCatalogBuilder`, `S3TablesCatalogBuilder`,
`HmsCatalogBuilder`, `SqlCatalogBuilder`. Adding the loader to a SQE
build that wanted only REST would still drag in the AWS SDK and
volo-thrift and sqlx. Slim builds existed for a reason. We were not
going to lose them.

The patch is small. Each backend gets its own cargo feature. The
registry assembles at call time instead of as a static array, so
`#[cfg(feature = "...")]` can include or exclude entries:

```rust
fn catalog_registry() -> Vec<(&'static str, CatalogBuilderFactory)> {
    let mut entries = Vec::with_capacity(5);
    entries.push(("rest", || Box::new(RestCatalogBuilder::default())));
    #[cfg(feature = "glue")]
    entries.push(("glue", || Box::new(GlueCatalogBuilder::default())));
    #[cfg(feature = "s3tables")]
    entries.push(("s3tables", || Box::new(S3TablesCatalogBuilder::default())));
    #[cfg(feature = "hms")]
    entries.push(("hms", || Box::new(HmsCatalogBuilder::default())));
    #[cfg(feature = "sql")]
    entries.push(("sql", || Box::new(SqlCatalogBuilder::default())));
    entries
}
```

A REST-only SQE build now includes a loader that has only one
registry entry. The other backends are not linked. The slim build
that started at 80 MB stayed at 80 MB. We filed the patch upstream
with a note that this is a forward-compatible change. Every
existing user gets a registry with all backends present by default;
nobody loses anything.

The second was async. The loader's `BoxedCatalogBuilder` trait
returned a future from `.load()` but the trait itself had no
`Send + Sync` bound. Box that returns a non-Send future cannot
cross an `await` point in an async context, which is exactly what
SQE does for every session catalog. We added the bound. Two words.

```rust
pub trait BoxedCatalogBuilder: Send + Sync {
```

Every existing upstream builder already satisfies it. AWS SDK
clients are `Send + Sync`. `reqwest::Client` is `Send + Sync`.
sqlx pools are `Send + Sync`. The bound just says the obvious thing
the impls were already doing.

Both patches are documented in `vendor/iceberg-rust/README.md` and
filed for upstream alignment when the next vendor refresh lands.

## What disappeared

The wrappers went away. `crates/sqe-catalog/src/backends/glue.rs`,
`hms.rs`, `sql.rs`. Five hundred lines. Three modules, three
configs, three factories, all of which translated SQE-typed config
into the same upstream HashMap-based API the loader now hands us
directly.

The backends module shrank to one file.

```
crates/sqe-catalog/src/backends/
└── hadoop.rs
```

Hadoop stayed. Hadoop is not a catalog in the iceberg-rust sense.
It is a filesystem prefix that you walk for `metadata.json` files
and treat the result as a catalog. Useful for read-only access to
a warehouse another engine wrote, or for one-off investigations on
a S3 prefix without standing up Polaris. The upstream loader has
no Hadoop backend because Hadoop is not really a backend. It is
the absence of a metadata service, dressed up as one. SQE keeps
that path because operators ask for it.

## Adding a new backend now

The cost of adding a seventh backend is no longer a wrapper module.
It is three things.

1. A serde variant in `sqe_core::config::CatalogBackend` so the
   user can write the right TOML.
2. A match arm in `for_session_other_backend` that translates the
   typed config to the upstream prop keys. Fifteen lines.
3. A cargo feature on `sqe-catalog` that pulls in the upstream
   crate and forwards to the loader's matching feature.

That is it. Adding S3 Tables took an afternoon. Adding the next
upstream catalog (Snowflake, Polaris-as-a-service, whatever lands
in iceberg-rust) will take less.

## What the operator sees

From the user's side, nothing changed. The TOML has the same shape.

```toml
[catalog.backend]
type = "s3tables"
table_bucket_arn = "arn:aws:s3tables:eu-west-1:123456789012:bucket/my-bucket"
```

The username and password flow at the SQE Flight SQL endpoint stays
the same. AWS credentials come from the standard SDK chain. Tools
connect, browse the schema, read tables. The user does not know
whether SQE is talking to Polaris or Glue or S3 Tables. The
catalog API on the client side is the same regardless of where the
metadata actually lives.

That is the point of putting the loader at the seam. The user
chooses a backend in config. Everything past that is uniform.

## The validation pass

We ran the same end-to-end test against three live backends to
prove the dispatch worked. REST against the local Polaris stack.
Glue against a production AWS account. S3 Tables against a managed
bucket in eu-west-1.

```
SHOW SCHEMAS
SHOW TABLES IN <namespace>
SELECT * FROM <namespace>.<table> LIMIT 5
SELECT col, COUNT(*), SUM(amount) FROM <table> GROUP BY col ORDER BY ...
SELECT * FROM <table> WHERE col >= ... AND amount > ... ORDER BY ...
```

Every one of those queries worked on every backend. The Glue test
read 1.5 million rows from `iceberg_user_events`. The S3 Tables
test read 9 rows from `daily_sales`. Both went through the same
SQE scan path, the same DataFusion planner, the same Arrow Flight
return path. The only thing that changed was which constructor the
loader picked.

## The lesson

The catalog API for clients (chapter 6) is one-sided. The engine
describes itself. The operator-facing catalog API is the mirror.
The operator describes their world. The engine connects.

For years the SQE codebase had wrappers around upstream builders
because we wrote the first wrapper before the upstream factory
existed, and once we had wrappers we stopped looking. The upstream
factory had been there for a year. We had written 600 lines of
code that did exactly what the factory did, just with our typing
on top.

Read the upstream code. The library you depend on is also a
codebase someone is improving. The wrapper you wrote in 2024 might
be redundant in 2025 because someone landed the cleaner version
two PRs after yours. Check.
