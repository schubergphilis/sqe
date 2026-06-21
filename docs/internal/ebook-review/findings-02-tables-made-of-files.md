# Findings: 02-tables-made-of-files.md

## Thesis
An Iceberg table is metadata (a JSON/Avro tree) layered over Parquet files on storage you
own, not a thing inside a database. That inversion is what makes a sovereign query engine
possible, and the chapter shows how SQE bridges iceberg-rust to DataFusion to realize it.

## Opening
> "A database table exists inside a database. You move the database, you move the tables. You lose the database, you lose the tables."
Verdict: strong hook. Anaphora and concrete cause/effect set the table-vs-database tension immediately; the epigraph (L3-4) primes it well.

## Closing
> "The bridge was about 1,200 lines of Rust across six files. ... We wrote the initial catalog integration in about 1,200 lines. We got access to every table, in every namespace, in every catalog that speaks the Iceberg REST protocol."
Verdict: fizzles after a perfect button. The real landing is L485 ("The engine is disposable. The data is not."). L487-489 then re-explain and restate "1,200 lines" twice, diluting it.

## Voice & editorial issues
1. **L418** `## Why Rust Changes the Game` -- the "game-changer" idiom in verb form, a forbidden phrase family per voice.md/CLAUDE.md. Rewrite: `## Why Rust Changes the Math` or `## What Rust Buys at Scale`.
2. **L489** Trailing summary + redundancy: "The bridge was about 1,200 lines of Rust across six files. That is all it takes... The ratio matters. We wrote the initial catalog integration in about 1,200 lines. We got access to every table..." States "about 1,200 lines" twice in four sentences; "The ratio matters" names no ratio. Rewrite: end the chapter at L485 ("The engine is disposable. The data is not."), or fold L487-489 into one tight paragraph that states the line count once. Drop "The ratio matters."
3. **L66** Sentence opens with "This" referring to the prior sentence: "This is schema evolution without data migration." CLAUDE.md forbids leading "This" that points back. Rewrite: "That is schema evolution without data migration." or "Schema evolution without data migration: you never rewrite files for a schema change."
4. **L450** "This is a workaround, not a solution." Same leading-"This" pattern. Rewrite: "It is a workaround, not a solution."
5. **L359** "This is not optional instrumentation." Leading "This." Rewrite: "The instrumentation is not optional."
6. **L57** "It has been battle-tested at Netflix, Apple, and Airbnb scale" leans on name-drop as evidence; fine in voice, minor. No rewrite required.

## Mechanical violations (PROSE only)
none. grep for emdash/endash/unicode arrows returns zero hits; the `->` arrows at L370-375 (credential chain) and inside code fences are diagram/code, excluded.

## Exclamation marks in prose
none. The four `!` hits are all non-prose: L87 inside a quoted article title ("Duckberg!"), L220 and L407 are the Rust `format!` macro, L291 is the `!=` operator in a code span.

## Continuity data
### Concepts INTRODUCED / defined here
- Iceberg metadata tree -> four-level JSON/Avro/Parquet hierarchy
- manifest list -> per-snapshot Avro index of manifests
- manifest file -> Avro file with per-data-file column stats
- snapshot / time travel -> point-in-time table via snapshot id
- snapshot isolation via pointer swap -> atomic metadata commit, no MVCC
- schema evolution by field ID -> rename/add without data rewrite
- partition evolution -> spec is metadata, old/new layouts coexist
- predicate pushdown (manifest/file/row-group/row) -> three-plus levels of pruning
- SqeTableProvider -> wraps Iceberg Table for DataFusion
- SqeCatalogProvider -> maps namespaces to schemas, caches namespace list
- SqeSchemaProvider -> lazy per-namespace table/view loader
- IcebergScanExec -> custom DataFusion physical scan node
- SessionCatalog -> one RestCatalog per user, holds bearer token
- SessionCatalogBridge -> wrapper implementing iceberg-rust Catalog trait
- expr_to_predicate -> DataFusion Expr to Iceberg Predicate translation
- Inexact vs Exact pushdown -> whether DataFusion keeps the post-scan filter
- credential vending -> per-table scoped S3 creds in REST response
- catalog-to-scan credential chain -> unbroken user-identity flow

### Concepts ASSUMED (used as if already known)
- DataFusion (engine, optimizer, ExecutionPlan, LogicalPlan, SessionContext)
- Arrow / RecordBatch / zero-copy
- Parquet, ORC, Avro file formats
- Polaris REST catalog and bearer-token passthrough (ch1 territory)
- OIDC / JWT / bearer token, service-account-free model
- Arrow Flight (client transport, L472)
- OpenDAL, moka cache, tokio async runtime, RwLock
- dbt compatibility (referenced at L456 as a driver for views)
- Copy-on-Write / Merge-on-Read, position deletes (L148)

### Key factual / numeric claims
- iceberg-rust started at 0.8, reached 0.9 by production integration tests (L137)
- "0.9 changed the catalog builder API significantly" / CatalogBuilder 0.8->0.9: RestCatalogConfig -> RestCatalogBuilder::default().load() (L137, L210)
- RisingWave Labs fork of iceberg-rust, pinned rev 1978911ec4 for iceberg, iceberg-catalog-rest, iceberg-datafusion (L143-145, L148)
- fork provides OverwriteAction (CoW DELETE/UPDATE) and PositionDeleteFileWriter (MoR) (L148)
- storage layer uses OpenDAL, not iceberg-rust native FileIO (L150)
- Polaris 0.9 vs 1.0 return different file-io / credential property keys and expiry formats (L198, L202-206)
- "The fix was three lines. The debugging was three days." (L204-205)
- credential extraction tries RFC3339 first, then epoch millis, then static fallback (L205-206)
- credential vending module: ~150-line cache backed by moka, TTL 50 minutes vs typical 60-minute STS lifetime (L208)
- with_storage_factory required for write ops; undocumented, found by reading source (L226)
- 25 unit tests for predicate translation (L319)
- session name includes token fingerprint = last 8 characters (L413)
- iceberg-rust 0.9 has no first-class view support; no load_view/create_view on Catalog trait (L439)
- views implemented via direct Polaris REST: POST/GET/GET/DELETE /v1/{prefix}/namespaces/{ns}/views (L444-446)
- "We built the REST workaround in a day." (L457)
- TableProvider trait described as having "four key methods" (schema, table_type, supports_filters_pushdown, scan) (L246, L249-261)
- bridge was "about 1,200 lines of Rust across six files" (L489)
- example metadata sizing: 10,000 data files ~ 10 manifests, 1 manifest list, 1 metadata file; metadata "a few megabytes," data "terabytes" (L38)
- DataFusion calls scan() with projection [0, 2] for order_id/total example (L468)
- PyIceberg example: "Five lines to go from a REST catalog to a Pandas DataFrame" (L114)
- Glue, Unity Catalog (Databricks), DuckDB iceberg extension all implement/read Iceberg REST (L92, L116, L118)
- Delta Lake added liquid clustering; Hudi added partition evolution; Iceberg "pioneered transparent partition evolution" (L79)

### Cross-references
- L84-89: forward to dev.to articles "Glue Iceberg Rest Api and PyIceberg" and "Duckberg!"
- L120-124: dev.to "DuckDB S3 Tables with Iceberg using Iceberg Rest API"
- L12: "the single most important change in data infrastructure in the last decade" / "foundation that makes a sovereign query engine possible" (book thesis tie)
- L148: "Migration to upstream Apache iceberg-rust is planned once these features are merged" (forward-looking promise; cross-check against MEMORY note on upstream layering)
- L450, L453-457: views workaround tied to dbt compatibility need (forward to dbt chapter)
- No explicit "as we saw in ch X" / "we'll cover in ch Y" phrasing.

## Pacing
Flows well. Strong progressive disclosure (principle -> metadata tree -> isolation -> evolution -> PyIceberg -> iceberg-rust gaps -> the bridge -> lifecycle -> payoff). No walls of text; paragraphs stay 3-5 sentences. "Building the Bridge" is dense with code but each block is preceded by its concept, which keeps it readable. Only soft spot is the closing paragraph (L489), which over-stays.

## Grade
Voice adherence: A-. Tight rhythm, opinionated, transparent about workarounds and dead ends, mechanically clean. Held below A by the "Why Rust Changes the Game" idiom (L418), the diluted/repetitive closing (L489), and three leading-"This" sentences (L66, L359, L450).
