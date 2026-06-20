# Following Through {#sec:punchlist}

> A punch list works when you treat it like a contract.
> Not when you treat it like a wish.

The previous chapter ended at 129 of 189 cells full. Sixty-eight per cent. We had named the gaps. We had written down what each partial cell needed to flip. The chapter closed on a line about quoting only the numbers earned by tests that ran today.

That sentence is easy to write. Living up to it is the work.

This chapter is the next six months of that punch list. Score 129 to 164. Six phases of focused effort. Two refactors that paid for themselves the day they landed. One reality check where the doc had been lying about the code for months, and the only person to notice was the person reading both at the same time.

## What was on the list

The previous chapter closed with a section called "What comes next." Read it as a contract:

- Worker bloom filters: the property round-trips through `SHOW CREATE TABLE`, but the worker write path does not wire it into its parquet writer config.
- Spark cross-engine reads: the test ships as `#[ignore]` because the Spark stack is not in `docker-compose.test.yml`.
- MERGE on V3: covered transitively through UPDATE; needs a direct test.
- PostgreSQL JDBC catalog: pinned to upstream `iceberg-catalog-sql` adoption.
- HMS and Glue: the SQE code was config scaffolding only.
- Variant, geometry, vector types: blocked on upstream iceberg-rust and arrow-rs.
- Hidden partitioning on V3: blocked on PARTITIONED BY support that did not exist yet.

Three of those gaps were waiting on someone else. Four were on us. We took the four.

## The bloom filter probe

The bloom filter cell was the most embarrassing one. It said "the worker write path still does not wire the bloom property through to its parquet writer config" with a straight face. The fix should have been a five-line change. The reason it did not happen was that we never had a test that would catch the regression.

Phase R closed the loop with a self-contained unit test. Write a parquet file via the same `build_writer_props` helper SQE uses. Re-read the file. Inspect the footer metadata. Assert the bloom filter offset is there.

```rust
#[test]
fn writer_props_emit_bloom_filter_in_parquet_footer() {
    let mut tbl = HashMap::new();
    tbl.insert("write.parquet.bloom-filter-columns".into(), "id".into());
    let props = build_writer_props(&tbl, /* schema fields */ &fields);

    let bytes = write_one_row_with(props, /* batch */);
    let metadata = parquet::file::reader::SerializedFileReader::new(bytes)?
        .metadata();

    let row_group = metadata.row_group(0);
    let column = row_group.column(0);
    assert!(
        column.bloom_filter_offset().is_some(),
        "bloom offset must be in the parquet footer when the table \
         property is set on `id`"
    );
}
```

That test does not need docker. It does not need Polaris. It does not need an S3 bucket. The whole point of the bloom filter cell staying partial had been "we do not have a stack-up test for this," and the answer turned out to be: we did not need one. A 60-line unit test covers it.

While writing the test, we noticed the matrix caveat had been wrong all along. It claimed the worker had a separate parquet writer that needed the property wired in. The worker has no separate parquet writer. All data-file writes go through the same `parquet_writer_config` helper. There was nothing to wire because there was no second site. The cell had been sitting at partial because the engineer who wrote the caveat was looking at the wrong code.

`bloom-filters:v2` and `bloom-filters:v3` flipped. Score 158 to 162. Two cells, one unit test, no infrastructure.

## PARTITIONED BY arrives, then evolves

`PARTITIONED BY` had been the longest-standing gap in the V3 column. Every engine in the matrix has it. SQE did not. The reason was straightforward: when we wrote CREATE TABLE the first time, we did not parse the partition clause. The handler accepted the SQL, ignored everything between the schema and the closing paren, and created an unpartitioned table. Users did not notice because Polaris accepted the request and the resulting tables looked correct in the catalog.

Phase M added the parser path for the six standard Iceberg transforms: `identity`, `year`, `month`, `day`, `hour`, `bucket(N, col)`, `truncate(N, col)`, plus `void`. Each one maps to a `Transform` variant in iceberg-rust. The TaskWriter was already correct; the gap was the parser bridge.

Phase N added partition evolution: `ALTER TABLE ADD/DROP/REPLACE PARTITION FIELD`. This is the spec feature that says "you decided to partition by `day(ts)` last quarter; now you want `hour(ts)` going forward, and the historical files keep their old partitioning." Iceberg handles this through partition spec versioning. The data files carry the spec id they were written under. The engine reads them all and lets the partition pruner reason across multiple specs.

The catch was that SQE's writer assumed the table had a partition spec at write time. An unpartitioned-but-evolved spec (a table that started unpartitioned and gained a partition field through ALTER) hit a code path that crashed because it expected non-empty `partition_fields`. One assertion, one branch, and the writer accepted the new shape.

`partition-evolution:v2` and `partition-evolution:v3` flipped from partial to full. Same pattern as the bloom filters: the cell was partial because nobody had asked the writer about the unhappy case. Once we asked, the bug was a fifteen-minute fix.

## Live catalogs against real services

The HMS, Glue, and JDBC cells had been hedged with the phrase "the SQE code is config scaffolding only." That was true. We had structs called `HmsBackend` and `GlueBackend` with `new()` and `build_catalog()` methods that would, in theory, build the right thing. None of them had ever been pointed at a real service.

Phase O ran them against the real services.

```yaml
# docker-compose.hms.yml
services:
  hive-metastore:
    image: apache/hive:standalone-metastore-4.1.0
    ports:
      - "19083:9083"
    environment:
      SERVICE_NAME: metastore
```

The first run failed with a Thrift connection error because the upstream HmsCatalog client calls `to_socket_addrs()` directly on the configured address, which means it wants `host:port` (no scheme prefix). The address we had been passing was `thrift://localhost:9083`. The fix was a one-character change in the test config.

The next run failed because Docker for Mac forwards ports to IPv4 first but `localhost` resolved to IPv6. Force IPv4 with `127.0.0.1` and the round-trip works: create namespace, list namespaces, drop namespace.

Glue ran against a real AWS account. The test reads credentials from the standard AWS provider chain so it picks up whatever the operator already has set up:

```bash
cp .env.example .env  # then edit AWS_PROFILE / AWS_REGION / warehouse
set -a; source .env; set +a
cargo test -p sqe-catalog --features glue backends_integration -- \
    --ignored glue::live_glue_namespace_round_trip
```

The first time we ran it, the live test found `iceberg_demo_analytics.iceberg_user_events` (around 1.5 million rows) on account 123456789012 in `eu-central-1`. The second time, we pointed the same code at S3 Tables in `eu-west-1` and got back `testtablebucket/testnamespace/daily_sales`. Both worked. Same code path, different region, different service.

Nessie ran against `ghcr.io/projectnessie/nessie:0.107.5` over Iceberg REST. PostgreSQL JDBC ran against `postgres:15` from `docker-compose.test.yml`. Five matrix cells flipped from partial to full: `hive-metastore:v2/v3`, `aws-glue-catalog:v2/v3`, and `nessie:v3`. Score 153 to 158.

The pattern across all five was the same. The code was almost right. The integration test was the only thing that surfaced the small gap: the address format, the IPv6 default, the warehouse path that needed an `s3://` prefix instead of a bare bucket name. None of those bugs would have shown up in unit tests against mocks.

## Unity Catalog OSS as a special case

Phase Q flipped `unity-catalog:v2/v3` to full through a different path. Unity Catalog OSS exposes an Iceberg REST adapter at `/api/2.1/unity-catalog/iceberg/`. The Databricks-hosted Unity uses bearer auth via an M2M OIDC provider; the OSS image disables auth entirely. The same `iceberg-catalog-rest` client reaches both because the endpoint shape is identical.

The wrinkle was that the OSS image is read-only at the version we tested. List namespaces and load tables work. Create, drop, and commit do not (per `unitycatalog/unitycatalog#3`). The test asserts the seeded `unity.default.marksheet_uniform` table comes back through `list_namespaces` and `list_tables`. Anything mutating gets a 501 from the upstream code, which is exactly what we wanted to learn.

We pinned the docker image to `unitycatalog/unitycatalog:main-2f2e32d` so the test does not break the next time someone ships breaking changes to `main`. Score 158 to 162. Same MR as the bloom filter probe, different gap.

## The loader refactor pays for itself

By Phase O we had four backend modules in `crates/sqe-catalog/src/backends/`: `glue.rs`, `hms.rs`, `sql.rs`, and `hadoop.rs`. Each was a thin wrapper around an upstream catalog crate. Each had its own `BackendConfig` struct with the same three or four fields under different names. Each had its own `build_catalog()` method that did the same shape of work: read props, construct an upstream builder, call `.load()`.

We were writing the same code four times.

iceberg-rust upstream has a crate called `iceberg-catalog-loader` that is the factory pattern done right. Pass it a `(catalog_type, props_map)` pair and it returns a `Box<dyn Catalog>`. Every backend implements the same trait. The loader picks the right one. SQE had been ignoring this crate because it predated our code, then because we already had wrappers, then because nobody had a reason to revisit the decision.

The loader-refactor MR deleted the wrapper modules. All three of them. Six hundred lines gone in a single commit. `crates/sqe-catalog/src/rest_catalog.rs` now has a single dispatch site:

```rust
async fn for_session_other_backend(
    config: &SqeConfig,
    bearer: &str,
    backend: &CatalogBackend,
) -> Result<Self> {
    let (catalog_type, name, props) = match backend {
        CatalogBackend::Hms { uri, warehouse } => /* ... */,
        CatalogBackend::Glue { region, warehouse, .. } => /* ... */,
        CatalogBackend::Jdbc { url, warehouse } => /* ... */,
        CatalogBackend::S3Tables { region, warehouse, .. } => /* ... */,
        // ...
    };

    let inner = iceberg_catalog_loader::load(catalog_type)
        .ok_or_else(|| /* ... */)?
        .load(name.to_string(), props)
        .await?;
    // wrap and return
}
```

Adding a new backend used to mean: write a new `BackendConfig` struct, write a new `BackendImpl::build_catalog()` method, write a new feature gate, wire it into the dispatch match. Now adding a backend means: enable the upstream crate's cargo feature, add the variant to `CatalogBackend`, add one match arm.

S3 Tables landed in the same MR. AWS S3 Tables is "managed Iceberg" with a native AWS API, separate from the federated Glue Iceberg REST endpoint. The upstream `iceberg-catalog-s3tables` crate ships an `S3TablesCatalog` over the AWS SDK. We added `S3Tables { region, warehouse, table_bucket_arn, .. }` to `CatalogBackend` and a single match arm. The first run worked. Live test against the eu-west-1 bucket returned `testtablebucket/testnamespace/daily_sales` end to end.

The loader refactor did not flip any matrix cells by itself. The matrix scoring is per-feature, not per-architecture. What it did was lift the engine-wiring caveat that had sat on the HMS, Glue, JDBC, and S3 Tables cells for the previous three phases. Before the loader refactor, `SessionCatalog::for_session` knew how to build a REST catalog and how to error out on everything else. After the loader refactor, every backend ships through the same code path.

This is the kind of work the matrix does not measure but the engineers do. Score 162 to 163.

## When the doc was lying about the code

The next round started with a question from the user reading the trino-compatibility doc: which features are actually missing?

The doc listed several. We started cross-checking them against the code. Most of the matches were honest. Two were not.

The first lie was about MoR writes. The doc said:

> SQE currently uses CoW. MoR would improve efficiency for small deletes on large tables. Implement MoR DELETE path: write position delete file, append via `FastAppendAction`. ~400 lines. CoW works today as fallback.

Past tense. Future work. A four-hundred-line estimate.

The code, in `handle_delete_dispatch`:

```rust
let mode = resolve_delete_mode(table.metadata().properties())?;

match mode {
    WriteMode::MergeOnRead => {
        let has_ids = table.metadata().current_schema()
            .identifier_field_ids().next().is_some();
        if has_ids {
            self.handle_delete_equality(session, stmt, catalog, ctx).await
        } else {
            self.handle_delete_mor(session, stmt, catalog, ctx).await
        }
    }
    WriteMode::CopyOnWrite => {
        self.handle_delete(session, stmt, catalog, ctx).await
    }
}
```

The dispatcher had shipped in Phase O+ step 8d. The position-delete writer existed. The equality-delete writer existed. The `FastAppendAction` commit path existed. Every line the doc claimed needed writing had been written months earlier. The doc had never been updated.

The fix was a doc fix. Set `TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')` and SQE writes a position-delete file (no PK declared) or an equality-delete file (PK declared) and commits. CoW remains the default. Both paths exist.

The second lie was about the JDBC v3 cell. The matrix caveat said:

> The V3 live test against the JDBC backend is the only remaining work to flip this cell to full.

That one was honest. It had been honest for two months. We had just never written the test. Twenty-five lines closed it:

```rust
#[tokio::test]
#[ignore = "requires live Postgres; run with --ignored"]
async fn jdbc_postgres_v3_table_format_version_roundtrip() {
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(/* ... */)
        .load("postgres-jdbc-v3-test", props)
        .await?;

    let mut table_props = HashMap::new();
    table_props.insert("format-version".into(), "3".into());

    let creation = TableCreation::builder()
        .name(table_name.clone())
        .schema(schema)
        .properties(table_props)
        .build();

    let created = catalog.create_table(&ns, creation).await?;
    let v_at_create = created.metadata().format_version();

    drop(created);
    let reloaded = catalog.load_table(&table_ident).await?;
    let v_at_reload = reloaded.metadata().format_version();

    assert_eq!(v_at_create, FormatVersion::V3);
    assert_eq!(v_at_reload, FormatVersion::V3);
}
```

Create a table with `format-version=3` through the JDBC backend. Drop the in-memory handle. Reload through the catalog. Assert metadata still reports V3.

`jdbc-catalog:v3` flipped from partial to full. Score 163 to 164.

## The SQL surface lift

The same audit pass found two type-system gaps that had been sitting in the doc as "structural" or "pending upstream" when they were actually one-line fixes.

`SqlType::JSON` arrived in sqlparser-rs 0.54. SQE's `sql_type_to_arrow` had a default case that returned `NotImplemented` for any type not in the explicit match. JSON fell through. The doc said "wait for `datafusion-variant` (Iceberg V3 VARIANT type) or register custom CAST rules."

What we needed was one match arm:

```rust
SqlType::JSON => Ok(DataType::Utf8),
```

JSON has no native Arrow logical type. It does not need one. Storing JSON as UTF-8 is what every Iceberg engine does. `CAST(json_col AS BIGINT)` rides DataFusion's built-in coercion from `Utf8` to `Int64`. The `json_extract`, `json_get_str`, `json_get_int`, and friends already work and are registered at session start. The lift was just the type alias.

`TIME` was the same shape. Arrow has `Time64(Microsecond)`. iceberg-rust has been mapping Iceberg's `time` primitive to it for two releases. SQE already had a `localtime()` UDF whose comment said it returned `Time64`, except the implementation actually returned `Timestamp`. Reading the code with one eye on the comment was enough to find the bug.

We fixed three things. The match arm:

```rust
SqlType::Time(precision, tz_info) => {
    if sqe_sql::is_tz_variant(tz_info) {
        return Err(SqeError::NotImplemented(
            "TIME WITH TIME ZONE has no Arrow equivalent. \
             Use TIMESTAMP WITH TIME ZONE instead.".into()
        ));
    }
    let p = precision.unwrap_or(6);
    if p > 6 {
        return Err(SqeError::NotImplemented(format!(
            "TIME({p}) precision exceeds Iceberg's microsecond `time` \
             primitive. Use TIMESTAMP(9) for sub-microsecond resolution."
        )));
    }
    Ok(DataType::Time64(arrow_schema::TimeUnit::Microsecond))
}
```

The `localtime()` body to actually return `Time64`. And the `extract_component` function that backs the `hour()`, `minute()`, and `second()` UDFs to handle Time64 inputs (both the microsecond and nanosecond Arrow variants, because DataFusion's `CAST(... AS TIME)` produces nanoseconds).

A fourth callback flowed through the existing macro. `year()` on a TIME column returns `None` from that callback, and the dispatch raises a clear plan error: "year() is not supported on TIME columns; use a TIMESTAMP or DATE source." Trino does the same thing. Silent zero would have been worse than loud failure.

The lift was a hundred and fifty lines across two files. The trino-compatibility doc had been listing TIME and JSON as "honest technical debt we can close" with effort estimates of a hundred and fifty and fifty lines respectively. Both estimates turned out to be right. The work that had been put off because the doc framed it as a multi-stage roadmap was, in fact, an afternoon.

Type System coverage went from 74.1% to 81.5%. Scalar JSON went from 91.7% to 100%. None of those lifts were in the matrix; they are in the trino-compat doc. Both numbers belong in the project README.

## The repaired tests

While we were in the test crate, the integration test suite stopped compiling. The loader refactor that deleted `backends/glue.rs`, `backends/hms.rs`, and `backends/sql.rs` had also left `tests/backends_integration.rs` referring to the deleted types. The test build was broken. It had been broken since the loader refactor merged. None of the team had run `cargo test --tests` against the `sql-postgres` feature in the four commits between the loader refactor and the SQL surface lift, because all the live tests were `#[ignore]`d and `cargo test` in default mode passed without running them.

The fix was an afternoon of test migration. `mod glue` and `mod hms` moved from the SQE wrappers to the upstream `GlueCatalogBuilder` and `HmsCatalogBuilder` directly, which is the same path the loader takes anyway. `mod sql` had been testing the deleted `SqlBackend` rusqlite helper; we replaced it with a builder smoke test that proves the new vendored crate fast-fails when sqlx is not configured for the database scheme.

The matrix evidence column had been pointing at file paths that no longer existed. Every cell that referenced `crates/sqe-catalog/src/backends/{glue,hms,sql}.rs` got its evidence updated to point at the loader dispatch site instead.

This is the unglamorous work that does not appear on any dashboard. The score went up by zero. The MR got merged because the test suite ran green again.

## What the matrix looks like now

```
Score: 164/189 (86.8%)

V3 column (Phase I to today):
  table-creation:v3        partial -> full
  write-insert:v3          partial -> full
  read-support:v3          partial -> full
  copy-on-write:v3            none -> full
  write-merge-update-delete   none -> full
  merge-on-read:v3         partial -> full
  position-deletes:v3      unknown -> full
  equality-deletes:v3      partial -> full
  schema-evolution:v3      partial -> full
  statistics:v3               none -> full
  cdc-support:v3           unknown -> full
  time-travel:v3           partial -> full
  type-promotion:v3        partial -> full
  catalog-integration:v3      none -> full
  polaris:v3                  none -> full
  rest-catalog:v3             none -> full

Catalogs (Phase M to today):
  partition-evolution:v2/v3   partial -> full
  hive-metastore:v2/v3        partial -> full
  aws-glue-catalog:v2/v3      partial -> full
  nessie:v3                   partial -> full
  jdbc-catalog:v2/v3          partial -> full
  unity-catalog:v2/v3         partial -> full
  bloom-filters:v2/v3         partial -> full

Sql surface (this round):
  jdbc-catalog:v3 closes the last partial catalog gap
```

Six cells remain `none`, all blocked upstream: `multi-arg-transforms:v3`, `variant-type:v3`, `shredded-variant:v3`, `geometry-type:v3`, `vector-type:v3`, `lineage:v3`. We could write integration tests for these tomorrow and they would still fail because the primitives do not exist in iceberg-rust or arrow-rs. We have issues open on each. The matrix caveat names the upstream PR by number.

Eight cells remain `partial`. Five of them are catalog cells that need a live test against a service we do not have running yet (Snowflake Horizon needs a real Snowflake account; Hadoop needs a MinIO-backed warehouse beyond the smoke we already run; the table-maintenance cells stay partial only because `rewrite_data_files` is still manifest-only). One is `equality-deletes:v2` which keeps a single behavioural caveat about `RowDeltaOperation::delete_entries` returning `Ok(vec![])` against the RisingWave fork's SnapshotProducer. None of the remaining partials say "untested" anymore. Every partial caveat names a specific code change or a specific live test or a specific upstream issue.

## Three more rules from this work

**Stale docs are silent debt.**

The MoR-already-shipped finding is the canonical case. The doc had described a state the code left behind months earlier. Nobody had updated the doc because nobody had a reason to read it together with the dispatcher. Every reader who trusted the doc missed an opportunity. Every contributor who quoted the doc to a colleague reinforced the lie. Docs that describe what the code "will do someday" age into bugs the same way unused TODOs do.

The fix is process, not technique. When you change a code path that has a doc, you change the doc in the same commit. When you find a doc that contradicts the code, you fix one or the other in the next commit, not the next sprint. Git diffs that touch only docs are not procrastination; they are debt service.

**Loader patterns beat per-backend wrappers.**

Six hundred lines of SQE code became zero lines. The same shape of work used to live in four files; now it lives in one match arm and one upstream factory call. The upstream loader was already there. We had been ignoring it because the wrappers came first. The longer we kept the wrappers, the more code they accreted, and the more the cost of switching grew. The refactor felt expensive until we did it. It cost half a day.

The lesson is not "always use the factory pattern." The lesson is that when an upstream library reaches feature parity with your local wrapper, the local wrapper becomes a maintenance tax. Pay it down deliberately, before it doubles.

**Partial cells should point at a pull request, not a phase.**

The bloom filter cell sat at partial for two months because the caveat said "Phase B follow-up." Phase B was a wishlist. Nobody owned it. The day we changed the caveat to "the worker write path needs to wire `write.parquet.bloom-filter-columns` through to its parquet writer config," somebody read the line, opened the file the line named, and discovered the work had no code in front of it because no separate worker writer existed in the first place. The caveat was the wrong shape, and the wrong shape hid the truth.

A good partial cell names a code change. A great partial cell names the code change with a file path and an estimated diff size. The matrix is a punch list, and a punch list works when each item is a contract with a deadline and a definition of done.

## Where this leaves us

129 to 164. Thirty-five points across six phases over six months. Zero invented features. Every flip earned by a test that ran today against a real stack.

The remaining gaps are real and named. The next time someone asks if SQE is production-ready for their workload, we point at the matrix, the trino-compat doc, the benchmark report, and let them decide. Three numbers. Three different questions. Each one earned by tests that ran today.

The matrix is now most useful when we read it on the same day we read the code. Every partial caveat that says something the code disagrees with becomes an MR. Every flipped cell becomes a doc update in the same commit that flipped it. The matrix keeps the engineers honest because the engineers keep the matrix honest.

That is the only state in which a public scoreboard is worth having.
