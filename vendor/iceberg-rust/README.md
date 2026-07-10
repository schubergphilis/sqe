# iceberg-rust (SQE vendored fork)

This is a vendored copy of the [RisingWave Labs iceberg-rust fork](https://github.com/risingwavelabs/iceberg-rust),
branch `dev_rebase_main_20260303` at commit `813e54419b43`. The branch carries
DataFusion 53.0 / Arrow 58 / Parquet 58 (RW PR #148, 2026-04-15) plus writer
and transaction fixes on top.

## Vendored crates

| Crate | Purpose | Used by |
|-------|---------|---------|
| `iceberg` | Core Iceberg types, expressions, scan API, transactions | always |
| `iceberg-catalog-rest` | Iceberg REST protocol client (Polaris, Nessie, Unity, Glue REST, S3 Tables REST) | always |
| `iceberg-catalog-glue` | AWS Glue Data Catalog over the AWS SDK | sqe-catalog `glue` feature |
| `iceberg-catalog-hms` | Hive Metastore over Thrift | sqe-catalog `hms` feature |
| `iceberg-catalog-s3tables` | AWS S3 Tables (managed Iceberg) over the AWS SDK | sqe-catalog `s3tables` feature |
| `iceberg-catalog-sql` | JDBC catalog (Postgres / MySQL / SQLite) via sqlx | sqe-catalog `sql` feature |
| `iceberg-catalog-loader` | Dispatches catalog construction by string type name with a uniform `(name, props)` shape; used by SQE in `crates/sqe-catalog/src/rest_catalog.rs::for_session_other_backend` | always |
| `iceberg-datafusion` | DataFusion `TableProvider`, `IcebergTableScan`, runtime filter bridge, predicate converter | always |
| `iceberg_test_utils` | Shared test helpers | dev-only |

The loader crate is patched (vs upstream) to make its backend deps
optional, gated on cargo features, so a slim build does not
transitively pull every backend's AWS SDK / Thrift / sqlx weight.
The trait `BoxedCatalogBuilder` is also patched to require
`Send + Sync` so the returned box can cross await points in async
contexts.

## Why a fork?

Apache upstream iceberg-rust (v0.9.0) lacks:
- `RewriteFilesAction` / `OverwriteFilesAction` (Copy-on-Write DELETE/UPDATE)
- `PositionDeleteFileWriter` (Merge-on-Read position deletes)
- `DeletionVectorWriter` (Iceberg V3)

The RisingWave fork provides all of these.

## Upstream tracking

- RisingWave fork: `dev_rebase_main_20260303` @ `813e54419b43`
- Apache upstream: tracking PRs #2185 (OverwriteAction) and #2203 (RowDeltaAction)
- When upstream merges these, SQE will migrate to official apache/iceberg-rust

## SQE-only patches in this vendor copy

These patch families ride on top of the upstream snapshot. Each is
documented inline at the touch site so a future rebase can re-apply
them quickly.

1. **`iceberg::expr::dynamic` (DynamicPredicate API)**: runtime
   filter pushdown into IcebergTableScan. Files: `crates/iceberg/src/expr/dynamic.rs`,
   `crates/iceberg/src/scan/mod.rs`, `crates/iceberg/src/arrow/reader.rs`.
   Filed upstream as apache/iceberg-rust#2376; not yet landed.
2. **`iceberg-catalog-rest::sigv4`**: AWS SigV4 signer gated behind
   the `aws-sigv4` cargo feature. Files: `crates/catalog/rest/src/sigv4.rs`,
   `crates/catalog/rest/src/client.rs`, `crates/catalog/rest/src/lib.rs`.
   Used for AWS S3 Tables and Glue REST federation. Not filed upstream yet.
3. **`CatalogBuilder::with_storage_factory`**: trait default in
   `iceberg::catalog`, added so the upstream HMS / Glue / SQL catalog
   crates compile against the fork's trait unmodified.
4. **`FileIOBuilder` scheme-string shims**: in the vendored apache
   v0.9.0 catalog crates (`hms`, `glue`, `sql`) so they speak the
   fork's FileIO API.
5. **`iceberg-catalog-loader` feature gates + `Send + Sync`**: added
   to the loader so SQE's slim builds work and so the boxed builder
   can cross await points. Files:
   `crates/catalog/loader/Cargo.toml`,
   `crates/catalog/loader/src/lib.rs`.
6. **Current-schema projection for non-time-travel scans (issue #358)**:
   `TableScanBuilder::build` resolves the scan projection schema from the
   table's current schema when no `snapshot_id` is requested, instead of the
   schema tagged on the latest snapshot. ALTER TABLE ADD COLUMN is
   metadata-only (no new snapshot), so the latest snapshot keeps the pre-ADD
   schema; reading against it dropped added columns and failed with "Column
   <c> not found in table". Explicit snapshot scans still use the snapshot
   schema (time-travel semantics preserved). Files:
   `crates/iceberg/src/scan/mod.rs` (build + two regression tests). Not filed
   upstream yet.
7. **`DecodeGate` decode admission hook (SQE issue #367)**: optional
   `Arc<dyn DecodeGate>` on `ArrowReaderBuilder` / `TableScanBuilder`,
   consulted before each file scan (sub)task decode on the parallel path
   and held until the subtask's batches are forwarded. SQE implements it
   (`sqe-catalog::scan_memory::ScanDecodeGate`) to bound decode
   concurrency per scan node and reserve the estimated decode memory
   against the DataFusion pool, so pressure fails typed instead of
   OOM-killing the host. All sites marked `SQE PATCH (sqe#367)`. Files:
   `crates/iceberg/src/arrow/reader.rs`, `crates/iceberg/src/scan/mod.rs`.
   Survived the `813e544` refresh (issue #370) via auto-merge; verify the
   `SQE PATCH (sqe#367)` markers on the next refresh too. Not filed
   upstream yet.

## Cherry-picks from apache/iceberg-rust main

Six fixes from apache main applied on top of the RW base. RisingWave
has not backported these yet; they apply on the SQE vendor because
the touched files diverge minimally from apache. The original PR
authorship and commit messages are preserved.

- **#2118** Make `convert_filters_to_predicate` public — visibility
  change on the DataFusion integration so SQE can reuse the filter
  conversion logic.
- **#2348** support `fixedbinary(n)` — adds datum conversion for
  `FixedSizedBinaryArray` (Arrow schema mapping).
- **#2307** fix nested `build_fallback_field_id_map` — predicates on
  columns after struct/list/map in migrated Parquet files no longer
  crash with "Leaf column ... isn't a root column." Test addition
  skipped (depends on the apache-only `serde_arrow` dev-dep).
- **#2351** NaN pushdown correctness — float predicates now handle
  NaN semantics correctly. Test addition skipped (apache-only test
  helpers).
- **#2360** EXPLAIN pushed-down limit — `IcebergTableScan` EXPLAIN
  output shows the pushed-down LIMIT.

Three apache fixes were considered but skipped:

- **#2301** INT96 Parquet timestamps — depends on a `ParquetRecordBatchStreamBuilder`
  flow that exposes `arrow_metadata` and `parquet_file_reader` as
  separate locals. Our base uses an opaque `create_parquet_record_batch_stream_builder`
  helper, so the coercion block does not compile against our shape.
  Initially landed then reverted; revisit when the reader flow is
  refactored or apache lands a port-friendlier patch.
- **#2349** `read_with_metrics` — depends on apache #2358 (arrow
  reader split into modules `pipeline.rs`, `positional_deletes.rs`,
  `projection.rs`, `row_filter.rs`). Bring back when the next vendor
  refresh includes #2358.
- **#2285** snapshot ancestor utils — apache renamed `utils.rs` to
  `util/mod.rs` and added `util/snapshot.rs`. Rename conflict makes
  cherry-pick non-surgical.

## What landed in this rebase

The bump from `645f02a4b533` to `8f7c952f66de` brings 11 upstream
commits from the RW rebase branch:

- DF 53 / Arrow 58 / Parquet 58 upgrade (PR #148) — replaces SQE's
  hand-rebased DF 53 work; the deltas are now identical to upstream.
- `IcebergWriter::write_with_position` (PR #149)
- `DeletionVectorWriter::write` perf — avoid extra copy (PR #150)
- Transaction snapshot summary rollup fixes (PR #151, #152) — fixes
  the `previous_snapshot` lookup that resolved to None and zeroed
  out the `total-*` rollup.
- `object_cache` accurate memory sizing (PR #153)
- `DataFile::set_partition` (PR #154)
- `support dropping schema fields` (PR #155)
- Schema-evolution: handle all Iceberg types when filling missing
  columns after schema change (PR #156)
- REST client: refresh token after 401 unauthorized (PR #157)
- opendal: skip TimeoutLayer under madsim (PR #160)

## Bumped `8f7c952f66de` -> `c034b19105fa` (2026-06-02)

Two more fixes from the same `dev_rebase_main_20260303` branch, applied on top
of the vendored tree as patches. The branch tip `9491dcab` ("Add myself to code
owners") is CODEOWNERS-only and is not vendored.

- **#161 `fix: avoid panic in snapshot summary update_totals on malformed
  values`** (vovacf201). `update_totals` called `.unwrap()` parsing the
  `total-*` / `added-*` / `removed-*` summary properties and used checked
  subtraction that could panic in debug on a corrupt prior summary. Now
  parse-or-zero + saturating arithmetic, matching Java Iceberg's
  `SnapshotSummary`, so one bad prior summary cannot wedge compaction/commit.
  Adds 3 tests. File: `crates/iceberg/src/spec/snapshot_summary.rs`.
- **`fix: iceberg V3 puffin file reader`** (Dylan, commit `c034b191`). Corrects
  the V3 puffin (deletion-vector) read path. Files:
  `crates/iceberg/src/arrow/{caching_delete_file_loader,delete_filter,reader}.rs`
  and `crates/iceberg/src/scan/{context,mod,task}.rs`. Touches the same two
  files as SQE-only patch family 1 (DynamicPredicate) but in disjoint regions;
  verified by a clean workspace build.

How this bump was validated: SQE builds the iceberg crate as a path dep
inside its own workspace, where the lib compiles clean and the workspace
`cargo test --lib` suite (sqe-catalog, sqe-coordinator, sqe-worker, ...)
passes against it. The two fixes' own behavioral tests did not run here:
the 3 bundled snapshot-summary tests compile in-module but the V3 puffin
read path needs the docker-compose stack to exercise end-to-end (deferred).

Note for the next rebaser: the vendored crate's **standalone** test target
does not compile on its own. Running `cargo test --manifest-path
vendor/iceberg-rust/Cargo.toml -p iceberg --lib` fails with ~49 errors
(`ScanResult` not satisfying `futures::TryStreamExt` in the scan/arrow
test modules, plus the resulting `type annotations needed`). This is a
pre-existing futures/scan-test skew in the fork's isolated workspace, not
introduced by this bump (it reproduces identically with these patches
reverted). SQE excludes this vendor copy from its workspace and never
compiles the vendor test target, so the breakage is invisible in practice.
Validate iceberg changes through the SQE workspace path-dep
(`cargo test --workspace --lib`), not the vendor's own test suite.

## Bumped `c034b19105fa` -> `813e54419b43` (2026-07-10, issue #370)

Twelve commits from `dev_rebase_main_20260303`, merged with a 3-way
merge (vendor state committed onto `c034b19`, `git merge 813e544`,
resolve, copy back):

- **#166** account for added delete files in snapshot summary
- **#167** sort position-delete entries globally by `(file_path, pos)`
- **#169** auto-set `referenced_data_file` on `PositionDeleteFileWriter`
- **#170 / #171 / #172** stream manifest lists and manifest loading to
  bound memory (snapshot expiration, rewrite-manifests, append/overwrite
  validation)
- **#174 / #175** rewrite_manifests target size, honored from snapshot
  properties
- **#145** Variant support (Iceberg V3); SQE maps `Type::Variant` to
  `variant` in information_schema
- **#179** fix rewrite/overwrite transactions mishandling DELETE
  manifests (CoW DELETE/UPDATE path)
- **#164 / #176** CODEOWNERS-only, pruned

Merge notes for the next rebaser:

- The vendor tree had already split `utils.rs` into `util/mod.rs` +
  `util/snapshot.rs` and backported early versions of the manifest
  loaders. Upstream's streaming rewrites (#170-#172) superseded those
  backports; conflicts in `transaction/{remove_snapshots,
  rewrite_manifests,snapshot}.rs` were resolved to upstream semantics
  with `crate::util` paths. Upstream still uses a flat `utils.rs`, so
  every refresh will re-hit this rename; git's rename detection folds
  upstream's `utils.rs` edits into `util/mod.rs` automatically.
- SQE patch families 1 (DynamicPredicate) and 7 (DecodeGate) both touch
  `arrow/reader.rs`, which upstream's Variant support also modified;
  all three auto-merged in disjoint regions.
- The five apache cherry-picks (#2118, #2348, #2307, #2351, #2360) are
  still absent upstream and remain applied; verified present post-merge.
- Validation: full workspace `cargo build --all`, 16/16 lib test targets
  green (374 sqe-catalog, 604 sqe-coordinator among them), clippy
  `--all-targets --all-features -D warnings` clean. The #179 CoW
  round-trip and write-mode checks need the quickstart stack (SQE issue
  #371 tracks that verification).

## Catalog config: URL and bucket conventions

User-facing config in `sqe.toml`. Each `[catalog]` block selects
exactly one backend; the keys per backend mirror the upstream
`*_CATALOG_PROP_*` constants and what the upstream builders expect.

### REST (Polaris / Nessie / Unity OSS / Glue REST / S3 Tables REST)

```toml
[catalog]
polaris_url = "https://polaris.example.com:18181/api/catalog"
warehouse   = "test_warehouse"
# `backend` defaults to "rest" so it can be omitted.
[catalog.backend]
type = "rest"
```

REST is the default. AWS endpoints engage SigV4 automatically when
the server's `/v1/config` response advertises
`rest.sigv4-enabled=true` (see SQE-only patch family 2 above).

### Hive Metastore

```toml
[catalog.backend]
type      = "hms"
uri       = "metastore.example.com:9083"   # Thrift host:port
warehouse = "s3a://bucket/warehouse"        # default warehouse path
```

Requires the `hms` cargo feature on `sqe-catalog` (default-on).
Pulls in `volo-thrift` and `pilota`.

### AWS Glue

```toml
[catalog.backend]
type      = "glue"
region    = "us-east-1"
warehouse = "s3://my-bucket/warehouse"
# endpoint = "http://localhost:4566"        # optional, e.g. LocalStack
```

Requires the `glue` cargo feature (default-on). Pulls in
`aws-sdk-glue` + `aws-config`. Authentication uses the standard AWS
SDK chain (env vars, profiles, IMDS).

### AWS S3 Tables (managed Iceberg)

```toml
[catalog.backend]
type             = "s3tables"
table_bucket_arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket"
# endpoint_url   = "http://localhost:4566"  # optional, custom endpoint
```

Requires the `s3tables` cargo feature (default-on). Pulls in
`aws-sdk-s3tables` (shares the AWS SDK runtime already pulled by
`glue`, so the incremental binary cost is small). Authentication
uses the standard AWS SDK chain.

The bucket ARN format is `arn:aws:s3tables:REGION:ACCOUNT:bucket/NAME`.
S3 Tables namespaces map to S3 Tables namespaces; tables map to
S3 Tables tables; storage is automatically managed by AWS.

### JDBC (Postgres / MySQL / SQLite)

```toml
[catalog.backend]
type      = "jdbc"
url       = "postgresql://user:pass@host:5432/iceberg"
warehouse = "s3://my-bucket/warehouse"
```

Requires the `sql-postgres` cargo feature (default-on). The url
prefix selects the driver: `sqlite:` for local files, `postgresql:`
for Postgres, `mysql:` for MySQL.

### Hadoop (filesystem-only, SQE-native)

```toml
[catalog.backend]
type      = "hadoop"
warehouse = "s3://my-bucket/warehouse"
```

No metadata service. SQE walks `warehouse` for `metadata.json` files
and treats the prefix as the catalog. Implemented in
`crates/sqe-catalog/src/backends/hadoop.rs`. Requires the `hadoop`
cargo feature (default-on, no extra dependency cost).

## Slim builds

A REST-only build (no AWS SDK, no Thrift, no sqlx, no S3 Tables)
ships in roughly 80 MB compressed:

```bash
cargo build --release --no-default-features --features rest
```

Add features as needed: `--features rest,glue,s3tables` for
"REST plus AWS." `--features rest,hms` for "REST plus Hive."
The default ships every backend (see
`crates/sqe-catalog/Cargo.toml`).
