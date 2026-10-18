# iceberg-rust (SQE vendored copy)

Built from three layers, in this order:

1. **apache/iceberg-rust `main` @ `119fad92c`** (2026-09-24). Base for everything:
   core, catalogs, the OpenDAL storage crate. Arrow / Parquet 59.2, no DataFusion.
2. **risingwavelabs/iceberg-rust `dev_rebase_main_20260807` @ `07906417c`**, merged on
   top in one merge commit. It carries the row-level write stack apache still lacks:
   `ReplaceFilesAction` (rewrite and overwrite), position-delete and deletion-vector
   writers, rewrite manifests, snapshot and orphan cleanup, delete pruning by file
   metrics (#213, #214), the V3 row-lineage reader (#239) and ~40 fixes.
3. **SQE patch families**, re-applied on the result. Every site is marked
   `SQE PATCH (sqe#NNN)`.

The DataFusion integration moved out of iceberg-rust on 2026-09-17 (apache #3228). It
is vendored separately in `../datafusion-iceberg`.

Geometry / geography types are not in any upstream yet.

## Crates

| Crate | Used by |
|---|---|
| `iceberg` | always |
| `iceberg-storage-opendal` | always; object storage left core, SQE builds it via `sqe_catalog::storage_factory()` |
| `iceberg-catalog-rest` | always (`aws-sigv4` feature for SigV4) |
| `iceberg-catalog-loader` | always (SQE feature gates, `Send + Sync` builder) |
| `iceberg-catalog-{glue,hms,s3tables,sql}` | sqe-catalog features |
| `iceberg-property-macro`, `iceberg_test_utils` | build / dev |

Not vendored: Python bindings, examples, integration tests, the moka cache integration.
Upstream `testdata/` is vendored in full, so `cargo test -p iceberg --lib` runs here.

## SQE patch families

| # | Patch | Where |
|---|---|---|
| 1 | `DynamicPredicate` runtime-filter pushdown | `expr/dynamic.rs`, `scan/mod.rs`, `arrow/reader/file_reader.rs` |
| 6 | Current-schema projection for non-time-travel scans (#358) | `scan/mod.rs` |
| 7 | `DecodeGate` decode admission (#367) | `arrow/reader/mod.rs`, `pipeline.rs` (`read_parallel`) |
| 8 | SBBF bloom row-group probing for runtime filters (#369) | `expr/visitors/sbbf_row_group_evaluator.rs`, `arrow/reader/pipeline.rs`; skips when apache's own bloom pass is on |
| 9 | Fetch / decode pipelining (`with_fetch_ahead`, `DecodeGate::admit_fetch`) | `arrow/reader/pipeline.rs` |
| 10 | Strict-metrics residual elimination | `scan/context.rs` |
| — | Intra-file row-group split (#131), `FileScanTask::with_byte_range` | `arrow/reader/pipeline.rs`, `scan/task.rs` |
| — | Hash-set membership for large `IN` row filters | `arrow/reader/predicate_visitor.rs` |
| — | Read amplification: 64 KiB footer prefetch and 64 KiB range-coalesce gap (upstream 512 KiB / 1 MiB) | `arrow/reader/mod.rs` |
| 2 | REST SigV4 as an `AuthManager` (`rest.sigv4-enabled`, server-advertised, never replaces a bearer) | `catalog/rest/src/sigv4.rs` |
| — | Vended credentials win: `storage-credentials` longest-prefix selection, vended `s3.*`/`gcs.*`/`adls.sas-token*` keys replace local static keys in table FileIO (`SQE PATCH (vended-credentials)`) | `catalog/rest/src/catalog.rs` (`table_file_io_props`) |
| — | Empty-token guards (#2) and W3C trace propagation | `catalog/rest/src/auth/oauth2.rs`, `client.rs` |
| — | Refresh-after-401 on `AuthSession::on_unauthorized` (replaces RW #157; apache has no refresh) | `catalog/rest/src/auth/` |
| 5 | Loader feature gates + `Send + Sync` | `catalog/loader` |
| — | sqlx 0.9 (`AssertSqlSafe`, `LIMIT 0` schema probe) | `catalog/sql` |
| — | Ring-only TLS: no `default-https-client`, opendal no-provider transport installed on first use | root `Cargo.toml`, `storage/opendal` |
| — | New-delete conflict guard on `ReplaceFilesAction` (`set_validate_from_snapshot_id`) | `transaction/replace_files.rs` |
| — | `RowDeltaAction` (apache PR #2203; no upstream equivalent) | `transaction/row_delta.rs` |
| — | Puffin blob range check | `puffin/reader.rs` |
| — | `ManifestFile::load_manifest` shim over apache's `ManifestReader` | `spec/manifest_list/manifest_file.rs` |
| — | V3 `unknown` columns are never written to a data file (top-level projected out, nested refused) (`SQE PATCH (v3-unknown)`) | `writer/file_writer/parquet_writer.rs` |
| — | Shredded V3 variants read: variant manifest bounds skipped, `typed_value` storage accepted and unshredded on promote (`SQE PATCH (v3-shredded-variant)`) | `spec/manifest/_serde.rs`, `arrow/reader/projection.rs`, `arrow/record_batch_transformer.rs` |

Dropped because the new base already has them: the INT96, `read_with_metrics`, NaN,
equality-delete NULL, positional-delete wakeup and run-buffering, constant-column and
temporal-partition-path backports; RW #186 #188 #190 #196 #208; sqe#388 (apache's
`post_form` sets the content type); FileIO scheme shims; SQE's OpenDAL operator cache.
RW's BigLake GCP token exchange was dropped (SQE does not use it).

## Fixes the merged test suite surfaced

- Invalid-UTF-8 string bounds are treated as unknown in manifest pruning (a lossy
  decoded lower bound sorts above the real one).
- Snapshot summary totals never underflow (a remove larger than the known total
  skipped the total instead of writing `u64::MAX`), matching iceberg-java.
- SQL catalog schema detection reports a missing table instead of calling it V0.
- Apache's pos-delete routing test used random paths that RW #214 pruning made flaky.

## Refreshing

Work in a scratch clone of apache/iceberg-rust with `rw` as a second remote:

```
git checkout -B sqe-base origin/main
git merge --no-ff rw/dev_rebase_main_<date>        # one merge, not per-commit picks
# SQE's delta = diff between the previous vendored tree and its merge base:
git diff <old-base> <old-sqe-tree> -- crates > sqe-delta.patch
git apply -3 sqe-delta.patch                       # port what conflicts by intent
cargo test --workspace --exclude pyiceberg_core_rust --exclude iceberg-integration-tests --lib
```

Then copy `crates/{iceberg,property-macro,catalog,storage,test_utils}`, `Cargo.toml`,
`LICENSE`, `NOTICE` here, trim the workspace members, and run SQE's gates plus
`make test-integration`. Compare the integration result against a main baseline;
main carries its own reds.
