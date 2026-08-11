# Vendor fork delta: vendor/iceberg-rust vs pristine RW 813e54419b43

Date: 2026-08-10. Method: shallow-cloned risingwavelabs/iceberg-rust branch
`dev_rebase_main_20260303`, checked out `813e54419b43` in the scratchpad
(`pristine-iceberg/`), `diff -ruN` against `vendor/iceberg-rust` (excluding
.git/target/Cargo.lock). Full diff: `vendor-delta.diff`; per-file counts:
`perfile.txt`. Cross-checked against `vendor/iceberg-rust/README.md` and the
65-commit `git log -- vendor/iceberg-rust` history in the SQE repo.

## 0. How the vendor tree is tracked

Plain committed source in the SQE repo (no submodule, no nested .git).
Excluded from the SQE workspace (`Cargo.toml:26` `exclude = ["vendor/iceberg-rust", "vendor/jiter"]`)
and consumed via path deps (`Cargo.toml:62-86`). 65 SQE commits touch
`vendor/iceberg-rust`. Policy per `vendor/iceberg-rust/README.md`: RW fork
baseline + selectively audited apache-main backports as focused commits, each
documented; every SQE-only change is supposed to carry an inline
`SQE PATCH (sqe#...)` marker (most do; several do not, see section 3b).

## 1. Hard ceiling: DataFusion / Arrow / Parquet

`vendor/iceberg-rust/Cargo.toml`:

- `datafusion = "54"`, `datafusion-cli = "54"`, `datafusion-sqllogictest = "54"` (lines 85-87)
- `arrow-* = "58"` (lines 52-60), `parquet = "58"` (line 127)
- No `object_store` dep at the vendor root (FileIO is opendal-based).
- `crates/integrations/datafusion/Cargo.toml` inherits all of these via `workspace = true`.

CRITICAL NUANCE: pristine `813e544` pins `datafusion = "53.0.0"`. The DF 54 in
the vendor tree is itself a LOCAL SQE patch (SQE commit `0f986cb` "bump
DataFusion 53.1 -> 54.0"), including code adaptations across
`integrations/datafusion` (e.g. `expr_to_predicate.rs`: `c.data_type` ->
`c.field.data_type()`). So:

- The vendor tree compiles against DF 54 / Arrow 58 / Parquet 58 today. Any
  SQE DataFusion bump beyond 54 means SQE hand-ports the vendored
  `iceberg-datafusion` again; the RW branch (still DF 53 at this pin) does not
  do it for you.
- Apache 0.10.0 `iceberg-datafusion` targets DF 53 — a downgrade (README:88-95).
- Apache cherry-pick MSRV floor: rust 1.94 (README:92-93). SQE workspace
  `rust-version = "1.85"`, toolchain bumped to 1.97.1 (commit `0fee767`).

## 2. Size of the local delta (empirical)

~7,200 changed/added lines across 69 files that exist in the vendor tree
(blank added/removed lines not counted). Discount ~960 lines that are the
`utils.rs -> util/mod.rs + util/snapshot.rs` rename artifact and ~150 bench
lines: the real functional delta is roughly 6,000 lines. Top hotspots
(changed lines, `perfile.txt`):

| Lines | File | What |
|---|---|---|
| 1310 | `crates/iceberg/src/arrow/reader.rs` | 4+ interleaved patch families (DecodeGate #367, SBBF #369, fetch pipeline, parallel decode #131, hash-set IN filters, RW #190, int96 hooks) — THE rebase hotspot |
| 518 | `crates/iceberg/src/arrow/int96.rs` (new) | apache #2301 INT96 coercion |
| 485 | `.../datafusion/src/physical_plan/physical_to_predicate.rs` (new) | sealed runtime-filter -> Predicate conversion + capped CASE-of-InLists union (#369) |
| 408 | `crates/iceberg/src/transaction/row_delta.rs` (new) | apache #2203 RowDeltaAction cherry-pick |
| 394 | `crates/iceberg/src/transaction/manifest_filter.rs` | RW #188 dangling-DV backport (`99a270a`) |
| 332 | `crates/iceberg/src/transaction/branch.rs` (new) | SQE-authored branch/tag transaction actions |
| 319 | `crates/catalog/rest/src/client.rs` | sigv4 wiring + #388 header-clobber fix |
| 266 | `crates/iceberg/src/scan/mod.rs` | families 1/6/7/8/9 knobs on TableScanBuilder |
| 257 | `crates/iceberg/src/transaction/rewrite_files.rs` | no-new-deletes validation (`8b43814`) + multi-file position-delete conflict (`fdc0f3c`) |
| 240 | `.../datafusion/src/physical_plan/scan.rs` | runtime-filter pushdown, per-partition file fix (`e5de71e`), #2360 limit display |
| 200 | `crates/catalog/rest/src/sigv4.rs` (new) | AWS SigV4 signer (feature `aws-sigv4`) |
| 170 | `crates/iceberg/src/expr/visitors/sbbf_row_group_evaluator.rs` (new) | bloom row-group pruning (#369) |
| 118 | `.../datafusion/src/schema.rs` | #195 block_on fix |
| 107 | `crates/catalog/sql/src/catalog.rs` | sqlx 0.9 dynamic-SQL gate patch |
| 106 | `crates/iceberg/src/arrow/scan_metrics.rs` (new) | ScanMetrics (incl. #2349-inspired delete-file byte counting) |

Also pruned relative to pristine: bindings/, examples/, integration_tests/,
sqllogictest/, cache-moka, playground, all crate tests/testdata, CI config.
Marker census: `SQE PATCH (sqe#369)` x26, `(sqe#scan-fetch-pipeline)` x16,
`(sqe#367)` x9, `(sqe#388)` x2, `(sqe#deps-sqlx-0.9)` x1.

## 3. The re-apply checklist for a future rebase

### 3a. Documented SQE patch families (README.md:97-237)

1. `expr::dynamic` DynamicPredicate — `crates/iceberg/src/expr/dynamic.rs` (60 lines), `scan/mod.rs`, `arrow/reader.rs`. Filed upstream as apache #2376, not landed.
2. REST SigV4 — `crates/catalog/rest/src/{sigv4.rs,client.rs,lib.rs}`, cargo feature `aws-sigv4` (rest/Cargo.toml).
3. `CatalogBuilder::with_storage_factory` trait default — `crates/iceberg/src/catalog/mod.rs` (14-line delta).
4. FileIOBuilder scheme-string shims in vendored apache-0.9.0 hms/glue/sql catalogs (`catalog/{hms,glue,sql}/src/catalog.rs`, 63-107 lines each incl. AWS client surgery).
5. Loader feature gates + `Send + Sync` on `BoxedCatalogBuilder` — `catalog/loader/{Cargo.toml,src/lib.rs}` (59+22 lines).
6. Current-schema projection for non-time-travel scans (sqe#358) — `scan/mod.rs` `TableScanBuilder::build` + 2 regression tests.
7. DecodeGate decode admission (sqe#367) — `arrow/reader.rs`, `scan/mod.rs`; markers `SQE PATCH (sqe#367)`.
8. SBBF bloom row-group probing (sqe#369) — `expr/visitors/sbbf_row_group_evaluator.rs`, `expr/{mod,visitors/mod}.rs`, `arrow/{reader,scan_metrics}.rs`, `scan/mod.rs`, `physical_plan/physical_to_predicate.rs` (incl. the 65536 CASE-union cap from `c18f5cf`, the q21/q12 regression fix).
9. Fetch/decode pipelining (`with_fetch_ahead`, `DecodeGate::admit_fetch`) — `arrow/reader.rs`, `scan/mod.rs`; markers `SQE PATCH (sqe#scan-fetch-pipeline)` (commit `a8fcfdb`).
10. Strict-metrics residual elimination — `scan/context.rs`, `expr/visitors/strict_metrics_evaluator.rs` (commit `f9a7f8e`).
11. REST `execute()` header-clobber fix (sqe#388) — `catalog/rest/src/client.rs` (+ marker in `catalog.rs:1320`). Upstream likely has the same bug; not yet reported.

### 3b. Local patches NOT in the README patch-family list (found empirically / via git log)

- **#195 block_on wedge fix** (commit `019e00e`, MR !453):
  `crates/integrations/datafusion/src/schema.rs:51` `block_on_runtime_compat`
  (flavor-aware: `block_in_place`+`Handle::block_on` on multi-thread, scoped
  OS-thread `handle.block_on` on current-thread), used in `register_table`
  (schema.rs:214-217) and `deregister_table` (schema.rs:252-253). No `SQE PATCH`
  marker; the doc comment cites SQE #195. 118-line delta.
- **DF 53 -> 54 port** (`0f986cb`): vendor root Cargo.toml + API adaptations in
  `integrations/datafusion` (e.g. `expr_to_predicate.rs` column API change).
- **`row_delta.rs`** — apache PR #2203 RowDeltaAction cherry-pick adapted to the
  fork's `SnapshotProducer` (commit `b36e10f`). Load-bearing: MoR write path.
- **`branch.rs`** — SQE-authored `CreateBranchAction`/`CreateTagAction`/`RemoveRefAction`
  (commit `6520065`).
- **`int96.rs`** — apache #2301 re-applied (`7e80978`). README:269-274 is STALE
  (still says reverted/skipped).
- **`rewrite_files.rs` compaction-safety patches**: `8b43814` (RewriteFilesAction
  validates no new deletes arrived for rewritten data files, +200 lines) and
  `fdc0f3c` (multi-file position deletes = conflict). Post-audit, undocumented
  in README.
- **REST auth guards** (commits `3ea5dd3`, `9d523fa`, `05d4bfc`):
  `catalog/rest/src/catalog.rs` (75-line delta) — reject `/v1/config`
  token-clobber, unmask 401/403, log outbound auth presence.
- **`ObjectCache::get_manifest` made pub** (`7505929`) — `io/object_cache.rs`.
- **Per-partition file reading fix** (`e5de71e`) + runtime-filter pushdown
  surface (`dd300b3`, `c564a89`, `3ef2662` empty-IN-list prunes everything) —
  `physical_plan/scan.rs`.
- **Parallel intra-file row-group decode (#131)** (`90e11cd`, `83b1bd7`) +
  hash-set IN row filters (`e34f6eb`) — `arrow/reader.rs`.
- **Dependency surgery** (`e7fcc34`, `0fee767`, `5012492`, `515c370`): AWS SDK
  crates `default-features = false` + explicit `aws-smithy-http-client`
  rustls-ring client (RUSTSEC #133); sqlx 0.8.1 -> 0.9 (+ `sqe#deps-sqlx-0.9`
  patch in `catalog/sql/src/catalog.rs:32`); opentelemetry pinned 0.32 in
  rest/Cargo.toml with a "MUST match workspace" drift-trap comment (silent
  traceparent loss on mismatch); explicit `license = "Apache-2.0"` on all
  vendored crates (workspace-inherit broken by the exclude).
- **RW backports** past the pin (documented in README audit section):
  `dfdac5a` (RW #187 lenient truncated string bounds), `99a270a` (RW #188
  dangling DVs on rewrite), `7e512e4` (RW #190 Arrow-schema field-ID fallback).
  These disappear if the rebase target is at/after RW tip `ac90a10d`.
- **ScanMetrics / delete-file byte counting** — `arrow/scan_metrics.rs` (new)
  plus `delete_file_loader.rs` / `caching_delete_file_loader.rs` threading a
  shared `bytes_read_counter` (comment credits apache #2349's idea; the full
  #2349 `read_with_metrics` API is NOT present).

### 3c. The six claimed apache cherry-picks: corrected status

Cargo.toml:60 and README claim six apache-main cherry-picks. Empirically, four
are ALREADY IN pristine `813e544` (absorbed by RW's 2026-03-03 rebase) and are
no longer local delta:

- #2118 `pub fn convert_filters_to_predicate` — in pristine (`expr_to_predicate.rs:45` both sides). Absorbed.
- #2348 fixedbinary(n) — in pristine (`arrow/schema.rs:541/544`). Absorbed.
- #2307 `build_fallback_field_id_map` — in pristine (`arrow/reader.rs`). Absorbed.
- #2351 NaN pushdown — no NaN-related line in the vendor-vs-pristine diff of `expr_to_predicate.rs`. Absorbed.
- **#2360** EXPLAIN pushed-down limit — REAL local delta: `physical_plan/scan.rs:415-416` (` limit:[{limit}]` in `fmt_as`); absent in pristine.
- **#2616** stale dead-code cleanup — REAL local delta: pristine still has `Snapshot::log` + `#[allow(dead_code)]` (`spec/snapshot.rs`, `delete_file_index.rs:72`); vendor removed them.

Consequence: a rebase onto anything at/after apache-main-2026-03 only needs
#2360 and #2616 re-checked, not all six. The Cargo.toml comment overstates the
carry.

## 4. SQE-side coupling surface (fork-only APIs)

Grep of `crates/` (src + tests):

| Fork-only API | SQE consumers |
|---|---|
| `RewriteFilesAction` | `sqe-compaction/src/{lib,rewrite,dispatch}.rs`, `sqe-coordinator/src/maintenance.rs`, `sqe-worker/src/compaction.rs`, `sqe-sql/src/procedures.rs`, `sqe-core/src/config.rs`, + 4 coordinator tests |
| `PositionDeleteFileWriter` | `sqe-coordinator/src/writer.rs` (+ conflict test) |
| `RowDelta` (apache #2203 pick) | `sqe-coordinator/src/{write_handler,writer}.rs`, `sqe-core/src/table_properties.rs`, + 3 it tests (v3_e2e, mor_update_merge, equality_delete) |
| Branch/tag actions (`create_branch`/`create_tag`/`remove_ref`) | `sqe-coordinator/src/{catalog_ops,maintenance}.rs`, `sqe-sql/src/{ddl,classifier}.rs`, lease_cas_spike_test |
| `expr::dynamic` / `DynamicPredicate` | `sqe-catalog/src/iceberg_scan.rs` |
| `DecodeGate` (+`admit_fetch`, `with_fetch_ahead`) | `sqe-catalog/src/{scan_memory,iceberg_scan}.rs` |
| Bloom-probe knobs (`with_bloom_filter_probing_enabled` etc.) | `sqe-catalog/src/iceberg_scan.rs`, `sqe-coordinator/src/{streaming,query_handler}.rs`, `sqe-catalog/tests/bloom_probe_369.rs` |
| `convert_filters_to_predicate` (pub via #2118) | `sqe-catalog/src/{table_provider,expr_to_predicate}.rs` |
| `iceberg-catalog-loader` (feature-gated) | `sqe-catalog/src/rest_catalog.rs::for_session_other_backend` |

No direct SQE references found for `OverwriteFilesAction`, `DeletionVectorWriter`,
or `MergeAppend` (named in Cargo.toml:58-59 but not imported by name in
`crates/`; they may be reached indirectly through transaction entry points —
unverified). INSERT OVERWRITE (#378) went through `rewrite_files` per MR !669.

Anything on this table breaks on a move to plain apache/iceberg-rust 0.10.0,
which lacks rewrite/overwrite actions, position-delete/DV writers,
`expr::dynamic`, and targets DF 53 (README:88-95).

## 5. Feature-flag / build subtleties a rebase must preserve

- `iceberg-catalog-rest` path dep WITHOUT the `aws-sigv4` feature by default;
  `sqe-catalog`'s `rest-sigv4` feature turns it on (root Cargo.toml:63-66).
- `iceberg-catalog-loader = { ..., default-features = false }` (root
  Cargo.toml:86) — mandatory, or every backend's AWS SDK/Thrift/sqlx weight
  returns; the loader's feature gates are themselves a local patch (3a.5).
- Slim Polaris-only build contract: `cargo build --no-default-features
  --features rest` on sqe-catalog (~80 MB compressed, README:474-481).
- Version-lockstep traps (vendor is workspace-excluded so nothing inherits):
  sqlx must equal SQE workspace sqlx (0.9) or cargo `links = "sqlite3"`
  conflict; opentelemetry in rest/Cargo.toml must equal workspace otel (0.32)
  or traceparent propagation silently dies; sqlparser pin note at root
  Cargo.toml:89-94.
- `vendor/jiter` is a second, unrelated vendored crate ([patch.crates-io],
  pyo3 bump).
- The vendor tree's OWN test target does not compile standalone (~49 errors,
  pre-existing); validation path is SQE workspace `cargo test --workspace
  --lib` (README:330-340).

## 6. Cost estimate

**Rebase within the RW fork (new RW tip, e.g. ac90a10d or a fresh
dev_rebase branch):** carry ~6,000 functional lines. Mechanical for most
families (disjoint regions, markers present, prior refresh `4b195c8`
auto-merged families 1/7 against upstream Variant changes), EXCEPT:
`arrow/reader.rs` (1,310-line delta, 4+ families interleaved — every refresh
so far needed hand-resolution there), `transaction/rewrite_files.rs` +
`manifest_filter.rs` (local compaction-safety semantics on top of moving
upstream transaction code; README:60-79 already flags `c3ac742`/`61a8941` as
requiring the write-regression suite), and the utils.rs/util/ rename re-hit.
Plus re-verifying the DF 54 port if RW is still on 53. Realistic effort:
days, not hours, dominated by reader.rs conflict resolution and write-path
regression validation (which needs the quickstart stack; not unit-testable).

**De-vendor to apache/iceberg-rust 0.10.0:** not viable (section 4). The
blocking upstream PRs are #2185 (OverwriteAction) and #2203 (RowDeltaAction)
per README:41, plus everything in section 3a that has no upstream PR at all
(only #2376 is filed). Even then DF 53 vs SQE's DF 54 blocks.

**Do nothing:** the carry is stable but growing (~10 vendor-touching commits
in the last ~6 weeks); every new SQE scan/write feature lands more unmarked
delta in reader.rs / rewrite_files.rs.

## 7. Documentation drift found (README.md corrections for next audit)

- #2301 int96: README says reverted/skipped; it is applied (`arrow/int96.rs`).
- Six-cherry-pick claim: only #2360 + #2616 remain local (3c).
- `rewrite_files.rs` safety patches (`8b43814`, `fdc0f3c`) and REST auth
  guards are not in the patch-family list.
- `branch.rs` / `row_delta.rs` are not mentioned in the README at all.
