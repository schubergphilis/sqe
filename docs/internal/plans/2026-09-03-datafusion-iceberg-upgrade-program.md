# DataFusion and Iceberg upgrade program

Date: 2026-09-03
Status: approved for implementation
Owners: SQL Engine team

## Objective

Harden the completed DataFusion 55 / Arrow 59 migration, backport the highest-
value correctness and bounded-memory fixes from RisingWave and Apache
iceberg-rust, reduce the vendor delta where upstream now provides equivalent
code, and prepare a repeatable path to DataFusion 56 and official Apache
iceberg-rust when those become viable.

DataFusion 55 is the newest published DataFusion release at the time of this
plan. Apache iceberg-rust v0.10.1 and `main` cannot replace SQE's RisingWave
fork: they lack `PositionDeleteFileWriter`, the rewrite/overwrite and RowDelta
transaction actions, `DeletionVectorWriter`, dynamic predicates, and several
SQE scan extensions. Apache PRs #2185 and #2203 remain open and conflicted;
issue #2376 remains open.

## Working rules

- Each correctness concern gets a focused MR, or a clearly ordered commit in a
  stacked MR when two changes modify the same state machine.
- Record the upstream SHA, conflicts, SQE adaptations, tests, and retirement
  condition in `vendor/iceberg-rust/README.md`.
- Do not combine an engine-major dependency bump with an Iceberg transaction
  rewrite.
- Preserve all `SQE PATCH` families unless the replacement is proven by a
  behavioral test and the upstream implementation is in the selected base.
- Coordinator and workers move atomically when DataFusion physical-plan proto
  compatibility changes.
- Any benchmark regression above 5% or peak-memory increase above 10% requires
  an explicit explanation and approval.

## Phase 0: close the audit

MR !909 is the source-of-truth audit. Before merging it:

- push the Apache-main correction (Apache has an equality-delete writer but no
  `PositionDeleteFileWriter` implementation);
- include this implementation plan;
- require its pipeline to pass.

## Phase 1: safety suite

Add reusable V2/V3 fixtures covering:

- optional and required schema evolution, with and without initial defaults;
- nullable and composite equality-delete keys;
- position deletes and deletion vectors;
- Copy-on-Write DELETE/UPDATE/MERGE and Merge-on-Read DELETE;
- concurrent delete loading, cancellation, late consumers, and retry;
- commit conflicts and manifest retries;
- truncated Parquet, manifest, Puffin, and delete files;
- REST, Glue, HMS, SQL, and S3 Tables catalogs;
- multiple S3/GCS buckets and credential refresh.

All concurrency tests use bounded timeouts. Capture baseline correctness,
runtime, peak memory, allocation-sensitive microbenchmarks, and representative
physical plans before applying backports.

## Phase 2: correctness backports

Implement in this order:

| Order | Source | Scope | Required evidence |
|---|---|---|---|
| 1 | Apache #2781 (`22c256e`) | Preserve null data rows for non-null equality-delete values and match null delete values only to null rows. | Nullable single/composite keys and multiple delete rows. |
| 2 | Apache #2797 (`78df272`) | Error when a required field is absent and has no initial default. | Optional-null, required-default, and required-missing cases. |
| 3 | Apache #2859 (`dbbefa4`) | Eliminate positional-delete lost wakeups. | Completion-before-wait, many waiters, cancellation, timeout. |
| 4 | RisingWave #196 (`6bd8e98`) | Propagate/cache equality-delete load failures. | Retryable/non-retryable failure, cancellation, late consumer, poisoned state. |
| 5 | RW #186 (`550393d`) + Apache #2867 (`c0db770`) | Return typed errors for truncated data and malformed Puffin footer lengths. | No panic/hang under corrupt fixtures. |
| 6 | Apache #2509 (`252bb60`) | De-duplicate paths within a FastAppend batch. | First occurrence wins; snapshot totals and manifests stay correct. |
| 7 | Apache #3062 (`6678583`) | Propagate SQL catalog transaction commit failures. | Deferred-constraint failure through the public catalog API. |

Apache #2781, #2859 and RW #196 touch the delete-loader state machine. Keep
their branches stacked in the order above and review the final combined state
for lost notifications and failure publication.

## Phase 3: bounded-memory and performance backports

After Phase 2 is green:

1. Take RW `61a8941` and `0b65d716` together: reuse manifests across conflict
   retries and bound rewrite-manifest memory.
2. Take RW `0d9e873`: stream manifest loading during orphan-file removal.
3. Evaluate Apache #3015 (`7b33321`): buffer positional-delete runs.
4. Evaluate Apache #3080 (`a5f162f`): build repeated constant columns without
   per-row clones.
5. Evaluate RW #208 (`deb4a3c8`): cache OpenDAL operators by the full storage
   identity, not merely bucket, if endpoint/credential differences require it.

Memory changes require large-manifest and maintenance benchmarks. Operator
caching requires multi-bucket isolation, endpoint variation, serialization,
clone sharing, and credential-rotation tests.

## Phase 4: RisingWave baseline refresh

Once accepted focused backports cover the wanted RisingWave delta:

1. Reconstruct the vendor tree from RisingWave `1657c184`.
2. Reapply DataFusion 55 / Arrow 59 compatibility.
3. Reapply the six Apache cherry-picks and all retained SQE patch families.
4. Reapply accepted Phase 2/3 Apache fixes.
5. Compare the reconstructed tree with the incremental vendor tree.
6. Accept only documented intentional differences, then update the pinned
   baseline from `813e544` to `1657c184`.

Do not take `c3ac742` (transaction unification) until the full write regression
suite proves Copy-on-Write, Merge-on-Read, conflict, and compaction behavior.

## Phase 5: DataFusion 55 stabilization

- Update Arrow and Parquet patch releases from lockfile 59.2 to 59.3.
- Assert one DataFusion and Arrow major in `cargo tree`.
- Keep `object_store` 0.13 while DataFusion 55 uses that API; 0.14 would create
  incompatible trait types.
- Keep the vendored `datafusion-functions-json` until upstream publishes a
  DataFusion-55-compatible release.
- Preserve named TVF argument rewriting, empty-input grouping-set wrapping,
  mutable external-store list-cache disabling, and required
  `apply_expressions` implementations.
- Snapshot plans for joins, runtime filters, spills, grouping sets, and Iceberg
  scans and repeat the TPC-H, SSB, and TPC-DS A/B.

## Phase 6: statistics adoption

Behind a disabled experimental flag:

1. Implement an Iceberg/Puffin provider for DataFusion `StatisticsRequest`.
2. Map Iceberg field IDs to DataFusion columns and cache per snapshot.
3. Add the optimizer request producer.
4. Enable `StatisticsRegistry` only in the benchmark profile.
5. Compare estimates, join order/build side, runtime, and peak memory.

Default-on requires correctness parity and a repeatable suite-level benefit.

## Phase 7: DataFusion 56 readiness

No DataFusion 56 release exists as of this plan. When published, upgrade as one
coherent stack:

- DataFusion, datafusion-expr and datafusion-proto;
- matching Arrow/Parquet, object_store and sqlparser;
- the vendored Iceberg DataFusion integration;
- datafusion-functions-json, either upstream or a renewed minimal vendor port.

Required gates are a single DataFusion/Arrow major, coordinator-worker proto
round trips, full plan review, spill/memory tests, the three benchmark suites,
and a fresh audit of every workaround.

## Phase 8: dependency and MSRV cleanup

- Update the development/CI toolchain independently of dependency semantics.
- Measure the actual minimum compiler and make workspace/vendor `rust-version`
  declarations truthful; dependencies currently make the declared 1.85 floor
  suspect while CI uses Rust 1.98.
- Add an MSRV CI job.
- Remove reqwest 0.12 only after all selected Iceberg/catalog code uses 0.13.
- Remove the JSON vendor patch once a compatible upstream release exists.
- Defer object_store 0.14 until DataFusion selects the same API generation.

## Phase 9: Apache-main shadow migration

Refresh a non-production Apache-main port monthly. A future viable rebase can
remove:

- the six recorded Apache cherry-pick overlays;
- `CatalogBuilder::with_storage_factory` and old FileIO compatibility shims;
- the REST header-precedence patch, because Apache's new OAuth form path does
  not merge catalog headers and enforces form content type;
- duplicated metadata-column code where upstream behavior is equivalent.

It cannot remove SQE's loader feature gates/`Sync` bound, current-schema
projection, SigV4, strict-metrics residual elimination, dynamic predicate,
decode gate, SBBF probing, or fetch-ahead unless those separately land
upstream. De-vendoring also remains blocked on the missing writer and
transaction APIs listed in the objective.

## Per-MR completion checklist

- Upstream SHA and authorship recorded.
- Focused unit/regression tests pass.
- Relevant catalog feature matrix passes.
- V2/V3 write/delete round trips pass when applicable.
- Timed concurrency and corrupt-input tests cannot hang or panic.
- `cargo tree` has no unintended DataFusion/Arrow duplication.
- Clippy and formatting are clean without unrelated rewrites.
- Performance/memory gates pass for hot-path changes.
- Vendor ledger and retirement condition are updated.
- Branch is rebased on the latest accepted predecessor and the worktree is
  clean before push.
