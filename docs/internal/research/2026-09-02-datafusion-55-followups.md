# DataFusion 55 follow-up audit

Date: 2026-09-02

## Outcome

SQE is already fully migrated to DataFusion 55 and Arrow/Parquet 59. The A/B
results in `docs/evidence/perf/df55-arrow59-ab.md` show no correctness
regressions and lower mean runtimes for TPC-H (-26.3%), SSB (-43.1%), and
TPC-DS (-45.3%). This audit therefore covers workaround retirement, Iceberg
fork retirement, and feature adoption rather than another dependency bump.

The RisingWave iceberg-rust fork cannot be retired. Apache v0.10.1 lacks SQE's
required rewrite/overwrite transaction actions, position-delete writer,
deletion-vector writer, and dynamic predicates. Apache `main` has an equality-
delete writer, but still lacks `PositionDeleteFileWriter`, the transaction
actions, deletion-vector writer, dynamic predicates, and SQE scan extensions.
(A module-level comment mentions `PositionDeleteFileWriter`, but there is no
implementation.) It also targets an older DataFusion release. Upstream
OverwriteAction PR #2185 and RowDelta PR #2203 remain open and conflicted.

## Existing workarounds

| SQE behavior | DataFusion 55 status | Decision |
|---|---|---|
| Named table-function argument rewrite (`key => value` to positional `key=value`) | The normal table-factor planner still rejects `FunctionArg::Named` as unsupported. | Keep. |
| Empty-input `ROLLUP` / `CUBE` / `GROUPING SETS` wrapper | DataFusion issue #21570 remains open. PR #24039 adjusts statistics for a grand-total row that exists; it does not emit the missing row. | Keep. |
| Disable `list_files_cache` for mutable external TVFs | DataFusion 55 adds cache limit/TTL controls, but the default remains a 1 MiB cache with no TTL. Stale `ObjectMeta` is still a correctness risk. | Keep disabled for external mutable stores. Consider a finite configurable TTL separately. |
| Deferred Puffin statistics consumer | DataFusion 55 provides `StatisticsRequest` and opt-in `StatisticsRegistry` / `StatisticsProvider`, not the previously anticipated `StatisticsSource` integration. Stock DataFusion does not provide SQE's request rule or Iceberg/Puffin consumer. | Update the TODO. Implement a provider and request rule; benchmark join plans before enabling the registry. |

## Iceberg fork delta

The vendored baseline is RisingWave `813e54419b43`; the audited branch tip is
`1657c184`. Existing focused backports already cover predicate field IDs,
truncated metric bounds, and dangling deletion vectors.

This MR also takes RW #205 (`1657c184`), mapping `Timestamptz` and
`TimestamptzNs` to Glue `timestamp` and `timestamp_ns`. Glue has no timezone-
aware timestamp type, and this matches Spark's Glue behavior.

Two newer fixes are deliberately deferred:

- RW #196 (`6bd8e98`) correctly propagates equality-delete load failures, but
  replaces a concurrency state machine across three highly modified scan
  files. It deserves a dedicated backport with cancellation/retry tests.
- RW #208 (`deb4a3c8`) caches OpenDAL operators per S3/GCS bucket. It is useful
  for credential and token reuse, but should be validated separately for
  multi-bucket isolation, credential refresh, and serialization.

## DataFusion 55 functionality

Already active without SQE code changes:

- file-stream work stealing (`datafusion.execution.enable_file_stream_work_stealing`, default true) for DataFusion-native file scans;
- bounded spill merge fan-in, broader hash-aggregate spilling, and async sort-merge-join spill primitives;
- Parquet intra-file early stopping using statistics and dynamic filters;
- the memory-limiting Parquet metadata cache.

These DataFusion-native scan improvements do not replace iceberg-rust's custom
scan scheduler or SQE's dynamic predicate, SBBF probing, decode gate, and
fetch-ahead patches.

Ranked follow-ups:

1. Implement and benchmark the Puffin `StatisticsRequest` provider path; only
   then trial `datafusion.optimizer.use_statistics_registry`.
2. Backport RW #196 in isolation because silently losing equality-delete load
   failures is a correctness concern.
3. Evaluate RW #208 under real S3/GCS credential refresh and multi-bucket use.
4. Prototype Parquet virtual row indices/file selections for Iceberg metadata
   columns; custom scan integration is required.
5. Revisit native `MERGE INTO` hooks only as a longer-term simplification.
   They do not replace Iceberg transactions, authorization, or SQE write
   routing today.

## Apache `main` follow-up (2026-09-02)

Apache `main` was refreshed at `a18a0b2`, 162 commits after v0.10.1. None of
SQE's runtime patches can be deleted while the vendor tree remains based on
the RisingWave branch. A future Apache-main rebase would absorb the six
recorded Apache cherry-picks, the `CatalogBuilder::with_storage_factory` shim,
and the REST header-precedence fix (the new REST auth path posts OAuth forms
without merging catalog headers and forces form content type). It would not
absorb SQE's loader feature gates/`Sync` bound, current-schema projection,
SigV4 support, strict-metrics residual elimination, dynamic predicates,
decode gate, SBBF probing, or fetch-ahead pipeline.

High-priority Apache-main fixes missing from the vendor snapshot are:

- #2781: preserve null data rows for non-null equality-delete values;
- #2797: reject a missing required field with no initial default;
- #2859: avoid a lost-wakeup hang in positional-delete loading;
- #2509: de-duplicate file paths inside one FastAppend batch;
- #3062: propagate SQL-catalog transaction commit failures.

These are focused backport candidates. They improve correctness but do not
remove any SQE integration layer. Performance candidates after those include
#3015 (buffer positional-delete runs) and #3080 (constant column construction).
