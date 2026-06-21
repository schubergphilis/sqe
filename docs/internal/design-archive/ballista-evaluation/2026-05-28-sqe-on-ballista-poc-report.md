# SQE-on-ballista PoC spike report

Date: 2026-05-28
Plan: `docs/superpowers/specs/2026-05-28-sqe-on-ballista-poc-plan.md`
Branch: `feat/sqe-ballista-poc-spike`
Verdict: **GREEN (with vendor patch + two codecs)**

## TL;DR

`SELECT COUNT(*) FROM iceberg.tpch_sf0_1.lineitem` ran end-to-end through
a real ballista 53.0.0 scheduler + executor and returned **600,000** —
matching plain SQE.  Getting there surfaced three concrete gaps, all
solvable on the SQE side without forking ballista.  None is an
architecture-shaped blocker.  Direct cutover to ballista is viable; the
work is "fill these gaps", not "rethink the approach".

```
+--------+
| n      |
+--------+
| 600000 |
+--------+
verification #3 PASS: COUNT(*) = 600000
PoC end-to-end success
```

## Verification points

| # | Check | Result |
|---|-------|--------|
| 1 | Iceberg `TableProvider` reachable from ballista executor | **PASS** — registered via `ctx.register_catalog` on a `SessionContext::standalone_with_state`, decoded on the executor by the logical codec. |
| 2 | Per-query OIDC bearer reaches the executor | **PASS** — confirmed two ways.  First, the bearer was visible *embedded in the serialized scan node* in the pre-codec error dump (token travels in `IcebergTableScan.table.file_io.props`).  Second, our physical codec reloads the table from the catalog on the executor side, where the executor's own catalog handle carries the bearer.  Either path delivers per-query credentials. |
| 3 | `COUNT(*)` == 600,000 | **PASS** |

## What it took (the three gaps)

### Gap 1 — no `LogicalExtensionCodec` for iceberg tables

Ballista serializes the logical plan to ship it to executors (even in
standalone, where the executor builds its own server-side
`SessionContext`).  `iceberg-datafusion` ships no `LogicalExtensionCodec`,
so submission failed immediately:

```
Internal error: failed to serialize logical plan:
  Error serializing custom table ...
  NotImplemented("LogicalExtensionCodec is not provided")
```

**Fix:** `IcebergLogicalCodec` (`crates/sqe-ballista-poc/src/codec.rs`).
Encodes the table *reference* (`schema.table`); on decode looks the table
up in an `IcebergCatalogProvider` the codec holds.  Delegates all
non-table calls to `BallistaLogicalExtensionCodec::default()`.

### Gap 2 — no `PhysicalExtensionCodec` for `IcebergTableScan`

Past the logical layer, the physical plan failed: ballista couldn't
serialize iceberg's custom `ExecutionPlan` node, which holds a live
`Table` (FileIO handle, S3 config, bearer):

```
Internal error: Unsupported plan and extension codec failed with
  [unsupported plan type: IcebergTableScan { table: Table { ... } }]
```

**Fix:** `IcebergPhysicalCodec`.  Encodes `(namespace, table,
snapshot_id, projection, limit, output_schema)`; on decode reloads the
table from the catalog and rebuilds the scan.  Uses a 1-byte
discriminator so non-iceberg nodes (e.g. `ShuffleWriterExec`) delegate
to `BallistaPhysicalExtensionCodec::default()` — **critical**: replacing
ballista's default codec without delegation breaks ballista's own
distributed plan nodes.

Required a small vendor patch: `IcebergTableScan::from_codec_parts(...)`
(the stock `new()` is `pub(crate)` and takes raw DataFusion `Expr`
filters / projection indices that aren't recoverable after planning).
See `vendor/iceberg-rust/crates/iceberg/.../physical_plan/scan.rs`.

### Gap 3 — codec decode deadlocked the executor

First run past the codecs hung 180s and the executor heartbeat timed
out.  Cause: codec `try_decode` is sync but runs inside a tokio worker;
`futures::executor::block_on` parked the worker without pumping the tokio
reactor, so the iceberg REST client's HTTP call never progressed.

**Fix:** `block_on_in_runtime` — `tokio::task::block_in_place` +
`Handle::current().block_on`.  Requires a multi-threaded runtime (the
standalone executor has one).

## What this means for the full migration

**Direct cutover stays viable.**  The gaps are all SQE-side glue, not
ballista limitations:

- Ballista's extension surface is sufficient:
  `with_ballista_logical_extension_codec`,
  `with_ballista_physical_extension_codec`,
  `ExecutorProcessConfig::{override_config_producer, override_runtime_producer}`,
  `SchedulerConfig::{override_config_producer, override_session_builder}`.
- Per-query auth works.  The bearer reaches the executor; the
  reload-from-catalog pattern is cleaner than embedding state and lets
  the executor mint its own fresh credentials.
- SQE's existing `SqePhysicalCodec` / `DistributedScanExec` are *not*
  needed on the ballista path — ballista does task distribution, the
  iceberg `TableProvider` returns a plain scan, and our codecs bridge
  the serialization gap.  The bespoke `distributed_scan.rs` /
  `shuffle.rs` / `worker_registry.rs` (~3.3K lines) become removable.

## Upstreaming opportunities (findings worth a PR)

1. **`iceberg-datafusion` should own a `LogicalExtensionCodec` +
   `PhysicalExtensionCodec`** for `IcebergTableProvider` /
   `IcebergTableScan`, parameterized over the catalog.  Every distributed
   DataFusion engine (ballista, datafusion-comet, our SQE) hits exactly
   this wall.  Our two codecs are a ready-made starting point.  This is
   the highest-value upstream PR.
2. **`IcebergTableScan::from_codec_parts` (or equivalent public
   constructor)** — needed by any out-of-crate codec.  Trivial; pairs
   with #1.
3. **(Minor) ballista docs** could call out that a custom
   `PhysicalExtensionCodec` MUST delegate to the default for non-custom
   nodes — the failure mode (breaking `ShuffleWriterExec`) is non-obvious.

## Open questions deferred to the full design

- **Multi-executor scaling** — PoC was single standalone executor.
- **Predicate / runtime-filter pushdown across stages** — the physical
  codec bails on `had_predicates`; the iceberg `Predicate` and SQE's
  `DynamicPredicate` runtime filters both need wire serialization.  This
  is the one place that might need real design work.
- **Policy enforcement** — planning stays in the SQE coordinator, so the
  policy-rewritten LogicalPlan is what gets submitted; should be
  transparent, but untested here.
- **Multi-process auth** — standalone shares a process; a real cluster
  needs `override_config_producer` on the executor to install the
  per-query bearer into the object store, and scheduler↔executor auth
  (ballista mTLS vs SQE's `x-sqe-worker-secret`).

## Artifacts

- `crates/sqe-ballista-poc/` — the PoC binary + both codecs.
- `vendor/iceberg-rust/.../physical_plan/scan.rs` —
  `from_codec_parts` constructor (SQE-only patch family candidate).
- Run log: `COUNT(*) = 600000`, exit 0.

## Recommendation

Proceed to the full SQE-on-ballista design (brainstorming → writing-plans).
Anchor the design on:
1. Upstreaming the iceberg codecs (or vendoring them in
   `iceberg-datafusion` until merged).
2. Replacing `distributed_scan.rs` / `shuffle.rs` / `worker_registry.rs`
   with ballista scheduler+executor processes.
3. A dedicated work-package for predicate / runtime-filter
   serialization — the one open architectural question.
