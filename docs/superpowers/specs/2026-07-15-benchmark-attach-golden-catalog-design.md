# Benchmark: attach preloaded golden Iceberg tables to speed up testing

Date: 2026-07-15
Status: design approved, pending spec review
Branch: feat/bench-attach-golden-catalog

## Problem

Benchmark runs are slow because every run rebuilds Iceberg tables from
scratch. `benchmark-publish-data.sh` already solves the *generate* cost
(parquet is generated once and published to an S3 bucket as immutable
source). But `benchmark-load.sh` still *loads* that parquet into fresh
Iceberg tables on every run: a CTAS/insert pass that rewrites all data,
builds manifests, and commits snapshots. For the large read suites
(TPC-DS `store_sales`, TPC-H `lineitem`, SSB `lineorder`) that load step
dominates wall-clock and adds nothing to the query measurement.

Goal: skip the per-run load by publishing the Iceberg tables once and
attaching them read-only. Handle the write suites with a cheap clone.

## Founding facts (verified in-repo, 2026-07-15)

- **ATTACH exists.** `crates/sqe-sql/src/attach.rs` parses
  `ATTACH '<location>' AS <name> (TYPE <kind>, <opt>=<val>, ...)`.
  `crates/sqe-catalog/src/mount.rs` builds the catalog. Working kinds:
  `sqlite`, `iceberg_rest`, `glue`, `s3tables`, `hms`. **`hadoop` and
  `jdbc` are `error_not_yet` stubs** — not available.
- **`sqlite` has an S3 gap.** `build_sqlite` (mount.rs:93) derives a
  `file://` warehouse and never threads an S3 endpoint/credentials into
  the catalog FileIO. An `s3://` warehouse on a custom endpoint
  (StorageGrid) is unproven and likely broken. `sqlite` is also feature
  gated behind `sql-sqlite`.
- **`iceberg_rest` is the proven S3 path.** `build_iceberg_rest`
  (mount.rs:190) is the same REST + S3 FileIO path the main stack
  already uses against Polaris + object storage.
- **`register_table` primitive exists.** `rest_catalog.rs:1838`
  `register_table(ident, metadata_location)` calls Polaris `registerTable`.
  `maintenance.rs:161` exposes `CALL <cat>.system.register_table(...)`
  (mirrors Spark's `system.register_table`). This is the load-bearing
  call for a shallow clone.
- **No clone surface today.** There is no `CREATE TABLE LIKE` /
  `SHALLOW CLONE` / clone parser or handler anywhere. CTAS into the
  embedded catalog is explicitly out of scope
  (`writable_iceberg_catalog.rs:22`). The full coordinator write path
  works (that is how tpcc/tpce load today).
- **Suite read/write split (from the query files).**
  - Read-only (zero write DML): `tpch`, `ssb`, `tpcds`, `tpcbb`,
    `clickbench`, `bank` (its 8 benchmark queries are pure SELECT; the
    Iceberg ingest sink is separate and not part of the query benchmark).
  - Write: `tpcc` (9 files), `tpce` (8 files).
- **`sqe-bench` has no attach/skip-load mode** today.

## Constraint: golden tables are location-pinned

Iceberg metadata and manifests embed **absolute** `s3://` URIs. The
golden tables' metadata therefore hardcodes the StorageGrid bucket and
prefix. The golden catalog is location-pinned, not a ship-anywhere
bucket. A shallow clone must deliberately keep the copied metadata
pointing at the original StorageGrid data-file paths (they are immutable
and shared); only the table `location` and future write paths move local.
The design does not attempt relocatability.

## Architecture: three reuse tiers

Rebuild cost increases down the list; each tier reuses the one above.

1. **Tier 1 - parquet source.** Already exists. `benchmark-publish-data.sh`
   generates parquet once to `s3://<bucket>/<bench>/sf<scale>/<table>/*.parquet`
   on StorageGrid, immutable, skip-if-present.

2. **Tier 2 - golden Iceberg tables (read-only, attached).** Published
   once into a **persistent Polaris on StorageGrid**. Benchmarks attach
   the golden catalog read-only instead of loading:
   `ATTACH '<polaris-url>' AS golden (TYPE iceberg_rest, WAREHOUSE '...', SECRET <bearer>)`.
   The 6 read-only suites run their queries against `golden.<ns>.<table>`
   with **zero load**.

3. **Tier 3 - writable working copy (shallow clone).** Only for write
   suites, only when a test mutates. A clone step copies the golden
   table's single `metadata.json` into the local writable catalog's
   warehouse (rewriting table `location` + `write.data.path` /
   `write.metadata.path` to local), then calls the existing
   `register_table(local_ident, copied_metadata_location)`. Data files
   and manifests stay shared on StorageGrid (immutable). Writes append
   new snapshots to the local catalog; reads merge shared golden data
   with local deltas.

## Chosen options

- **Golden publish target: StorageGrid** (shared, survives between runs,
  team-shareable, matches the existing `aws --profile storagegrid`
  setup). Not co-located local RustFS.
- **Clone surface: bench-internal step first** (copy one JSON + call
  `system.register_table`), not a user-facing SQL `SHALLOW CLONE` yet.
  Promote to a SQL surface later if it proves generally useful.
- **Golden catalog backend: `iceberg_rest` (persistent Polaris)**, not
  `sqlite`, because of the verified S3 credential/endpoint gap in
  `build_sqlite` and its feature gating.

## Phased delivery

The read path is independent of and far higher-value than the write
path, so it ships first.

### Phase 0 - feasibility spike (de-risk, before any build)

Local, cheap, against the already-running Polaris + RustFS stack:

1. Load `tpch` SF0.01 into the running local Polaris (fast).
2. Start SQE. From a session, `ATTACH` that same Polaris as a *second*
   catalog `golden` via `TYPE iceberg_rest`.
3. `SELECT count(*) FROM golden.tpch.lineitem`.

Success = both FileIO surfaces reach the custom endpoint: (a) the
catalog reads `metadata.json` + manifests, and (b) DataFusion's own
`register_s3_store_if_needed` reads the data files. RustFS's endpoint
(`http://localhost:19000`) is the same custom-endpoint case as
StorageGrid, so a local pass predicts a StorageGrid pass.

Exit gate: if the spike fails, stop and revisit the backend choice
(fall back to fixing `build_sqlite` S3 threading, or a co-located
golden Polaris) before building Phase 1.

### Phase 1 - attach read-only golden catalog (the big win)

- **`scripts/benchmark-publish-iceberg.sh`** (new): one-time load of the
  6 read-only suites' parquet -> golden Polaris on StorageGrid. Skip
  if the namespace/tables already exist (mirror the parquet publisher's
  skip-if-present and `BENCH_FORCE=1` override). Reuses the existing
  load path against a StorageGrid-backed Polaris rather than the local
  stack.
- **`sqe-bench` attach mode** (new): a `--source attach` (or
  `BENCH_DATA_SOURCE=attach://<polaris-url>`) that, instead of loading,
  issues the `ATTACH` and points generated query SQL at
  `golden.<ns>.<table>`. Query rewriting is limited to the catalog/
  namespace qualifier.
- **`benchmark-load.sh`** wiring: accept the attach source and pass it
  through; when set, skip the entire generate+load block for the 6 read
  suites.

Result: tpch/ssb/tpcds/tpcbb/clickbench/bank run with no load step.

### Phase 2 - write suites via shallow clone

- Build the bench-internal clone step: given a golden table ident and a
  target local ident, copy `metadata.json` to the local warehouse with
  rewritten location/write paths, then `system.register_table`.
- tpcc/tpce: clone the golden tables into the local writable catalog,
  then run their write DML against the clones. Data files stay shared;
  only deltas are written locally.
- Gate: only after Phase 0 + Phase 1 prove out.

## Components and interfaces

- `benchmark-publish-iceberg.sh` - inputs: `BENCH_DATA_BUCKET`,
  `BENCH_S3_ENDPOINT`, `BENCH_S3_PROFILE`, `BENCH_GOLDEN_POLARIS_URL`,
  `BENCH_SCALE`, benchmark list. Effect: golden Iceberg tables exist in
  the golden Polaris. Idempotent.
- `sqe-bench` attach mode - input: golden Polaris URL + credentials.
  Effect: emits `ATTACH`, rewrites query table refs to the golden
  catalog. No load.
- clone step (Phase 2) - input: `(golden_ident, local_ident)`. Effect: a
  writable local table sharing golden data files. Depends on
  `register_table` + object-store copy of one JSON.

## Error handling

- Attach failure (bad creds, unreachable Polaris, FileIO endpoint
  mismatch): fail the run loudly with the ATTACH error; do not silently
  fall back to a full load (that would mask the very cost this design
  removes).
- Missing golden table: instruct the operator to run
  `benchmark-publish-iceberg.sh` first; do not auto-load.
- Clone conflict (target exists): drop-and-reclone or fail per a
  `BENCH_FORCE`-style flag, matching the publisher's semantics.

## Testing

- Phase 0 spike is itself the first correctness check.
- Phase 1: a smoke run at SF0.01 attaching a locally-published golden
  Polaris, asserting all 6 read suites return the same row counts as a
  freshly-loaded run (reuse `benchmarks/expected`).
- Phase 2: clone a golden table, run a write suite, assert the golden
  table is unchanged (its snapshot id is stable) and the clone reflects
  the writes.

## Out of scope

- User-facing SQL `CREATE TABLE ... SHALLOW CLONE` (Phase 2 is
  bench-internal; SQL surface is a later promotion).
- `sqlite`-catalog-on-S3 (blocked by the `build_sqlite` credential gap;
  revisit only if Phase 0 rules out `iceberg_rest`).
- Relocatable / ship-anywhere golden buckets (metadata is location-pinned).
- Distributed-mode attach specifics (Phase 0/1 target single-node; the
  attach primitive is coordinator-level and already exists).
