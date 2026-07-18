# Unified Benchmark Harness Design

**Status:** Draft for review
**Date:** 2026-07-17
**Supersedes operationally:** the sprawl of `scripts/benchmark-*.sh` (attach-golden, generate-all, load, publish-data, publish-iceberg, split) and folds the read-path work from `2026-07-15-benchmark-attach-golden-catalog-design.md`.

## Summary

Collapse the benchmark tooling into two clean layers: a bash script that owns infrastructure (docker stack, bootstrap, coordinator lifecycle) and a Rust `sqe-bench` binary that owns the benchmark logic via three verbs (`provision`, `run`, `reset`), driven by one config profile per environment. The harness serves three goals:

1. Run TPC + other suites (tpch, ssb, tpcds, clickbench, tpcc, tpce, tpcbb, bank) as performance tests against SQE.
2. Exercise the engine with the full complex-query set per suite.
3. Validate performance and output against Trino.

Data lives in a configurable S3-compatible store (AWS, Cloudflare R2, or custom such as StorageGRID). Read suites attach a pregenerated immutable golden catalog (zero load). Write suites mutate golden and reset to a recorded baseline, with a configurable reset mode.

## Motivation

Today the flow is spread across ~10 scripts with overlapping responsibility and a large `BENCH_*` environment surface. Loading data on every run is slow, especially over links limited to ~4 MB/s per stream. The attach-golden work (2026-07-15) proved the read path can skip load entirely, but write suites (which mutate tables) were left out of scope. This design unifies both paths and removes the script sprawl.

## Scope

In scope:
- All suites: read-only analytical (tpch, ssb, tpcds, clickbench) and write/transactional (tpcc, tpce, tpcbb, bank).
- Read suites: attach immutable golden, query `golden.<suite>_sf<scale>.*` directly.
- Write suites: mutate then reset, with `RESET_MODE=rollback` (default) or `copy`.
- Configurable data location (aws | r2 | storagegrid | local | custom) via profiles.
- Trino comparison retained via `--compare-trino`.
- Same output artifacts: `BENCH_SUMMARY` lines, per-query timing, JSON in `benchmarks/results/`, `compare-*.json`.

Out of scope (surfaced, not built here):
- The `target_partitions = 1` single-core compute wall (issue #131). Network fetch is already concurrent; compute parallelism is a separate engine lever.
- Concurrent write-suite runs under `RESET_MODE=rollback` (rollback mutates shared golden; concurrent write runs require `copy`).

## Architecture

### Verbs

```
sqe-bench provision [suites...]   # build golden once on the configured store (skip-if-exists)
sqe-bench run       [suites...]   # attach + query (read) or mutate+reset (write) + emit + optional compare
sqe-bench reset     [suites...]   # restore write tables to their recorded baseline
```

`run` is the everyday command. `provision` is run once per (store, scale). `reset` is called automatically at the end of a write `run` and is also available standalone for recovery.

### Component boundary

Two layers with one responsibility each. Infrastructure orchestration stays in bash (where it already works today); benchmark logic lives in the binary (where it is testable and connect-only).

**Script layer (`scripts/benchmark.sh`), infrastructure:**
- Build the binaries for the requested profile.
- Bring up and tear down the docker stack (postgres, Polaris, rustfs), gated by the profile flag `manage_stack`. Local and external-warehouse profiles manage the stack; remote-Polaris profiles skip it.
- Bootstrap (bucket, warehouse, grants).
- Spawn the SQE coordinator with the profile's config, health-wait, and tear it down on exit. Remote profiles may instead point at an existing coordinator.
- Dispatch to `sqe-bench <verb>` passing `--host`/`--port`, `--profile`, and the suite list.

**Binary layer (`sqe-bench`), benchmark logic (connect-only):**
- `provision`: build golden via the catalog sink (option 2) or multi-stream load (option 3); record baselines. Talks to Polaris + S3, not docker.
- `run`: execute the suite's queries against the running coordinator over Flight, time them, validate rows, emit `BENCH_SUMMARY` + per-query timing + JSON; handle `RESET_MODE` for write suites.
- `reset`: roll back write tables via Polaris REST (coordinator-independent).
- `compare`: Trino output and timing validation.

`sqe-bench` never invokes docker and never spawns the coordinator; it connects via `--host`/`--port`. The script is thicker than a pure shim, but the two layers stay cleanly separated: infra in bash, run/parse/compare/reset in Rust.

### Topology

- **Golden warehouse:** immutable Iceberg tables on the configured S3 store. One namespace per suite and scale: `<suite>_sf<scale>` (e.g. `tpch_sf10`), matching the attach-golden contract. This is the reusable "prepared global model" that any rig can attach.
- **Coordinator:** runs with `tests/benchmark-attach/coordinator-attach.toml` (admin role for the ATTACH gate). Attaches golden as catalog `golden`. Its primary/writable catalog is the profile's warehouse (local rustfs for fast writes, or the same remote store).
- **Read suites:** resolve `golden.<suite>_sf<scale>.<table>`, zero load.
- **Write suites:** see Reset below.

### Reset

At `provision` time, each write table's current snapshot id is recorded as the baseline. Storage: a table property `sqe.bench.baseline_snapshot_id` on each write table (Polaris `updateProperties`), plus a per-suite `golden-baseline.json` manifest in the warehouse for auditability.

`RESET_MODE=rollback` (default):
- Write suite mutates the golden write tables in place (new snapshots appended to `main`).
- After the run (and on `reset`), set the table's `main` branch reference back to the baseline snapshot id via a Polaris Iceberg REST `updateTable` commit (a `set-snapshot-ref` update guarded by an `assert-ref-snapshot-id` requirement). Metadata-only, near-instant, no data copied. This is the standard Iceberg rollback operation; verified against the running Polaris, which serves the standard REST catalog surface and exposes `refs.main` on golden tables (format-version 2).
- The reset talks directly to Polaris. It needs no SQE engine code and no running coordinator, so it can run between or after runs independently of the coordinator/docker lifecycle. `sqe-bench reset` is therefore a thin REST client: `loadTable` (read current snapshot + the recorded baseline property), then `updateTable`.
- Rollback is non-destructive: it only moves the `main` ref, so the write suite's mutation snapshots remain in the table (inspectable) until GC. Orphaned snapshots are garbage-collected with the existing `ALTER TABLE ... EXECUTE expire_snapshots(...)`, run on a cadence (for example every N runs or on demand), not every reset.
- Constraint: sequential runs only. Concurrent write runs would race on the shared table.
- Constraint: the baseline snapshot must still be present in the table's `snapshots` list at rollback time (i.e. not yet expired). Recording baseline at provision and expiring only on demand guarantees this.

`RESET_MODE=copy`:
- At run start, CTAS each golden write table into a per-run namespace `run_<id>` in the writable catalog.
- Write suite mutates the copy.
- Teardown drops the `run_<id>` namespace.
- Fully isolated and concurrent-safe. Cost: one data copy per run over the (possibly slow) link.

Rollback is implemented as catalog operations inside `sqe-bench reset`, using the existing catalog client (iceberg-rust / Polaris REST). No dependency on a new SQL DDL statement, though a future `ALTER TABLE ... EXECUTE rollback_to_snapshot(...)` could back it.

### Multi-stream S3

The concurrency mechanisms already exist and are exposed as profile settings:
- Write path (`sink/iceberg.rs`): permit-bounded concurrent multipart uploads, per-day parallelism.
- Read path (`iceberg_scan.rs`): `manifest_concurrency` (default 64), `direct_read_concurrency` (default 8), `scan_fetch_ahead` (default ~3x cores).

The profile carries these so slow-link environments (StorageGRID at ~4 MB/s per stream) can raise stream counts. `target_partitions` stays a documented engine lever, referenced but not changed here.

## CLI surface

```
sqe-bench provision <suites...>
    --profile <name>            # local | storagegrid | r2 | aws | <custom file>
    --scale <N>
    --from-parquet <s3-uri>     # optional: option 3 multi-stream load from pregenerated parquet
                                # (default: option 2, generate direct to iceberg, no local staging)

sqe-bench run <suites...>
    --profile <name>
    --scale <N>
    --reset-mode <rollback|copy>
    --compare-trino             # optional Trino output+timing validation
    --host <h> --port <p>       # optional: connect to an existing coordinator instead of spawning
    --smoke                     # optional: attach-vs-primary parity smoke (replaces ci/attach-parity-smoke.sh)

sqe-bench reset <suites...>
    --profile <name>
    --scale <N>
    --expire                    # also run expire_snapshots to GC orphaned write snapshots
```

## Config profile schema

One file per environment under `benchmarks/profiles/<name>.toml`:

```toml
name = "storagegrid"
manage_stack = true            # bring up docker (postgres/polaris/rustfs); false = connect only

[s3]
endpoint    = "https://s3.storage.acc.schubergphilis.com"
region      = "us-east-1"
path_style  = true
profile     = "storagegrid"    # aws credentials profile, OR access_key/secret_key below
warehouse_bucket = "sqe-benchmark"

[polaris]
url       = "http://localhost:18181/api/catalog"
warehouse = "test_warehouse"
# token minted via client_credentials at runtime

[reset]
mode = "rollback"              # rollback | copy

[concurrency]
manifest       = 64
direct_read    = 16            # raised for slow-link, many-stream reads
write_streams  = 16            # raised for slow-link, many-stream writes
```

Shipped presets: `local` (rustfs, manage_stack=true), `storagegrid`, `r2`, `aws`. `--profile <path>` accepts a custom file.

## Output contract (unchanged)

- Per-query line: `<status> <name> <secs> <rows>`.
- `BENCH_SUMMARY:<suite>:pass:fail:diff:skip:error:total:ms` per suite.
- JSON report per run in `benchmarks/results/<suite>-sf<scale>-<mode>-<ts>.json`.
- Trino comparison JSON in `benchmarks/results/compare-<suite>-sf<scale>-<ts>.json`.
- Logs preserved as today.

## Migration

Folded into `sqe-bench` and deleted:
- `benchmark-attach-golden.sh` -> `run` attach logic
- `benchmark-generate-all.sh`, `benchmark-publish-data.sh`, `benchmark-publish-iceberg.sh` -> `provision`
- `benchmark-load.sh`, `benchmark-split.sh` -> `provision --from-parquet`
- `ci/attach-parity-smoke.sh` -> `run --smoke`

Retained (referenced, evaluated separately): `benchmark-matrix.sh`, `benchmark-mor-vs-cow.sh`, the trino/parity/tempto scripts. `benchmark-test.sh` is kept until `run` reaches parity, then removed.

## Success criteria

1. `sqe-bench run tpch ssb tpcds clickbench --profile local` reproduces today's full-attach results (175 read queries pass) with identical output artifacts.
2. `sqe-bench run tpcc --profile local --reset-mode rollback` runs a write suite, then `reset` restores the table (row counts and snapshot id match baseline; verified by a follow-up read).
3. `--reset-mode copy` produces an isolated namespace, mutates it, and drops it, leaving golden untouched.
4. `provision` against a StorageGRID profile builds golden once and is skip-if-exists on re-run.
5. `--compare-trino` emits the same `compare-*.json` shape as today.
6. Script count in `scripts/` drops by at least the six folded scripts; `benchmark.sh` is a thin shim.

## Rollback strategy

The old scripts stay in git history and are removed in a dedicated commit after `run` reaches parity (criterion 1). Reverting that commit restores the previous flow. Golden data is unaffected by tooling changes.

## Risks and open questions

- **Rollback via catalog (RESOLVED):** verified against the live Polaris. Rollback is a standard Iceberg REST `updateTable` commit with a `set-snapshot-ref` update on `main`; Polaris serves the standard REST surface and golden tables expose `refs.main` (format-version 2). No SQE engine code required; the reset is a direct Polaris REST client. `set-snapshot-ref` is spec-level, so any recent Polaris (including 1.6) supports it. Only remaining check: a one-time live rollback round-trip (mutate a throwaway table, roll `main` back, confirm row counts) during implementation to prove Polaris permits the backward ref move end to end.
- **Docker/coordinator lifecycle in Rust:** spawning docker and the coordinator via `std::process` is testable but adds OS-dependent surface. Mitigate by keeping stack management behind `manage_stack` and covering it with an integration test, not unit tests.
- **Baseline drift:** if a write run is killed mid-mutation before reset, the next `run` must reset first. `run` for write suites resets to baseline before starting.
- **expire_snapshots cadence:** orphaned snapshots accumulate under rollback. Decide a default cadence (proposed: opt-in `reset --expire`, plus a warn when snapshot count exceeds a threshold).
