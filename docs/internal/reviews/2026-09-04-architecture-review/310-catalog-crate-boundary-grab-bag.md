# 310: sqe-catalog carries non-catalog concerns: file TVFs, platform grant client, system.runtime mirrors, HTTP object stores

- Scope: sqe-catalog crate boundary, Cargo.toml dependencies
- Severity: medium
- Category: architecture
- Effort: L (week+)
- Status: open

## Problem

CLAUDE.md describes `sqe-catalog` as "Iceberg REST catalog client, information_schema virtual providers". The crate (44K lines) also holds:

| Module | Lines | Concern |
|---|---|---|
| `read_csv.rs`, `read_json.rs`, `read_json_buffer.rs`, `read_parquet.rs`, `file_tvf_common.rs`, `read_delta.rs` (unwired) | ~4,850 | File table functions over object stores |
| `access_control.rs`, `grant_chameleon.rs` | ~910 | HTTP client for the platform grant API, implementing `sqe_policy::grants` traits (`grant_chameleon.rs:16`) |
| `system_runtime.rs` | 852 | `system.runtime` tables over mirrored coordinator types: "Mirrors `sqe_coordinator::query_tracker::QueryState`" (`system_runtime.rs:14-60`) |
| `system_jdbc.rs` | 924 | JDBC type catalog tables |
| `hf_tree_cache.rs`, `lazy_object_store.rs` | ~1,000 | HuggingFace tree API client, lazy HTTP object-store registry |
| `late_materialize.rs`, `scan_memory.rs`, `parquet_writer_config.rs`, `circuit_breaker.rs`, `topk.rs` | ~2,850 | Execution-time reader machinery, write config, generic breaker |

Consequences visible in the code:

- `crates/sqe-catalog/Cargo.toml:200-202`: the crate depends on `sqe-policy` and `sqe-sql`; `sqe_policy` is imported for `PolicyStore` (`info_schema.rs:15`, `catalog_provider.rs:11`) and the whole `grants` module (`grant_chameleon.rs:16`). The intended dependency direction in CLAUDE.md is core -> catalog -> policy -> coordinator; the catalog crate now sits beside policy, and the grant backend (a coordinator/policy concern) must be built before any catalog consumer compiles.
- `system_runtime.rs:14-60` duplicates `QueryState`, `FragmentInfo` and `QueryRecord` "to avoid a circular crate dependency"; the coordinator converts into them. Two copies of the same enum drift silently.
- `lib.rs:1-52` gates 13 modules on `feature = "rest"`; the slim build (`--no-default-features --features rest`) still compiles all TVF and HF code because those are unconditional.
- Any change to a TVF (`read_json.rs`) or the grant client rebuilds `sqe-catalog` and every crate above it; the build-time program measured the coordinator rebuild at 4m50s versus 35s for a coordinator-only change (`Cargo.toml:26-35` comment).

## Why it matters

The crate is the compile-time hub of the workspace and the place where policy, execution and catalog concerns meet without a stated owner. The review scope asked whether the catalog crate "knows about coordinator or policy concepts it should not"; it does, in two directions (policy traits in, coordinator types mirrored). Each added concern makes the feature matrix in `Cargo.toml` harder to reason about, which is how the default-feature drift in the same file's comment happened.

## Proposed change

1. Extract `sqe-tvf` (file TVFs, `file_tvf_common`, `hf_tree_cache`, `lazy_object_store`, `parquet_writer_config`) depending on `sqe-core` only.
2. Move `access_control.rs` and `grant_chameleon.rs` into `sqe-policy` (they implement its traits) or into the coordinator's `policy_wiring.rs`, and drop the `sqe-policy` dependency from the catalog crate except for `PolicyStore` in `info_schema.rs`. Consider passing a narrow `RestrictedColumns` closure instead so the dependency disappears entirely.
3. Move `system_runtime.rs` into the coordinator (it only renders coordinator state) and delete the mirrored types.
4. Keep `iceberg_scan.rs`, `late_materialize.rs`, `scan_memory.rs` in the catalog crate for now, but name the crate's contract in `lib.rs` docs: catalog access, providers, scan.

## Acceptance criteria

- `cargo tree -p sqe-catalog -e normal | grep sqe-` shows `sqe-core`, `sqe-sql`, `sqe-metrics` only (or `sqe-policy` with a one-line justification).
- `system_runtime.rs` no longer exists in `sqe-catalog`; `RuntimeQueryState` has one definition.
- Coordinator incremental rebuild after a one-line change in a TVF file does not rebuild `sqe-coordinator`'s dependents (measure as in the build-time plan).

## Related

- `docs/internal/plans/2026-09-04-build-test-time-program.md`.
- 2026-06-26 analysis, "Clean multi-crate separation" (the claim this issue qualifies).
- 803 (cross-cutting: catalog depends on policy and metrics) states the dependency edge; this issue lists what to move to remove it.
- 309.
