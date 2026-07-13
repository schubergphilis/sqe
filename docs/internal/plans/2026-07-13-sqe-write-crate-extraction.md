# Plan: extract `sqe-write` from `sqe-coordinator` (audit Q-01)

Date: 2026-07-13. Planning only; no code changed.

## Summary / Motivation

Audit finding Q-01 (`docs/internal/audit/2026-07-10-sqe-full-audit.md:105`) flags the coordinator god-crate: `write_handler.rs` is now 8,371 lines (grown from 8,262 at audit time; production code ends at line 6627, the `#[cfg(test)]` module spans 6628-8371, ~1,744 lines of tests). The write path (INSERT, CTAS, UPDATE, DELETE, MERGE, ingest) is self-contained enough to extract into a new `sqe-write` crate below `sqe-coordinator`. The extraction removes ~11.6k LOC from the coordinator (`write_handler.rs` 8,371 + `writer.rs` 1,633 + `merge_sql.rs` 1,087 + `write_memory.rs` 308 + `merge_target_provider.rs` 273), shrinks incremental rebuilds of the coordinator bins, and gives the write path its own unit-test surface and API boundary.

Two entanglements block a naive move. Both are small and both break with mechanical refactors that land before any file moves.

## Current coupling (verified, with file:line)

### Verified claims

- `write_handler.rs` has zero references to `query_handler`. Confirmed: the full `crate::` reference list in `write_handler.rs` names only `catalog_ops` (line 34), `write_memory` (35), `writer` (36-40, 780), `merge_target_provider` (3295), `merge_sql` (3407-4077, 24 references), and `session_context` (5136). One-way dependency holds.
- Consumers of `WriteHandler`:
  - `query_handler.rs`: import at line 36, field at 85, construction at 169-173/231, `with_table_cache` at 278, accessor `write_handler()` at 297-298, method calls at 1063 (`handle_create_table`), 1078 (`handle_ctas_streaming`), 1108 (`handle_insert_streaming`), 1133 and 1198 (`handle_delete_dispatch`; 1198 is the TRUNCATE rewrite), 1150 (`handle_update_dispatch`), 1323 (`handle_merge_dispatch`), plus the free function `crate::write_handler::affected_rows_batch(0)` at 1436.
  - `flight_sql.rs:1904-1906`: `query_handler.write_handler().handle_ingest_streaming(...)` (Flight DoPut ingest).
  - `lib.rs:215-253` (`__test_support`): re-exports `sql_type_to_arrow` (222), `arrow_schema_to_iceberg_with_defaults` (245), `requires_v3_features` (251) for `tests/it/v3_types_integration.rs`.
  - `tests/it/in_subquery_view_rewrite.rs:35`: `use sqe_coordinator::write_handler::lift_in_subqueries` (pub free fn at `write_handler.rs:4841`).
- Satellite modules are clean: `merge_sql.rs` and `merge_target_provider.rs` have zero `crate::` references; `write_memory.rs` has zero; `writer.rs` has exactly one (`writer.rs:32`, `crate::write_memory::WriteReservation`). One addition the initial analysis missed: `maintenance.rs:31-33` (which stays in the coordinator) imports `crate::writer::{new_upload_tracker, parse_parquet_compression, write_data_files, WriteCleanupGuard}` plus `crate::writer::UploadedPaths` at 1203. All five symbols are already `pub` (`writer.rs:45, 91, 245, 311, 723`), so this is a staying-crate consuming a moving module. Allowed direction; only the import path changes.
- No crate outside `sqe-coordinator` depends on it (grep of all `Cargo.toml`: nothing lists `sqe-coordinator` as a dependency). Blast radius is one crate plus its integration tests.

### Correction 1: external dep list is slightly wider than first stated

`write_handler.rs` also uses `sqe_metrics` (632, 654), `sqe_lineage` (`PlanOrHint` in signatures: 801, 1144, 1344, 2611, 2880, 3583), and `sqe_policy` (640, 668-712), in addition to sqe-core/sqe-catalog/sqe-sql. All three sit below the coordinator (`sqe-lineage` depends on sqe-core/sqe-sql/sqe-auth/sqe-metrics), so nothing changes the verdict, but the `sqe-write` public API takes `sqe_policy::PolicySummary`, `sqe_lineage::PlanOrHint`, `datafusion::prelude::SessionContext`, and `sqlparser::ast::Statement` too, not just sqe-core/sqe-catalog/iceberg types. That is acceptable: every one of those crates is at or below the planner layer.

### Entanglement 1: `write_handler` -> `session_context` (exactly one symbol)

- Symbol: `crate::session_context::discover_session_catalog` (defined `session_context.rs:859`), signature:
  ```rust
  pub(crate) async fn discover_session_catalog(
      warehouse: &str,
      config: &SqeConfig,
      session: &Session,
      table_cache: Option<&TableMetadataCache>,
  ) -> Option<Arc<SessionCatalog>>
  ```
- Sole write_handler use: `write_handler.rs:5136`, inside `WriteHandler::resolve_session_catalog` (5125-5160). The match arm fires only when the target warehouse differs from `config.catalog.warehouse` and `catalog_discovery == PolarisAuto`; a `None` result becomes the "Unknown catalog '{warehouse}'" error.
- Why it cannot move: it calls `discovery_template` (`session_context.rs:791`) and `build_session_catalog` (`session_context.rs:107`), which resolve per-catalog bearers via `sqe_auth::per_catalog::resolve_bearer` and are deliberately shared with the read path (`build_catalog_provider`). The comment at `session_context.rs:104-106` pins read and write on one code path. Moving it into sqe-write would drag read-path catalog construction into a write crate and add a sqe-auth edge; moving it into sqe-catalog would create a production `sqe-catalog -> sqe-auth` edge that does not exist today (sqe-auth is dev-dep only, `sqe-catalog/Cargo.toml:172`).

### Entanglement 2: `catalog_ops` <-> `write_handler` mutual dependency

Confirmed, and fully enumerable. Every symbol in both directions is a pure, stateless converter; the `CatalogOps`/`WriteHandler` structs themselves never reference each other.

What `write_handler` imports from `catalog_ops` (`write_handler.rs:34`):

| Symbol | Defined at | Nature |
|---|---|---|
| `fold_unquoted_ident` | `catalog_ops.rs:1390` | pure, sqlparser `Ident` -> String |
| `parse_table_ref` | `catalog_ops.rs:1462` | pure, `ObjectName` -> iceberg `TableIdent` |
| `catalog_qualifier` | `catalog_ops.rs:1537` | pure, `ObjectName` -> `Option<String>` |

What `catalog_ops` imports from `write_handler` (`catalog_ops.rs:17-20`; used at 542, 582, 663, 1190, 1215):

| Symbol | Defined at | Nature |
|---|---|---|
| `sql_type_to_arrow` | `write_handler.rs:5673` | pure, sqlparser type -> arrow type |
| `default_to_iceberg_literal` | `write_handler.rs:6124` | pure, DEFAULT literal -> iceberg `Literal` |
| `parse_partition_transform_sql` | `write_handler.rs:6431` | pure, transform SQL -> `(col, name, Transform)` |

What `catalog_ops` needs from staying coordinator modules: `crate::tag_source_impl` (`parse_column_tags`, `apply_tag_ops`, `PROP_KEY` at `catalog_ops.rs:1072-1080, 1125`) and `crate::session_context::discover_session_catalog` (`catalog_ops.rs:1345`, inside `session_catalog_for` at 1334). Both stay in the coordinator alongside `CatalogOps`, so neither blocks anything.

Bonus finding: `resolve_table_ident` is duplicated verbatim, private in `write_handler.rs:353` and `pub(crate)` in `catalog_ops.rs:1498` (used by `query_handler.rs:4738, 5068`). The extraction should unify them.

## Target design

### Crate layout

```
crates/sqe-write/
  Cargo.toml
  src/
    lib.rs                  # module decls + trait CatalogDiscovery + re-exports
    ident.rs                # NEW: parse_table_ref, resolve_table_ident,
                            # catalog_qualifier, schema_catalog_qualifier,
                            # fold_unquoted_ident (from catalog_ops.rs)
    write_handler.rs        # moved verbatim (split into submodules later, out of scope)
    writer.rs               # moved verbatim
    write_memory.rs         # moved verbatim
    merge_sql.rs            # moved verbatim
    merge_target_provider.rs # moved verbatim
```

Layering: `sqe-write` sits between the planner tier and the coordinator. It depends on sqe-core, sqe-sql, sqe-catalog (default features `rest`+`rest-sigv4`), sqe-policy, sqe-metrics, sqe-lineage. `sqe-coordinator` gains a dependency on `sqe-write`. Nothing else changes: no crate depends on the coordinator, and `sqe-cli`'s slim `sqe-catalog` build (`default-features = false`, `sqe-cli/Cargo.toml:30`) is untouched because sqe-cli never pulls sqe-write.

### Cargo.toml (derived from actual imports of the five files)

```toml
[dependencies]
sqe-core = { path = "../sqe-core" }
sqe-sql = { path = "../sqe-sql" }
sqe-catalog = { path = "../sqe-catalog" }
sqe-policy = { path = "../sqe-policy" }
sqe-metrics = { path = "../sqe-metrics" }
sqe-lineage = { path = "../sqe-lineage" }
arrow = { workspace = true }
arrow-array = { workspace = true }
arrow-schema = { workspace = true }
async-trait = { workspace = true }        # new trait
datafusion = { workspace = true }
futures = { workspace = true }
iceberg = { workspace = true }
parquet = { workspace = true }
sqlparser = { workspace = true }
tokio = { workspace = true }              # retry sleeps, spawn (write_handler.rs:541, 900)
tracing = { workspace = true }
uuid = { workspace = true }

[dev-dependencies]
chrono = { workspace = true }             # test sessions (write_handler.rs:6660)
serde_json = { workspace = true }         # one test (write_handler.rs:6823)
```

Workspace: add `"crates/sqe-write"` to `members` (root `Cargo.toml:3-22`, between `sqe-trino-functions` and `sqe-coordinator`).

### Public API surface

`WriteHandler` construction: `new(SqeConfig)`, `with_metrics(Arc<MetricsRegistry>)`, `with_table_cache(TableMetadataCache)`, `with_policy_enforcer(Arc<dyn PolicyEnforcer>)` (`write_handler.rs:644-676`), plus new `with_catalog_discovery(Arc<dyn CatalogDiscovery>)`.

Methods the coordinator calls (signatures unchanged; types all at-or-below planner tier):

- `handle_create_table(&Session, &Statement) -> Result<Vec<RecordBatch>>` (1490)
- `handle_ctas_streaming(&Session, &Statement, &DFSessionContext, &str, &mut Option<PlanOrHint>, &mut Option<PolicySummary>) -> Result<Vec<RecordBatch>>` (1138)
- `handle_insert_streaming(...)` same shape (1338)
- `handle_delete_dispatch(&Session, &Statement, Arc<SessionCatalog>, &DFSessionContext, &mut Option<PlanOrHint>, &mut Option<PolicySummary>)` (2605)
- `handle_update_dispatch(...)` same shape (2874)
- `handle_merge_dispatch(&Session, &Statement, Vec<RecordBatch>, Arc<SessionCatalog>, &DFSessionContext, Option<LogicalPlan>, &mut Option<PlanOrHint>)` (3575)
- `handle_ingest_streaming<S, E>(&Session, &str, S) -> Result<usize>` (1918)

Free functions promoted from `pub(crate)` to `pub` because staying code consumes them: `affected_rows_batch` (133; query_handler.rs:1436), `sql_type_to_arrow` (5673), `arrow_schema_to_iceberg_with_defaults` (6041), `requires_v3_features` (6184) (lib.rs `__test_support`), `default_to_iceberg_literal` (6124), `parse_partition_transform_sql` (6431) (catalog_ops). The `ident` module functions are pub in the new crate. Everything else (`ensure_namespace`, `build_partition_spec`, `decorrelate_scalar_subqueries`, ...) stays crate-private inside sqe-write; verified no external users. `lift_in_subqueries` (4841) is already `pub`.

### Interface break 1: `CatalogDiscovery` trait (write_handler -> session_context)

Decision: narrow trait defined in sqe-write, implemented by the coordinator. Not a function move, for the layering reasons above. One call site, one method, and it makes `resolve_session_catalog` unit-testable for the first time (inject a fake resolver; today the Polaris-discovery arm has zero unit coverage).

```rust
// sqe-write/src/lib.rs
#[async_trait::async_trait]
pub trait CatalogDiscovery: Send + Sync {
    /// Resolve a non-default warehouse to a SessionCatalog using the caller's
    /// bearer. Returns None when discovery is off or Polaris rejects the
    /// warehouse (unknown or unauthorized). Mirrors
    /// session_context::discover_session_catalog semantics.
    async fn discover_session_catalog(
        &self,
        warehouse: &str,
        session: &Session,
    ) -> Option<Arc<SessionCatalog>>;
}
```

Coordinator side: a small `PolarisCatalogDiscovery { config: SqeConfig, table_cache: Option<TableMetadataCache> }` in `session_context.rs` whose impl forwards to `discover_session_catalog(warehouse, &self.config, session, self.table_cache.as_ref())`. `query_handler.rs:169-173` injects it at `WriteHandler` construction (and re-injects after `with_table_cache` at 278, mirroring the existing cache threading). In `resolve_session_catalog` the match-arm guard (`warehouse != default && PolarisAuto`) is unchanged; the body becomes `self.catalog_discovery.as_ref()? ... .discover_session_catalog(warehouse, session).await`, with `None` (resolver absent or discovery failed) producing the identical "Unknown catalog" error. Production behavior is bit-identical because the coordinator always injects; unit tests never reach the arm (requires `PolarisAuto` config).

Note: `catalog_ops.rs:1334-1360` (`session_catalog_for`) has the same shape but stays in the coordinator and keeps calling `session_context` directly. No change there.

### Interface break 2: `ident` module (catalog_ops <-> write_handler cycle)

The cycle consists entirely of the six pure functions tabled above. The break:

- Move `parse_table_ref`, `catalog_qualifier`, `schema_catalog_qualifier`, `fold_unquoted_ident`, and a unified `resolve_table_ident` out of `catalog_ops.rs` into a new `ident.rs`. Delete the `write_handler.rs:353` duplicate. `schema_catalog_qualifier` (`catalog_ops.rs:1555`) moves for family cohesion even though only catalog_ops uses it.
- The three schema converters (`sql_type_to_arrow`, `default_to_iceberg_literal`, `parse_partition_transform_sql`) stay in `write_handler.rs` and move with it. Post-extraction, `catalog_ops` imports them from `sqe_write`, which is the allowed direction (coordinator depends on sqe-write).

After both breaks the moving set `{write_handler, writer, write_memory, merge_sql, merge_target_provider, ident}` has zero edges into the staying set. All remaining edges point staying -> moving: catalog_ops -> ident + schema converters, query_handler -> WriteHandler + ident, maintenance -> writer, flight_sql -> WriteHandler (via query_handler), lib `__test_support` -> schema converters.

## Incremental PR sequence

Four MRs, each independently mergeable and independently revertable. Branch prefix `refactor/`.

### MR 1: `refactor/write-ident-module` (decoupling, no file moves)

- Scope: create `crates/sqe-coordinator/src/ident.rs`; move the five ident helpers from `catalog_ops.rs` (1390, 1462, 1498, 1537, 1555) plus their unit tests (e.g. `catalog_ops.rs:1725`); delete the `write_handler.rs:353` duplicate; update imports in `catalog_ops.rs`, `write_handler.rs:34`, `query_handler.rs:4738, 5068`; register `pub mod ident;` in `lib.rs`.
- Files: `catalog_ops.rs`, `write_handler.rs`, `query_handler.rs`, `lib.rs`, new `ident.rs`. Diff ~300 lines, almost all moves.
- Risk: low. Pure-function relocation inside one crate.
- Verification: `cargo build --all`, `cargo test --all`, `cargo test -p sqe-coordinator --features test-sqlite`, `cargo clippy --all-targets --all-features -- -D warnings`. Grep gate: `grep -n 'crate::catalog_ops' src/write_handler.rs` returns nothing.

### MR 2: `refactor/write-catalog-discovery-trait` (decoupling, no file moves)

- Scope: define `CatalogDiscovery` (temporarily in `write_handler.rs`; it moves with the file), add the `catalog_discovery` field + builder to `WriteHandler`, implement `PolarisCatalogDiscovery` in `session_context.rs`, inject in `query_handler.rs:169-173, 278`, rewrite `write_handler.rs:5136` to call the trait. Add a unit test with a fake resolver asserting (a) `Some` catalog is used, (b) `None` yields the exact "Unknown catalog '{warehouse}'" error string.
- Files: `write_handler.rs`, `session_context.rs`, `query_handler.rs`. Diff ~150 lines.
- Risk: medium-low. The only behavior-adjacent MR of the sequence; the multi-catalog discovery arm is not covered by local unit tests today (the new fake-resolver test closes part of that gap).
- Verification: full local gate as MR 1, plus manually trigger the `integration-test` CI job (`.gitlab-ci.yml:175`, manual + allow_failure on MRs). Grep gate: `grep -n 'crate::session_context' src/write_handler.rs` returns nothing.

### MR 3: `refactor/extract-sqe-write-crate` (the move)

- Scope: create `crates/sqe-write` (Cargo.toml above); add to workspace members; `git mv` the six modules (`write_handler.rs`, `writer.rs`, `write_memory.rs`, `merge_sql.rs`, `merge_target_provider.rs`, `ident.rs`) in a dedicated move-only commit (no reformatting, so `git log --follow` and blame survive); a second commit does the mechanical fixups: intra-crate `crate::` paths, `pub(crate)` -> `pub` bumps listed above, sqe-write `lib.rs`, coordinator `Cargo.toml` dep. In coordinator `lib.rs`, replace the module declarations (lines 14-15, 42-44 plus the new `ident`) with `pub use sqe_write::{ident, merge_sql, merge_target_provider, write_handler, write_memory, writer};`. Because the re-export preserves the `crate::write_handler::...` paths, `query_handler.rs`, `flight_sql.rs`, `maintenance.rs`, `catalog_ops.rs`, `__test_support`, and `tests/it/in_subquery_view_rewrite.rs` compile unchanged.
- Risk: medium. Large textual diff but zero intended semantic change; the move-only commit makes review tractable (reviewers diff the fixup commit only).
- Verification: full local gate; the ~1.7k-line test module now runs as `cargo test -p sqe-write`; assert test count parity with the pre-move `cargo test -p sqe-coordinator write_handler` count. Manually trigger `integration-test` and `distributed-smoke` (`.gitlab-ci.yml:218`) on the MR. Keep the MR window short (see risk R1).

### MR 4: `refactor/sqe-write-cleanup` (tighten, document)

- Scope: re-point coordinator imports from `crate::write_handler::` to `sqe_write::` and drop re-exports that are no longer load-bearing (keep `pub use` for the integration-test path or update `tests/it/in_subquery_view_rewrite.rs:35` to `sqe_write::write_handler::lift_in_subqueries` and slim `__test_support`); update `CLAUDE.md` crate table, `README.md` roadmap, `nextsteps.md`, and mark Q-01 in the audit doc; note the follow-up (splitting `write_handler.rs` into `insert`/`ctas`/`dml`/`merge`/`schema` submodules inside sqe-write) as a separate future task, explicitly out of Q-01 scope, as is Q-02 (13 `panic!` sites, all in test code, which must not be touched inside move commits).
- Risk: low. Verification: full local gate.

## Verification and CI strategy

Per-MR local gate (matches CI `cargo-check`/`cargo-test`, `.gitlab-ci.yml:66, 107, 111`):

```bash
cargo build --all
cargo test --all          # known noise: drop_secret_in_use_by_attached_catalog_errors
                          # requires --features test-sqlite; pre-existing, not ours
cargo test -p sqe-coordinator --features test-sqlite
cargo clippy --all-targets --all-features -- -D warnings
```

CI beyond the default gate: `integration-test` (Polaris + storage compose stack via `scripts/integration-test.sh`) and `distributed-smoke` are manual + allow_failure on MRs and automatic on main. Trigger both manually on MR 2 and MR 3 before merge; watch the scheduled main runs after each merge.

What CANNOT be verified pre-merge, and residual risk:

1. Multi-catalog Polaris discovery on the write path (the exact arm MR 2 touches) needs a second live warehouse; the compose stack runs one. Residual: a regression in the trait wiring would surface as "Unknown catalog" on 3-part writes to non-default catalogs. Mitigation: fake-resolver unit test pins both branches and the error string; the code path is config-gated (`PolarisAuto`) so default deployments are unaffected.
2. Write-path memory-safety behavior under pool pressure is stack-gated (`docs/internal/plans/2026-07-02-write-path-memory-safety-stack-validation.md`); `TrackedBatchBuffer` semantics move verbatim, but no pre-merge run proves it.
3. Object-store-specific paths (cleanup-on-abort against S3, large MoR merges) are exercised only by manual benchmarks and the parity rig. Nothing in this plan touches their logic; risk is limited to the trait rewiring in MR 2.

## Risk register and rollback

| # | Risk | Likelihood | Mitigation |
|---|---|---|---|
| R1 | Hot-file collision: `write_handler.rs` is touched by in-flight branches (write-path memory safety, audit span work). A rebase across MR 3 is painful. | High | Land MRs 1-2 first (small, rebase-friendly). Time MR 3 for a quiet window, keep it open under 48h, announce a freeze on write_handler MRs. |
| R2 | Cycle discovered mid-extraction (an overlooked moving -> staying edge appears after rebase). | Low (edges enumerated above) | Grep gates in MRs 1-2 (`crate::catalog_ops`, `crate::session_context` absent from write_handler). If a new edge lands via rebase, abort MR 3 and cut another decoupling MR first; MRs 1-2 stand alone as coordinator hygiene. |
| R3 | Feature unification surprises. | Low | sqe-write requests sqe-catalog defaults (`rest`, `rest-sigv4`), identical to the coordinator's existing request; `test-sqlite` stays on the coordinator (it still depends on sqe-catalog directly); sqe-cli's slim graph never includes sqe-write. Check with `cargo tree -p sqe-cli -e features` in MR 3. |
| R4 | `#[cfg(test)]` helpers referencing coordinator internals. | Retired | Verified: the test module (6628-8371) imports only `super::*`, arrow-schema, sqlparser, sqe-core, chrono, serde_json. No coordinator-only symbols. |
| R5 | Tracing target drift: module path moves from `sqe_coordinator::write_handler` to `sqe_write::write_handler`. The O3 spans carry explicit names (`name = "sqe.write_commit"`, write_handler.rs:497) and survive; bare `#[instrument]` spans and `log target=` filters do not. | Medium | In MR 3, grep dashboards/alert rules and any `RUST_LOG` examples in docs/Helm values for `sqe_coordinator::write` and update. |
| R6 | API over-exposure from `pub(crate)` -> `pub` bumps. | Low | Bump only the enumerated externally-consumed symbols; mark implementation details `#[doc(hidden)]`. |

Rollback per MR: MRs 1, 2, 4 are plain `git revert` (single-crate, no structural change). MR 3 reverts cleanly too since `git mv` is tracked and no wire format, config schema, or persisted state changes; a revert restores the coordinator-internal layout with the MR 1/2 decouplings intact. There is no partial-rollback hazard: at every merge point the workspace builds and the write path behaves identically.

## Key verified facts for the reviewer, in one place

- The only `write_handler -> coordinator` edges are `write_handler.rs:34` (three pure ident helpers from `catalog_ops.rs:1390/1462/1537`) and `write_handler.rs:5136` (`discover_session_catalog`, `session_context.rs:859`).
- The reverse edge is `catalog_ops.rs:17-20` (three pure converters at `write_handler.rs:5673/6124/6431`).
- `maintenance.rs:31` is an additional staying-consumer of `writer.rs`.
- `resolve_table_ident` is duplicated at `write_handler.rs:353` and `catalog_ops.rs:1498`.
