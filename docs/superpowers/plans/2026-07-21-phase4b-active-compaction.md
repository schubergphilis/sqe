# Phase 4b: Active Single-Coordinator Adaptive Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans / subagent-driven-development. Checkbox steps.

**Goal:** Make the maintenance scheduler actually compact opted-in tables when `mode = "active"`, reusing the shipped `rewrite_data_files` path under the ephemeral maintenance session, with explicit write-authority, snapshot stamping, and real job-row logging. Runs coordinator-local (single-node and cluster); worker fan-out is Phase 4c.

**Architecture:** Builds on Phase 4a (config, `MaintenancePrincipal`, `table_health`, `maintenance_log`, advisory scheduler). 4b adds the active execution arm: the scheduler, on a due opted-in table in `active` mode, mints a maintenance session, runs `rewrite_data_files` with `[maintenance.compaction]` params (+ per-table property overrides), stamps the resulting snapshot with job identity, and records a real `maintenance_log` job row. `distribution.mode = "auto"` resolves to coordinator-local in 4b (no worker path yet).

**Tech Stack:** Rust, DataFusion, vendored iceberg-rust, tokio, prometheus.

## Global Constraints

- Zero changes under `vendor/`. Every needed API exists (`RewriteFilesAction::set_snapshot_properties` is in the fork).
- `mode = "off"` (default) and `mode = "advisory"` MUST still mutate no user table. Only `mode = "active"` on a table with `sqe.maintenance.enabled = true` AND a working principal grant may compact.
- The interactive query path stays 100% user-identity; the maintenance principal remains constructed only from the maintenance path.
- Write authorization must be EXPLICIT for the maintenance session (not the role-name heuristic); Polaris remains the real enforcer.
- No emdash/endash/unicode-arrows in docs/comments.
- Never push to main; branch + MR via glab.

---

### Task 1: Snapshot stamping for `rewrite_data_files`

**Files:** Modify `crates/sqe-coordinator/src/maintenance.rs` (thread snapshot properties into `rewrite_data_files_once`'s `RewriteFilesAction`).

**Interfaces produced:** `rewrite_data_files_once(..., snapshot_properties: Option<HashMap<String,String>>)` (internal); the `RewriteFilesAction` commit calls `.set_snapshot_properties(props)` when `Some`. The public `CALL system.rewrite_data_files` path passes `None` (unchanged behavior); the scheduler passes `Some({sqe.maintenance.job-id, sqe.maintenance.principal, sqe.maintenance.trigger})`.

- [ ] Step 1: Failing test — a unit or integration assertion that a rewrite invoked with snapshot properties produces a snapshot whose summary carries them (integration `#[ignore]`/test-sqlite: run rewrite with props, reload table, assert `current_snapshot().summary()` contains the keys). Confirm the manual `CALL` path is unaffected (props None).
- [ ] Step 2: Run -> FAIL.
- [ ] Step 3: Implement — add the param, chain `.set_snapshot_properties(props)` on the action when `Some`. Verify `RewriteFilesAction::set_snapshot_properties` signature in `vendor/iceberg-rust/.../transaction/rewrite_files.rs`. Update the manual dispatch call site to pass `None`.
- [ ] Step 4: Run -> PASS. `cargo clippy -p sqe-coordinator --all-targets -- -D warnings`.
- [ ] Step 5: Commit `feat(maintenance): stamp compaction snapshots with job identity`.

---

### Task 2: Explicit maintenance write-authority

**Files:** Modify `crates/sqe-core/src/session.rs` (add an explicit maintenance-authority signal), `crates/sqe-coordinator/src/maintenance_principal.rs` (set it when minting), `crates/sqe-coordinator/src/maintenance.rs` (`authorize_or_deny` / `session_has_write_privilege` honor it).

**Interfaces produced:** a `Session` carries an explicit internal maintenance-authority marker (a dedicated method e.g. `with_maintenance_authority(bool)` + accessor, or a reserved sentinel role constant `SQE_MAINTENANCE_ROLE`). `authorize_or_deny` grants write to a session bearing the marker without consulting the role-name heuristic. Polaris still enforces server-side.

- [ ] Step 1: Failing unit test — `session_has_write_privilege` (or a new `is_maintenance_authorized`) returns true for a session minted with the marker even if its roles would otherwise be read-only; and a normal read-only session without the marker is still denied.
- [ ] Step 2: Run -> FAIL.
- [ ] Step 3: Implement the marker on `Session` (minimal, internal), set it in `MaintenancePrincipal::mint_session`, and make `authorize_or_deny` accept it. Keep the existing role-name path for non-maintenance sessions unchanged. Do NOT weaken any existing check.
- [ ] Step 4: Run -> PASS. Clippy clean.
- [ ] Step 5: Commit `feat(maintenance): explicit write-authority for the maintenance session`.

---

### Task 3: Active execution in the scheduler

**Files:** Modify `crates/sqe-coordinator/src/maintenance_scheduler.rs` (add the active arm), reuse `maintenance_log` job-row helpers, `MaintenanceHandler`/`rewrite_data_files` (called under the maintenance session), config `[maintenance.compaction]` + per-table property overrides.

**Interfaces produced:** `MaintenanceLogRow` job-row constructor(s) for `running`/`success`/`failed`/`skipped` (extend `maintenance_log.rs`); `resolve_compaction_params(cfg, table_props) -> CompactionParams` (per-table `sqe.maintenance.compaction.*` overrides win over `[maintenance.compaction]`). Scheduler: on a due, opted-in table in `active` mode where health shows eligible work, mint session, run `rewrite_data_files` with those params + snapshot-stamp props, record a real job row, emit `sqe_maintenance_job_total{status}` + `_bytes_rewritten_total` + audit. `advisory`/`off` unchanged.

- [ ] Step 1: Failing integration test (test-sqlite): an `active`-mode tick over one opted-in table with many small files reduces the file count and preserves rows, writes a `maintenance_log` row with `status="success"` and correct `files_in/out`, and stamps the new snapshot; a NON-opted table is untouched; an `advisory`-mode tick on the same setup mutates nothing.
- [ ] Step 2: Run -> FAIL.
- [ ] Step 3: Implement `resolve_compaction_params` (pure, unit-tested), the job-row helpers, and the active arm in the scheduler. Reuse `analyze_table_health` to decide eligibility (skip tables with no debt, logging `skipped` reason). Catch per-table errors -> `failed` row + metric, never abort the tick. Refresh the session token before the commit (do NOT trust `token_expiry()`).
- [ ] Step 4: Run -> PASS. Clippy clean; full `cargo test -p sqe-coordinator --lib`.
- [ ] Step 5: Commit `feat(maintenance): active-mode autonomous compaction`.

---

### Task 4: Cron scheduling (separable)

**Files:** `crates/sqe-core/Cargo.toml` or `sqe-coordinator/Cargo.toml` (add a cron parser dep, e.g. `croner`), `maintenance_scheduler.rs` (`table_due` honors `schedule` cron + per-table `sqe.maintenance.compaction.schedule`, combined with the existing tick-window + jitter).

**Interfaces produced:** `table_due` evaluates the cron expression against wall-clock (next-fire within the current tick window), retaining deterministic per-table jitter. Invalid cron -> log once + skip that table (never panic).

- [ ] Step 1: Failing unit test — a daily `"0 2 * * *"` schedule is due only in the 02:00 tick window, not at 14:00; per-table override wins; invalid cron is skipped not panicked.
- [ ] Step 2: Run -> FAIL.
- [ ] Step 3: Add the dep; implement cron evaluation in `table_due` (keep the 4a tick-window fix so it is not aliased). Keep jitter as an offset within the scheduled window.
- [ ] Step 4: Run -> PASS. Clippy clean.
- [ ] Step 5: Commit `feat(maintenance): cron schedule evaluation`.

Note: if the cron dep or semantics balloon, this task may be deferred without blocking 4b's active-compaction payoff (the 4a fixed periodic behavior remains correct); flag to the controller rather than forcing it.

---

### Task 5: Docs

**Files:** `docs/site/book/src/deployment/configuration.md` (document `mode="active"`, the per-table `sqe.maintenance.compaction.*` overrides, the cron `schedule`, the audit/`maintenance_log` job rows, and the snapshot stamps), `docs/site/book/src/sql-reference/procedures.md` if the manual path gained anything.

- [ ] Step 1: Document active mode + overrides + safety (advisory default, opt-in, Polaris grant, rollback within retention window).
- [ ] Step 2: Forbidden-char grep -> zero. Commit `docs(maintenance): active-mode compaction`.

---

## Verification (whole phase)

- `cargo build --all`; `cargo clippy --all-targets --all-features -- -D warnings` clean.
- `cargo test -p sqe-core -p sqe-coordinator --lib` green.
- Integration (test-sqlite + docker stack): active tick compacts an opted-in table (files reduced, rows preserved, snapshot stamped, `maintenance_log` success row); advisory/off mutate nothing; non-opted never touched; manual `CALL system.rewrite_data_files` unchanged (snapshot props None); `rewrite_data_files_deletes` still green.
- `git diff --stat vendor/` empty.

## Self-review notes

- 4b is coordinator-local only; `distribution.mode="auto"` == local until 4c. State this in docs.
- The write-authority marker (Task 2) closes design Risk 1.
- Snapshot stamping (Task 1) enables `table_health.last_compaction_snapshot_ms` (which was None in 4a) to become real; optionally wire that read in Task 3 or note it for 4c.
