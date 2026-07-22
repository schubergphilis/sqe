# Phase 4d: HA Lease (multi-coordinator) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: subagent-driven-development. Checkbox steps.

**Goal:** Prevent multiple coordinators from redundantly compacting the same table when several run the scheduler. Add a catalog-native lease over `sqe_system.maintenance_log`. Correctness is already guaranteed by Iceberg optimistic commit (two coordinators cannot double-commit); the lease is an EFFICIENCY layer that stops both from reading and rewriting terabytes only to have one throw the work away.

**Architecture:** A per-job lease claim/renew/release stored as rows in the existing `sqe_system.maintenance_log` state table, arbitrated by an Iceberg commit action that actually conflicts on concurrent writers. Task 1 is a spike that determines which action gives real mutual exclusion (fast-appends are commutative and will not conflict). The scheduler acquires the lease before an active compaction, renews it for long jobs, releases it after, and steals an expired lease after its TTL. `lease = "none"` (single coordinator, config-gated by `single_scheduler_acknowledged`) bypasses entirely.

**Tech Stack:** Rust, vendored iceberg-rust, tokio, Iceberg table commits.

## Global Constraints

- Zero changes under `vendor/`.
- Correctness NEVER depends on the lease: Iceberg optimistic commit (`check_file_existence` + seq pin, already shipped) is the real guard against double-commit/data-loss. The lease only reduces wasted duplicate work. State this everywhere.
- `lease = "none"` (default alongside `scheduler.enabled=false`) must behave exactly as Phase 4b/4c today (no lease traffic, no behavior change) - single-coordinator deployments are unaffected.
- The lease lives in `sqe_system.maintenance_log` (operator-created), NOT in user-table properties.
- No emdash/endash/unicode-arrows. Never push to main; branch + MR via glab.

---

### Task 1: CAS spike - determine the mutual-exclusion primitive

**Files:** a `#[ignore]`/test-sqlite integration test `crates/sqe-coordinator/tests/` that races two concurrent claimers against a state table.

**Goal:** Empirically determine which Iceberg commit path gives mutual exclusion on the state table. Two tasks concurrently attempt to "claim" (write a claim row) against the SAME base snapshot; verify whether exactly one commit succeeds and the other gets a retryable conflict, for:
- a plain `fast_append` (EXPECTED to NOT conflict - both succeed, commutative);
- an action that carries a base-snapshot / ref assertion (EXPECTED to conflict - the loser must re-read).

**Interfaces produced:** a documented conclusion (in the test + the report) naming the exact vendored action/method to use for a conflicting claim commit, which Task 2 builds on.

- [ ] Step 1: Write the two-racer test (test-sqlite catalog is fine; it exercises the same commit-conflict semantics). Race two claim commits against one base snapshot.
- [ ] Step 2: Run it; record which action conflicts. If `fast_append` does not conflict (likely), find the action that does (e.g. an overwrite/replace-style commit, or explicitly asserting the current snapshot id / a named ref). Confirm the vendored `Transaction` API exposes a base-snapshot assertion that fails on a concurrent writer.
- [ ] Step 3: Document the chosen primitive in the test module doc + report. This GATES Task 2's design.
- [ ] Step 4: clippy clean; commit `test(maintenance): CAS spike for catalog lease exclusion`.

Note: if NO vendored action gives hard mutual exclusion via the state table, fall back to a best-effort advisory lease (a claim row + read-back check + short random backoff) - document that it reduces but does not guarantee exclusion, which is acceptable because Iceberg optimistic commit is the real correctness guard. Report this outcome; do not force a vendor change.

---

### Task 2: Catalog lease (claim / renew / release / steal)

**Files:** `crates/sqe-coordinator/src/maintenance_lease.rs` (new), reuse `maintenance_log` schema/append.

**Interfaces produced:**
- `struct LeaseHandle { job_key: String, holder_id: String, expires_at_ms: i64 }`.
- `async fn try_acquire(catalog, state_table, job_key, holder_id, ttl_secs, now_ms) -> Result<Option<LeaseHandle>>` (None = someone else holds a live lease). Uses the Task-1 primitive; a claim is valid only when the newest claim row for `job_key` is absent/released/expired.
- `async fn renew(&mut LeaseHandle, ...)`, `async fn release(LeaseHandle, ...)`.
- Steal: acquisition succeeds against an EXPIRED lease (TTL from `lease_ttl_secs`), and the steal is auditable.
- `holder_id` = a per-coordinator stable id (e.g. hostname+pid or a config value / generated uuid at startup).

- [ ] Step 1: Failing unit tests for the pure lease-state logic: given the latest claim/release rows for a job_key + now_ms, decide acquirable (absent/released/expired) vs held. Cover: no rows -> acquirable; live claim by other -> held; expired claim -> acquirable (steal); own released -> acquirable.
- [ ] Step 2-4: implement `try_acquire`/`renew`/`release` over the state table using the Task-1 commit primitive; a real conflicting commit means two concurrent `try_acquire` cannot both return Some. Integration test (test-sqlite): two `try_acquire` for the same job_key -> exactly one Some; after `release`, the other can acquire; an expired lease is steal-acquirable.
- [ ] Step 5: clippy; commit `feat(maintenance): catalog lease over state table`.

---

### Task 3: Wire the lease into the scheduler

**Files:** `crates/sqe-coordinator/src/maintenance_scheduler.rs`, config (`lease`, `lease_ttl_secs`, `single_scheduler_acknowledged` already exist).

**Interfaces produced:** in the active tick, before compacting a due table (job_key = table ident), `try_acquire` the lease; if None (held elsewhere), skip that table this tick (debug log, not a failure); if acquired, run the compaction, `renew` before the commit for long jobs (do NOT trust wall clock beyond `now_ms` inputs), and `release` after (success or failure). `lease = "none"` bypasses acquisition entirely (current behavior). `lease = "kubernetes"` is OUT of scope for 4d (leave a clear `unimplemented`/config-reject with a message, or accept-and-treat-as-catalog with a warning - pick one and document). Steal on expiry is automatic via `try_acquire`.

- [ ] Step 1: Failing integration test (test-sqlite): with `lease="catalog"`, two scheduler instances (or two `advisory_tick`/`active` invocations sharing a state table) do not both compact the same table in one window - the second observes the lease and skips; after release, a later tick can acquire. And `lease="none"` compacts as today (no lease traffic).
- [ ] Step 2-4: implement; ensure `lease="none"` path is unchanged from 4c. Audit lease acquire/steal/release as `AuditKind::Maintenance` events.
- [ ] Step 5: clippy; full `cargo test -p sqe-coordinator --lib`; commit `feat(maintenance): scheduler acquires catalog lease before active compaction`.

---

### Task 4: Docs

**Files:** `docs/site/book/src/deployment/configuration.md` (the `lease` ladder: none/catalog[/kubernetes-deferred], `lease_ttl_secs`, `single_scheduler_acknowledged`, multi-coordinator HA guidance), the maintenance design-notes page (lease = efficiency not correctness; Iceberg optimistic commit is the real guard; the external K8s CronJob alternative for HA-safe scheduling without the internal lease).

- [ ] Document; forbidden-char grep zero; commit `docs(maintenance): HA lease + multi-coordinator guidance`.

---

## Verification (whole phase)

- `cargo build --all`; `cargo clippy --all-targets --all-features -- -D warnings` clean.
- Lease unit + test-sqlite integration green; `lease="none"` path unchanged (4b/4c behavior).
- `git diff --stat vendor/` empty.
- HA safety argument re-stated in docs: with the lease, two coordinators do not both rewrite; WITHOUT the lease (or on lease failure), Iceberg optimistic commit still guarantees exactly one commit wins and the loser re-plans to a no-op - waste, never corruption.

## Deferred (tracked, NOT in 4d)

- `lease = "kubernetes"` backend (K8s Lease API).
- Partial-progress commits for very large tables.
- Vended per-table S3 credentials to workers (once Polaris vending is un-stubbed).
- Promote duplicated `collect_live_delete_files` to `sqe-compaction`; dispatch-loop pipelining; true incremental worker progress; manual-CALL require-error e2e (all from 4c follow-ups).

## Risk

- Task 1 may find no hard-exclusion primitive on the state table. That is acceptable: fall back to best-effort advisory lease, because correctness never depends on it. Do not force a vendored change.
