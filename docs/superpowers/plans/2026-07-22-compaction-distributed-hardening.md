# Compaction Distributed Hardening (pipelining + incremental progress + partial-progress commits)

> **For agentic workers:** REQUIRED SUB-SKILL: subagent-driven-development. Checkbox steps.

**Goal:** Three deferred distributed-compaction follow-ups: (1) continuously pipeline group dispatch instead of per-wave lockstep; (2) emit incremental worker progress so `group_heartbeat_timeout` catches a mid-compute hang; (3) opt-in partial-progress commits so a huge table can commit successful groups in batches instead of all-or-nothing.

**Architecture:** All three live in the distributed path shipped in Phase 4c/4d: `crates/sqe-coordinator/src/compaction_dispatch.rs` (dispatch loop + heartbeat), `crates/sqe-worker/src/compaction.rs` + `flight_service.rs` (worker rewrite + frames), `crates/sqe-coordinator/src/maintenance.rs` (`rewrite_data_files_distributed_once` commit), `crates/sqe-compaction/src/dispatch.rs` (pure placement). Base is stacked on `chore/compaction-followup-cleanups` (the deduped helpers).

**Tech Stack:** Rust, Arrow Flight, tokio, vendored iceberg-rust.

## Global Constraints

- Zero `vendor/` changes.
- Correctness invariants preserved end-to-end: worker snapshot-pin assert + missing-path guard + per-group `expected_rows_after_deletes`; coordinator global `added<=removed` (per commit); every commit uses seq-pin(`seq_at_start`) + `check_file_existence(true)` + covered-position-delete drop; conflict-retry re-plans.
- **Default behavior byte-identical** to today: partial-progress is OPT-IN (default off = one atomic commit for the whole job); pipelining must not change the committed result, only scheduling; incremental progress must not change worker output.
- The coordinator-local path and single-node are untouched.
- No emdash/endash/unicode-arrows. Never push to main; branch + MR via glab.

---

### Task 1: Continuous dispatch pipelining

**Files:** `crates/sqe-coordinator/src/compaction_dispatch.rs` (`dispatch_and_collect_groups`), `crates/sqe-compaction/src/dispatch.rs` (placement helpers if needed).

**Problem:** the loop computes a wave via `place_groups_largest_first`, spawns a `FuturesUnordered`, and WAITS for the entire wave before recomputing. A worker that finishes early idles until the slowest group in the wave finishes.

**Change:** rework to a continuous scheduler: maintain a pending-group queue (largest-first) and a per-worker in-flight count; keep every healthy worker filled up to `max_inflight_groups_per_worker`; as each group future resolves, immediately assign the next pending group to a now-free worker slot. Preserve ALL existing semantics: transport-vs-application failure classification, per-group retry on a different worker (`group_attempts`) with the exclusion set, the `is_permanently_stuck` stall guard, and NO commit here (Task 3 owns commit changes; this task still returns the full collected set for one commit). The committed result must be identical; only wall-clock/scheduling changes.

**Interfaces:** `dispatch_and_collect_groups` signature unchanged (or minimally); the pure placement/refill decision (which group to a which worker given current in-flight) should be a unit-testable function in `sqe-compaction::dispatch`.

- [ ] Step 1: Unit tests for the refill decision: given pending groups + per-worker in-flight counts + `max_inflight`, the next assignment goes to a free worker respecting the cap, largest-first; a worker at cap is skipped; no group double-assigned.
- [ ] Step 2: implement the continuous loop; keep retry/stall/classification intact.
- [ ] Step 3: build + clippy; lib tests; run the docker `rewrite_data_files_*` regression + (if runnable) the distributed parity test to confirm identical results.
- [ ] Step 4: commit `feat(compaction): continuous dispatch pipelining`.

---

### Task 2: Incremental worker progress + meaningful heartbeat

**Files:** `crates/sqe-worker/src/compaction.rs` + `flight_service.rs` (emit progress during the rewrite), `crates/sqe-coordinator/src/compaction_dispatch.rs` (reset heartbeat on each frame; a stalled worker is cancelled + retried).

**Problem:** `compact_pinned_table` computes the WHOLE group then emits `Progress` + `Done` together, so `group_heartbeat_timeout` only bounds frame delivery, not a mid-compute hang; `group_timeout` (default 3600s) is the real bound.

**Change:** make the worker emit `Progress { group_id, rows_read }` frames DURING the delete-applying read + rolling write (e.g. every N record batches or per rolled output file), then `Done`. The `do_action` stream yields those frames as they occur. On the coordinator, `drain_group_stream` already applies `heartbeat_timeout` per `stream.message()`; confirm each real `Progress` frame resets that per-frame timer, so a worker that stops making progress for `heartbeat_timeout` is detected and its group cancelled + retried (transport-class). Worker OUTPUT (the data files) must be byte-identical; only added progress frames.

- [ ] Step 1: implement periodic `Progress` emission in the worker rewrite (thread a frame-sender / yield through the streaming write). Keep the final `Done` unchanged.
- [ ] Step 2: coordinator: confirm/adjust that a `Progress` frame resets the heartbeat window and that heartbeat expiry (no frame within `group_heartbeat_timeout`) cancels + retries the group as a transport failure (not application). Unit-test the heartbeat/reset decision if it can be isolated.
- [ ] Step 3: build + clippy; lib tests; docker rewrite regression (output identical). Document that heartbeat now bounds mid-compute stalls.
- [ ] Step 4: commit `feat(compaction): incremental worker progress frames + live heartbeat`.

---

### Task 3: Opt-in partial-progress commits

**Files:** `crates/sqe-core/src/config.rs` (`[maintenance.distribution]` gains `partial_progress: bool` default false + `partial_progress_batch: usize` default e.g. 10), `crates/sqe-coordinator/src/maintenance.rs` (`rewrite_data_files_distributed[_once]`), `crates/sqe-coordinator/src/compaction_dispatch.rs`.

**Problem:** today the distributed job collects ALL groups then commits ONE `RewriteFilesAction`; one late group failure discards the whole job. For very large tables, committing successful work incrementally is valuable.

**Change (OPT-IN, default off = current all-or-nothing):** when `partial_progress` is true, commit successful groups in batches of `partial_progress_batch`: each batch is its own `RewriteFilesAction` over that batch's `(new_files, old_files, covered_position_deletes)`, using the SAME commit sequence (seq-pin `seq_at_start`, `check_file_existence(true)`, enable_delete_filter_manager, snapshot stamp) and the SAME per-attempt conflict-retry. Batches remove disjoint data-file sets, so each commit is independent. On a terminal group failure with partial_progress on, commit the already-succeeded batches and report `status="partial"` in `maintenance_log` (files_in/out reflect what committed) rather than failing the whole job; without partial_progress, behavior is unchanged (one commit, fail = no commit).
CORRECTNESS: the global `added<=removed` invariant applies PER COMMIT (per batch); the seq-pin stays `seq_at_start` for every batch (each batch's compacted output must out-rank no concurrent equality delete committed after `seq_at_start`); `check_file_existence` validates each batch's removed files still exist at that commit. Document that partial-progress trades a larger commit-conflict surface (N commits) for incremental durability.

- [ ] Step 1: config field + validation (batch >= 1 when partial_progress). Unit-test config parse/default (default false).
- [ ] Step 2: Failing test (test-sqlite or docker-gated as fits): with partial_progress on and a forced failure on the last group, the earlier groups' rewrites ARE committed (table shows consolidation for them) and `maintenance_log` records `partial`; with partial_progress OFF, the same forced failure leaves the table UNCHANGED (one atomic commit, none applied). Assert both.
- [ ] Step 3: implement batched commit in `rewrite_data_files_distributed_once`; keep the default (off) path exactly as today. Preserve every guard per batch.
- [ ] Step 4: build + clippy; lib + docker rewrite regression (default path unchanged); the partial-progress test passes.
- [ ] Step 5: commit `feat(compaction): opt-in partial-progress commits for distributed rewrite`.

---

### Task 4: Docs

**Files:** `docs/site/book/src/deployment/configuration.md` (`partial_progress` + `partial_progress_batch`; note the conflict-surface tradeoff and that it is opt-in), `docs/site/book/src/design-notes/distributed-compaction.md` (pipelining + live heartbeat + partial-progress semantics), `docs/site/book/src/features/autonomous-compaction.md` if the distribution bullet needs it.

- [ ] Document; forbidden-char grep zero; commit `docs(compaction): pipelining, live heartbeat, partial-progress`.

---

## Verification (whole phase)

- `cargo build --all`; `cargo clippy --all-targets --all-features -- -D warnings` clean.
- All existing rewrite + distributed tests green; default (non-partial, pre-pipelining-result) committed output identical (docker rewrite regression 11/11).
- Partial-progress opt-in test proves both on (incremental commit) and off (atomic) behavior.
- `git diff --stat vendor/` empty.

## Risks

- Task 3 is correctness-sensitive: the per-batch seq-pin + check_file_existence must hold for each independent commit; the whole-branch review (Opus) must scrutinize that a batch commit after a prior batch advanced the snapshot still removes only files that exist and cannot resurrect or double-count. Default-off keeps the safe path.
- Task 1 must not change the committed result (only scheduling); the distributed parity test is the guard.
- Task 2 must not change worker output (only add frames).
