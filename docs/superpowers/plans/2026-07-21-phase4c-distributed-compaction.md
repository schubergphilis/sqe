# Phase 4c: Distributed Worker Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: subagent-driven-development. Checkbox steps.

**Goal:** Fan out file-group rewrites across the worker fleet. Workers read (delete-applying), sort, and write Parquet directly to object storage and return only `DataFile` metadata; the coordinator plans, validates, and issues one atomic commit. `distribution.mode = "auto"` uses the fleet when `>= min_workers` are healthy, else falls back to the shipped coordinator-local path.

**Architecture:** Extract the shared rewrite primitives into a new `sqe-compaction` crate that both `sqe-coordinator` and `sqe-worker` depend on (workers cannot depend on `sqe-coordinator`). Add a signed worker `do_action("compact_file_group")` that runs the group rewrite against a `StaticTable` built from the pinned `metadata.json` (S3 creds only, no catalog token, no commit rights). The coordinator's job runner dispatches groups over the existing `WorkerRegistry` + load-tracker, collects avro-encoded `DataFile`s, and commits one `RewriteFilesAction` (seq pin + `check_file_existence` + snapshot stamp + covered-position-delete drop) exactly as the local path does today.

**Tech Stack:** Rust, Arrow Flight, DataFusion, vendored iceberg-rust, tokio, serde, HMAC-SHA256.

## Global Constraints

- Zero changes under `vendor/`. Verified APIs: `StaticTable::from_metadata_file` (table.rs:317), `write_data_files_to_avro`/`read_data_files_from_avro`, `RewriteFilesAction::{set_snapshot_properties,set_new_data_file_sequence_number}`.
- Commit authority NEVER leaves the coordinator. Workers produce files in object storage; only the coordinator changes table state.
- The coordinator-local path (single-node, and `CALL system.rewrite_data_files`) must stay behavior-identical: `distribution.mode="auto"` with no fleet == today's local rewrite. All existing rewrite tests must stay green.
- All correctness guards preserved end-to-end under distribution: sequence-number pin, `expected_rows_after_deletes` per-group cross-check, global `added_rows <= removed_rows`, `check_file_existence`, covered-position-delete drop.
- Workers get S3 credentials only (same trust model as `ScanTask`'s s3 block), never a catalog token, signed with the existing worker-secret + HMAC scheme.
- No emdash/endash/unicode-arrows. Never push to main; branch + MR via glab.

---

### Task 1: Extract `sqe-compaction` crate (shared primitives)

**Files:** Create `crates/sqe-compaction/` (Cargo.toml + src/lib.rs). Move from `crates/sqe-coordinator/src/maintenance.rs` the reusable, catalog-independent rewrite primitives: `rewrite_group`, `plan_delete_aware_read` + `DeleteAwareReadPlan`, `pack_file_groups`/`pack_file_groups_partition_aware`/`group_files_by_partition`, `delete_heavy_files`, `expected_rows_after_deletes`, `covered_position_deletes`, and the sort context (`SortSpec`/`SortCtx`/`OneShotStream`/`sort_group_stream`) + `zorder` module. Move the streaming writer bridge pieces they need from `writer.rs` (or re-export from a shared location). `sqe-coordinator` depends on `sqe-compaction` and re-exports so `maintenance.rs` call sites are unchanged.

**Interfaces produced:** `sqe_compaction::{rewrite_group, plan_delete_aware_read, DeleteAwareReadPlan, pack_file_groups_partition_aware, group_files_by_partition, delete_heavy_files, expected_rows_after_deletes, covered_position_deletes, SortSpec, ...}` with identical signatures to today.

- [ ] Step 1: Create the crate skeleton; add to workspace members + coordinator deps. Build empty.
- [ ] Step 2: Move the pure/primitive functions listed above (and their unit tests) into `sqe-compaction`; keep signatures identical; make `sqe-coordinator` re-export (`pub use sqe_compaction::...`) so `maintenance.rs`/`maintenance_scheduler.rs`/`table_health.rs` compile unchanged. Resolve visibility (`pub` where crossing the boundary).
- [ ] Step 3: `cargo build --all`; `cargo test -p sqe-coordinator --lib` and the moved unit tests -> all green (this is a pure refactor; behavior must not change).
- [ ] Step 4: Run the `rewrite_data_files_deletes` + `rewrite_data_files_real` integration tests on the docker stack to prove the local rewrite path is behavior-identical after extraction.
- [ ] Step 5: clippy clean. Commit `refactor(compaction): extract sqe-compaction crate`.

Note: keep the extraction MINIMAL - move only what the worker path needs plus what must move to satisfy the borrow/visibility graph. If a function pulls in coordinator-only deps (catalog bridge, session), leave it in coordinator and pass its outputs in.

---

### Task 2: Shared wire types + signing

**Files:** `crates/sqe-compaction/src/wire.rs` (types), a shared HMAC sign/verify helper (reuse the worker-secret + signature scheme; factor the verify from `sqe-worker/src/flight_service.rs:160-230` if cleanly shareable, else mirror it).

**Interfaces produced:**
- `CompactGroupRequest { job_id: String, group_id: u32, table_ident: String, metadata_location: String, snapshot_id: i64, group_file_paths: Vec<String>, target_file_size_bytes: u64, compression: String, sort: Option<SortSpecWire>, s3: S3Conn }` (S3Conn = same field set as `ScanTask`'s s3 block; reuse that struct if exported).
- `enum CompactGroupFrame { Progress { group_id, rows_read }, Done(CompactGroupResponse) }`.
- `CompactGroupResponse { group_id, new_data_files_avro: Vec<u8>, rows_written: u64, bytes_written: u64, uploaded_paths: Vec<String> }`.
- serde (bincode/JSON, match what `ScanTask::to_bytes` uses) + `sign(bytes, secret)`/`verify(bytes, sig, secret)` constant-time.

- [ ] Step 1: Failing unit test: request/response round-trip serialize/deserialize; sign+verify accepts a valid sig and rejects a tampered one (constant-time).
- [ ] Step 2: Run -> FAIL. Step 3: implement. Step 4: PASS + clippy. Step 5: commit `feat(compaction): compact_file_group wire types + signing`.

---

### Task 3: Worker `compact_file_group` action

**Files:** `crates/sqe-worker/src/flight_service.rs` (add the `do_action` arm + handler), `crates/sqe-worker/src/` (a `compaction` executor using `sqe-compaction`). `sqe-worker` gains a dep on `sqe-compaction`.

**Interfaces produced:** worker handles `do_action("compact_file_group")`: verify worker-secret + HMAC over the request bytes; decode `CompactGroupRequest`; build `FileIO` from `s3`; `StaticTable::from_metadata_file(metadata_location)`; assert `current_snapshot().snapshot_id() == snapshot_id` (refuse otherwise); `plan_delete_aware_read` restricted to `group_file_paths` (fail loud if any path missing); delete-applying read; optional `sort` via the worker's DataFusion runtime; rolling write via the shared streaming writer to the table data location; per-group `expected_rows_after_deletes` cross-check; `write_data_files_to_avro`; stream `Progress` then `Done`. Workers never touch a catalog token.

- [x] Step 1: Failing test (unit where possible; integration behind the distributed harness): a `compact_file_group` request over a single-file group returns a `Done` frame with avro `DataFile`s whose `read_data_files_from_avro` round-trips, and rejects a request whose `snapshot_id` does not match the metadata.
- [x] Step 2-4: implement + test; the delete-application + row cross-check reuse `sqe-compaction`. Verify a missing-path in the group fails loud (resurrection guard).
- [x] Step 5: clippy; commit `feat(worker): compact_file_group rewrite action`.

---

### Task 4: Coordinator job runner + atomic commit

**Files:** `crates/sqe-coordinator/src/` (a `compaction_job` runner, likely in `sqe-compaction` for the pure dispatch/assembly logic + coordinator glue for catalog/commit), reuse `WorkerRegistry` + `WorkerLoadTracker::reserve`, `maintenance.rs` commit path.

**Interfaces produced:** given a loaded table + planned groups + `seq_at_start` + `metadata_location`, dispatch each group to a healthy worker (largest-first, bounded by `max_inflight_groups_per_worker`), collect `CompactGroupResponse`s, `read_data_files_from_avro`, aggregate `new_files`/`old_files`/`rows`, re-run global `added_rows <= removed_rows`, and issue ONE `RewriteFilesAction` (enable_delete_filter_manager, check_file_existence, set_new_data_file_sequence_number(seq_at_start), snapshot stamp, covered position deletes) with the existing conflict-retry loop. On group failure: retry on another worker (`group_attempts`), heartbeat-timeout cancel; orphaned worker outputs left to the orphan sweep.

- [ ] Step 1: Failing unit test for the pure assembly: given synthetic worker responses (avro DataFiles), the runner aggregates counts and the invariant check rejects `added > removed`; dispatch placement is largest-first and respects `max_inflight_groups_per_worker`.
- [ ] Step 2-4: implement; the commit reuses the exact `maintenance.rs` `RewriteFilesAction` sequence (seq pin etc.). Integration behind the distributed harness: a distributed rewrite of a multi-group table equals the local rewrite's result (same surviving rows, files consolidated, deletes applied).
- [ ] Step 5: clippy; commit `feat(compaction): distributed job runner + atomic commit`.

---

### Task 5: `distribution.mode` wiring (auto/local/require)

**Files:** `crates/sqe-coordinator/src/maintenance.rs` (rewrite entry chooses local vs distributed), `maintenance_scheduler.rs` (active path), config already has `[maintenance.distribution]`.

**Interfaces produced:** `rewrite_data_files` (and the scheduler active path) consult `distribution.mode`: `auto` -> distributed when `WorkerRegistry` reports `>= min_workers` healthy, else local; `local` -> always coordinator-local; `require` -> error/skip if fleet below `min_workers` (scheduled jobs skip loudly with an audit + metric; interactive `CALL` errors). Manual `CALL system.rewrite_data_files` defaults to `prefer`/`auto` per config; a per-call `distributed => 'require'|'local'` override optional.

- [x] Step 1: Failing unit test: mode resolution (`auto` with N>=min -> distributed, with N<min -> local; `require` with N<min -> skip/err; `local` -> local) as a pure decision function over (mode, healthy_count, min_workers).
- [x] Step 2-4: implement + wire; ensure `local`/no-fleet path is byte-identical to today. Integration: `require` below floor skips loudly (scheduled) and errors (manual).
- [x] Step 5: clippy; commit `feat(compaction): distribution.mode auto/local/require`.

---

### Task 6: Distributed integration test + benchmark gate

**Files:** a distributed rewrite integration test (use the existing distributed test harness: `scripts/test.sh scenario distributed`, or a coordinator+2-worker compose), asserting the correctness parity + a wall-clock comparison.

- [ ] Step 1: Distributed rewrite of an N-group table on >=2 workers: surviving rows identical to a local rewrite of the same fixture; files consolidated; MoR deletes applied; snapshot stamped; ONE commit.
- [ ] Step 2: Benchmark gate (informational): distributed rewrite of SF10-scale data on 4 workers should beat coordinator-local wall-clock (target >2.5x); record the number, do not hard-fail CI on it.
- [ ] Step 3: commit `test(compaction): distributed rewrite parity + benchmark`.

---

### Task 7: Docs

**Files:** `docs/site/book/src/deployment/configuration.md` (`[maintenance.distribution]` now active: auto/local/require, min_workers, per-worker inflight, timeouts), a design-notes page on the distributed compaction data flow (workers read+write S3, coordinator commits), `procedures.md` if `CALL` gained a `distributed` arg.

- [ ] Document; forbidden-char grep zero; commit `docs(compaction): distributed compaction`.

---

## Verification (whole phase)

- `cargo build --all`; `cargo clippy --all-targets --all-features -- -D warnings` clean.
- All existing rewrite tests green (local path unchanged after extraction).
- Distributed parity test green; `require`-below-floor behavior correct; `git diff --stat vendor/` empty.

## Risks

- Crate extraction (Task 1) is the highest-risk mechanical change; gate it on the full existing rewrite test suite before proceeding.
- `FileScanTask` has skip-serialized fields, so workers MUST re-plan locally from `StaticTable` (never receive serialized tasks) - enforced by Task 3's design.
- Distributed testing needs a real multi-worker stack; pure-unit-test the dispatch/assembly/serde, integration-test the end-to-end on the harness.
- Orphan-on-retry: a commit-conflict retry re-dispatches groups; the prior attempt's worker outputs become orphans reclaimed by the age-thresholded sweep (accepted, documented).
- Second FairSpillPool from 4b: revisit sharing the runtime here (`TODO(4c)`).
