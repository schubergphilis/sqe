# Write-path memory safety

Date: 2026-07-02
Status: Draft for review
Scope: Extension of subsystem A (memory safety) from the enterprise concurrency-hardening program. The 2026-06-21 spec (`docs/internal/specs/2026-06-21-memory-safety-oom-prevention-design.md`) governs read-side and operator memory: the shared node pool, spill, admission, and the failure taxonomy. This spec covers the one surface that spec does not: the Iceberg write sink's own buffers on the coordinator. It defines no new governance machinery. It registers the write path with the machinery subsystem A already defines.

## Context and motivation

The DataFusion memory pool built in `crates/sqe-coordinator/src/runtime.rs` (`build_memory_pool`, line 46) tracks DataFusion operators: joins, aggregates, sorts. The Iceberg write sink is not a DataFusion operator. Every byte it buffers is invisible to the pool. A MERGE that materializes a 20 GB target table reports zero pool usage while the process balloons past its cgroup limit and the OS sends SIGKILL. The pool cannot spill what it cannot see, and the governor from subsystem A cannot fail the query cleanly because the allocation never passed through it.

One honest note first. The observed 167M-row CTAS that grew the coordinator to 7.2 GiB was not a write-path leak. CTAS and INSERT already stream batch by batch through `write_data_files_streaming` (`crates/sqe-coordinator/src/writer.rs:493`, a `while let Some(batch)` loop, O(batch_size) resident). The 7.2 GiB was DataFusion operator state, tracked and bounded under the 8 GB pool limit. The actual demo crash was RustFS dying at a 512 MB cgroup cap. That incident is closed. What it exposed during investigation is the set of write paths that genuinely are unbounded, and those are what this spec hardens.

Four write-path buffers are unbounded and pool-invisible today. All live on the coordinator; `sqe-worker` and `sqe-planner` carry no write code, so the blast surface is exactly one process.

1. MERGE copy-on-write, the worst offender. `handle_merge_dispatch` (`crates/sqe-coordinator/src/write_handler.rs:3240`) routes copy-on-write merges into a path that reads the entire target table into `target_batches: Vec<RecordBatch>` (write_handler.rs:2906-2912), runs the merge SELECT and `df.collect()`s the full merged output into `result_batches` (write_handler.rs:3137), then writes it buffered through `write_data_files_with_metrics` (write_handler.rs:3183). Peak residency is source plus full target plus full merged output, none of it tracked. The merge-on-read path `handle_merge_equality` (write_handler.rs:3369) collects only matched and inserted rows, which is bounded by the delta, not the table. Lower risk, but still untracked.
2. Flight DoPut ingest. `do_put_statement_ingest` (`crates/sqe-coordinator/src/flight_sql.rs:1865`) `try_collect()`s the entire uploaded Arrow stream into a `Vec<RecordBatch>` (flight_sql.rs:1897) before handing it to `handle_ingest` (write_handler.rs:1653). A client can upload an arbitrarily large stream; the coordinator buffers all of it.
3. UPDATE and DELETE copy-on-write. `read_parquet_via_table` (write_handler.rs:3942) reads one whole compressed data file into memory as `Bytes`, then decodes all of its batches into a `Vec<RecordBatch>` per iteration. The compressed side is bounded near 512 MB by the rolling-writer file target, but a wide file decompresses to several GB of Arrow, resident all at once, per file.
4. Partitioned fanout. Both write entry points build a `TaskWriter` with fanout enabled (writer.rs:412-418 batched, writer.rs:593-599 streaming). The vendored `FanoutWriter` (`vendor/iceberg-rust/crates/iceberg/src/writer/partitioning/fanout_writer.rs:52`) holds one open `RollingFileWriter` per distinct partition value in a `HashMap`, each buffering an in-progress parquet row group. Memory is live-partition-count times per-writer buffer, with no cap on either factor. A write clustered badly against the partition spec opens hundreds of writers.

The single-file write side is fine: parquet row groups flush progressively to opendal multipart uploads, and `RollingFileWriter` rolls at the 512 MB target (`vendor/iceberg-rust/crates/iceberg/src/writer/file_writer/rolling_writer.rs:182`, `should_roll`). One open writer is bounded. Many open writers, or a Vec that holds a whole table, is not.

The precedent for the fix already exists twice in the codebase. The worker scan path registers `MemoryConsumer::new(...)` against the session pool and reserves through it (`sqe-worker/src/executor.rs:143` and `:565`, plus the sites at `:210`, `:327`, `:793`). The old coordinator result-buffer OOM was fixed by removing the buffer entirely in favor of streaming (`sqe-coordinator/src/streaming.rs:10-18`, module doc). This spec applies both patterns to the write sink.

## Relationship to subsystem A

This spec reuses, without redefining:

- The shared per-node pool. Reservations here register against the coordinator's pool from `runtime.rs:46`, which subsystem A wraps in the `NodeMemoryGovernor` owning one `FairSpillPool` per node. The write path takes reservation handles from that same pool; it adds no second budget.
- The failure taxonomy. A denied write reservation surfaces as `SqeError::MemoryExhausted` in the query-level class (`MemoryFailureClass::QueryLevel`): fails one query, never the node, never counts against the node. Flush hiccups to object store during fanout cutover map to the transient-I/O class and retry. The shuffle-data-loss class is not used here; the write path holds no shuffle state.
- The admission floor. Write statements pass the same `AdmissionGate`; this spec adds no write-specific admission logic.
- Observability plumbing. The metrics, structured-log, and OCSF-audit hooks from subsystem A's component 6 gain write-path counters; no new pipeline.
- The sort-on-write `can_spill=false` fix and all operator spill wiring. Owned entirely by the 2026-06-21 spec. Not restated here.

Sequencing note: phase A of this spec (pool tracking) does not wait for subsystem A to land. The `FairSpillPool` and `MemoryConsumer` API exist today in `runtime.rs` and are already used by `executor.rs`. Reservations registered now become governor-visible automatically when the governor wraps the pool. Only the typed `MemoryExhausted` variant and its class flags arrive with subsystem A; until then the deny path maps to the closest existing `SqeError` variant and upgrades in place.

## Goal and non-goals

### Goal

No write statement can OOM-kill the coordinator, at any table size, partition count, or upload size. Every write-path buffer is either removed (streamed) or registered with the shared pool so that exhaustion becomes a clean typed error for that one query. Small and uncontended deployments behave byte-for-byte as they do today.

### In scope

- Pool registration for the four unbounded buffers: MERGE copy-on-write target and result, DoPut ingest collect, UPDATE/DELETE per-file decode, and fanout writer buffers. Plus light tracking of the merge-on-read collects.
- Streaming replacements where the algorithm permits: the MERGE output side, the DoPut ingest path, and per-file batch iteration for UPDATE/DELETE.
- A bounded fanout writer with flush-on-pressure cutover, and the explicit small-file tradeoff it creates, repaired by the existing `system.rewrite_data_files` procedure (`crates/sqe-coordinator/src/maintenance.rs`).
- Config knobs on `QueryConfig`, auto-tuned, all `#[serde(default)]`.
- Write-path counters and audit events on the subsystem A observability surface.

### Out of scope

- The CTAS and INSERT streaming path. `write_data_files_streaming` is already O(batch_size); it appears here only as the sink that other paths converge onto. Do not touch its data flow.
- Everything the 2026-06-21 spec owns: governor, admission, operator spill, shuffle receiver spill, sort-on-write fix, the web UI panels. Referenced, not redefined.
- Spilling the write buffers themselves to disk. The write sink's buffers are removed or bounded, not spilled; spill remains an operator concern inside DataFusion where subsystem A wires it.
- Distributed writes. Writes are coordinator-only today; if a distributed write path arrives later, it inherits this design through the same pool on worker nodes.
- Compaction scheduling policy. Cutover relies on `system.rewrite_data_files` existing; when to run it stays an operator or pipeline decision.

## Design decision: track first, then stream

Two layers, applied per path.

Layer A, pool tracking, is the safety net and lands first. Every unbounded buffer gets a `MemoryConsumer` reservation against the shared pool, mirroring `executor.rs:143`. Growth goes through `try_grow`; denial becomes `MemoryExhausted`, query-level class. Layer A converts a silent process kill into a clean single-query failure. It is cheap, mechanical, and correct regardless of what layer B later removes.

Layer B, streaming and bounding, removes the buffer where the algorithm allows instead of merely accounting for it. The MERGE output and DoPut ingest become streams into the existing streaming sink. UPDATE/DELETE iterates batches per file instead of collecting them. Fanout gets a hard cap with flush cutover. Layer B is where the ceiling on writable table size actually lifts; layer A is what stands behind it when streaming is not possible.

The ordering matters. A alone is shippable and safe. B without A leaves the residual buffers (merge join state fallback, fanout estimate error) unguarded. Both layers draw from the one shared pool, so a write competes fairly with the query's own operators under the governor, which is exactly the intent.

## Shared mechanism: TrackedBatchBuffer

One new module, `crates/sqe-coordinator/src/write_memory.rs`, provides the tracking primitive both layers use.

- `TrackedBatchBuffer`: wraps `Vec<RecordBatch>` plus a `MemoryReservation`. `push(batch)` calls `try_grow(batch.get_array_memory_size())` before appending; a denial returns the memory error with an operator label (for example `merge-target-buffer`). Dropping the buffer shrinks the reservation to zero. Construction takes the pool handle from the session's `RuntimeEnv`, the same handle `executor.rs` uses.
- `WriteReservation`: a bare resizable reservation for non-Vec cases, used by the fanout budget and the per-file `Bytes` read.

Both are drop-safe: the reservation releases on drop, so early returns, `?` short-circuits, and panics reclaim to zero. The existing `WriteCleanupGuard` (write_handler.rs, used at the merge write site near :3155) keeps owning object-store cleanup; the reservation guard owns memory. Two guards, two resources, no coupling.

## Per-path design

### MERGE copy-on-write

Layer A hooks:
- write_handler.rs:2906-2912: `target_batches` becomes a `TrackedBatchBuffer` labeled `merge-target-buffer`. The per-file read loop pushes through `try_grow`.
- write_handler.rs:3137: `result_batches` becomes a `TrackedBatchBuffer` labeled `merge-result-buffer`.
- The scratch `MemTable`s registered for the merge SELECT hold references into the same tracked batches, so no double count; the reservation must outlive the `MergeScratchCleanup` guard.

Layer B replaces both collects. The design is in the next section because it is the hardest piece.

The merge-on-read path (`handle_merge_equality`, write_handler.rs:3369) gets layer A only: its matched-plus-inserted collects become tracked buffers. Its residency is proportional to the change set, so streaming it buys little; tracking it closes the hole.

### Flight DoPut ingest

Layer A hook: flight_sql.rs:1897, the `try_collect()` result becomes a `TrackedBatchBuffer` labeled `ingest-buffer`.

Layer B removes the collect. `FlightRecordBatchStream` is already a stream; adapt it into a `SendableRecordBatchStream` and pass it to a new `handle_ingest_streaming(&session, &qualified, stream)` on the write handler, which resolves the target table (schema comes from table metadata, so no need to peek the stream) and feeds `write_data_files_streaming` at writer.rs:493. After layer B the ingest path holds O(batch_size), the same shape as INSERT, and the layer A buffer disappears along with the code that filled it. The existing `handle_ingest(batches)` signature stays for internal callers and tests; the Flight entry point switches to the streaming variant.

Layer B here is low-risk and high-value: it is the same transformation `streaming.rs` already performed on the result path, in reverse direction.

### UPDATE and DELETE copy-on-write

Layer A hooks, both inside `read_parquet_via_table` (write_handler.rs:3942) and its callers:
- The whole-file `Bytes` read takes a `WriteReservation` sized to the input length, labeled `cow-file-bytes`. Bounded near 512 MB compressed by the rolling target, but under a small pool it must still be visible.
- The decoded `Vec<RecordBatch>` becomes a `TrackedBatchBuffer` labeled `cow-decode-buffer`.
- The kept-rows accumulation in the DELETE/UPDATE rewrite loop becomes a tracked buffer labeled `cow-keep-buffer`.

Layer B: split `read_parquet_via_table` into a `read_parquet_batches_iter` that returns the `ParquetRecordBatchReader` iterator instead of collecting it. The sync parquet reader decodes row group by row group, so iterating lazily caps decoded residency at one batch plus one row group. Callers filter each batch and push kept rows straight into the streaming sink (or into the tracked keep-buffer where the current commit shape requires the full file's kept rows before rewrite). The compressed `Bytes` stays whole-file in memory; that is an iceberg-rust `FileIO` input constraint, it is bounded by the roll target, and after layer A it is pool-visible. Accepting it is the pragmatic call; a range-read refactor of `FileIO` inputs is not worth the churn here.

### Partitioned fanout

Layer A hook: the two `TaskWriter::new_with_partition_splitter(..., true, ...)` sites (writer.rs:412-418 and :593-599). A `WriteReservation` labeled `fanout-buffer` tracks the estimated total buffered bytes across open partition writers. The estimate accumulates `batch.get_array_memory_size()` per partition since that writer's last observed roll, which overstates slightly (arrow size exceeds encoded parquet size). Overstating is the safe direction for a budget.

Layer B is a bounded fanout with cutover. The vendored `FanoutWriter` keeps an unbounded `HashMap` of open writers; rather than patch the vendor crate (rebase debt, see the #195 vendor-patch precedent), SQE owns a `BoundedFanoutWriter` in `crates/sqe-coordinator/src/writer.rs` that replaces the `TaskWriter` fanout at both call sites. Same partition splitter, same `RollingFileWriter` builder, plus two limits:

- An open-writer cap. When a batch arrives for a new partition and the map is full, close and flush the least-recently-written writer first, collect its `DataFile`s, then open the new one.
- A byte budget. When the tracked estimate exceeds the budget, close-and-flush writers in least-recently-written order until under budget.

Cutover is correct by construction: Iceberg permits any number of data files per partition, and a partition that receives rows after its writer was cut simply gets a fresh writer and another file. The cost is real and stated plainly: cutover trades bounded memory for small-file debt. A badly clustered write under a tight budget produces many files below the 512 MB target, and scan performance degrades until `system.rewrite_data_files` (bin-pack, already implemented in `maintenance.rs` and parsed in `crates/sqe-sql/src/procedures.rs`) compacts them. For dbt pipelines this means a maintenance call belongs after large partitioned loads. The response to a cutover-heavy write is a log line and a counter, not a failure.

The alternative, sorting the input by partition key so one writer is open at a time, was rejected as the default: it reintroduces the sort-on-write memory cliff this program exists to remove (the `ExternalSorterMerge` `can_spill=false` issue is subsystem A's to fix, and even spilled, a mandatory sort taxes every partitioned write). Once subsystem A lands the sort fix, sort-to-cluster becomes a legitimate opt-in for layout-sensitive loads; the cutover path stays the bounded default.

## MERGE streaming design

The hardest problem: the copy-on-write MERGE currently needs the whole target resident twice, once as input batches and once inside the merged output. Two changes remove both, in order of payoff.

Output side first. Replace `df.collect()` at write_handler.rs:3137 plus the buffered `write_data_files_with_metrics` at :3183 with `df.execute_stream()` feeding `write_data_files_streaming` (writer.rs:493). The merged output then never materializes as a Vec. The join and case-evaluation state of the merge SELECT lives inside DataFusion operators, which are pool-tracked and, under subsystem A, spillable. Moving the bytes from an invisible Vec into governed operators is the entire point: the same memory now spills or fails cleanly instead of ballooning. The `WriteCleanupGuard` and commit sequence are unchanged; `write_data_files_streaming` already returns the `DataFile` set the commit needs. One behavioral note: the streamed write starts producing parquet before the merge query finishes, so a mid-query failure leaves uploaded files for the guard to delete, which it already does for CTAS (#58).

Input side second. The target is read into a `MemTable` today for one reason: the commit must replace exactly the file set `old_data_files` that was read, pinning a snapshot. Streaming it means registering a table provider over exactly those file paths instead of materializing them. Concretely: build a DataFusion parquet scan (via a `ListingTable` or a small file-set provider) over `old_data_files`, using the same object-store credentials the Iceberg `FileIO` carries, and register it as the target-side relation of the merge SELECT. The file set is captured before planning, so the replace-set contract holds identically. Target rows then stream through the join, and the join build side is governed operator memory.

Fallback and phasing. Output-side streaming is phase B1: low-risk, mechanical, biggest single win (the merged output is at least as large as the target). Input-side streaming is phase B2 and carries real integration work (credential wiring for the raw parquet scan, schema-evolution casts that `read_parquet_via_table` currently applies through the Iceberg reader). Until B2 lands, the layer A tracked `target_batches` buffer is the input-side behavior: a MERGE whose target does not fit the pool fails with `MemoryExhausted` and the message names the buffer, which is the honest outcome for a copy-on-write merge of a table larger than memory. The durable answer for very large targets is the merge-on-read path, which is already delta-bounded; the error text should say so.

## Configuration

All knobs extend `QueryConfig` (`crates/sqe-core/src/config.rs`, `#[serde(rename_all = "kebab-case")]`), each `#[serde(default)]`, so existing config files stay valid. No new mandatory configuration.

- `fanout-max-open-writers`. Cap on concurrently open partition writers per write. Default `0` means auto: derived from the pool size and the per-writer row-group estimate, floored at 8 and capped at 64. Small deployments with few partitions never reach it.
- `fanout-buffer-budget`. Byte budget for total buffered fanout memory, same string format as `max_query_memory` ("512MB"). Default `"0"` means auto: a fixed fraction of the node pool, bounded below so a tiny pool still allows one writer at full row-group size.
- `write-buffer-tracking`. Boolean, default `true`. Escape hatch that disables the layer A reservations (never the layer B streaming) if a deployment hits an accounting false positive. Documented as a diagnostic, not a tuning knob.

Auto-tuning keeps the small-deployment promise: under no pressure, `try_grow` on a mostly empty pool always succeeds, the fanout caps sit above any realistic partition count, and cutover never triggers. The mechanisms are pressure-triggered; an uncontended write is byte-for-byte unchanged, and the layer B streaming paths produce identical files to the buffered paths they replace.

## Failure and degradation behavior

Reuses the subsystem A taxonomy unchanged.

- A denied `try_grow` on any tracked write buffer returns `SqeError::MemoryExhausted`, class `QueryLevel`, carrying the buffer label, requested and available bytes, and the statement kind. One query fails; the node and every other query continue. The error text for the MERGE target buffer recommends the merge-on-read table property.
- Fanout cutover is degradation, not failure. Exceeding the writer cap or byte budget flushes writers and continues. It emits one structured info line per write (count of cutovers, files produced), never per batch.
- Object-store errors during a cutover flush are class `TransientIo`: retried per subsystem A policy, escalating to `QueryLevel` on exhaustion, at which point the `WriteCleanupGuard` deletes uploaded files and the commit never happens.
- Cleanup is deterministic. Memory reservations release on drop, uploaded files delete through the existing guard, and a killed write leaves no reservation and no orphan (the orphan backstop remains `remove_orphan_files`).
- Admission rejection (`Rejected`) is untouched; writes pass the same gate as reads.

## Observability

Three additions to the subsystem A surface, same pipeline, no new data path.

Metrics (Prometheus via `sqe-metrics`):
- Gauge: `write_buffer_bytes` per node (sum of live write reservations), and `fanout_open_writers` for the current write high-water mark.
- Counters: `write_memory_kills_total` (subset of subsystem A's kills, labeled by buffer), `fanout_cutovers_total`, `fanout_cutover_files_total`.

Logging: one structured warn on a write memory kill (query id, user, buffer label, requested versus available), mirrored as the same OCSF audit event subsystem A defines for kills. One structured info per write that cut over (cutover count, extra files), so small-file debt is visible without noise.

Web UI: the killed-queries and per-query memory views from subsystem A pick these up through the shared data source; the only addition is the buffer label on the kill record. No new panel.

## Testing strategy

- Unit. `TrackedBatchBuffer` grow, deny, and drop-to-zero accounting. `BoundedFanoutWriter` cutover order (least-recently-written first), cap and budget enforcement, and `DataFile` completeness across cutovers.
- Tiny-pool forcing, the core gate. With the pool forced small: a copy-on-write MERGE against an oversized target fails with typed `MemoryExhausted` naming `merge-target-buffer` (phase A) and completes by streaming once B1/B2 land; an oversized DoPut ingest fails typed (phase A) and completes streaming (phase B); a high-cardinality partitioned write completes via cutover within budget. In every case the process is never OOM-killed.
- Correctness parity. For each streamed or bounded path, results must match the buffered baseline: MERGE row counts and snapshot contents, ingest row counts, UPDATE/DELETE surviving rows, and partitioned-write contents before and after cutover.
- Cutover-plus-compaction round trip. A write forced into heavy cutover produces many small files; `system.rewrite_data_files` compacts them; row counts and query results are identical before and after.
- Leak check. After a write kill: reservations at zero, no uploaded orphans (guard ran), scratch MemTables deregistered.
- Small-deployment no-regression. Default config, ample memory: zero cutovers, zero denials, identical files and timings to a pre-change baseline for CTAS, INSERT, MERGE, ingest, and partitioned writes. Tested, not assumed.
- Lints. `cargo clippy --all-targets --all-features -- -D warnings` clean; config defaults round-trip through existing TOML files.

## Success criteria

On a memory-constrained coordinator: no write statement of any size can get the process OOM-killed. Every unbounded write buffer is either gone (streamed) or pool-visible, exhaustion fails exactly one query with a typed error that names the buffer, fanout stays within its budget and repairs its small-file debt through existing compaction, and a default small deployment shows no measurable change.

## Open decisions for the implementer

1. MERGE input-side scan (B2): `ListingTable` over the pinned file paths versus a small custom file-set provider, and how the Iceberg schema-evolution casts port to the raw parquet scan. B1 and layer A do not depend on the answer.
2. Fanout buffered-bytes estimator: arrow `get_array_memory_size` accumulation (proposed, overstates) versus reading the parquet writer's in-progress size, which would need vendor surface. Confirm the overstatement is acceptable at the default budget.
3. Whether `handle_merge_equality`'s tracked collects ever need a phase B, or delta-bounded plus tracked is final. Proposed: final.
4. The auto-tuning formula constants for `fanout-max-open-writers` and `fanout-buffer-budget` (floor 8, cap 64, pool fraction) pending measurement of real row-group residency per writer.
5. Whether the Flight `handle_ingest` batched signature is retired once all callers use the streaming variant, or kept for the tests that exercise its name parsing (write_handler.rs:7231).

## References

- Subsystem A spec: `docs/internal/specs/2026-06-21-memory-safety-oom-prevention-design.md`
- Write routing: `crates/sqe-coordinator/src/query_handler.rs:1056` (CTAS), `:1085` (INSERT), `:1280` (MERGE); `crates/sqe-coordinator/src/flight_sql.rs:1865` (DoPut ingest)
- Write sinks: `crates/sqe-coordinator/src/writer.rs:412-418`, `:493` (`write_data_files_streaming`), `:593-599`
- MERGE copy-on-write: `crates/sqe-coordinator/src/write_handler.rs:2906-2912`, `:3137`, `:3183`; merge-on-read `:3369`; per-file reader `:3942`
- Pool and precedent: `crates/sqe-coordinator/src/runtime.rs:46` (`build_memory_pool`), `sqe-worker/src/executor.rs:143,210,327,565,793` (`MemoryConsumer` pattern), `sqe-coordinator/src/streaming.rs:10-18` (result-buffer fix)
- Vendored writer internals: `vendor/iceberg-rust/crates/iceberg/src/writer/partitioning/fanout_writer.rs:52`, `vendor/iceberg-rust/crates/iceberg/src/writer/file_writer/rolling_writer.rs:182`
- Compaction: `crates/sqe-coordinator/src/maintenance.rs` (`rewrite_data_files`, `expire_snapshots`, `remove_orphan_files`), `crates/sqe-sql/src/procedures.rs`
- Related specs: `docs/internal/specs/2026-03-30-error-handling-design.md`, `docs/internal/specs/2026-06-01-sqe-web-ui-design.md`
