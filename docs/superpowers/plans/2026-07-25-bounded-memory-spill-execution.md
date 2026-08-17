# Bounded-Memory and Spill Execution Implementation Plan

> **For agentic workers:** Implement this plan one phase/MR at a time. Do not
> combine the scan-accounting, spill framework, external operators, and durable
> exchange into one change. Every phase has a standalone memory-safety gate.

**Goal:** Make SQE complete zero-pruning scans, joins, aggregations, sorts, and
distributed exchanges whose input and intermediate data are many times larger
than aggregate RAM, without an OS OOM, unbounded queue, silent fallback, or
whole-query restart after an ordinary worker failure.

**Primary invariant:** Total resident data owned by SQE is bounded by explicit
byte budgets. Scans use backpressure rather than spill. Blocking operators and
distributed exchange spill partitioned state to the configured spill backend:
local NVMe, S3-compatible object storage, or tiered (local first, S3 for
overflow/durability). Either backend is optional per deployment; at least one
must be usable when spill is enabled. Resilient execution can persist
completed exchange output to object storage.

**Reference design:** DuckDB-style streaming vector pipelines, one memory
governor for temporary state, spillable pages/segments, radix partitioning, and
partition-wise completion; Trino-style distributed stage exchange and task
retry; DataFusion/Arrow remain SQE's execution foundation.

**Tech stack:** Rust, Tokio, Arrow 58, Arrow IPC, Arrow Flight, DataFusion
54.0.0, the vendored iceberg-rust fork (`vendor/iceberg-rust/`, RisingWave
`dev_rebase_main_20260303` @ `813e54419b43`, pinned in the root
`Cargo.toml:55-63`; not a released apache 0.8.x/0.9.x tag), local NVMe,
optional S3-compatible spill and durable exchange.

## Related plans, in-flight work, and vendor reality

This plan supersedes
`docs/internal/plans/2026-06-21-memory-safety-oom-prevention.md`
(NodeMemoryGovernor, AdmissionGate, shuffle spill tier, `can_spill=false`
fix). That plan's spec branch `feat/memory-safety-oom-spec` (github remote,
unmerged, paused) carries task-level detail worth mining, but do not run
both plans. Where they conflict, this plan wins.

Merged work to build on, not re-implement:

- `ScanDecodeGate` (`crates/sqe-catalog/src/scan_memory.rs`, commit
  `f65b808`): decode admission plus per-decode `MemoryConsumer` pool
  reservation on the embedded/coordinator Iceberg scan path. Phase 1's
  `ByteBudget`-on-`MemoryConsumer` design matches it deliberately.
- Fetch/decode pipelining (merged via `6ed409f`, vendor patch family 9):
  staged fetch admission (`DecodeGate::admit_fetch`) on the vendored reader.
- `TrackedBatchBuffer` (`crates/sqe-compaction/src/write_memory.rs`):
  pool-tracked write-path buffering, already on main.
- Memory-safe partitioned write (`fix/memory-safe-partitioned-write`,
  merged): skips the sort on partitioned CTAS.

Vendor patch exposure: SQE-local patch families 7 (`DecodeGate`, sqe#367)
and 9 (fetch staging) live in
`vendor/iceberg-rust/crates/iceberg/src/arrow/reader.rs` and
`crates/iceberg/src/scan/mod.rs` (see `vendor/iceberg-rust/README.md`).
Phases 1 and 2 change behavior in exactly that admission area. Any semantic
change must update the vendor README patch list so the next fork rebase
carries it forward.

## Why this work is required

Iceberg pruning is an accelerator, not a capacity guarantee. The engine must
handle:

- an unpartitioned table;
- overlapping or missing file statistics;
- no useful Bloom filters or page indexes;
- a cold metadata/footer cache;
- a full projection or low-selectivity filter;
- unknown or incorrect join cardinality;
- high-cardinality `GROUP BY`;
- a slow client or downstream stage;
- data and exchange volume much larger than RAM.

The current code has four concrete unsafe boundaries (verified 2026-07-26):

1. `sqe-worker::executor::execute_scan_streaming` registers one
   `MemoryConsumer` per fragment
   (`crates/sqe-worker/src/executor.rs:142-144`) and calls
   `reservation.try_grow(batch.get_array_memory_size())` for every emitted
   batch (`executor.rs:210` and `executor.rs:327`). Nothing ever shrinks the
   reservation; it releases only when the producer task drops. Two failures
   follow: any scan whose cumulative output exceeds `worker.memory_limit`
   fails with `ResourcesExhausted` even when actual resident bytes are tiny,
   and the pool's view never matches real residency, so the limit does not
   bound RSS.
2. the scan channel is bounded at 16 record batches, not bytes
   (`mpsc::channel::<anyhow::Result<RecordBatch>>(16)` at
   `executor.rs:176-177`);
3. shuffle channels are bounded at 64 record batches per partition
   (`DEFAULT_CHANNEL_CAPACITY: usize = 64` at
   `crates/sqe-worker/src/shuffle.rs:81`, one `mpsc::channel(capacity)` of
   `RecordBatch` per partition at `shuffle.rs:105`), not bytes, and the
   module has no spill path;
4. missing or inexact join statistics are treated as zero bytes
   (`estimate_build_side_size` at
   `crates/sqe-planner/src/join_strategy.rs:121-137` returns 0 on a stats
   error or a non-exact `total_byte_size`), and the rule converts to
   sort-merge only when `build_side_size > threshold`
   (`join_strategy.rs:87`), so unknown inputs keep the non-spillable
   `HashJoinExec`.

## Scope

### In scope

- accurate ownership-based memory accounting;
- byte-bounded scan, Flight, and shuffle buffers;
- row-group/byte-range scan morsels and work stealing;
- a reusable, query-scoped local spill framework;
- spillable distributed shuffle;
- safe join behavior for known and unknown input sizes;
- external, radix-partitioned hash join and hash aggregation;
- bounded external sort integration and merge headroom;
- spill quotas, cleanup, observability, and failure behavior;
- optional durable exchange and task-attempt retry;
- zero-pruning and larger-than-memory qualification.

### Not in scope

- replacing DataFusion;
- implementing a general persistent page cache;
- using spill to compensate for an unbounded scan producer;
- coordinator HA in the first six phases;
- caching user table data across security principals;
- treating RustFS as evidence for STS policy enforcement;
- making every complex/holistic aggregate externally spillable in the first
  aggregate MR.

## Architectural rules

1. **Backpressure before spill.** Scan, decode, projection, and result delivery
   are streaming operations. Bound them and slow producers. Do not spill raw
   scan queues merely because the consumer is slow.
2. **Spill blocking state.** Join builds, aggregate states, sort runs, and
   exchange partitions can grow with the input and must spill.
3. **Account ownership, not history.** Bytes are charged only while an SQE
   component owns live buffers. "Bytes processed" is a metric, not a memory
   reservation.
4. **One worker budget.** Arrow batches, encoded Flight data, operator state,
   shuffle, spill I/O buffers, and caches share a worker-level limit.
5. **Protected headroom.** Never grant the full cgroup limit. Reserve memory for
   the Rust runtime, allocator fragmentation, gRPC, credentials, control
   traffic, and spill merge/read buffers.
6. **Partition before exhaustion.** External operators switch at a soft
   watermark, not after an allocation failure.
7. **Immutable spill segments.** A published segment is never modified. It has
   a version, schema fingerprint, length, checksum, and owning attempt.
8. **Fail typed.** Quota, corruption, disk-full, cancellation, and retry errors
   must never abort the worker process.
9. **No secret persistence.** Spill metadata must not contain S3 credentials,
   bearer tokens, signed URLs, or raw worker tickets.
10. **Correctness before adaptive speed.** Until a safe adaptive operator is
    available, unknown/oversized work chooses the slower spillable path.

## Target pipeline

```text
Iceberg snapshot plan
  -> row-group/byte morsels
  -> byte-admitted S3 range fetch
  -> byte-admitted Parquet decode
  -> projection/filter
  -> dense AccountedBatch
  -> Flight encode under byte budget
  -> network backpressure
  -> spillable exchange or bounded downstream operator
  -> partition-wise join/aggregate/sort
```

The scan pipeline's resident memory must depend on configured concurrency and
batch size, not on total table size.

## Configuration shape

Add explicit configuration. Names may be adjusted to existing conventions, but
the concepts and validation are required.

The worker config already has `memory_limit`, `spill_to_disk`, and `spill_dir`
(`crates/sqe-core/src/config.rs:784-789`). `[worker.spill].directory` replaces
`worker.spill_dir` as the single spill root; keep `spill_dir` as a deprecated
alias that maps onto it, and fail validation if both are set to different
paths. Do not ship two spill directory settings.

```toml
[worker.memory]
# Existing worker.memory_limit remains the hard tracked limit.
process_headroom = "2GB"
scan_budget = "2GB"
flight_budget = "1GB"
operator_budget = "6GB"
shuffle_memory_budget = "2GB"
spill_io_budget = "512MB"
budget_granularity = "64KB"

[worker.spill]
enabled = true
backend = "local" # local | s3 | tiered
# Required for local/tiered; omit for s3-only deployments.
directory = "/var/lib/sqe/spill"
max_bytes = "1TB"
min_free_bytes = "20GB"
segment_target_size = "256MB"
compression = "lz4"
max_concurrent_writes = 4
max_concurrent_reads = 4
cleanup_on_start = true
orphan_age = "24h"

[worker.spill.s3]
# Required for s3/tiered; omit for local-only deployments.
# Reuses the engine's existing object_store S3 client; no new dependency.
endpoint = ""
region = ""
bucket = ""
prefix = "sqe-spill/"
max_bytes = "10TB"
max_objects = 1000000
# Dedicated spill credential; never a table-vended STS credential.
access_key = ""
secret_key = ""

[query.execution]
scan_morsel_target_size = "128MB"
scan_morsel_max_size = "256MB"
max_scan_morsels_per_worker = 8
external_join_soft_limit = "1GB"
external_aggregate_soft_limit = "1GB"
external_partition_target_size = "256MB"
max_repartition_depth = 4

[query.exchange]
mode = "local_spill" # memory | local_spill | durable
segment_target_size = "256MB"
durable_uri = ""
```

Validation rules:

- individual sub-budgets must not imply more than the worker tracked limit;
- tracked limit plus process headroom must be below the pod/cgroup limit;
- spill enabled requires at least one usable backend: `backend = "local"` or
  `"tiered"` requires `directory`; `backend = "s3"` or `"tiered"` requires
  `[worker.spill.s3]` bucket and credential; a spill-enabled config with no
  usable backend fails startup;
- spill directory (when configured) must be writable and on an allowed
  explicit path;
- the S3 spill bucket/prefix must be dedicated to spill: reject the
  warehouse/table bucket or any prefix shared with table data;
- S3 spill credentials are distinct from table-vended STS credentials;
- spill quota must exceed at least two segment targets;
- `min_free_bytes` must remain available after startup probes (local
  backend);
- production rejects memory-only exchange for resilient/multi-TB workload
  classes;
- durable exchange credentials are distinct from table-vended credentials.

`[query.exchange].mode` and `[worker.spill].backend` are related but distinct
axes: exchange mode selects memory/local/durable exchange semantics; spill
backend selects where operator and exchange spill segments physically live.
`mode = "durable"` requires the S3 spill backend to be enabled.

## New shared execution primitives

Create a new `crates/sqe-spill` crate (verified absent from the workspace).
Keep it catalog- and network-agnostic.

### Relation to DataFusion's MemoryPool

SQE already runs DataFusion memory management. The worker builds a
`FairSpillPool` sized by `worker.memory_limit`
(`crates/sqe-worker/src/runtime.rs:41`) and wires DataFusion operator spill
through the `DiskManager` at `worker.spill_dir`
(`crates/sqe-worker/src/runtime.rs:22-25`). The coordinator has a
greedy/fair pool choice (`crates/sqe-core/src/config.rs:502-518`) and
pressure-based admission (`crates/sqe-coordinator/src/memory.rs`). A second
independent worker-wide limit would fight this: bytes charged only to
`ByteBudget` would be invisible to DataFusion operators, and the two systems
would together admit more than `worker.memory_limit`.

Decision: the DataFusion `MemoryPool` remains the single source of truth for
the worker tracked limit. Each `ByteBudget` is backed by a `MemoryConsumer`
registered on the worker pool. `acquire` performs `try_grow` (or waits at the
budget's own capacity), and permit `Drop` performs `shrink`. Scan, Flight, and
shuffle buffer bytes therefore appear in pool accounting, and DataFusion
operators see correspondingly less headroom while those buffers are resident.
Sub-budget capacities are validated so their sum stays at or below the pool
limit. DataFusion-managed spill (sort runs) keeps using the `DiskManager`;
point its temp path at a subdirectory of the `SpillManager` root so disk
quota and free-space probes observe both, and treat `DiskManager` files as
observed-but-unaccounted until upstream exposes accounting hooks.

### `ByteBudget`

```rust
pub struct ByteBudget { /* semaphore in fixed-size units + metrics */ }

pub struct BytePermit { /* owned permit; releases on Drop */ }

impl ByteBudget {
    pub async fn acquire(&self, bytes: usize) -> Result<BytePermit>;
    pub fn try_acquire(&self, bytes: usize) -> Result<BytePermit>;
    pub fn capacity_bytes(&self) -> usize;
    pub fn used_bytes(&self) -> usize;
}
```

Use fixed-size accounting units, initially 64 KiB, rather than one semaphore
permit per byte. Round every charge upward. A single request larger than the
budget returns a typed `ItemTooLarge` error instead of waiting forever.

### `Accounted<T>`

```rust
pub struct Accounted<T> {
    value: T,
    permit: BytePermit,
    logical_bytes: usize,
}
```

The permit travels with the owned buffer. Moving `Accounted<T>` between queues
does not double-charge it. Creating an encoded representation acquires a
separate permit until the Arrow input is released.

### `SpillManager`

```rust
pub struct SpillManager { /* backends, quota, I/O gates, registry */ }
pub struct SpillScope { /* query/stage/operator/attempt */ }
pub struct SpillSegment { /* immutable committed descriptor */ }

pub trait SegmentStore: Send + Sync {
    /* write .partial, publish atomically, stream-read by range,
       delete scope, list orphans; local-disk and S3 implementations */
}
```

The storage backend is abstract from the start. `SpillManager` writes and
reads immutable segments through `SegmentStore`; local NVMe and
S3-compatible object storage are the two implementations, selected by
`[worker.spill].backend` (`local`, `s3`, or `tiered`). Every spill consumer
(join build, aggregate state, sort runs, shuffle partitions, durable
exchange) targets the same interface. Tiered mode prefers local disk for
hot, short-lived spill and uses S3 for overflow, durability, or when no
local disk exists; S3 has higher latency and per-request cost, so it is
never the default for latency-sensitive spill when a local disk is
available. Reuse the engine's existing `object_store` client (workspace
dependency, `crates/sqe-worker/Cargo.toml:34`; builder pattern in
`build_object_store_with_creds`, `crates/sqe-worker/src/executor.rs:158`)
and the `StorageConfig` S3 field conventions
(`crates/sqe-core/src/config.rs:1624`); add no new S3 dependency.

Required behavior:

- creates directories/keys only below the configured validated root or
  dedicated bucket/prefix;
- scopes files by opaque query, stage, operator, partition, and attempt IDs;
- writes to an attempt-local `.partial` file (local) or staged key (S3) and
  atomically publishes;
- tracks logical, physical, resident I/O, and quota bytes;
- refuses writes before violating quota: `max_bytes`/`min_free_bytes` on
  local, byte and object-count budgets on S3 (no free-space probe exists
  there);
- supports cancellation;
- deletes scope contents on successful completion or terminal failure, on
  both backends;
- cleans abandoned attempts older than the configured age at startup
  (directory scan locally, prefix listing plus lifecycle tags on S3);
- never follows symlinks out of the configured root (local);
- refuses a bucket/prefix shared with table data (S3 analogue of the
  unsafe-broad-path guard);
- uses restrictive permissions locally and server-side encryption per
  deployment policy on S3;
- streams reads on both backends: local sequential reads, S3 range GETs;
  never a full-object GET of a segment;
- exposes a test-only deterministic fault injector.

### Spill segment format

Use Arrow IPC initially:

```text
magic + version
schema fingerprint
query/stage/operator/partition/attempt identifiers
record batch IPC payloads
logical row and byte counts
CRC32C per payload
whole-segment checksum
commit trailer
```

The reader rejects unknown versions, schema mismatch, truncation, checksum
failure, and an attempt mismatch. It must stream batches and never materialize
the whole segment.

## Delivery phases

Each phase is one MR unless explicitly divided. Do not begin Phase 4 until
Phases 1-3 pass their larger-than-memory gates.

---

## Phase 0: Reproducers, metrics, and red safety gates

**Outcome:** We can reproduce the current unsafe behavior and measure resident
bytes at every boundary before changing implementation.

**Files:**

- Create: `crates/sqe-worker/tests/zero_pruning_memory.rs`
- Create: `crates/sqe-worker/tests/slow_consumer.rs`
- Modify: `crates/sqe-worker/src/executor.rs`
- Modify: `crates/sqe-worker/src/flight_service.rs`
- Modify: `crates/sqe-worker/src/shuffle.rs`
- Modify: `crates/sqe-metrics/src/lib.rs` or the existing worker metric modules
- Modify: benchmark harness files under `crates/sqe-bench/`

- [x] Add a generated Parquet fixture that is at least 20 times the configured
      worker memory limit without requiring a 20-times-RAM test host. Generate
      it incrementally in a temporary directory.
- [x] Add a no-filter, no-pruning projected scan test with a 64 MiB worker
      memory limit.
- [x] Add a slow Flight consumer test that pauses between batches and records
      peak tracked bytes/RSS.
- [x] Add a wide-variable-length batch test so batch-count bounds visibly fail
      to bound bytes.
- [x] Add a shuffle test whose input is at least 10 times the configured
      shuffle memory budget.
- [x] Add an unknown-statistics join plan test proving the current plan keeps
      `HashJoinExec`.
- [x] Add metrics:
      `scan_fetch_resident_bytes`, `scan_decode_resident_bytes`,
      `scan_queue_resident_bytes`, `flight_encode_resident_bytes`,
      `flight_inflight_bytes`, `shuffle_resident_bytes`,
      `operator_resident_bytes`, `spill_bytes_written`,
      `spill_bytes_read`, `spill_files`, `spill_failures`,
      and `memory_backpressure_seconds`.
- [x] Record a baseline JSON artifact with wall time, bytes, peak RSS, tracked
      peak, and failure reason.
- [x] Keep the tests ignored or marked as expected failure only until their
      owning phase turns them green. Add a tracking comment and the exact
      command, e.g.
      `cargo test -p sqe-worker --test zero_pruning_memory -- --ignored`.
- [x] Keep every Phase 0 test runnable on a laptop: local temp-dir Parquet
      plus a `file://` or local object store, no Polaris, no N-times-RAM
      host. The larger-than-memory illusion comes from the 64 MiB configured
      limit, not from real RAM exhaustion.

**Gate (all objectively checkable):**

- the zero-pruning scan test fails with a `ResourcesExhausted` error caused
  by the cumulative fragment reservation (`executor.rs:210`/`executor.rs:327`)
  once cumulative batch bytes exceed the 64 MiB limit;
- the wide-batch slow-consumer test records queue-resident bytes exceeding
  4 times the narrow-batch case at the same 16-batch bound;
- the unknown-statistics join test asserts the physical plan still contains
  `HashJoinExec`;
- the baseline JSON artifact exists and records wall time, bytes, peak RSS,
  tracked peak, and failure reason for each reproducer.

---

## Phase 1: Correct scan ownership and byte backpressure

**Outcome:** A streaming scan can read arbitrarily more data than worker RAM
while keeping resident scan and Flight buffers within configured budgets.

**Files:**

- Create: `crates/sqe-spill/src/budget.rs`
- Create: `crates/sqe-spill/src/accounted.rs`
- Create: `crates/sqe-spill/src/lib.rs`
- Modify: workspace `Cargo.toml`
- Modify: `crates/sqe-worker/Cargo.toml`
- Modify: `crates/sqe-worker/src/executor.rs`
- Modify: `crates/sqe-worker/src/flight_service.rs`
- Modify: `crates/sqe-worker/src/runtime.rs`
- Modify: `crates/sqe-core/src/config.rs`
- Modify: configuration documentation and examples

- [x] Implement and unit-test `ByteBudget`, including rounding, cancellation,
      oversized items, fairness, and permit release on error/panic unwind.
- [x] Implement `Accounted<T>` and tests proving moves do not change the
      charge and `Drop` releases it exactly once.
- [x] Remove the fragment-lifetime cumulative `reservation.try_grow(...)`
      behavior from `execute_scan_streaming`.
- [x] Charge fetched compressed bytes only while the fetch buffer is resident.
- [x] Charge decoded Arrow batches by `get_array_memory_size()` while owned by
      the worker pipeline.
- [x] Replace `mpsc<RecordBatch>(16)` with a byte-admitted
      `mpsc<Accounted<RecordBatch>>`; keep a small item cap only as a secondary
      scheduling guard.
- [x] Implement an `AccountedFlightStream` or equivalent encoder ownership
      wrapper. It must retain the Arrow permit while encoding and charge
      encoded `FlightData` until gRPC releases/sends it.
      (This box was ticked when only the first half shipped: `AccountedEncodeStream`
      held the Arrow permit, but encoded `FlightData` was written to a GAUGE that
      nothing gated on, so the encoded copy stayed unbounded under concurrency.
      Audit #407 found the gap; `accounted_frame_stream` closes it. A half-done
      item reads exactly like a done one on a checklist.)
- [x] Ensure cancellation or client disconnect drops queued and encoding
      permits immediately.
- [ ] Keep decode concurrency and byte admission separate: concurrency protects
      CPU; byte admission protects RAM.
- [ ] Add process-headroom startup validation against cgroup/container memory
      where available.
- [x] Add a slow-consumer integration test with wide rows.
- [x] Turn the Phase 0 zero-pruning scan test green.

**Important implementation note:** Do not release an Arrow permit merely when
the batch is removed from the scan queue. Ownership has moved to the encoder,
not disappeared. Release it after the encoder no longer owns the batch.

**Scope note (two scan paths):** SQE has two scan paths. The
embedded/coordinator path goes through the vendored `IcebergTableScan` and
is already byte-admitted by `ScanDecodeGate`
(`crates/sqe-catalog/src/scan_memory.rs`): per-scan-node decode semaphore
plus fail-fast `try_grow` pool reservations. Phase 1 targets the worker
distributed `ScanTask` path (`crates/sqe-worker/src/executor.rs`), which has
neither. Reuse the `ScanDecodeGate` pattern and keep the two paths from
double-charging the same bytes when a query crosses both.

**Gate:**

- a scan of at least 20 times the worker memory limit completes;
- peak tracked scan+Flight bytes stay within budget plus one accounting unit;
- RSS stays below the documented process headroom;
- pausing the client for 30 seconds caps additional fetched-plus-decoded
  bytes at `scan_budget + flight_budget` plus one in-flight batch; no further
  S3 GETs are issued once those budgets are full;
- cancellation returns all byte permits (pool used bytes return to the
  pre-query value).

---

## Phase 2: Row-group morsels and adaptive scan scheduling

**Outcome:** Full scans use all workers and CPUs without assigning huge files or
static file groups as indivisible work.

**Files:**

- Modify: `crates/sqe-planner/src/scan_task.rs`
- Create or modify: `crates/sqe-planner/src/scan_morsel.rs`
- Modify: `crates/sqe-coordinator/src/query_handler.rs`
- Modify: `crates/sqe-coordinator/src/scheduler.rs`
- Modify: `crates/sqe-coordinator/src/distributed_scan.rs`
- Modify: `crates/sqe-worker/src/executor.rs`
- Modify: coordinator/worker codec and ticket tests

Add a versioned scan unit:

```rust
pub struct ScanMorsel {
    pub morsel_id: String,
    pub file_path: String,
    pub file_size_bytes: u64,
    pub row_group_start: u32,
    pub row_group_end: u32,
    pub compressed_bytes_estimate: u64,
    pub decoded_bytes_estimate: u64,
}
```

- [x] Extend scan planning to read Parquet footer row-group offsets without
      reading data pages.
- [x] Group adjacent row groups into 64-128 MiB target morsels, capped at the
      configured maximum.
- [x] Preserve Iceberg snapshot ID, deletes, projection, predicate, field IDs,
      and credential scope in the signed task.
- [x] Version the signed `ScanTask` encoding and reject unsupported versions.
- [x] Replace the `max_bins = num_workers * 3` static binning
      (`crates/sqe-coordinator/src/query_handler.rs:3103`) with a larger
      pending morsel queue.
- [x] Align with the existing split machinery instead of inventing a second
      one. The vendored fork already splits large whole-file tasks into
      byte-range subtasks that resolve to row groups at read time
      (`TableScanBuilder::with_task_split_target_size`,
      `vendor/iceberg-rust/crates/iceberg/src/scan/mod.rs:148-155`;
      `split_file_scan_task` in
      `vendor/iceberg-rust/crates/iceberg/src/arrow/reader.rs:397-433`), and
      SQE enables it on the embedded path
      (`crates/sqe-catalog/src/iceberg_scan.rs:1394`,
      `DEFAULT_SCAN_SPLIT_TARGET_SIZE`). A `ScanMorsel` may therefore carry a
      byte range rather than explicit row-group indices; the distributed
      `ScanTask` ticket is what lacks ranges today.
- [ ] Start with coordinator push scheduling, then add worker pull/lease if the
      existing Flight protocol makes pull practical.
- [ ] Limit active morsels from live worker byte pressure, CPU, spill backlog,
      and outbound Flight bytes.
- [ ] Retry an individual morsel on another worker using its stable morsel ID.
- [ ] Deduplicate duplicate attempt output at the coordinator.
- [ ] Add work-stealing tests with one slow worker and one large file.
- [ ] Add delete-aware row-group correctness tests. The hedge is confirmed
      against current code: SQE distributes byte-range splits only for
      delete-free scans (`use_split_assignment = total_partitions > 1 &&
      !use_direct && !has_deletes` at
      `crates/sqe-catalog/src/iceberg_scan.rs:1063`). Preserve that gate: if
      the fork cannot safely restrict deletes to a sub-file morsel, retain
      file-level morsels for that table rather than weakening correctness.

**Gate:**

- one multi-gigabyte Parquet file uses multiple workers/cores;
- each row is returned exactly once under retry;
- with one worker throttled to one tenth speed on a scan of at least 16
  morsels, query wall time stays within 1.5 times the balanced-cluster run
  (work stealing moves morsels off the slow worker);
- memory remains within the Phase 1 bound.

---

## Phase 3: Reusable local spill substrate

**Outcome:** SQE can write, stream-read, account, corrupt-detect, and reliably
clean immutable Arrow spill segments under a hard disk quota.

**Files:**

- Create: `crates/sqe-spill/src/manager.rs`
- Create: `crates/sqe-spill/src/scope.rs`
- Create: `crates/sqe-spill/src/segment.rs`
- Create: `crates/sqe-spill/src/store.rs` (`SegmentStore` trait)
- Create: `crates/sqe-spill/src/store_local.rs`
- Create: `crates/sqe-spill/src/store_s3.rs` (may land as its own MR; must
  land before Phase 8)
- Create: `crates/sqe-spill/src/format.rs`
- Create: `crates/sqe-spill/src/quota.rs`
- Create: `crates/sqe-spill/src/fault.rs`
- Modify: `crates/sqe-worker/src/bootstrap.rs`
- Modify: `crates/sqe-core/src/config.rs`
- Add unit and integration tests under `crates/sqe-spill/tests/`

- [x] Define the `SegmentStore` trait and route every manager operation
      through it. Operator spill and durable exchange share this one
      abstraction; Phase 8 adds semantics on top, never a second backend
      interface.
- [x] Implement the local backend: validated spill-root creation and
      restrictive permissions.
- [x] Implement the S3 backend on the existing `object_store` client with a
      dedicated bucket/prefix, dedicated credential, staged-key publish, and
      range-read streaming. Reject table-vended STS credentials and any
      prefix shared with table data.
- [ ] Implement tiered mode: write local first; route to S3 on local quota
      or free-space pressure, or when no local backend is configured.
- [x] Implement query/stage/operator/partition/attempt scopes.
- [x] Implement quota reservation before writing, branched per backend:
      `max_bytes`/`min_free_bytes` locally, byte plus object-count budgets
      on S3.
- [ ] Implement asynchronous IPC segment writer with per-batch and whole-file
      checksums.
- [x] Publish through `.partial` plus atomic rename.
- [ ] Implement a streaming reader bounded by `spill_io_budget`.
- [x] Add write/read semaphores and cancellation.
- [x] Implement cleanup guards for normal completion, error, panic unwind, and
      process restart.
- [x] Add startup orphan cleanup without touching recent/live attempts:
      directory scan locally, prefix listing on S3, plus lifecycle tags so
      bucket expiry policies reap anything cleanup misses.
- [x] Add fault injection: short write, ENOSPC, read error, corruption, slow
      disk, cancellation, and rename failure; for S3 add throttling (429/503),
      partial upload, and timeout.
- [x] Assert in tests that no segment header, footer, or filename embeds a
      `ScanTask` or any of its S3 credential fields. Scan tickets carry live
      credentials (`crates/sqe-planner/src/scan_task.rs:36-41`); persisting
      one to disk is a credential leak.
- [ ] Add metrics and structured spill lifecycle tracing.
- [ ] Document capacity planning and Kubernetes ephemeral-volume requirements.

**Gate (run once per enabled backend: local, S3 against a local MinIO/RustFS,
and tiered):**

- round-trip preserves schema, ordering within a segment, rows, and nulls;
- corruption and truncation fail typed;
- quota is never exceeded (local bytes/free-space; S3 bytes/object count);
- cancellation leaves no published or partial orphan on either backend;
- reading a segment never loads the whole segment into memory; the S3 reader
  issues range GETs only;
- a local-only config with no S3 keys and an s3-only config with no
  `directory` both start and spill; a spill-enabled config with neither
  backend fails startup with a typed config error.

---

## Phase 4: Spillable distributed shuffle

**Outcome:** Exchange at least 10 times aggregate worker RAM completes through
the configured spill backend with bounded receiver memory.

**Files:**

- Modify: `crates/sqe-worker/src/shuffle.rs`
- Modify: `crates/sqe-worker/src/flight_service.rs`
- Modify: `crates/sqe-planner/src/shuffle_exec.rs`
- Modify: `crates/sqe-planner/src/stage_planner.rs`
- Modify: `crates/sqe-planner/src/distributed_join.rs` and
  `crates/sqe-planner/src/distributed_sort.rs` (stage orchestration lives in
  these exec nodes plus `stage_planner.rs`, not in a separate coordinator
  scheduler module)
- Add: shuffle spill integration and chaos tests

**Dependency:** requires the Phase 3 `SpillManager`, segment format, and
fault injector. Do not fork a shuffle-private spill writer.

Replace each `mpsc<RecordBatch>` partition with a stateful buffer:

```text
Open
  append accounted memory batch
  spill immutable segment at soft watermark
  expose memory batches and committed segments to reader
Finished
  reader drains remaining data and verifies counts/checksums
Failed
  reader receives the original typed failure
Cancelled
  all memory and segments released
```

- [x] Add query, stage, partition, producer task, and attempt IDs to exchange
      descriptors.
- [x] Implement `SpillablePartitionBuffer`.
- [x] Hash/range partition one bounded input batch at a time.
- [x] Avoid constructing all output partition batches simultaneously when the
      total would exceed the budget; process partition IDs in bounded groups.
- [x] Spill at the soft watermark before a hard allocation failure.
- [x] Stream committed spill segments to the downstream reader.
- [x] Define completion manifests with rows, batches, logical bytes, physical
      bytes, and checksums.
- [x] Reject late data from a losing/obsolete task attempt.
- [x] Propagate downstream cancellation to DoExchange intake and spill writers.
- [x] Protect spill-read/merge headroom from scan and shuffle writers.
- [x] Expose per-partition skew, resident bytes, spill bytes, and blocked time.
- [x] Test multiple concurrent producers and one slow consumer.
- [x] Test worker shutdown, disk-full, corrupted segment, duplicate attempt,
      and cancellation.

**Gate:**

- exchange volume at least 10 times aggregate worker RAM completes;
- receiver memory stays within `shuffle_memory_budget`;
- no rows are lost or duplicated;
- no spill files remain after completion/cancellation/failure;
- a disk-full event fails only affected queries, not the worker process.

---

## Phase 5: Memory-adaptive joins

**Outcome:** No equi-join can OOM merely because estimates are missing or wrong.

**Files:**

- Modify: `crates/sqe-planner/src/join_strategy.rs`
- Modify: `crates/sqe-planner/src/distributed_join.rs`
- Create: `crates/sqe-planner/src/grace_hash_join.rs` or a dedicated execution
  module/crate if planner dependencies become inappropriate
- Modify: coordinator plan/profile reporting
- Add: skew and unknown-statistics tests

**DataFusion 54 reality (verified in the vendored workspace's
`datafusion-physical-plan-54.0.0` source):** sort-merge join spills its
buffered state (spill handling across `src/joins/sort_merge_join/`), so the
5a fallback leans on an existing spill path. The hash join build side has no
spill path (`src/joins/hash_join/` contains no `SpillManager`), so building
the Grace/radix join in 5b is warranted, not duplication.

### Phase 5a: Immediate safe fallback

- [x] Change unknown build-side statistics from "zero/keep hash" to
      "unknown/choose spillable".
- [x] Keep an explicit small-known-build exception.
- [x] Use existing spillable sort-merge fallback until Grace hash is ready.
- [x] Add tests for absent, inexact, zero, and underestimated statistics.

This is deliberately conservative and can ship before the adaptive join.

### Phase 5b: Grace/radix hash join

**Dependency note:** the negotiating governor ships in Phase 7. Until then,
"the worker governor" means a fixed per-operator grant carved from
`operator_budget` via `ByteBudget`. Define the `ReclaimableConsumer` trait in
`sqe-spill` during this phase so 5b registrations upgrade to negotiated
grants in Phase 7 without an interface change.

- [x] Register desired and minimum memory with the worker governor.
- [x] Begin in-memory build only under an explicit grant.
- [x] At the soft watermark (`external_join_soft_limit`), partition build and
      probe by unused hash bits.
- [x] Keep fitting partitions resident and spill only excess partitions.
- [x] Join one partition pair at a time and release it immediately.
- [x] Recursively repartition a partition that still exceeds its grant.
- [x] Detect heavy hitters/skew and isolate them rather than recursively hashing
      forever.
- [x] Cap recursion and fall back to sort-merge for pathological partitions.
- [x] Support inner/semi/anti first; add outer joins only with explicit matched
      state and correctness tests.
- [ ] Preserve null-equality and join-filter semantics.
- [x] Profile chosen strategy, estimate, observed build bytes, partitions,
      recursion depth, skew, and spill.

**Gate:**

- a build side 10 times the join grant completes;
- deliberately wrong/absent estimates cannot cause OOM;
- skewed-key tests terminate within the recursion cap;
- results match the non-spillable reference for every supported join type.

---

## Phase 6: External hash aggregation and bounded sort

**Outcome:** High-cardinality grouping and global sorting complete beyond RAM.

**Files:**

- Modify: `crates/sqe-planner/src/distributed_aggregate.rs`
- Create: external aggregation execution module
- Modify: `crates/sqe-planner/src/distributed_sort.rs`
- Modify: worker runtime/governor registration
- Add: aggregate-state compatibility and large-cardinality tests

### External aggregation

**DataFusion 54 reality:** grouped hash aggregation already spills.
`GroupedHashAggregateStream` carries a `SpillState` with DataFusion's own
`SpillManager` and merges sorted spill files
(`datafusion-physical-plan-54.0.0/src/aggregates/row_hash.rs:78-107`). The
known gap is emission behavior, not absence of spill: DataFusion 53/54's
partial aggregate cannot emit early under a constant GROUP BY ordering key,
surfacing raw `ResourcesExhausted` (documented at
`crates/sqe-core/src/config.rs:507-516`). Order of work: first configure and
gate DataFusion's existing aggregate spill under the common governor; build
the radix-partitioned external aggregation below only for the cases where
the DataFusion path fails its larger-than-memory gate.

- [x] Run the Phase 6 aggregate gate against DataFusion's built-in spill
      first and record which cases pass; skip custom work for those cases.
- [x] Use small thread/task-local pre-aggregation tables with a fixed grant.
- [x] Flush partial tuples into radix-partitioned spill pages/segments when the
      table reaches its soft watermark (`external_aggregate_soft_limit`).
- [x] Unpin/release flushed state immediately.
- [x] Over-partition so active final partitions fit under concurrent grants.
- [x] Combine one partition at a time, emit results, and release it.
- [x] Recursively repartition oversized partitions using additional hash bits.
- [x] Define supported decomposable aggregate states explicitly.
- [x] Route unsupported holistic/variable states to a safe sort-based path or
      return a typed unsupported-at-this-budget error; never attempt unbounded
      memory.
- [x] Test `COUNT`, `SUM`, `MIN/MAX`, `AVG`, distinct, nulls, decimals, strings,
      and multi-column keys.

### Sort

- [ ] Verify DataFusion sort run creation is charged to the common governor.
- [ ] Reproduce and gate the known merge-phase failure: sort-on-write CTAS at
      SF10 hard-OOMs because merge reservations go through
      `ExternalSorterMerge` with `can_spill=false` (see
      `docs/internal/plans/2026-06-21-memory-safety-oom-prevention.md`).
      DataFusion 54 still splits the consumers this way: the sort consumer
      is created `.with_can_spill(true)` while the separate
      `ExternalSorterMerge[{partition_id}]` consumer is not
      (`datafusion-physical-plan-54.0.0/src/sorts/sort.rs:284-288`). The fix
      must degrade to a typed error or a capped-fan-in merge, never an OS
      OOM.
- [ ] Reserve protected merge buffers before admitting sort work.
- [ ] Cap merge fan-in and recursively merge when needed.
- [ ] Account both Arrow input and encoded spill buffers during run creation.
- [ ] Test global sort, window sort, distributed range sort, and cancellation.

**Gate:**

- unique group count producing at least 10 times the aggregate grant completes;
- a global sort 10 times the sort grant completes;
- memory remains bounded through final merge/combine;
- supported results match the in-memory reference.

---

## Phase 7: DuckDB-style temporary memory governor

**Outcome:** Concurrent blocking operators negotiate memory instead of racing
until one fails.

**Files:**

- Create: `crates/sqe-spill/src/governor.rs`
- Modify: worker runtime/bootstrap
- Modify: spillable join/aggregate/sort/shuffle registrations
- Modify: admission and metrics

Interface:

```rust
pub trait ReclaimableConsumer: Send + Sync {
    fn minimum_bytes(&self) -> usize;
    fn desired_bytes(&self) -> usize;
    fn current_bytes(&self) -> usize;
    async fn set_grant(&self, bytes: usize) -> Result<()>;
    async fn reclaim(&self, target_bytes: usize) -> Result<usize>;
}
```

- [x] Reconcile with the existing machinery instead of stacking on it. The
      worker pool is a `FairSpillPool` today
      (`crates/sqe-worker/src/runtime.rs:41`), and dividing the pool by
      registered consumers already caused the documented TPC-DS q39 pool/N
      pathology (`crates/sqe-core/src/config.rs:507-516`). The governor
      replaces fair division: switch the worker pool to greedy-with-tracking
      and let the governor own grant arbitration. Coordinator pressure-based
      admission (`crates/sqe-coordinator/src/memory.rs`) and
      `query.per_user_memory_budget` stay as the cross-query layer above it.
- [x] Register every blocking consumer by query and workload class.
- [x] Guarantee minimum viable grants only when total minima fit.
- [x] Distribute remaining memory using weighted fair shares.
- [x] Reduce grants at a soft process/worker watermark.
- [x] Trigger asynchronous spill/repartition and wait for reclaimed bytes
      before admitting new large work.
- [x] Preserve spill read/merge and control-plane headroom.
- [x] Reject admission before execution when summed minima cannot fit.
- [x] Prevent one query with many plan nodes from claiming all grants.
- [x] Add concurrency tests with simultaneous joins, aggregates, sorts, and
      shuffle.

**Gate:** Four concurrent larger-than-memory queries complete or are fairly
queued; none is killed by another operator's allocation race.

---

## Phase 8: Durable exchange and task retry

**Outcome:** A worker loss does not discard successful upstream work for
resilient multi-TB queries.

**Files:**

- Modify: `crates/sqe-spill/src/store_s3.rs` (durable-exchange semantics on
  the existing S3 `SegmentStore` backend from Phase 3)
- Modify: `crates/sqe-planner/src/stage_planner.rs` and
  `crates/sqe-coordinator/src/query_tracker.rs` (stage/attempt state)
- Modify: shuffle writer/reader manifests
- Modify: credential configuration in `crates/sqe-core/src/config.rs`
- Add: worker-kill and object-store fault tests

**Dependency:** the S3 `SegmentStore` backend from Phase 3 is the storage
layer. Phase 8 adds durable-exchange semantics on top of it: attempt
manifests, winner commit, and segment reuse. It does not introduce a second
object-store backend or segment format.

- [x] Reuse the Phase 3 `SegmentStore` abstraction; extend, never fork it.
- [x] Use a dedicated exchange bucket/prefix and credential, not a table-vended
      credential (this generalizes the Phase 3 S3-spill rule; exchange may use
      its own prefix under the spill bucket or a separate bucket).
- [x] Publish task-attempt manifests atomically after every segment is durable.
- [x] Commit one winning attempt per task.
- [x] Reuse completed upstream segments on retry.
- [x] Reject losing-attempt output.
- [ ] Add lifecycle tags and object-store expiry policies.
- [ ] Encrypt transport and server-side objects according to deployment policy.
- [ ] Persist enough coordinator stage state to retry workers during the
      coordinator's lifetime. Coordinator restart recovery remains a later
      phase unless included explicitly.
- [x] Test worker kill during write, after publish, and during downstream read.
- [ ] Test throttling, partial upload, timeout, and duplicate completion.

**Gate:** Killing one worker after an upstream stage completes retries only the
lost task/stage work and returns correct results.

---

## Phase 9: Qualification and default-on rollout

**Outcome:** Bounded memory and spill are production defaults for SF100 and
multi-TB workload classes.

- [ ] Run SF10 regression first: correctness, wall time, CPU, bytes, RSS, spill.
- [ ] Run SF100 on separate coordinator/worker hosts.
- [ ] Add synthetic zero-pruning scans at 20, 100, and 500 times worker RAM.
- [ ] Run broadcast, repartition join, skew join, high-cardinality aggregate,
      global sort, window, DML, and compaction cases.
- [ ] Run concurrency at 1, 4, 8, and overload.
- [ ] Inject slow client, slow worker, worker kill, S3 429/503, credential
      expiry, disk-full, corrupt spill segment, and cancellation.
- [ ] Compare spill disabled/enabled for in-memory workloads to cap regression.
- [ ] Compare SQE with DuckDB locally and Trino distributed on identical
      Parquet/Iceberg data where semantics overlap.
- [ ] Enable scan byte accounting by default first.
- [ ] Enable local spill exchange for batch workloads after its gate.
- [ ] Enable adaptive joins/aggregates after per-operator compatibility gates.
- [ ] Retain feature flags for one release rollback window.
- [ ] Publish documented sizing guidance and alerts.

## Test matrix

| Area | Small/in-memory | Larger than memory | Failure |
|---|---|---|---|
| Scan | narrow/wide, filter/no filter | 20-500x RAM | slow consumer, cancel, S3 error |
| Flight | compressions, variable batches | output 20x budget | disconnect, timeout |
| Shuffle | hash/range, skew | 10x aggregate RAM | disk-full, corrupt, producer loss |
| Join | all supported semantics | build 10x grant | unknown stats, heavy hitter |
| Aggregate | supported states | groups 10x grant | unsupported state, corrupt segment |
| Sort/window | local/distributed | input 10x grant | merge cancellation, disk-full |
| Retry | no failure baseline | durable exchange | kill before/after publish |

## Required metrics and profile fields

### Memory

- worker tracked limit, current, and peak;
- process RSS and cgroup limit;
- scan fetch/decode/queue bytes;
- Flight encode/in-flight bytes;
- operator state by type;
- shuffle resident bytes;
- spill I/O resident bytes;
- governor desired/granted/current bytes;
- backpressure and reclamation time.

### Spill

- bytes and files written/read/deleted;
- logical-to-physical compression ratio;
- write/read duration and throughput;
- partition count and recursion depth;
- quota used/free and disk free (local); byte/object budget used and request
  counts (S3);
- checksum, quota, ENOSPC, throttling, cleanup, and retry failures;
- orphan cleanup count/bytes per backend.

### Scheduling

- morsels planned/completed/retried/stolen;
- compressed/decoded estimates versus actual;
- worker skew and straggler time;
- task attempts and winning attempt;
- exchange reused versus recomputed bytes.

## Operational safeguards

- Put local spill on dedicated NVMe or a dedicated Kubernetes ephemeral
  volume, not the container root filesystem.
- Put S3 spill in a dedicated bucket or prefix with a lifecycle expiry
  policy as the cleanup backstop; never share the warehouse bucket.
- Prefer local disk for hot, short-lived spill in tiered mode; S3 pays
  request latency and per-request cost, so it serves overflow, durability,
  and diskless pods.
- Alert at spill quota 70/85/95 percent and `min_free_bytes` approach
  (local), and at byte/object-budget 70/85/95 percent plus sustained 429/503
  throttling (S3).
- Protect the worker from pod eviction by including expected ephemeral storage
  requests/limits.
- Remove spill contents through scope cleanup, never a broad recursive deletion
  of an unresolved path.
- Refuse startup when the configured spill root is `/`, a workspace root, home,
  or another unsafe broad directory.
- Scrub query SQL, credentials, and user identifiers from spill filenames.
- Keep only opaque IDs in paths; put safe diagnostics in structured metrics.
- Rate-limit concurrent spill I/O so NVMe saturation does not starve control and
  scan reads.
- Reject new batch work when disk pressure is red; allow running queries enough
  reserved capacity to finish or cleanly fail.

## Definition of done

The program is complete when all of the following are true:

1. A zero-pruning scan reads at least 100 times worker memory with flat bounded
   resident memory.
2. A slow client propagates backpressure to the S3 reader.
3. No queue uses record-batch count as its only memory bound.
4. Exchange at least 10 times aggregate RAM completes through spill.
5. Unknown or underestimated joins cannot select an unbounded hash build.
6. High-cardinality aggregation and global sort complete at 10 times their
   grants.
7. Four concurrent large queries are fairly admitted/governed.
8. Disk-full, corruption, S3 errors, cancellation, and credential refresh fail
   or recover without killing a worker.
9. Worker loss in resilient mode retries from durable exchange.
10. All spill and exchange objects are cleaned or lifecycle-expired.
11. SF10 has no correctness regression and documented acceptable latency
    overhead.
12. SF100 completes on separate hosts with query profiles proving byte bounds,
    spill, retry, and cleanup.

## Recommended first three MRs

Start here:

1. **MR 1: scan accounting and byte backpressure**
   - Phase 0 scan reproducers;
   - `ByteBudget` and `Accounted<T>`;
   - fix cumulative scan reservation;
   - byte-bound scan-to-Flight;
   - make the 20-times-memory scan green.
2. **MR 2: spill substrate**
   - query-scoped local spill manager;
   - immutable Arrow IPC segments;
   - quota, checksums, cancellation, cleanup, and fault injection.
3. **MR 3: spillable shuffle**
   - replace batch-count partition channels;
   - bounded memory plus local segment spill;
   - make the 10-times-RAM exchange test green.

Only after these are stable should the team implement adaptive Grace join and
external aggregation. This order fixes the existing correctness/capacity risks
and creates one tested spill substrate that every blocking operator can reuse.
