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
distributed exchange spill partitioned state to NVMe. Resilient execution can
persist completed exchange output to object storage.

**Reference design:** DuckDB-style streaming vector pipelines, one memory
governor for temporary state, spillable pages/segments, radix partitioning, and
partition-wise completion; Trino-style distributed stage exchange and task
retry; DataFusion/Arrow remain SQE's execution foundation.

**Tech stack:** Rust, Tokio, Arrow, Arrow IPC, Arrow Flight, DataFusion,
iceberg-rust, local NVMe, optional S3-compatible durable exchange.

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

The current code has four concrete unsafe boundaries:

1. `sqe-worker::executor::execute_scan_streaming` grows one fragment reservation
   for every emitted batch and does not shrink it after consumption.
2. the scan channel is bounded at 16 record batches, not bytes;
3. shuffle channels are bounded at 64 record batches per partition, not bytes,
   and have no spill path;
4. missing join statistics are treated as zero bytes, preserving a
   non-spillable hash join.

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
directory = "/var/lib/sqe/spill"
max_bytes = "1TB"
min_free_bytes = "20GB"
segment_target_size = "256MB"
compression = "lz4"
max_concurrent_writes = 4
max_concurrent_reads = 4
cleanup_on_start = true
orphan_age = "24h"

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
- spill directory must be writable and on an allowed explicit path;
- spill quota must exceed at least two segment targets;
- `min_free_bytes` must remain available after startup probes;
- production rejects memory-only exchange for resilient/multi-TB workload
  classes;
- durable exchange credentials are distinct from table-vended credentials.

## New shared execution primitives

Create a new `crates/sqe-spill` crate. Keep it catalog- and network-agnostic.

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
pub struct SpillManager { /* root, quota, I/O gates, registry */ }
pub struct SpillScope { /* query/stage/operator/attempt */ }
pub struct SpillSegment { /* immutable committed descriptor */ }
```

Required behavior:

- creates directories only below the configured validated root;
- scopes files by opaque query, stage, operator, partition, and attempt IDs;
- writes to an attempt-local `.partial` file and atomically publishes;
- tracks logical, physical, resident I/O, and quota bytes;
- refuses writes before violating quota or `min_free_bytes`;
- supports cancellation;
- deletes scope contents on successful completion or terminal failure;
- cleans abandoned attempts older than the configured age at startup;
- never follows symlinks out of the configured root;
- uses restrictive permissions;
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

- [ ] Add a generated Parquet fixture that is at least 20 times the configured
      worker memory limit without requiring a 20-times-RAM test host. Generate
      it incrementally in a temporary directory.
- [ ] Add a no-filter, no-pruning projected scan test with a 64 MiB worker
      memory limit.
- [ ] Add a slow Flight consumer test that pauses between batches and records
      peak tracked bytes/RSS.
- [ ] Add a wide-variable-length batch test so batch-count bounds visibly fail
      to bound bytes.
- [ ] Add a shuffle test whose input is at least 10 times the configured
      shuffle memory budget.
- [ ] Add an unknown-statistics join plan test proving the current plan keeps
      `HashJoinExec`.
- [ ] Add metrics:
      `scan_fetch_resident_bytes`, `scan_decode_resident_bytes`,
      `scan_queue_resident_bytes`, `flight_encode_resident_bytes`,
      `flight_inflight_bytes`, `shuffle_resident_bytes`,
      `operator_resident_bytes`, `spill_bytes_written`,
      `spill_bytes_read`, `spill_files`, `spill_failures`,
      and `memory_backpressure_seconds`.
- [ ] Record a baseline JSON artifact with wall time, bytes, peak RSS, tracked
      peak, and failure reason.
- [ ] Keep the tests ignored or marked as expected failure only until their
      owning phase turns them green. Add a tracking comment and exact command.

**Gate:** Baselines reliably reproduce cumulative scan reservation growth and
batch-count queue variability.

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

- [ ] Implement and unit-test `ByteBudget`, including rounding, cancellation,
      oversized items, fairness, and permit release on error/panic unwind.
- [ ] Implement `Accounted<T>` and tests proving moves do not change the
      charge and `Drop` releases it exactly once.
- [ ] Remove the fragment-lifetime cumulative `reservation.try_grow(...)`
      behavior from `execute_scan_streaming`.
- [ ] Charge fetched compressed bytes only while the fetch buffer is resident.
- [ ] Charge decoded Arrow batches by `get_array_memory_size()` while owned by
      the worker pipeline.
- [ ] Replace `mpsc<RecordBatch>(16)` with a byte-admitted
      `mpsc<Accounted<RecordBatch>>`; keep a small item cap only as a secondary
      scheduling guard.
- [ ] Implement an `AccountedFlightStream` or equivalent encoder ownership
      wrapper. It must retain the Arrow permit while encoding and charge
      encoded `FlightData` until gRPC releases/sends it.
- [ ] Ensure cancellation or client disconnect drops queued and encoding
      permits immediately.
- [ ] Keep decode concurrency and byte admission separate: concurrency protects
      CPU; byte admission protects RAM.
- [ ] Add process-headroom startup validation against cgroup/container memory
      where available.
- [ ] Add a slow-consumer integration test with wide rows.
- [ ] Turn the Phase 0 zero-pruning scan test green.

**Important implementation note:** Do not release an Arrow permit merely when
the batch is removed from the scan queue. Ownership has moved to the encoder,
not disappeared. Release it after the encoder no longer owns the batch.

**Gate:**

- a scan of at least 20 times the worker memory limit completes;
- peak tracked scan+Flight bytes stay within budget plus one accounting unit;
- RSS stays below the documented process headroom;
- pausing the client stops additional S3/decode work within a bounded window;
- cancellation returns all byte permits.

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

- [ ] Extend scan planning to read Parquet footer row-group offsets without
      reading data pages.
- [ ] Group adjacent row groups into 64-128 MiB target morsels, capped at the
      configured maximum.
- [ ] Preserve Iceberg snapshot ID, deletes, projection, predicate, field IDs,
      and credential scope in the signed task.
- [ ] Version the signed `ScanTask` encoding and reject unsupported versions.
- [ ] Replace `num_workers * 3` static bins with a larger pending morsel queue.
- [ ] Start with coordinator push scheduling, then add worker pull/lease if the
      existing Flight protocol makes pull practical.
- [ ] Limit active morsels from live worker byte pressure, CPU, spill backlog,
      and outbound Flight bytes.
- [ ] Retry an individual morsel on another worker using its stable morsel ID.
- [ ] Deduplicate duplicate attempt output at the coordinator.
- [ ] Add work-stealing tests with one slow worker and one large file.
- [ ] Add delete-aware row-group correctness tests. If iceberg-rust cannot
      safely restrict deletes to a row-group morsel, retain file-level morsels
      for that table rather than weakening correctness.

**Gate:**

- one multi-gigabyte Parquet file uses multiple workers/cores;
- each row is returned exactly once under retry;
- a 10x worker speed imbalance does not create a query-length straggler;
- memory remains within the Phase 1 bound.

---

## Phase 3: Reusable local spill substrate

**Outcome:** SQE can write, stream-read, account, corrupt-detect, and reliably
clean immutable Arrow spill segments under a hard disk quota.

**Files:**

- Create: `crates/sqe-spill/src/manager.rs`
- Create: `crates/sqe-spill/src/scope.rs`
- Create: `crates/sqe-spill/src/segment.rs`
- Create: `crates/sqe-spill/src/format.rs`
- Create: `crates/sqe-spill/src/quota.rs`
- Create: `crates/sqe-spill/src/fault.rs`
- Modify: `crates/sqe-worker/src/bootstrap.rs`
- Modify: `crates/sqe-core/src/config.rs`
- Add unit and integration tests under `crates/sqe-spill/tests/`

- [ ] Implement validated spill-root creation and restrictive permissions.
- [ ] Implement query/stage/operator/partition/attempt scopes.
- [ ] Implement quota reservation before writing.
- [ ] Implement asynchronous IPC segment writer with per-batch and whole-file
      checksums.
- [ ] Publish through `.partial` plus atomic rename.
- [ ] Implement a streaming reader bounded by `spill_io_budget`.
- [ ] Add write/read semaphores and cancellation.
- [ ] Implement cleanup guards for normal completion, error, panic unwind, and
      process restart.
- [ ] Add startup orphan cleanup without touching recent/live attempts.
- [ ] Add fault injection: short write, ENOSPC, read error, corruption, slow
      disk, cancellation, and rename failure.
- [ ] Add metrics and structured spill lifecycle tracing.
- [ ] Document capacity planning and Kubernetes ephemeral-volume requirements.

**Gate:**

- round-trip preserves schema, ordering within a segment, rows, and nulls;
- corruption and truncation fail typed;
- quota is never exceeded;
- cancellation leaves no published or partial orphan;
- reading a segment never loads the whole segment into memory.

---

## Phase 4: Spillable distributed shuffle

**Outcome:** Exchange at least 10 times aggregate worker RAM completes through
NVMe spill with bounded receiver memory.

**Files:**

- Modify: `crates/sqe-worker/src/shuffle.rs`
- Modify: `crates/sqe-worker/src/flight_service.rs`
- Modify: `crates/sqe-planner/src/shuffle_exec.rs`
- Modify: `crates/sqe-planner/src/stage_planner.rs`
- Modify: coordinator distributed stage execution
- Add: shuffle spill integration and chaos tests

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

- [ ] Add query, stage, partition, producer task, and attempt IDs to exchange
      descriptors.
- [ ] Implement `SpillablePartitionBuffer`.
- [ ] Hash/range partition one bounded input batch at a time.
- [ ] Avoid constructing all output partition batches simultaneously when the
      total would exceed the budget; process partition IDs in bounded groups.
- [ ] Spill at the soft watermark before a hard allocation failure.
- [ ] Stream committed spill segments to the downstream reader.
- [ ] Define completion manifests with rows, batches, logical bytes, physical
      bytes, and checksums.
- [ ] Reject late data from a losing/obsolete task attempt.
- [ ] Propagate downstream cancellation to DoExchange intake and spill writers.
- [ ] Protect spill-read/merge headroom from scan and shuffle writers.
- [ ] Expose per-partition skew, resident bytes, spill bytes, and blocked time.
- [ ] Test multiple concurrent producers and one slow consumer.
- [ ] Test worker shutdown, disk-full, corrupted segment, duplicate attempt,
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

### Phase 5a: Immediate safe fallback

- [ ] Change unknown build-side statistics from "zero/keep hash" to
      "unknown/choose spillable".
- [ ] Keep an explicit small-known-build exception.
- [ ] Use existing spillable sort-merge fallback until Grace hash is ready.
- [ ] Add tests for absent, inexact, zero, and underestimated statistics.

This is deliberately conservative and can ship before the adaptive join.

### Phase 5b: Grace/radix hash join

- [ ] Register desired and minimum memory with the worker governor.
- [ ] Begin in-memory build only under an explicit grant.
- [ ] At the soft watermark, partition build and probe by unused hash bits.
- [ ] Keep fitting partitions resident and spill only excess partitions.
- [ ] Join one partition pair at a time and release it immediately.
- [ ] Recursively repartition a partition that still exceeds its grant.
- [ ] Detect heavy hitters/skew and isolate them rather than recursively hashing
      forever.
- [ ] Cap recursion and fall back to sort-merge for pathological partitions.
- [ ] Support inner/semi/anti first; add outer joins only with explicit matched
      state and correctness tests.
- [ ] Preserve null-equality and join-filter semantics.
- [ ] Profile chosen strategy, estimate, observed build bytes, partitions,
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

- [ ] Use small thread/task-local pre-aggregation tables with a fixed grant.
- [ ] Flush partial tuples into radix-partitioned spill pages/segments when the
      table reaches its soft watermark.
- [ ] Unpin/release flushed state immediately.
- [ ] Over-partition so active final partitions fit under concurrent grants.
- [ ] Combine one partition at a time, emit results, and release it.
- [ ] Recursively repartition oversized partitions using additional hash bits.
- [ ] Define supported decomposable aggregate states explicitly.
- [ ] Route unsupported holistic/variable states to a safe sort-based path or
      return a typed unsupported-at-this-budget error; never attempt unbounded
      memory.
- [ ] Test `COUNT`, `SUM`, `MIN/MAX`, `AVG`, distinct, nulls, decimals, strings,
      and multi-column keys.

### Sort

- [ ] Verify DataFusion sort run creation is charged to the common governor.
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

- [ ] Register every blocking consumer by query and workload class.
- [ ] Guarantee minimum viable grants only when total minima fit.
- [ ] Distribute remaining memory using weighted fair shares.
- [ ] Reduce grants at a soft process/worker watermark.
- [ ] Trigger asynchronous spill/repartition and wait for reclaimed bytes
      before admitting new large work.
- [ ] Preserve spill read/merge and control-plane headroom.
- [ ] Reject admission before execution when summed minima cannot fit.
- [ ] Prevent one query with many plan nodes from claiming all grants.
- [ ] Add concurrency tests with simultaneous joins, aggregates, sorts, and
      shuffle.

**Gate:** Four concurrent larger-than-memory queries complete or are fairly
queued; none is killed by another operator's allocation race.

---

## Phase 8: Durable exchange and task retry

**Outcome:** A worker loss does not discard successful upstream work for
resilient multi-TB queries.

**Files:**

- Create: durable exchange object-store backend in `sqe-spill`
- Modify: stage planner and coordinator query state
- Modify: shuffle writer/reader manifests
- Modify: credential configuration
- Add: worker-kill and object-store fault tests

- [ ] Abstract local and object-store segment backends behind one immutable
      segment interface.
- [ ] Use a dedicated exchange bucket/prefix and credential, not a table-vended
      credential.
- [ ] Publish task-attempt manifests atomically after every segment is durable.
- [ ] Commit one winning attempt per task.
- [ ] Reuse completed upstream segments on retry.
- [ ] Reject losing-attempt output.
- [ ] Add lifecycle tags and object-store expiry policies.
- [ ] Encrypt transport and server-side objects according to deployment policy.
- [ ] Persist enough coordinator stage state to retry workers during the
      coordinator's lifetime. Coordinator restart recovery remains a later
      phase unless included explicitly.
- [ ] Test worker kill during write, after publish, and during downstream read.
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
- quota used/free and disk free;
- checksum, quota, ENOSPC, cleanup, and retry failures;
- orphan cleanup count/bytes.

### Scheduling

- morsels planned/completed/retried/stolen;
- compressed/decoded estimates versus actual;
- worker skew and straggler time;
- task attempts and winning attempt;
- exchange reused versus recomputed bytes.

## Operational safeguards

- Put spill on dedicated local NVMe or a dedicated Kubernetes ephemeral volume,
  not the container root filesystem.
- Alert at spill quota 70/85/95 percent and `min_free_bytes` approach.
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
