# Scan planning and decoded metadata retention

Date: 2026-10-01. Base: `b3244543` (current GitLab main at fetch time).
Tracking: #790 (concurrent snapshot initialization), #791 (shared scan planning),
#792 (metadata byte weight and pruning clones). Changes are proposed for review,
not merged or deployed.

## Implemented behavior

- Snapshot-file cache initialization uses Moka's fallible per-key initializer.
  Concurrent requests share one walk and immutable result. Miss counters count
  initializer executions; successful waiters count as hits. Failure and cancelled
  initialization allow retry. Shared errors preserve kind, retryability, HTTP
  status and the original source.
- Each Iceberg scan shares a lazy static plan of its pinned snapshot, projection,
  static predicate, file tasks, sorted file list and delete-manifest flag. Output
  partitions borrow the file list and split assignment is built once for all
  partitions. Changing the snapshot invalidates both caches; changing output
  partition count invalidates assignments. Runtime predicates, decoding gates,
  bloom probes and readers remain partition-local. `static_planning_runs` exposes
  actual initializer attempts in execution metrics.
- Decoded snapshot retention uses an estimated 256 MiB byte budget rather than
  200,000 files. The estimate includes file structs, Arc/vector storage, paths,
  map buckets, bound string/binary payloads, partition values, encryption metadata
  and split/equality/reference fields. Unsupported nested partition values receive
  an oversized weight; normal Iceberg partition values are primitive. Oversized
  results can serve the caller without being retained in the cache.
- Cached DataFiles are immutable Arc entries. Pruning selects Arc handles and
  borrows metadata during predicate evaluation; it no longer clones bounds maps
  for each filter. The distributed file-list helper projects each streamed task
  to `(path, length)` before collection, releasing its other fields promptly.

## Validation

`cargo test -p sqe-catalog --lib`: **593 passed**, no failures/ignored tests.
Tests run from the isolated worktree with local networking for existing HTTP
mock-server tests. The sandbox-only preliminary run failed on local-port binding;
those failures disappear with loopback networking available.

New and strengthened checks cover:

- 16 concurrent cold-cache requests sharing one result and one manifest walk.
- Failed initialization, cancelled initializer/waiter, retry, and eight callers
  sharing a classified/retryable HTTP 503 failure.
- Equal file counts with a 32 KiB bound payload receiving a larger cache weight,
  oversized non-retention, and valid caller ownership after non-admission.
- Shared static plan identity, disjoint and complete split assignments, projection
  and static-predicate preservation, attached delete tasks, cache invalidation,
  failed planning and cancelled initialization allowing retry.
- Real Parquet reads with eight output partitions through direct, split-reader
  and whole-file position-delete paths. Sealed runtime filters return the expected
  values on both direct and fallback readers. All rows appear exactly once;
  deleted rows are absent. `static_planning_runs` sums to **1** per tested scan.
- Pointer identity through metadata selection and pruning, alongside the existing
  real-manifest bounds/statistics checks.

`cargo check -p sqe-catalog -p sqe-coordinator --tests`,
`cargo clippy -p sqe-catalog --tests --no-deps -- -D warnings`, formatting and
`git diff --check` pass. Tests use a temporary RUSTC_WRAPPER selecting Apple clang/ld
and the active SDK because the configured LLVM Mach-O linker fails to resolve
system symbols locally. Repository build configuration is unchanged.

## Limits and remaining qualification

Byte weighting is an estimate of decoded cache retention, not allocator-exact
accounting or a process RSS ceiling. In-flight manifest loads, the underlying
Iceberg object cache, statistics entries and callers retaining Arc entries are
separate costs. Cache eviction releases cache ownership only.

Static plans and assignments retain task metadata for the scan-node lifetime.
Readers still own their assigned task copies. This trades one shared retained
plan for repeated full-table planning/sorting; it does not make metadata constant
in file count. Dynamic predicates are sampled by each reader as before.

No SF10/SF100 latency/RSS benchmark, external REST-catalog load or separate-host
qualification was run. #786 and #416 remain open for throughput, memory and scale
qualification. The tests establish removed duplicate initialization and result
correctness, not a measured latency speedup.
