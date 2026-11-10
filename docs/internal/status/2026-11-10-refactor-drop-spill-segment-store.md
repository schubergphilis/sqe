**STATUS 2026-11-10: worker spill segment store removed; `[worker.spill]` retired (`refactor/drop-spill-segment-store`).**

`sqe-worker` bootstrap opened a `SpillManager` over a local, S3 or tiered
`SegmentStore` and handed it to the Flight service, which never read it. The
store modules (`manager`, `scope`, `segment`, `store*`, `fault`) are deleted
from `sqe-spill`, with their error variants and the `arrow-*`, `object_store`,
`crc32fast`, `bytes`, `futures`, `async-trait` and `serde` dependencies. What
stays is live: `ByteBudget`, `Accounted`, `MemoryGovernor`,
`ResizableFairSpillPool`, `MemoryGrant`.

`WorkerConfig.spill` is now `Option<serde::de::IgnoredAny>`, so an old table
still parses (`retired_worker_spill_table_still_parses`) and the worker warns.
Its validation (#382 bucket cross-check, directory conflict, backend
checks) is gone with it. The hot-reload restart check now watches
`worker.spill_dir` / `worker.spill_to_disk`.

**NEXT:** `worker.memory.spill_io_budget` still takes a share of
`worker.memory_limit` for spill I/O that the worker never does; size it to
zero or fold it into `operator_budget`.
