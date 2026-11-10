### Changed

- `[worker.spill]` is retired; the unused worker spill segment store is removed
  The worker built a spill segment store at start (local, S3 or tiered,
  configured under `[worker.spill]`), but no query path ever wrote to it:
  workers only scan, and every sort, join and aggregate spills through
  DataFusion's disk manager on the coordinator. About 2,900 lines go, with
  the S3 tier's credentials and bucket checks. Old configs still load: a
  `[worker.spill]` table is accepted, ignored, and logged as a warning at
  worker start. `worker.spill_to_disk` and `worker.spill_dir` are unchanged.
