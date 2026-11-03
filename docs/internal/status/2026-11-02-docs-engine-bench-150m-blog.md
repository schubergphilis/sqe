**STATUS 2026-11-02: blog post on the 150M engine-bench run (`docs/engine-bench-150m-blog`).**

`docs/site/blog/2026-11-02-150m-rows-under-a-4-5-gib-cap.md` answers
"DuckDB 2.0 vs Polars vs Spark on one machine: 150M rows under a 4.5 GiB
cap" with SQE added in three modes. The numbers come from
`make bench` (150M, 3 reps, Spark on, local storage) on 2026-11-02, raw runs
in `.work/results/150m-20261102T170650Z.jsonl`. The run used a release build
of `fix/scan-static-row-filter` on top of `feat/top-row-number`.
`quickstart/engine-bench/OUTPUT.md` is that run's capture.

- All 24 answer cells match DuckDB's. Polars q3 has no output: the watchdog
  killed all three runs.
- Geomean query time: `sqe-parquet` 1.88 s, `sqe-iceberg` 2.36 s, DuckDB
  2.89 s, `sqe-polaris` 9.10 s, Spark 15.11 s. Polars has no geomean because
  q3 failed.
- The before/after table in the post compares this run with the morning run
  `150m-20261102T090422Z.jsonl`, which predates both fixes.
- The S3 numbers in the post are from `150m-s3-20261102T103135Z.jsonl`, also
  before both fixes, and the post says so.

Three images sit in `docs/site/blog/images/` as SVG source plus a PNG
rendered with `rsvg-convert -z 2`: `150m-peak-rss` (peak RSS per query
against the cap), `150m-scan-row-filter` (scan before/after, from the
8%-selectivity A/B and its EXPLAIN ANALYZE) and `150m-q3-rewrite` (window
plan against the group-by plan). The PNGs are not leak-gated; the SVGs are.
The post's "Next to the article's numbers" table compares wall seconds on
both sides, not `query_s`.

`scripts/leak-scan-site.sh` now skips `.work/`: after any local bench run the
gitignored venv's pyarrow test files tripped the account-id check, so
`make leak-scan` failed on files that are never committed.

`bench.py summary` printed the last line of a failed run's stderr, which for
Spark is a stack frame (`... 1 more`). `failure_reason` now picks the line
that names the error, skipping the Py4J wrapper, so OUTPUT.md shows
`SparkOutOfMemoryError: [UNABLE_TO_ACQUIRE_MEMORY]`. OUTPUT.md's timings are
regenerated from the same jsonl; only that line changed.

A controlled A/B (the two engines alone, alternating, `/usr/bin/time -l`)
changed how the post reads the DuckDB gaps. q3 is a tie in isolation (SQE
4.8-4.9 s, DuckDB 4.4-5.1 s at 3 GB); the bench's 9.03 s DuckDB median
tracked low free memory with the Docker stack up, where DuckDB's 2.9 GB spill
hurts more than SQE's 1.4 GB. q4 is a memory result: same ~30 CPU s, but
DataFusion skips the partial aggregate (149.1M of 150M rows) and SQE stays at
~2 GiB, while DuckDB needs 10-11 GiB uncapped (then 3.0-3.2 s vs SQE's
3.3-3.5 s) and spills ~6 GB at 3 GB. The post now says so; the medians in its
tables are unchanged. Scripts were in `/tmp/whyfast/` (not kept).

**NEXT:** rerun `make bench BENCH_STORAGE=s3` on the merged tree and update
the post's S3 paragraph with after-fix numbers. Then run SQE q3/q4 with a
1 GB memory limit, the setting of the article's DuckDB follow-up (14.97 s /
1.12 GiB and 13.94 s / 1.08 GiB); the post says SQE was not run at 1 GB.
Then scale up: 300M and 600M rows (`bench.py run --sizes`), Docker down and
background apps quiet, comparing CPU seconds and busy cores as well as wall
time. Larger q4 runs push DuckDB into heavier spill and are the first real
test of SQE's own spill path on this workload.
