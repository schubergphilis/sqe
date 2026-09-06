---
name: benchmarks
description: Run, commit, and compare SQE benchmark results (TPC-H, TPC-DS, SSB). Use when running benchmarks, comparing against a baseline, or after any performance-sensitive change to spill, scan planning, I/O, or distributed execution.
---

# Benchmarks

Benchmark JSON results in `benchmarks/results/` are **committed to the repo** for historical comparison. After running benchmarks (TPC-H, TPC-DS, SSB, etc.):

1. **Commit the JSON report** -- `git add benchmarks/results/*.json` -- these files track performance over time
2. **Compare against baselines** -- Use historical results to detect regressions before merging
3. **Baseline to compare against**: the most recent committed result for the same suite, scale factor, and topology (single-node vs distributed) in `benchmarks/results/`. Only `PROFILE=release` runs count as baselines.
4. **Run benchmarks after performance-sensitive changes** -- spill, scan planning, I/O pipeline, distributed execution

```bash
# Quick smoke test: TPC-H SF1 single-node
BENCH_SCALE=1 ./scripts/benchmark-test.sh tpch

# Iterating on engine code between runs: dev-release profile (release
# opt-level, no LTO, incremental) rebuilds in a fraction of release time.
# Committed baseline numbers must still come from PROFILE=release.
PROFILE=dev-release BENCH_SCALE=1 ./scripts/benchmark-test.sh tpch

# Distributed test: coordinator + 2 workers (heavy; brings up its own stack)
scripts/test.sh scenario distributed
```
