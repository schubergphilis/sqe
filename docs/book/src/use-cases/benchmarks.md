# Benchmarks

SQE ships a benchmark harness (`sqe-bench`) that generates data, loads it into
Iceberg, and runs the standard query suites. It supports TPC-H, TPC-DS, SSB,
ClickBench, and a few others, over either Flight SQL or the Trino HTTP
protocol, against a single node or a distributed cluster.

Results are written as JSON to `benchmarks/results/` and committed to the repo
for historical comparison, so regressions show up in a diff.

## Run a single suite

```bash
# TPC-H at scale factor 1, single node, over Flight SQL.
BENCH_SCALE=1 ./scripts/benchmark-test.sh tpch
```

The script generates the data, brings up a coordinator, loads the tables, runs
the 22 queries, and writes a result JSON. Switch the protocol with
`BENCH_PROTOCOL=trino` and the scale with `BENCH_SCALE`.

## Result format

```json
{
  "benchmark": "tpch",
  "scale_factor": 1,
  "protocol": "flight",
  "timestamp": "...",
  "summary": {"total": 22, "pass": 22, "fail": 0, "total_duration_ms": 0},
  "queries": [{"id": "q01", "status": "pass", "duration_ms": 0, "rows": 4}]
}
```

## Distributed

```bash
docker compose -f docker-compose.test.yml -f docker-compose.bench-2w.yml up --build -d
BENCH_PROTOCOL=flight ./scripts/benchmark-matrix.sh
```

`benchmark-matrix.sh` sweeps protocols, scale factors, and worker counts
(single, 2-worker, 4-worker) so single-node and distributed numbers come from
the same run.

## Latest results

Baselines tracked in the repo:

| Suite | Scale | Topology | Queries | Wall time | Result file |
|-------|-------|----------|---------|-----------|-------------|
| TPC-H | SF1 | single | 22/22 | 37.5s | `tpch-sf1-flight-2026-04-02T14:16:27.json` |
| TPC-H | SF1 | distributed (3w) | 22/22 | 12.0s (3.1x) | `tpch-sf1-flight-2026-04-06T20:57:10.json` |

```text
<!-- FILL: refreshed TPC-H SF1 run summary -->
```

## Parity

The benchmark harness can diff SQE against a real Trino on the same data with
`--compare-trino`, which is how SQL correctness is checked at scale, not just
timing. See [Trino HTTP compatibility](./trino-http.md) for the parity scripts.

## How it is tested

- `crates/sqe-bench/`: the generate / load / test / compare runner.
- `scripts/benchmark-test.sh`, `scripts/benchmark-matrix.sh`.
- `benchmarks/results/*.json`: committed historical results.
