---
slug: distributed
title: "Distributed SQE: coordinator + workers"
description: "Run a real distributed SQE cluster: one coordinator and two stateless DataFusion workers over Arrow Flight, querying Iceberg tables in the shared Polaris + RustFS stack. The coordinator plans and schedules; workers execute plan fragments and stream results back."
---

# Distributed SQE: coordinator + workers

A real distributed cluster: one SQE coordinator and two stateless DataFusion
workers over Arrow Flight, querying Iceberg tables in the shared Polaris +
RustFS test stack. The coordinator plans and schedules; the workers execute plan
fragments and stream results back over Flight.

This is the scenario for seeing distribution actually happen: query history,
worker dispatch, the system tables, and the Trino HTTP endpoint, all against a
four-container cluster rather than the single-process coordinator the other
quickstarts run.

## What you get

The compose setup is a two-file overlay. `docker-compose.distributed.yml` (at
the repo root) adds the coordinator and two workers; it inherits Polaris, RustFS,
and Postgres from `docker-compose.test.yml`. Both files are required, which is
why `run.sh` brings the stack up with both `-f` flags.

| Service | Host port | Role |
|---|---|---|
| coordinator | `60051` Flight SQL, `28080` Trino HTTP, `29090` metrics | Plans, schedules, holds query history + result cache. |
| worker-1 | `60061` | Stateless DataFusion executor. |
| worker-2 | `60062` | Stateless DataFusion executor. |
| Polaris | `18181` | Iceberg REST catalog (`test_warehouse`). |
| RustFS | `19000` | S3-compatible storage for the Iceberg data. |
| Postgres | (internal) | Polaris's metastore (from `docker-compose.test.yml`). |

## Prerequisites

- Docker (with Compose v2).
- A Rust toolchain (`cargo`). `run.sh` builds `sqe-cli` in release mode on the
  host to drive the coordinator over Flight, and the coordinator/worker image
  builds from this repo on first run.
- `curl` and `python3` (used by the check assertions and the cache timing).

## Run it

```bash
./run.sh             # up -> bootstrap Polaris -> queries -> capture to OUTPUT.md
./run.sh --check     # up -> bootstrap -> assert the distributed invariants
./run.sh --down      # tear the whole stack down (-v)
```

Or through the unified entry point from the repo root:

```bash
scripts/test.sh scenario distributed
```

`run.sh` brings up both compose files, runs `scripts/bootstrap-distributed.sh`
(creates the warehouse, namespace, and grants in Polaris), then runs the demo
queries and captures them to `OUTPUT.md` (written on each run, not committed).

## How it works

The coordinator receives a query over Flight SQL, plans it, and splits it into
fragments. It dispatches those fragments to the two workers, which run DataFusion
against the Iceberg data they read from RustFS, and stream the results back over
Flight. The coordinator assembles the final result and returns it to the client.

The workers are stateless: they hold no catalog or session state, only the plan
fragment and the data they fetch. They register with the coordinator over their
`coordinator_url` and send heartbeats. The proof that distribution is real is
`system.runtime.tasks`: after a query that forces enough data files to split, the
table shows fragments dispatched to `worker-1` / `worker-2`, not just the
coordinator.

## Configuration explained

This scenario has no `sqe.toml` of its own. The coordinator and worker configs
live at the repo root in `tests/distributed/coordinator.toml` and
`tests/distributed/worker.toml`, mounted by `docker-compose.distributed.yml`.

### `coordinator.toml`

```toml
[coordinator]
worker_urls = ["http://worker-1:50052", "http://worker-2:50052"]
allow_unauthenticated_workers = true
```

- `worker_urls` is the static list of workers the coordinator dispatches
  fragments to, addressed by their in-network names.
- `allow_unauthenticated_workers = true` opts out of the `worker_secret`
  requirement that production must satisfy. The test stack runs inside a private
  Docker network, so it is safe here and only here.
- `[query_cache]` enables the result cache (128 MB, 5-minute TTL); the cache is
  what the check proves gets invalidated on write.
- `[query_history]` keeps up to 10000 finished queries for 30 minutes, which is
  what `system.runtime.queries` reads from.
- `[auth]` uses Polaris's internal OAuth token endpoint with the `root` client,
  because the bootstrap drives Polaris with its own credential rather than an
  IdP token.
- `[catalog]` and `[storage]` point at Polaris (`test_warehouse`) and RustFS
  (path-style S3, plain HTTP), the same shared stack the other tests use.

### `worker.toml`

```toml
[worker]
flight_port = 50052
coordinator_url = "http://coordinator:50051"
heartbeat_interval_secs = 5
memory_limit = "512MB"
```

- `flight_port` is the port the worker listens on for fragments from the
  coordinator.
- `coordinator_url` is how the worker registers and heartbeats. The worker's
  `trino_http_port = 0` disables its Trino endpoint: only the coordinator serves
  clients.
- The worker's `[catalog]` and `[storage]` match the coordinator's, because each
  worker reads Iceberg data directly from RustFS.

### `run.sh` plumbing

`run.sh` resolves the repo root, builds `sqe-cli` in release, drives the
coordinator over Flight (`cli_query`) and over Trino HTTP (`trino_query`), and
uses both compose `-f` flags so the coordinator comes up with a catalog behind
it.

## Output

`run.sh` writes an `OUTPUT.md` on each run (it is not committed for this scenario,
because the stack is heavy and the run is on-demand). The capture shows the
cluster topology from `system.runtime.nodes`, a CTAS round-trip into
`test_warehouse.dist_test.numbers` read back in order, and recent query history
from `system.runtime.queries`. A correct run shows the coordinator node in the
topology and the finished queries in `FINISHED` state.

## How it is tested

`./run.sh --check` runs the end-to-end distributed assertions, ported 1:1 from
the retired `scripts/distributed-test.sh`:

- connectivity (`SELECT 1`) over Flight SQL,
- `system.runtime.nodes` lists the coordinator,
- `system.runtime.queries` records `FINISHED` query history,
- `system.metadata.catalogs` shows `test_warehouse` on the `iceberg` connector,
- a CTAS round-trip and read-back (`one`, `two`, `three`),
- `system.metadata.table_properties` and `table_comments` return rows,
- `system.runtime.tasks` has entries with state + timing,
- cache invalidation on write (the count reflects an `INSERT`),
- `system.runtime.tasks` shows fragments dispatched to `worker-1` / `worker-2`
  after a query that forces a split (the distributed-execution invariant),
- the Trino HTTP endpoint answers,
- `information_schema` reflects the created table.

The result-cache step reports the first-vs-second timing only; it is
informational, not a counted pass/fail. Ported from the retired
`scripts/distributed-test.sh`.

## Gotchas

- **Both compose files are required.** `docker-compose.distributed.yml` is an
  overlay; without `docker-compose.test.yml` the coordinator comes up with no
  catalog behind it. `run.sh` always passes both `-f` flags.
- **Not in `scenario all`.** The stack builds the SQE image and runs four
  containers plus Polaris and RustFS. That is heavy, so it is excluded from the
  self-contained `scripts/test.sh scenario all` set and run on demand with
  `scripts/test.sh scenario distributed`.
- **`allow_unauthenticated_workers` is test-only.** Production deployments must
  set a `worker_secret`. This flag is safe only because the stack runs on a
  private Docker network.
- **The host needs a Rust toolchain.** `run.sh` builds `sqe-cli` on the host to
  drive the coordinator; the cluster itself runs in Docker.
