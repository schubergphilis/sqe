# Distributed SQE: coordinator + workers

## Goal

Run a real distributed SQE cluster: one coordinator and two stateless DataFusion workers over Arrow Flight, querying Iceberg tables in the shared Polaris + RustFS stack. The coordinator plans a query, splits it into fragments, and dispatches them to the workers; the workers execute their fragments against the Iceberg data and stream results back over Flight; the coordinator assembles the final result.

This is the scenario for seeing distribution actually happen — query history, worker dispatch, the system tables, and the Trino HTTP endpoint, all against a four-container cluster rather than the single-process coordinator the other quickstarts run.

## Components

| Service | Host port | Role |
|---|---|---|
| coordinator | `60051` Flight SQL, `28080` Trino HTTP, `29090` metrics | Plans, schedules, holds query history + result cache. |
| worker-1 | `60061` | Stateless DataFusion executor. |
| worker-2 | `60062` | Stateless DataFusion executor. |
| Polaris | `18181` | Iceberg REST catalog (`test_warehouse`). |
| RustFS | `19000` | S3-compatible storage for the Iceberg data. |
| Postgres | (internal) | Polaris's metastore. |

The compose setup is a two-file overlay: `docker-compose.distributed.yml` adds the coordinator and the two workers, and inherits Polaris, RustFS, and Postgres from `docker-compose.test.yml`. Both files are required, which is why `run.sh` always passes both `-f` flags.

## Configuration

### `coordinator.toml`

```toml
[coordinator]
worker_urls = ["http://worker-1:50052", "http://worker-2:50052"]
allow_unauthenticated_workers = true
```

`worker_urls` is the static list of workers the coordinator dispatches fragments to. `allow_unauthenticated_workers = true` opts out of the `worker_secret` requirement that production must satisfy — safe here and only here, because the stack runs on a private Docker network. The coordinator also enables the result cache (`[query_cache]`, 128 MB, 5-minute TTL) and query history (`[query_history]`, up to 10000 finished queries for 30 minutes), which is what `system.runtime.queries` reads from.

### `worker.toml`

```toml
[worker]
flight_port = 50052
coordinator_url = "http://coordinator:50051"
heartbeat_interval_secs = 5
memory_limit = "512MB"
```

Workers are stateless: they hold no catalog or session state, only the plan fragment and the data they fetch. Each worker registers with the coordinator over its `coordinator_url` and sends heartbeats; its Trino endpoint is disabled (`trino_http_port = 0`) — only the coordinator serves clients. Each worker's `[catalog]` and `[storage]` match the coordinator's, because workers read Iceberg data directly from RustFS.

## The test

`run.sh` brings up both compose files, bootstraps Polaris (warehouse, namespace, grants), then runs the demo queries over Flight SQL and Trino HTTP and captures them to `OUTPUT.md`. A correct run shows the cluster topology from `system.runtime.nodes`, a CTAS round-trip read back in order, and the finished queries in query history.

`./run.sh --check` asserts the distributed invariants end to end: connectivity over Flight SQL, `system.runtime.nodes` lists the coordinator, `system.runtime.queries` records `FINISHED` history, a CTAS round-trip and read-back, cache invalidation on write, the Trino HTTP endpoint answers, and — the distributed-execution invariant — `system.runtime.tasks` shows fragments dispatched to `worker-1` / `worker-2` after a query that forces a split, not just the coordinator.

Tear down with `./run.sh --down`.

## Gotchas

- **Both compose files are required.** `docker-compose.distributed.yml` is an overlay; without `docker-compose.test.yml` the coordinator comes up with no catalog behind it.
- **The stack is heavy.** It builds the SQE image and runs four containers plus Polaris and RustFS, so it is excluded from the self-contained scenario suite and run on demand.
- **`allow_unauthenticated_workers` is test-only.** Production deployments must set a `worker_secret`.
- **The host needs a Rust toolchain.** `run.sh` builds `sqe-cli` on the host to drive the coordinator; the cluster itself runs in Docker.
