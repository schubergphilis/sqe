# Distributed SQE: coordinator + workers

A real distributed cluster: one SQE coordinator and two stateless DataFusion
workers over Arrow Flight, querying Iceberg tables in the shared Polaris +
RustFS test stack. The coordinator plans and schedules; the workers execute
plan fragments and stream results back over Flight.

## What it covers

This scenario is the end-to-end distributed check, ported from the retired
`scripts/distributed-test.sh`:

- connectivity (`SELECT 1`) over Flight SQL,
- `system.runtime.nodes` lists the coordinator,
- `system.runtime.queries` records `FINISHED` query history,
- `system.metadata.catalogs` / `table_properties` / `table_comments`,
- a CTAS round-trip and read-back,
- cache invalidation on write (count reflects an `INSERT`),
- `system.runtime.tasks` shows fragments dispatched to workers (the
  distributed-execution invariant),
- the Trino HTTP endpoint answers,
- `information_schema` reflects the created table.

The result-cache step reports timing only; it is informational, not a pass/fail
assertion.

## Stack

The compose setup is a two-file overlay. `docker-compose.distributed.yml` adds
the coordinator and two workers; it inherits Polaris, RustFS, and Postgres from
`docker-compose.test.yml`. Both files are required, which is why `run.sh` brings
the stack up with both `-f` flags.

| Service     | Host port                                |
|-------------|------------------------------------------|
| coordinator | `60051` Flight SQL, `28080` Trino HTTP, `29090` metrics |
| worker-1    | `60061`                                  |
| worker-2    | `60062`                                  |
| Polaris     | `18181`                                  |
| RustFS (S3) | `19000`                                  |

Coordinator and worker config live in `tests/distributed/coordinator.toml` and
`tests/distributed/worker.toml` at the repo root.

## Run it

```bash
./run.sh             # up -> bootstrap -> queries -> capture to OUTPUT.md
./run.sh --check     # up -> bootstrap -> assert the distributed invariants
./run.sh --down      # tear the whole stack down (-v)
```

Or through the unified entry point from the repo root:

```bash
scripts/test.sh scenario distributed
```

## Why it is not in `scenario all`

The distributed stack builds the SQE image and runs four containers plus
Polaris and RustFS. That is heavy, so it is excluded from the self-contained
`scripts/test.sh scenario all` set and run on demand instead. CI runs the
self-contained scenarios; this one is invoked explicitly when distributed
behaviour needs validating.
