# Limitations and Known Gaps

Every engine has edges. This page collects the ones SQE already documents elsewhere into a single list, grouped by area. Each entry says what the limitation is and what the workaround or roadmap status is, with a link to where it is covered in detail. Nothing here is new. If a constraint matters to your deployment, follow the link and read the full treatment.

## Availability

### The coordinator is a single point of failure

**Service level we ship: restartable, not HA.** One active instance. Client retry. No query survival across a restart (issue #405). Do not scale `coordinator.replicas` above 1.

The coordinator runs as a single replica. Sessions, the query tracker, cancellation, caches, `CREATE SECRET`, `ATTACH`, and shuffle orchestration are process-local. Session-file restore omits tokens, so it is not a live session. There is no shared store. A coordinator restart drops every in-flight query and invalidates client sessions, so connected clients re-authenticate and re-run. A node drain that moves the coordinator pod is a brief outage, not a transparent failover.

Running more than one coordinator replica is not yet safe. Two replicas do not share sessions or the registry, so a client would land on a coordinator that never saw its session. Keep `coordinator.replicas: 1`. Full coordinator HA with shared session and registry state is a separate design, not yet built.

Workers are different. They are stateless and scale horizontally. A worker loss costs the queries that were running fragments on it, not the cluster.

See [Kubernetes & Helm](../deployment/kubernetes.md#the-coordinator-is-a-single-point-of-failure). The chart ships a coordinator PodDisruptionBudget (`minAvailable: 1`) to block an unforced eviction, but a budget protects the SPOF, it does not remove it.

## Security and data path

### Read-path S3 access and `require_vended_credentials`

Writes already consume per-table credentials vended by the catalog: INSERT, MERGE, and DELETE go through the loaded table's file IO, which carries the vended credentials.

For Iceberg FileIO (single-node scans, metadata, writes), set `[catalog] require_vended_credentials = true` so the engine never injects the configured `[storage]` access and secret keys, and so FileIO does not load `AWS_*` env, `~/.aws/config`, IRSA, or IMDSv2. FileIO then needs Polaris-vended STS or remote signing. `production_mode` requires the flag on every REST catalog (issue #395). Dev and single-tenant stacks can leave it false and keep the static-key fallback.

Distributed `ScanTask`s still carry the configured `[storage]` key to workers. Per-user read credential vending on that path (put the loadTable STS triple on the ticket instead of the static key) is designed but not yet built. Multi-tenant clusters with workers still need that work; the FileIO flag does not close the worker ticket.

See [S3 Credential Vending](../design-notes/s3vending.md) for the remaining ScanTask work, and the [security model](../architecture/security-model.md) for where this sits in the trust boundary.

### Fine-grained policy enforcement is off by default

SQE parses the security SQL surface: `GRANT ... MASKED WITH`, `GRANT ... ROWS WHERE`, `SHOW EFFECTIVE GRANTS`, `CHECK ACCESS`. Plan-rewriting enforcement of row filters and column masks is shipped but off by default: the default `[policy] engine = "passthrough"` returns plans unmodified. Set `engine = "ranger"` (Apache Ranger fine-grained policies, row-filter + data-mask, shared with Spark/Kyuubi) or `engine = "in-memory"` (dev and tests) to turn enforcement on. The `opa` and `cedar` engines are defined in config but not yet wired (selecting them errors today).

The gap to know about is the default, not the capability: enforcement does nothing until you select an engine. See [Fine-grained access control](../features/fine-grained-access-control.md), [GRANT and REVOKE](../sql-reference/grant-revoke.md), and [Spark / Ranger Parity](../design-notes/sqe-spark-ranger-parity.md).

### Grant model gaps

Within the grant SQL surface itself, these things are not supported:

- No `WITH GRANT OPTION`. Grants are non-delegating. Only an admin can grant.
- No column-level `INSERT`. INSERT granularity is table-level.
- Mask expressions are scalar only. Aggregate and table-valued mask expressions are rejected.
- `REVOKE SELECT` does not stop reads. Privileges expand to Polaris access types, and `INSERT` keeps `table-properties-read`, which is what unlocks `LOAD_TABLE`. After the load, SQE reads files directly, so `table-data-read` is not the read gate. Unity Catalog keeps SELECT and MODIFY independent. SQE cannot match that with a grant-profile edit, because a writer has to load the table. Close a gate with `REVOKE ALL PRIVILEGES` and confirm with `CHECK ACCESS` (issue #396).

See [GRANT and REVOKE, Closing a gate](../sql-reference/grant-revoke.md#closing-a-gate) and [Known gaps](../sql-reference/grant-revoke.md#known-gaps).

## Iceberg and types

### Iceberg V3 advanced types are blocked upstream

V3 landed end to end (default values, schema evolution, nanosecond timestamps, partition evolution, equality and position deletes). Five advanced features are still blocked on upstream work, not on SQE:

| Feature | Blocker |
|---|---|
| Variant type (and shredded variant) | iceberg-rust PR not merged |
| Geometry type | DataFusion user-defined-type support |
| Vector / embedding type | Iceberg V3 vector spec not finalised |
| Multi-arg partition transforms | Iceberg Java spec alignment in progress |
| Row lineage | Deferred upstream |

There is no SQE-side workaround. These unblock when the upstream dependency ships. See [Roadmap, V3 features still blocked upstream](../development/roadmap.md#phase-7---iceberg-v3-done).

## SQL and policy surface

### Statements DataFusion's parser does not accept

`PIVOT`, `UNPIVOT`, `QUALIFY`, `ASOF JOIN`, and FROM-first syntax are not parseable. This is intentional and tracked upstream, not an SQE bug. Lambda expressions and list comprehensions have no AST node in DataFusion. The full list, with the reasoning for each, is on the [SQL Reference overview](../sql-reference/index.md#what-is-intentionally-not-in-sqe), and the [SQL cheat-sheet](sql-cheatsheet.md) carries the scannable version.

### read_parquet schema and write constraints

The file-format table-valued functions read external files directly. The constraints:

- All files matched by a glob must share an identical Arrow schema. Schema evolution across files in one glob is not supported.
- `read_parquet()` is read-only. It cannot be the target of an INSERT.
- Very large match sets (more than ten thousand files) can slow planning due to the object listing step.

See [read_parquet TVF, Limitations](../features/read-parquet.md#limitations).

## Scale

### Single-node memory cutoff around 100GB

Single-node mode is the default and the recommendation for development and datasets under roughly 100GB. Beyond that, enable workers so scans and joins distribute instead of funnelling every intermediate result through one process. See [System Overview, Single-node vs distributed](../architecture/overview.md#single-node-vs-distributed) and [Sizing and capacity](../deployment/sizing.md).

The cutoff is a guideline, not a hard limit. Spill-to-disk lets a memory-constrained coordinator survive large queries, so the real number depends on the query shape and the spill budget. Measure against your workload.

### Hash aggregation can still OOM on a memory-constrained single node

Spill-to-disk covers sorts and sort-merge joins. Hash aggregation spill is limited by what DataFusion supports upstream. The documented edge is TPC-H q18: a high-cardinality `GROUP BY` with `HAVING` that produces millions of intermediate groups overruns a 512MB single-node budget, because the grouped hash aggregate does not yet spill. The fix is distribution. Phase B two-phase aggregation spreads the groups across workers and q18 passes. On a single node, raise `memory_limit` or distribute. See [Streaming Execution, Benchmark Results](../architecture/streaming-execution.md#benchmark-results).

Hash joins are not spillable upstream (DataFusion #17267). SQE rewrites a hash join to a sort-merge join only when the build-side estimate is **exact** and above `hash_join_memory_threshold`. Iceberg scans usually report unknown stats, so those joins stay as HashJoin and a large build fails the query instead of slowing down (issue #411). Rewriting on `Unknown` was tried and doubled TPC-DS wall time at SF1 because each SMJ coalesced to one partition. Workaround: raise `query.hash_join_memory_threshold` only when you have exact stats you trust, or distribute. See [Streaming Execution, SortMergeJoin Fallback](../architecture/streaming-execution.md#sortmergejoin-fallback).

### IcebergScanExec advertises one output partition

Parallel I/O already happens inside a scan (prefetch, manifest concurrency). The plan still advertises a single output partition so broadcast / CollectLeft joins stay cheap. Wiring `target_partitions` to the DataFusion default flipped joins to CollectLeft and regressed TPC-DS q72 5-6x (issue #414, related #87 / #131).

`parallel_probe_scan` exists and stays **opt-in**. It helps SSB-style scan-bound shapes and costs TPC-DS about +26% at SF1. Default-on waits for a cost gate that keeps CollectLeft on the build side. Leave the flag off unless you have measured the workload.

### TPC-BB wall time is mostly q01

On the 2026-08-15 Flight SF1 run (`tpcbb-sf1-flight-2026-08-15T14:31:10.json`) the suite is 63.1s and **q01 is 35.5s** of that, 20 rows. The suite still passes 10/10. Do not quote a TPC-BB total without naming q01 (issue #417). Compare totals on the same day (`README.md`) use a different envelope and are 56.3s vs Trino 290.3s.
