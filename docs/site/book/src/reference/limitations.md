# Limitations and Known Gaps

Every engine has edges. This page collects the ones SQE already documents elsewhere into a single list, grouped by area. Each entry says what the limitation is and what the workaround or roadmap status is, with a link to where it is covered in detail. Nothing here is new. If a constraint matters to your deployment, follow the link and read the full treatment.

## Availability

### The coordinator is a single point of failure

The coordinator runs as a single replica. Session state, the worker registry, and in-flight query state are process-local. There is no shared store. A coordinator restart drops every in-flight query and invalidates client sessions, so connected clients re-authenticate and re-run. A node drain that moves the coordinator pod is a brief outage, not a transparent failover.

Running more than one coordinator replica is not yet safe. Two replicas do not share sessions or the registry, so a client would land on a coordinator that never saw its session. Keep `coordinator.replicas: 1`. Full coordinator HA with shared session and registry state is a separate design, not yet built.

Workers are different. They are stateless and scale horizontally. A worker loss costs the queries that were running fragments on it, not the cluster.

See [Kubernetes & Helm](../deployment/kubernetes.md#the-coordinator-is-a-single-point-of-failure). The chart ships a coordinator PodDisruptionBudget (`minAvailable: 1`) to block an unforced eviction, but a budget protects the SPOF, it does not remove it.

## Security and data path

### Read-path S3 access uses the static storage key, not a per-user credential

Writes already consume per-table credentials vended by the catalog: INSERT, MERGE, and DELETE go through the loaded table's file IO, which carries the vended credentials. Reads do not. The coordinator reads data files with the static key configured in the `[storage]` section, the same key for every user. Per-user read credential vending (the catalog returns short-lived, table-scoped S3 credentials and SQE reads with those) is designed but not yet built.

The practical consequence: the metadata path is gated per user (the catalog enforces table and namespace permission via the user's bearer token), but the data path is not gated per user on reads. Scope the `[storage]` key to the minimum the engine needs.

See [S3 Credential Vending](../design-notes/s3vending.md) for the full design and the phase shape, and the [security model](../architecture/security-model.md) for where this sits in the trust boundary.

### Fine-grained policy enforcement is off by default

SQE parses the security SQL surface: `GRANT ... MASKED WITH`, `GRANT ... ROWS WHERE`, `SHOW EFFECTIVE GRANTS`, `CHECK ACCESS`. The plan-rewriting enforcement that would apply those row filters and column masks is not the default. The active enforcer is `passthrough`, which returns plans unmodified. The config exposes `[policy] engine = "passthrough"` as the only option today. OPA and Cedar enforcers are planned, not yet wired.

Treat the access-control SQL as a documented surface, not a live control you can rely on out of the box. See [Security & Policy](../architecture/security.md), [GRANT and REVOKE](../sql-reference/grant-revoke.md), and the [Roadmap](../development/roadmap.md) (Phase 6, Security Policies, Planned).

### Grant model gaps

Within the grant SQL surface itself, three things are not supported:

- No `WITH GRANT OPTION`. Grants are non-delegating. Only an admin can grant.
- No column-level `INSERT`. INSERT granularity is table-level.
- Mask expressions are scalar only. Aggregate and table-valued mask expressions are rejected.

See [GRANT and REVOKE, Known gaps](../sql-reference/grant-revoke.md#known-gaps).

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

Hash joins are not spillable upstream either. SQE rewrites a hash join to a sort-merge join when the estimated build side exceeds `hash_join_memory_threshold`, trading speed for survival. See [Streaming Execution, SortMergeJoin Fallback](../architecture/streaming-execution.md#sortmergejoin-fallback).
