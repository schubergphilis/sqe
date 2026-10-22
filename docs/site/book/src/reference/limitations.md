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

With `[catalog] access_delegation = "vended-credentials"` (or `require_vended_credentials = true`, which implies it) SQE asks the catalog for table-scoped credentials on every loadTable. Vended keys beat the configured `[storage]` pair for Iceberg scans, writes, and distributed scan tickets, and SQE reloads the table before they expire. `require_vended_credentials` also keeps the `[storage]` keys and the AWS SDK fallback chain out of Iceberg FileIO entirely; `production_mode` requires it on every REST catalog (issue #395). Set `[coordinator] distributed_credentials = "vended"` too, so workers never receive the static key.

Paths that do not load an Iceberg table have nothing to vend and still use the `[storage]` pair or the SDK chain: file TVFs (gated by `[storage.tvf] allowed_object_store_prefixes`), Hive external tables, and distributed compaction. The vendored REST client does not call the catalog's credential-refresh endpoint; refresh is a full table reload.

See [S3 Credential Vending](../design-notes/s3vending.md) for precedence, expiry, and worker details, and the [security model](../architecture/security-model.md) for where this sits in the trust boundary.

### Fine-grained policy enforcement is off by default

SQE parses the security SQL surface: `GRANT ... MASKED WITH`, `GRANT ... ROWS WHERE`, `SHOW EFFECTIVE GRANTS`, `CHECK ACCESS`. Plan-rewriting enforcement of row filters and column masks is shipped but off by default: the default `[policy] engine = "passthrough"` returns plans unmodified. Set `engine = "ranger"` (Apache Ranger fine-grained policies, row-filter + data-mask, shared with Spark/Kyuubi) or `engine = "in-memory"` (dev and tests) to turn enforcement on.

The gap to know about is the default, not the capability: enforcement does nothing until you select an engine. See [Fine-grained access control](../features/fine-grained-access-control.md), [GRANT and REVOKE](../sql-reference/grant-revoke.md), and [Spark / Ranger Parity](../design-notes/sqe-spark-ranger-parity.md).

### Grant model gaps

Within the grant SQL surface itself, these things are not supported:

- `WITH GRANT OPTION` is admin-only. Its grantee can grant the privilege onward, but not the grant option itself.
- No column-level `INSERT`. INSERT granularity is table-level.
- Mask expressions are scalar only. Aggregate and table-valued mask expressions are rejected.
- `REVOKE SELECT` does not stop reads. Privileges expand to Polaris access types, and `INSERT` keeps `table-properties-read`, which is what unlocks `LOAD_TABLE`. After the load, SQE reads files directly, so `table-data-read` is not the read gate. Unity Catalog keeps SELECT and MODIFY independent. SQE cannot match that with a grant-profile edit, because a writer has to load the table. Close a gate with `REVOKE ALL PRIVILEGES` and confirm with `CHECK ACCESS` (issue #396).

See [GRANT and REVOKE, Closing a gate](../sql-reference/grant-revoke.md#closing-a-gate) and [Known gaps](../sql-reference/grant-revoke.md#known-gaps).

## Iceberg and types

### Iceberg V3 gaps

V3 is the default format-version for new tables and landed end to end: default values, schema evolution, nanosecond timestamps, partition evolution, equality deletes, deletion vectors, row lineage, `VARIANT`, and top-level `UNKNOWN` / `VOID` columns. What is still missing:

| Feature | Status |
|---|---|
| Geometry / geography types | Not supported. No SQE type and no DataFusion user-defined-type support. |
| Multi-arg partition transforms | Not supported. `PARTITIONED BY` accepts single-source transforms only. |
| Shredded variant reads | SQE writes and reads shredded variants, but unshreds on read: a shredded field gives no pruning or typed access, and only top-level variant columns are shredded on write. |
| Unknown list elements and map keys/values | `ARRAY<VOID>`, a map with a `VOID` key or value, and a struct whose members are all unknown can be declared and read, but a row cannot be written: Parquet has no column for them, and Iceberg Java refuses the same writes. Unknown struct members (`STRUCT<a INT, u VOID>`) work. |
| Nested type promotion | `ALTER COLUMN ... SET DATA TYPE` takes a top-level column; a field inside a struct cannot be promoted yet. |

See [Iceberg Integration, V3 features verified](../features/iceberg.md#v3-features-verified) and [DDL, VARIANT](../sql-reference/ddl.md#variant).

## SQL and policy surface

### Statements DataFusion's parser does not accept

`PIVOT`, `UNPIVOT`, `ASOF JOIN`, and FROM-first syntax are not parseable. This is tracked upstream, not an SQE bug. `QUALIFY` works. List comprehensions have no AST node in DataFusion; lambdas work in the higher-order array functions (`filter`, `transform`, `any_match`, `all_match`, `none_match`, `reduce`). The full list, with the reasoning for each, is on the [SQL Reference overview](../sql-reference/index.md#what-is-intentionally-not-in-sqe), and the [SQL cheat-sheet](sql-cheatsheet.md) carries the scannable version.

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

`parallel_probe_scan` and `parallel_scan` ship **on**, and have since 2026-07-08. Both work around the single-partition scan with a post-planning optimizer rule instead of wiring `target_partitions`, so a `CollectLeft` build side is never coalesced and the q72 regression cannot recur. The earlier "+26% on TPC-DS at SF1" figure does not reproduce: a four-arm A/B at SF1 puts the suite total at +4.0%, inside a 5% noise floor, with the cost concentrated in about eight window/rollup queries (q67 +45%, q30 +79%, q31, q22, q29, q47) and wins elsewhere (TPC-H q12 -19%). The two flags also overlap rather than complement: with both on, the probe rule takes the scans first and `parallel_scan` fires twice instead of 26 times. Turn `parallel_probe_scan` off for a TPC-DS-shaped workload of small window and rollup queries; leave it on otherwise. The SF1 A/B that measured this is recorded in the repo under `docs/evidence/perf/`.

### TPC-BB wall time is mostly q01

On the 2026-08-15 Flight SF1 run (`tpcbb-sf1-flight-2026-08-15T14:31:10.json`) the suite is 63.1s and **q01 is 35.5s** of that, 20 rows. The suite still passes 10/10. Do not quote a TPC-BB total without naming q01 (issue #417). Compare totals on the same day (`README.md`) use a different envelope and are 56.3s vs Trino 290.3s.
