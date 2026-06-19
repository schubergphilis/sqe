# SQE: Sovereign Query Engine

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**An Iceberg-first SQL server that scales.** Run SQE embedded as one binary on your laptop, or distributed across a cluster of stateless workers behind an Arrow Flight SQL or Trino HTTP endpoint. Same SQL surface, same Iceberg semantics, same identity model.

SQE is a Rust-based SQL query engine for [Apache Iceberg](https://iceberg.apache.org/) tables, built on [DataFusion 54](https://datafusion.apache.org/) and [iceberg-rust](https://github.com/apache/iceberg-rust). Every query runs as the authenticated user. No service account. No shared root.

```bash
# Point SQE at AWS S3 Tables (managed Iceberg) and run it as a SQL server.
cat > sqe.toml <<'EOF'
[catalog]
type             = "s3tables"
table_bucket_arn = "arn:aws:s3tables:us-east-1:ACCOUNT:bucket/sales"
EOF

cargo run --release --bin sqe-coordinator -- sqe.toml &
cargo run --bin sqe-cli -- --host localhost --port 50051

# Iceberg time travel + manifest-derived stats + per-query identity.
sqe> SELECT customer_id, sum(amount)
  ...> FROM s3tables.sales.orders FOR TIMESTAMP AS OF '2026-04-01'
  ...> WHERE region = 'EU' GROUP BY customer_id;

# Snapshot history straight from the metadata.
sqe> SELECT snapshot_id, committed_at FROM s3tables.sales."orders$snapshots";
```

## Why it is cool

**Top-five on the public [Iceberg matrix](https://icebergmatrix.org). 167/189. 88.4%.** Only non-Spark engine in the top five. Per-cell breakdown in [`docs/iceberg-matrix.md`](docs/iceberg-matrix.md); side-by-side against 20 other engines in [`docs/iceberg-matrix-compare.md`](docs/iceberg-matrix-compare.md).

**Wins six of seven benchmark suites against Trino 465 at SF1.** TPC-H, SSB, TPC-DS, TPC-C, TPC-E, TPC-BB, ClickBench. 222 of 222 queries pass, and the results are differentially validated: every query runs against both engines and the rows are diffed, with DuckDB's official `dsdgen` as an independent data oracle. Tables and method below.

**One binary scales from CLI to cluster.** `sqe-cli --embedded` is a DuckDB-class single-process engine with the same SQL surface as the distributed coordinator. Persistent SQLite-backed Iceberg catalogs at `~/.sqe/warehouse/` survive restarts. Cross-catalog joins across multiple `--catalog NAME=PATH` mounts, plus runtime mounts via SQL `ATTACH` against any of the six supported backends (REST, Glue, S3 Tables, HMS, JDBC, SQLite).

**Multi-catalog and multi-cloud, in one engine.** Apache Polaris, Project Nessie, Unity Catalog OSS, AWS Glue (native SDK), AWS S3 Tables (native SDK), Hive Metastore, JDBC (Postgres, MySQL, SQLite), and Hadoop storage-only. Object stores: S3 (with endpoint override for Ceph, R2, Garage, MinIO), Azure ADLS, GCS, local filesystem, HuggingFace `hf://`.

**Identity flows end to end.** OIDC password grant. The user's bearer token is passed through to Polaris and S3 on every query. Row filters and column masks at the LogicalPlan layer are now enforced when `[policy] engine = "ranger"` is set: SQE downloads the `hive` Ranger service policy set and rewrites the plan before DataFusion optimization (row filters above the scan, column masks that block predicate pushdown). Phase 1 covers `MASK_NULL` and row-filter expressions. Phase 2A delivers the full mask vocabulary: hash, partial show-first/last, date truncation to year/month/day, full redact, and custom expressions, all with type preservation through the physical planner. Phase 2B delivers session-context SQL functions (`is_role_in_session`, `current_user`, `current_database`, `current_schema`) that const-fold to literals before plan distribution. Phase 3a delivers tag-based masking: `TagSource` reads Iceberg `sqe.column-tags` properties, `PolicyStore::resolve_tags` maps tags to `MaskType` via Ranger `tagPolicies`, and the rewriter joins them with resource-mask-wins precedence, unmappable-tag fail-closed, and full multi-level namespace identity. Proven by executable rewriter tests; live quickstart demo deferred to Phase 3b. OPA and Cedar policy stores are also on the roadmap. The end-to-end demo is in `quickstart/polaris-ranger-keycloak/`.

**Lineage shipped.** Coordinator emits OpenLineage 2-0-2 events with column-level lineage on writes. File and HTTP sinks. Disk-spool fallback for collector outages. Off by default. [`docs/book/src/operations/openlineage.md`](docs/book/src/operations/openlineage.md).

## How SQE differs

|  | SQE | Trino | DuckDB |
|---|---|---|---|
| **Embedded mode** (one binary, no cluster) | yes | no | yes |
| **Distributed mode** (coordinator + workers) | yes | yes | no |
| **Iceberg V2 + V3 read + write** | native | V2 + partial V3 | extension, read-only |
| **Per-query OIDC bearer passthrough** | yes | service account only | n/a (single-tenant) |
| **Ranger row filters + column masks at LogicalPlan** | yes (Phase 1 + Phase 2A) | no | no |
| **OPA / Cedar policy at LogicalPlan** | roadmap | no | no |
| **GRANT/REVOKE to Polaris or Apache Ranger** | yes | no | no |
| **Multi-catalog in one engine** | 7 backends | one at a time | per-extension |
| **Wire protocols** | Arrow Flight SQL + Trino HTTP | Trino HTTP | extension |
| **Runtime** | Rust binary, no JVM | JVM | C++ binary |
| **Cold start** | sub-second | tens of seconds | sub-second |
| **OpenLineage emitter** | native, column-level | plugin | no |

Two longer comparison docs trace the lineage of these positions:

- vs Trino: [`docs/trino-compatibility.md`](docs/trino-compatibility.md). SQL function and feature parity by category. ~96% coverage.
- vs DuckDB: [`docs/duckdb-comparision.md`](docs/duckdb-comparision.md). What SQE has that DuckDB does not, and vice versa, with the V8 to V12 work that closed the embedded-mode gap.

## Performance receipts (SF1, vs Trino 465)

| Suite | SQE | Trino | Speedup | Pass |
|---|---|---|---|---|
| TPC-H (22) | 16.8s | 26.7s | **1.6x** | 22/22 |
| SSB (13) | 8.3s | 5.8s | **0.70x slower** | 13/13 |
| TPC-DS (99) | 13.4s | 45.6s | **3.4x** | 93/99 |
| TPC-C (8 read) | 0.41s | 2.65s | **6.5x** | 8/8 |
| TPC-E (11) | 9.3s | 172.0s | **18.5x** | 11/11 |
| TPC-BB (10) | 28.0s | 255.7s | **9.1x** | 10/10 |
| ClickBench (43) | 1.3s | 4.46s | **3.4x** | 43/43 |

SQE wins six of seven suites. TPC-DS collapsed from 42.5s to 13.4s after we wired DataFusion's runtime filters into iceberg-rust's scan path through the vendor's `DynamicPredicate` bridge: q82 dropped 1787ms to 113ms (16x), q80 1398ms to 103ms (14x), q13 1317ms to 220ms (6x). The fix is a two-tier pushdown: iceberg-rust samples once per file scan task for row-group / page-index pruning, and a per-batch wrapper catches filters that resolve after the task opened. Earlier in the month we landed the dynamic-filter type-coercion fix that flipped q72 from 10.7s to 0.77s ([docs/blog/2026-05-16-q72-the-nemesis.md](docs/blog/2026-05-16-q72-the-nemesis.md)). SSB is the one suite we still trail; lineorder's uniform FK distribution defeats row-group pruning, so the runtime filter only helps at row level and Trino's vectorized decoder still wins. The remaining 6/99 TPC-DS mismatches are upstream DataFusion ROLLUP / GROUPING() gaps (apache/datafusion#4763, #13993), not engine regressions. The earlier "Our Nemesis" investigation is preserved as [docs/blog/2026-04-16-our-nemesis-q72.md](docs/blog/2026-04-16-our-nemesis-q72.md).

## Performance receipts (SF10, vs Trino 481)

June 2026, after the scan-parallelism work. Both engines run containerized in the same Docker network, read the same Iceberg tables from the same S3 store, and get the same envelope: 8 CPUs, bounded heaps, 5GB per query. Totals per suite; full per-query compare reports live in `benchmarks/results/`.

| Suite | SQE single-node | SQE distributed (2 workers) | Trino 481 | Verdict |
|---|---|---|---|---|
| TPC-H (22) | 130.5s | **95.5s** | 106.4s - 138.6s | SQE distributed wins |
| SSB (13) | **42.0s** | 53.6s | 28.0s - 41.1s | Trino, gap closing |
| TPC-DS (99) | 543.9s | **338.3s** | 328.4s - 468.0s | even |

Trino shows a range because every compare run re-measures it; the high end is the run where two idle SQE worker containers shared the VM. Three changes carried SF10 from "3 to 5x slower than Trino" in the morning to this table in the evening:

1. **Parallel parquet decode.** The iceberg-rust reader overlapped I/O but decoded every file on the one thread polling the merged stream. Scan-bound queries pinned one core while ten sat idle. Now every 128MB byte-range subtask decodes on its own runtime task. TPC-H q06 went from 6.4s to 1.65s distributed.
2. **A fair benchmark rig.** The old rig ran SQE on the host, reading the dockerized S3 store through the port-forward at ~160 MB/s aggregate, while Trino read container-to-container at ~320 MB/s. Half the reported gap was the pipe, not the engine. Measure your rig before you profile your engine.
3. **A greedy memory pool.** FairSpillPool statically split the pool across every registered spillable consumer; wide TPC-DS plans register ~90 of them, capping each at ~95MB of an 8GB pool and failing queries Trino finished in 5GB. The pool is now greedy with tracked consumers; `coordinator.memory_pool = "fair"` restores the old behavior.

Distribution is a per-shape decision, not a default: big fact-to-fact joins (TPC-H, TPC-DS) gain 25 to 40 percent from two workers, while star schemas (SSB) pay shuffle costs that single-node avoids. Known open items at SF10: four TPC-DS inventory queries (q23, q37, q72, q82) fail distributed when the worker's scan buffer outruns Flight shipment and exhausts the 4GB worker pool, and SSB still trails Trino on raw star-join throughput.

A later SF10 pass found a separate blow-up: TPC-H q12, q17, and q10 ran 160 to 300 seconds against Trino's 2 to 8. The cause was not partition layout or join distribution. On a partitioned hash join the build-side runtime filter is a `CASE` over per-partition key sets (q12: eleven branches of ~28K keys, ~300K expression nodes), and the probe scan re-snapshotted it once per batch. Each snapshot rebuilt the whole tree (~10ms), so a 14,600-batch scan spent ~150s reconstructing a filter it then evaluated in under a second. We now cache the first sealed snapshot per scan: q12 161s to 2.7s, q17 176s to 7.1s, q10 from a 300s failure to 3.3s, result rows unchanged, no threshold touched. SSB improved too (q4.1 11.6s to 6.8s), which retired the IN-list-threshold tradeoff we thought we had. The walk-through is in [The Filter That Rebuilt Itself](docs/blog/2026-06-15-the-filter-that-rebuilt-itself.md); the EXPLAIN comparison is in [`docs/perf/sf10-slow-queries.md`](docs/perf/sf10-slow-queries.md).

### Clean-rig SF10: DataFusion 54 (June 18, dedicated host, cache off)

The table above came off a shared VM. These numbers come off a dedicated 8-core box, both engines containerized against the same Iceberg store, query cache off, on DataFusion 54. One run per query, so the cache cannot inflate a sweep. Single-node correctness held: TPC-H 21/22, SSB 13/13, TPC-DS 95/99, the same generator-boundary vacuous rows as SF1, zero dialect diffs, no OOM, and q72 completes.

| Suite | SQE single-node | SQE distributed (2w) | Trino 465 | Verdict |
|---|---|---|---|---|
| TPC-H SF10 | 89.0s | 85.8s | 105.7s | SQE 1.2x |
| SSB SF10 | 31.7s | 29.3s | 14.5s | Trino 2.2x |
| TPC-DS SF10 | 234.0s | 276.0s | 447.8s | SQE 1.9x |

The June-16 read on this rig called SF10 an "honest crossover" where SQE lost (TPC-H 126.4s/0.86x, SSB 31.8s/0.53x, TPC-DS 374s/1.22x on DataFusion 53). That reversed. The parallel Tier-2 scan filter and the move to DataFusion 54 flipped TPC-H to a 1.2x win and lifted TPC-DS to 1.9x. SSB is the one suite SQE still trails: lineorder's uniform foreign-key distribution defeats row-group pruning, so the runtime filter only helps at row level and Trino's vectorized decoder wins. These are single-run numbers on one box; read the ratios as directional, not certified.

Distribution earns little on a single host. Two workers sit co-tenant with the coordinator and Trino on the same eight cores: TPC-H gains four percent, SSB a little, and TPC-DS regresses to 276s while three inventory queries (q23, q37, q72, q82) exhaust the 4GB worker pool and drop the pass count from 95/99 to 92/99. Distribution is a per-shape decision, and its real payoff needs separate worker hosts, which this rig does not have. A true multi-node verdict is still missing, and it is the SF100 question below.

Loading SF10 surfaced a separate write-path gap worth naming: a partitioned `CREATE TABLE AS SELECT` with a sort-on-write clustering hint fans the sort into one merge buffer per output partition, and that merge phase cannot spill. At SF10 the monthly-partitioned TPC-H lineitem (60M rows across ~84 partitions) exhausted the pool where the unpartitioned SSB lineorder of the same size sorted fine. The bench loader now skips the redundant sort on already-partitioned tables, since partition pruning already delivers the clustering; the engine-level fix (bounded or spillable partition writers) is still open.

### SF100: coming

SF100 is the next frontier, and it inverts the SF1 and SF10 playbook. Broadcasting the build side, building hash tables in memory, and emitting one scan stream all win at SF10. Each becomes the bottleneck at SF100. Getting there needs three things: memory-pool discipline under concurrency (cap concurrent sort consumers, bound per-consumer reservations, proactive spill), a proven multi-node distributed path on separate worker hosts rather than one shared box, and a data generator that streams row groups to disk instead of buffering a whole table in memory. The predicted failure modes, each grounded in something we actually observed at SF1 or SF10, are written up in [`docs/perf/sf100-scaling-risks.md`](docs/perf/sf100-scaling-risks.md).

### How we know the numbers are real

A benchmark row that says "Match" can still validate nothing: if the generated data contains no rows a query can select, both engines agree on empty and the diff passes. We learned this the hard way. Since June 2026 the harness reports those cases as `Vacuous`, and the generators are validated against DuckDB's official `dsdgen` output as an engine-free oracle (`scripts/validate-generator-tpcds.py`). That oracle caught a TPC-C generator bug that zeroed every warehouse join at fractional scales and a set of TPC-DS vocabulary gaps that had silently blanked 16 query results. Details in [the validation blog post](docs/blog/2026-06-12-the-benchmark-that-lied.md).

The same validation pass found one query where the engines disagree and SQE is right: TPC-DS q75 returns 57 rows on SQE and 55 on Trino, because Trino's `DECIMAL(17,2)` division rounds two sales ratios of 0.8983 and 0.8984 up to 0.90 and drops them from the `< 0.9` filter. DuckDB returns SQE's exact 57 rows on the same parquet files.

Distributed mode gets the same scrutiny on a forced-distribution rig (single worker, distribution threshold zero, every fact scan shipped over Arrow Flight). That rig exposed that dynamic join filters never reached the workers; fixing the pushdown took TPC-DS SF1 under that worst case from 4.4x slower than Trino to 1.7x faster. SSB under the rig still trails: its star-join selectivity lives in the hash-set membership of the runtime filter, which a serialized predicate cannot carry. Shipping build-side key sets (bloom filters) to workers is the open follow-up.

Run your own:

```bash
BENCH_SCALE=1 ./scripts/benchmark-test.sh --compare-trino tpch tpcds ssb clickbench

# differential compare against a live Trino on the same Iceberg catalog
sqe-bench compare tpcds --scale 1 --trino-url http://localhost:38080

# validate generated TPC-DS data against DuckDB's official dsdgen
duckdb /tmp/dsdgen.db -c "INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=1)"
scripts/validate-generator-tpcds.py --ours data/tpcds/sf1 --dsdgen-db /tmp/dsdgen.db
```

## Architecture

```
Client (JDBC / Flight SQL / HTTP)
        |
        v
   +-----------+     OIDC Provider
   |Coordinator|<-- (Keycloak, Auth0,
   |           |     Okta, or any IdP)
   | DataFusion|
   |  + Policy |---> OPA / Cedar
   +-----+-----+
         | Bearer token passthrough
    +----+----+
    v    v    v
   +---++---++---+   Stateless workers
   | W1|| W2|| W3|  (distributed mode)
   +---++---++---+
    |     |     |
    v     v     v
   +-----------+
   |  Polaris  |---> S3-compatible storage
   |REST Catalog|
   +-----------+
```

Detailed Mermaid diagrams (query pipeline, crate dependencies, caching layers, distributed execution, write path) in [`docs/architecture.md`](docs/architecture.md).

## Web UI

A read-only ops dashboard ships in the binary, on the coordinator's health port (`metrics_port + 1`). Queries with per-fragment timing, cluster nodes, and live engine metrics (stat cards, a query-activity histogram, memory and concurrency gauges), all from the coordinator's in-memory state. No login (network-gated), no build step, no external assets. Toggle with `[metrics] web_ui`. Full reference: [`docs/book/src/operations/web-ui.md`](docs/book/src/operations/web-ui.md).

![SQE web UI: the Overview dashboard](docs/book/src/images/sqe-web-ui-overview.png)

## Get started

Five-minute walkthrough covering all seven catalog backends with sample TOML and verification queries: [`QUICKSTART.md`](QUICKSTART.md).

### Embedded mode (one binary, no cluster)

```bash
cargo install --path crates/sqe-cli
sqe-cli --embedded                  # persistent warehouse at ~/.sqe/warehouse/

sqe> SELECT * FROM '/data/sales.parquet' LIMIT 5;
sqe> SELECT * FROM read_csv('s3://bucket/orders.tsv.gz');
sqe> SELECT * FROM 'hf://datasets/squad/plain_text/train-00000-of-00001.parquet' LIMIT 5;
sqe> SELECT * FROM read_delta('/data/delta/sales', version => '5');

sqe> CREATE SECRET partner (TYPE bearer, TOKEN 'eyJ...');
sqe> ATTACH 'http://catalog.example.com/api/catalog' AS partner_cat
       (TYPE iceberg_rest, WAREHOUSE 'analytics', SECRET partner);
sqe> SELECT * FROM partner_cat.sales.orders LIMIT 10;
```

Full embedded reference: [`docs/cli-embedded.md`](docs/cli-embedded.md). Runtime ATTACH / SECRET reference: [`docs/book/src/operations/catalogs.md`](docs/book/src/operations/catalogs.md).

### Cluster mode (Polaris + S3 + SQE locally)

```bash
docker compose -f docker-compose.test.yml up -d
./scripts/bootstrap-test.sh
cargo run --release --bin sqe-coordinator -- tests/sqe-test.toml

# Connect with the CLI
cargo run --bin sqe-cli -- --host localhost --port 50051 --username root --protocol flight

sqe> SHOW CATALOGS;
sqe> SELECT * FROM test_warehouse.default.my_table LIMIT 10;
```

Same binary against external infrastructure (Glue, S3 Tables, HMS, JDBC, Hadoop): see [`QUICKSTART.md`](QUICKSTART.md) and [`docs/catalogs.md`](docs/catalogs.md).

Docker, Kubernetes, TLS, and auth provider setup: [`docs/deployment.md`](docs/deployment.md).

## Documentation

The reference docs:

| Doc | What |
|---|---|
| [Architecture](docs/architecture.md) | Mermaid diagrams across the engine |
| [Deployment](docs/deployment.md) | Docker Compose, K8s, TLS, auth providers, monitoring |
| [Operational Runbook](docs/runbook.md) | On-call triage: crashloops, catalog/OIDC outages, OOM, registry flap |
| [Iceberg Matrix](docs/iceberg-matrix.md) | Per-cell SQE coverage on the public scoreboard |
| [Iceberg Matrix Comparison](docs/iceberg-matrix-compare.md) | V2/V3 side-by-side against 20 engines |
| [Trino Compatibility](docs/trino-compatibility.md) | SQL function and feature matrix vs Trino |
| [DuckDB Comparison](docs/duckdb-comparision.md) | Symmetry between SQE and DuckDB on the embedded side |
| [Embedded CLI Reference](docs/cli-embedded.md) | All flags, dot-commands, TVFs, catalog backends, storage backends |
| [SQL Feature Comparison](docs/features.md) | SQE vs Trino vs Spark SQL vs DuckDB across windows, aggregates, DML, Iceberg, file-format TVFs |
| [SQL Reference (book)](docs/book/src/sql-reference/index.md) | Every function, statement, operator, TVF, CALL procedure, GRANT extension, with origin and Trino / Snowflake / Spark / DuckDB alias columns |
| [Catalog Backends](docs/catalogs.md) | Per-backend TOML, credentials, verification queries |
| [Storage Backends](docs/book/src/getting-started/storage-backends.md) | S3, R2, MinIO/Ceph, Azure ADLS Gen2, Google Cloud Storage, HTTPS, hf:// |
| [Operations: OpenLineage](docs/book/src/operations/openlineage.md) | Lineage emit, sinks, troubleshooting |
| [Benchmark history](docs/benchmark/index.md) | Per-suite, per-scale, per-query plots over time |
| [Roadmap](docs/roadmap.md) | Full feature checklist |
| [Security Audit](docs/issues.md) | 43 findings, all resolved |

## The book

SQE's design and development journey is documented in **"Sovereign by Design: Building a Production Query Engine on DataFusion"**.

Twenty chapters across five parts. Roughly 370 pages. The story of choosing DataFusion, surviving the Iceberg fork rebase, lifting the matrix from 31% to 88%, building the embedded mode that turned out to be a DuckDB-shaped surprise, and shipping column-level lineage. Source in [`docs/ebook/`](docs/ebook/). Build with `cd docs/ebook && make`.

A few chapters worth reading first:

- [chapter 04 ("You Are the Query")](docs/ebook/chapters/04-you-are-the-query.md) on per-query identity
- [chapter 09 ("What You Cannot See")](docs/ebook/chapters/09-what-you-cant-see.md) on observability
- [chapters 16b and 16c](docs/ebook/chapters/) on the Iceberg matrix journey from 99/189 to 164/189
- [chapter 16d ("The DuckDB Drift")](docs/ebook/chapters/16d-the-duckdb-drift.md) on building embedded mode in two days
- [chapter 16e ("The Lineage Trail")](docs/ebook/chapters/16e-the-lineage-trail.md) on shipping OpenLineage
- [chapter 17 ("What We Would Do Differently")](docs/ebook/chapters/17-what-wed-do-differently.md) on the retro

## Blog

Engineering posts that double as design rationale:

| Post | Topic |
|---|---|
| [Why We Replaced Trino with Rust](docs/blog/2026-03-22-why-we-replaced-trino-with-rust.md) | The decision to build SQE |
| [Five Layers of Caching and an 8.8x Speedup](docs/blog/2026-04-12-caching-and-the-8x-speedup.md) | Caching strategy across the stack |
| [Security Hardening: 43 Findings](docs/blog/2026-04-13-security-hardening-43-findings.md) | Production audit |
| [DataFusion 53 and the Iceberg Fork](docs/blog/2026-04-14-datafusion-53-and-the-iceberg-fork.md) | DF 53 upgrade and vendoring decision |
| [Our Nemesis: TPC-DS Q72](docs/blog/2026-04-16-our-nemesis-q72.md) | The one query we cannot beat |
| [Why a Public Iceberg Matrix Beats Vendor Spec Sheets](docs/blog/2026-04-29-the-iceberg-matrix-as-a-scoreboard.md) | A scoreboard for the lakehouse |
| [SQE Talks to Five Catalogs Now](docs/blog/2026-04-29-five-catalogs-live.md) | The live verification phase plus AWS SigV4 |
| [How We Accidentally Created a DuckDB](docs/blog/2026-05-07-accidentally-duckdb.md) | V8 to V12: file-format TVFs, hf://, Delta, smarter read_csv |
| [Shipping OpenLineage](docs/blog/2026-05-09-shipping-openlineage.md) | Column-level lineage from idea to merged MR |
| [The Benchmark That Lied](docs/blog/2026-06-12-the-benchmark-that-lied.md) | Vacuous results, the DuckDB oracle, and the day Trino was wrong |
| [The Filter That Rebuilt Itself 14,600 Times](docs/blog/2026-06-15-the-filter-that-rebuilt-itself.md) | A runtime filter re-snapshotted per batch made q12 161s instead of 2.7s |

Full archive in [`docs/blog/`](docs/blog/).

## Crate structure

| Crate | Purpose |
|---|---|
| `sqe-core` | Shared types, config (TOML), errors |
| `sqe-sql` | SQL parser, statement classifier, GRANT/REVOKE |
| `sqe-auth` | Pluggable auth chain (10 providers), token cache |
| `sqe-catalog` | Iceberg REST client, caching, scan execution |
| `sqe-policy` | Policy enforcement (passthrough, OPA) |
| `sqe-planner` | Plan splitting, star-schema reorder, join strategy |
| `sqe-coordinator` | Flight SQL server, query handler, Trino HTTP |
| `sqe-worker` | Stateless DataFusion executor |
| `sqe-cli` | Interactive SQL client (cluster + embedded modes) |
| `sqe-metrics` | Prometheus, OpenTelemetry, audit logger |
| `sqe-lineage` | OpenLineage 2-0-2 emitter; column-level lineage |
| `sqe-trino-compat` | Trino wire protocol |
| `sqe-bench` | Benchmark suite (7 suites, 222 queries) |

## Tech stack

| Component | Technology |
|---|---|
| Language | Rust |
| Query Engine | Apache DataFusion 54 |
| Table Format | Apache Iceberg V2 / V3 |
| Catalogs | Polaris, Nessie, Unity Catalog OSS, AWS Glue, AWS S3 Tables, Hive Metastore, JDBC, Hadoop |
| Wire Protocols | Arrow Flight SQL + Trino HTTP |
| Storage | S3, Ceph, R2, ADLS Gen2, GCS, local filesystem, HuggingFace `hf://` |
| Observability | OpenTelemetry, Prometheus, OpenLineage 2-0-2, read-only web UI (queries/tasks/workers/metrics dashboard with 12h history) on the health port |
| License | Apache 2.0 |


## Contributing

Issues, pull requests, and how to run tests: [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache License 2.0. See [LICENSE](LICENSE).
