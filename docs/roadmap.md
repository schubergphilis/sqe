# SQE Roadmap

## Completed

- [x] Single-node query engine (DataFusion + iceberg-rust + OIDC + Flight SQL)
- [x] Distributed execution (coordinator-worker, shuffle, spill-to-disk)
- [x] Full DML: CTAS, INSERT INTO, DELETE, UPDATE, MERGE INTO (Copy-on-Write)
- [x] CoW DML scales to TPC-E SF10+ (`IN (subquery)` lifted to a scratch-MemTable + LEFT JOIN; plan size O(1) in subquery cardinality, no stack overflow at 34K+ tuples)
- [x] Trino UDFs split into `sqe-trino-functions` crate (4,175 LOC moved; coordinator incremental builds skip UDF recompile)
- [x] macOS dev build config: `jobs = 8`, `ld64.lld` linker (2-3x faster link on M-series)
- [x] Streaming writes (constant-memory CTAS/INSERT, no OOM on SF1+)
- [x] Trino HTTP compatibility (pagination, headers, dual auth, system.jdbc.*)
- [x] Trino SQL compatibility ~95% (70+ UDFs, USE, SHOW CREATE TABLE, TRUNCATE, etc.)
- [x] Arrow Flight SQL (DoPut, DoExchange shuffle, GetTableTypes, GetXdbcTypeInfo)
- [x] Iceberg time travel (SYSTEM_TIME AS OF)
- [x] Iceberg metadata TVFs (snapshots, manifests, history, files, partitions, refs)
- [x] ALTER TABLE schema evolution (ADD/DROP/RENAME COLUMN, type widening)
- [x] Pluggable auth (OIDC, bearer, API key, mTLS, anonymous, AWS IAM, device code, token exchange)
- [x] 5-layer metadata caching (SessionContext, RestCatalog, table metadata, manifest, footer)
- [x] ETag validation with Polaris (30-50% fewer REST calls)
- [x] ZSTD compression (shuffle, Parquet writes, Flight SQL responses)
- [x] Star-schema join reorder optimizer rule
- [x] Dynamic filter pushdown with type coercion
- [x] Broadcast threshold 64MB (matches Trino/Spark)
- [x] Table statistics from Iceberg snapshot for CBO
- [x] Security audit: 43/43 findings resolved
- [x] DataFusion 53 upgrade (40x faster planning, hash join dynamic filters)
- [x] Vendored iceberg-rust fork with DF 53 rebase
- [x] GRANT/REVOKE/SHOW GRANTS SQL via platform API
- [x] Benchmark suite: 7 suites, 222 queries, --compare-trino
- [x] dbt adapter (dbt-sqe via ADBC Flight SQL)
- [x] OSS release preparation (Apache 2.0, CONTRIBUTING.md, docs)

## In Progress

- [ ] OPA/Cedar policy engine (row filters, column masks)
- [ ] Pluggable catalog backends (AWS Glue, Nessie, Hive Metastore)
- [ ] Helm chart for Kubernetes deployment
- [ ] CoW DML scales to TPC-E SF100 (`cow-dml-parallel-streaming` change: parallelise per-file rewrite + stream writes + drop double-WHERE; targets `trade_result_update_holding` under 120 s at SF100)

## Planned

- [ ] Full cost-based join enumeration (DataFusion upstream DF#3843)
- [ ] Local data file block cache (Alluxio-style)
- [ ] Iceberg Puffin bloom filter reading
- [ ] Semantic AI layer (RDF/SPARQL, property graph, vector search)
- [ ] Hash join spill support (DataFusion upstream DF#17267)
