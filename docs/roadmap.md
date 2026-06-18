# SQE Roadmap

## Iceberg matrix coverage

**Score: 167/189 (88.4%)** on the public [icebergmatrix.org](https://icebergmatrix.org) scoreboard, fifth overall behind only Spark distributions (EMR, AWS Glue, OSS Spark, Dataproc). See [`docs/iceberg-matrix.md`](iceberg-matrix.md) for the per-cell breakdown and [`docs/iceberg-matrix-compare.md`](iceberg-matrix-compare.md) for the V2/V3 comparison against every other engine on the public scoreboard.

## Completed

### Engine

- [x] Single-node query engine (DataFusion + iceberg-rust + OIDC + Flight SQL)
- [x] Distributed execution (coordinator-worker, shuffle, spill-to-disk)
- [x] DataFusion 54 upgrade (RepartitionExec throughput, hash-join dynamic comparator, faster semi/anti joins; `as_any` blanket-downcast port; `foldhash` shuffle hasher; sqlparser pin aligned to 0.62 so a single parser ships and SQE shares DataFusion's grammar)
- [x] DataFusion 53.1 upgrade (40x faster planning, hash join dynamic filters, three filter-pushdown bug fixes from 53.0 -> 53.1)
- [x] Vendored iceberg-rust fork: DF 53 rebase of `risingwavelabs/iceberg-rust` ported in-tree to DF 54 (apache main + RW rebase branch are both still on DF 53; apache PR #2648 open); AWS SigV4 cargo feature for federated REST endpoints
- [x] Streaming writes (constant-memory CTAS/INSERT, no OOM on SF1+)
- [x] CoW DML scales to TPC-E SF10+ (`IN (subquery)` lifted to a scratch-MemTable + LEFT JOIN; plan size O(1) in subquery cardinality, no stack overflow at 34K+ tuples)
- [x] Trino UDFs split into `sqe-trino-functions` crate (4,175 LOC moved; coordinator incremental builds skip UDF recompile)
- [x] macOS dev build config: `jobs = 8`, `ld64.lld` linker (2-3x faster link on M-series)
- [x] 5-layer metadata caching (SessionContext, RestCatalog, table metadata, manifest, footer)
- [x] ETag validation with Polaris (30-50% fewer REST calls)
- [x] ZSTD compression (shuffle, Parquet writes, Flight SQL responses)
- [x] Star-schema join reorder optimizer rule
- [x] Dynamic filter pushdown with type coercion
- [x] Runtime filter pushdown into IcebergTableScan (TPC-H SF1 -21.3%, SF10 -12.4%)
- [x] Broadcast threshold 64MB (matches Trino/Spark)
- [x] Table statistics from Iceberg snapshot for CBO

### SQL surface

- [x] Trino SQL compatibility ~96% (70+ UDFs, USE, SHOW CREATE TABLE, TRUNCATE, etc.)
- [x] Arrow Flight SQL (DoPut, DoExchange shuffle, GetTableTypes, GetXdbcTypeInfo)
- [x] Trino HTTP compatibility (pagination, headers, dual auth, system.jdbc.*)
- [x] Full DML: CTAS, INSERT INTO, DELETE, UPDATE, MERGE INTO (Copy-on-Write default)
- [x] Merge-on-Read DELETE via `TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')`: position-delete writer (no PK), equality-delete writer (with PK), commits via `FastAppendAction` / `RowDeltaAction`
- [x] JSON columns (alias to Utf8; CAST(json_col AS T) rides DataFusion coercion; full json_extract / json_get_* family)
- [x] TIME / TIME(p) columns mapped to `Time64(Microsecond)`; `localtime()`, `hour() / minute() / second()` work on TIME; `year() / month() / day()` raise plan errors per Trino spec
- [x] Iceberg time travel (FOR SYSTEM_TIME AS OF + FOR VERSION AS OF)
- [x] Iceberg metadata TVFs (snapshots, manifests, history, files, partitions, refs)
- [x] ALTER TABLE schema evolution (ADD/DROP/RENAME COLUMN, type widening, partition evolution)
- [x] PARTITIONED BY for the six standard Iceberg transforms (identity, year, month, day, hour, bucket, truncate, void)
- [x] V3 features end-to-end (TIMESTAMP_NS, column defaults, equality-delete UPDATE on identifier-fields, partition evolution, branching/tagging)

### File-format TVFs and embedded persona (V8 through V12.1)

- [x] V8: `read_parquet` / `read_csv` / `read_json` / `read_avro` TVFs; `SELECT * FROM 'file.ext'` quoted-string auto-detect; `COPY (...) TO 'path' (FORMAT ...)` (DuckDB-parity)
- [x] V9: `.describe`, `.summarize`, `.tables`, `.catalogs`, `.timer`, `.format`, `.read` dot-commands; `SELECT * EXCLUDE` / `REPLACE` documented (DataFusion-native)
- [x] V10: `LazyHttpObjectStoreRegistry` lazy HTTPS object-store; HuggingFace `hf://(datasets|models|spaces)/<owner>/<name>/<path>?revision=<rev>` resolver; AWS provider chain fallback for embedded mode
- [x] V11: `read_delta()` TVF (deltalake-core 0.32.1; time travel via `version` / `timestamp`) -- temporarily disabled on the DataFusion 54 bump (delta-rs has no DF 54 release yet; module kept on disk, re-enable when it ships)
- [x] V12: `'hf://...'` quoted-string auto-detect via SQL pre-rewriter
- [x] V12.1: hf:// inline `@<rev>` revision spec + `@~parquet` auto-generated parquet view (`refs/convert/parquet`)
- [x] V12 follow-up: smart `read_csv` (extension-based delimiter/codec, DuckDB-style `sep`/`delim`/`header`/`nullstr`/`compress` aliases)
- [x] HuggingFace tree-API cache module with TTL + `Link` pagination (V12.2 prerequisite)

### Catalogs

- [x] Apache Polaris (Iceberg REST, default)
- [x] Project Nessie via Iceberg REST (live test against `ghcr.io/projectnessie/nessie:0.107.5`)
- [x] Unity Catalog OSS via Iceberg REST adapter (live test against `unitycatalog/unitycatalog:main-2f2e32d`)
- [x] AWS Glue native SDK (live test against AWS account 311141556126 in eu-central-1)
- [x] AWS S3 Tables native SDK (live test against eu-west-1 testtablebucket/testnamespace/daily_sales)
- [x] AWS Glue / S3 Tables via federated Iceberg REST + SigV4 (alternative path)
- [x] Hive Metastore (Thrift, live test against `apache/hive:standalone-metastore-4.1.0`)
- [x] JDBC: PostgreSQL, MySQL, SQLite (live test against `docker-compose.test.yml` postgres at V2 and V3)
- [x] Hadoop storage-only (warehouse path scanner, read-only)
- [x] Generic loader dispatch through upstream `iceberg-catalog-loader` factory; per-backend wrapper code deleted
- [x] Runtime catalog mounting via SQL `ATTACH` / `DETACH` and credential management via `CREATE` / `DROP` / `SHOW SECRETS` (DuckDB-shape syntax; same six backends plus SQLite for local prototyping). See `docs/book/src/operations/catalogs.md`.

### Auth and security

- [x] Pluggable auth (OIDC, bearer, API key, mTLS, anonymous, AWS IAM, device code, token exchange)
- [x] GRANT/REVOKE/SHOW GRANTS SQL via platform API
- [x] Security audit: 43/43 findings resolved

### Observability and tooling

- [x] Benchmark suite: 7 suites, 222 queries, `--compare-trino`
- [x] SF1 + SF10 differential validation vs Trino on a dedicated rig (single-node + 2-worker distributed), re-confirmed on DataFusion 54; per-query compare reports committed under `benchmarks/results/`
- [x] Parallel + streaming TPC-H data generation (SF1000 in 6:23 on 32 cores)
- [x] dbt adapter (dbt-sqe via ADBC Flight SQL)
- [x] OpenTelemetry + Prometheus + JSON audit log + `system.runtime.queries` virtual table
- [x] OpenLineage 2-0-2 emitter (`sqe-lineage` crate; column-level lineage on INSERT/CTAS/MERGE/UPDATE/DELETE/DDL; file + HTTP sinks; disk-spool fallback; off by default). See `docs/book/src/operations/openlineage.md`.
- [x] OSS release preparation (Apache 2.0, CONTRIBUTING.md, docs)

## In Progress

- [ ] OPA/Cedar policy engine (row filters, column masks)
- [ ] Helm chart for Kubernetes deployment
- [ ] CoW DML scales to TPC-E SF100 (`cow-dml-parallel-streaming`: parallelise per-file rewrite + stream writes + drop double-WHERE; targets `trade_result_update_holding` under 120 s at SF100)
- [ ] Parallel + streaming generation for the other 6 benchmarks (SSB, TPC-DS, TPC-C, TPC-E, TPC-BB, ClickBench)
- [ ] Snowflake Horizon catalog: live integration test against a real Snowflake account (currently REST-compatible, no live test)
- [ ] V12.2: custom `HfObjectStore` plugged into `LazyHttpObjectStoreRegistry` so `SELECT * FROM 'hf://datasets/foo/bar/**/*.parquet'` enumerates files via the HF tree API and the V12 SQL pre-rewriter retires (design in `docs/hf-glob-research.md`)

## Planned

- [ ] SF100 scaling. The single-node tactics that win at SF1/SF10 (broadcast build sides, in-memory hash tables, single scan stream) invert at SF100. Needs memory-pool discipline under concurrency (cap concurrent sort consumers, bound per-consumer reservations, upstream proactive spill `apache/datafusion#17334`) and a proven multi-node distributed path. Generator must also stream row groups to disk first. Predicted failure modes and evidence in [`docs/perf/sf100-scaling-risks.md`](perf/sf100-scaling-risks.md).
- [ ] **DataFusion 54 follow-up: ship physical dynamic filters to workers.** SQE currently ships hash-join dynamic filters to workers via a lossy physical-to-logical `Expr` conversion (`scan_pushdown.rs::physical_filter_to_logical`) that drops the hash-set membership, which is why SSB trails when distributed (the star-join selectivity lives in that membership set). DF 54 added `DynamicFilter` protobuf serialization (`datafusion-proto-54`). Gated on a feasibility spike: confirm a serialized `DynamicFilter` carries the membership set *after the build side seals it*, and that SQE can serialize at that point rather than shipping an empty placeholder. If it works, this is the direct fix for the SSB-distributed gap.
- [ ] **DataFusion 54 follow-up: adopt native-scan wins into the vendored Iceberg reader.** `IcebergScanExec` bypasses DataFusion's native Parquet scan, so DF 54's scan-side improvements do not reach Iceberg queries: struct-field pushdown in the Parquet `RowFilter`, row-group reordering by statistics for TopK queries, and MIN/MAX resolution from Parquet metadata for single-mode aggregates. Port the worthwhile ones into `vendor/iceberg-rust/.../reader.rs`. (NDV-from-metadata is *not* replicable: Iceberg manifests carry min/max/null bounds, not distinct counts. Column min/max/null are already fed to the optimizer via `compute_table_statistics`.) Note: config-default audit (DF 53.1 vs 54) came back clean, so no default pinning is needed.
- [ ] Full cost-based join enumeration (DataFusion upstream DF#3843)
- [ ] Local data file block cache (Alluxio-style)
- [ ] Iceberg Puffin bloom filter reading
- [ ] Map-producing aggregates (`histogram`, `map_agg`, `multimap_agg`) via Arrow `MapBuilder` UDAFs
- [ ] Sort-on-write enforcement (writer pass after planner)
- [ ] Semantic AI layer (RDF/SPARQL, property graph, vector search)
- [ ] Hash join spill support (DataFusion upstream DF#17267)
- [ ] Azure ADLS Gen2 / GCS object stores (one-line `Cargo.toml` feature flip + `register_*_store_if_needed` helpers; tracked in [`cli-embedded.md`](cli-embedded.md))
- [ ] Smart-CSV byte sampling (current `read_csv` infers delimiter / codec from path extension; DuckDB samples bytes for the same)

## Blocked upstream

- [ ] Iceberg V3 Variant type (`apache/iceberg-rust#2188`, `apache/arrow-rs#7142`)
- [ ] Iceberg V3 shredded Variant (`apache/arrow-rs#9790`)
- [ ] Iceberg V3 Geometry types (DataFusion UDT `apache/datafusion#12644`)
- [ ] Iceberg V3 Vector / Embedding types (spec not finalised)
- [ ] Iceberg V3 row lineage (deferred upstream)
- [ ] Multi-argument partition transforms on V3 (spec not stable)
