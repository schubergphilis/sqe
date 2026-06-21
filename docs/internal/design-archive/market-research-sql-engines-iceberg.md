# Market Research: Open-Source SQL Query Engines for Apache Iceberg

**Date:** March 2026 | **License scope:** Apache 2.0 and MIT only
**Perspective:** Data lake data manager evaluating engines for a production Iceberg lakehouse

---

## Executive Summary

Fourteen query engines with Apache 2.0 or MIT licences support Apache Iceberg to varying degrees. They split into four strategic archetypes:

| Archetype | Engines | Best for |
|---|---|---|
| **Distributed MPP (JVM)** | Trino, Spark SQL, Presto | Large-scale batch analytics, existing Hadoop/JVM teams |
| **Distributed MPP (native)** | StarRocks, ClickHouse, Doris, Impala | Sub-second interactive queries, low latency ad-hoc |
| **Embedded / single-node** | DuckDB, DataFusion | Notebooks, embedded apps, local dev, custom engine building |
| **Streaming-first** | Flink SQL, RisingWave | Real-time ingestion into Iceberg, CDC, event-driven lakehouse |

**Short recommendation by role:**
- *Data engineer building a lakehouse:* Trino (read) + Flink SQL (write/ingest)
- *Analytics platform for BI tools:* StarRocks or Trino
- *Real-time lakehouse with streaming:* Flink SQL or RisingWave
- *Embedded analytics / light footprint:* DuckDB
- *Building a custom engine:* Apache DataFusion (this project: SQE)
- *Enterprise with Cloudera:* Impala (already there)
- *Maximum query performance without JVM:* StarRocks

---

## Evaluation Criteria

Scored 1–5 from a **data lake data manager** perspective. Weights reflect production lakehouse priorities.

| Criterion | Weight | What it measures |
|---|---|---|
| **Iceberg completeness** | 22% | Read, write, time travel, MERGE, schema evolution, Iceberg v3, REST catalog |
| **Security & governance** | 20% | Auth (OIDC/LDAP), RBAC, column masking, row filtering, audit log |
| **Operational burden** | 18% | Deployment complexity, K8s readiness, tuning overhead, resource usage |
| **Query performance** | 15% | Interactive latency, throughput, benchmark results |
| **Ecosystem / integrations** | 13% | BI tool support (JDBC/ODBC), dbt, Arrow Flight SQL, catalog range |
| **Community & longevity** | 12% | Stars, contributors, governance body, commercial backing, release velocity |

---

## Engine Profiles

### 1. Apache Trino
**License:** Apache 2.0 | **Runtime:** JVM | **Governance:** Trino Software Foundation

**Architecture:** Distributed MPP — coordinator + stateless workers. All execution in-memory across workers; optional spill to disk. Push-down execution.

**Iceberg support:**
- Read: full — partition pruning, column stats, predicate pushdown
- Write: full — INSERT, CTAS, INSERT OVERWRITE, UPDATE, DELETE, MERGE INTO (MoR with position and equality deletes)
- Time travel: `FOR VERSION AS OF <snapshot>` / `FOR TIMESTAMP AS OF`
- Schema evolution: add, drop, rename, nested struct changes
- REST catalog (Polaris, Nessie, Lakekeeper), HMS, AWS Glue, JDBC, Hadoop
- Table maintenance: `expire_snapshots`, `remove_orphan_files`, `rewrite_data_files`, `rewrite_manifests`
- **Iceberg v3:** not yet complete; v2 fully supported

**Security:**
- OPA authorizer: column masking, row filtering, batch policy evaluation — first-class since early 2024
- Apache Ranger integration: full ACL + data masking
- File-based access control
- OIDC / JWT / Kerberos / LDAP at coordinator
- ⚠️ Row/column security is Trino-side enforcement — NOT pushed into the Iceberg REST catalog spec

**Deployment:** K8s (Helm, Stackable operator), Docker; Trino Gateway for multi-cluster routing. JVM startup and GC tuning required.

**Performance:** Best-in-class for large Iceberg lakes. LinkedIn: millions of queries/month, hundreds of PB. Netflix: 3,500+ QPS at exabyte scale. Weakness: high JVM memory pressure on large aggregations/sorts.

**Commercial:** Starburst Enterprise / Galaxy — adds Warp Speed (reflection acceleration), advanced Ranger RBAC, cluster lifecycle management.

**Who uses it and why:**
- Netflix, LinkedIn, Apple, Lyft, Stripe, Goldman Sachs, Nielsen
- Chosen for: the widest Iceberg connector ecosystem, best MERGE INTO support, OPA integration, proven petabyte-scale deployments

**Weaknesses:** JVM GC overhead; no built-in streaming ingestion; no materialized view acceleration in OSS (Starburst Enterprise only); REST catalog row/column security is Trino-enforced not catalog-enforced.

---

### 2. Apache Spark SQL
**License:** Apache 2.0 | **Runtime:** JVM (+ Comet/Rust, Gluten/Velox) | **Governance:** Apache Foundation

**Architecture:** Unified batch + streaming DAG engine. Driver + executors on YARN / Kubernetes. Spark 4.0 (May 2025). DataFusion Comet (Dec 2025) replaces JVM physical plans with Rust execution.

**Iceberg support:**
- The reference implementation — all Iceberg features are Spark-first
- Read + Write: full — INSERT, CTAS, MERGE INTO, UPDATE, DELETE, INSERT OVERWRITE, UPSERT
- Time travel: `VERSION AS OF` / `TIMESTAMP AS OF`
- Schema evolution: complete — add, drop, rename, reorder, type promotion, nested structs
- Partition evolution: hidden partitioning, all transform types
- REST, HMS, Hadoop, Glue, Nessie, JDBC catalogs
- Maintenance: full procedure suite
- **Iceberg v3:** Iceberg 4.0 + Spark 4.0 are first movers (row lineage, variant type)

**Security:**
- No built-in column masking or row filtering at engine level
- Fully delegated: Apache Ranger (HMS-based), Lake Formation, Unity Catalog, Polaris RBAC
- YARN: Kerberos; Kubernetes: service accounts; Spark Thrift Server: LDAP

**Deployment:** K8s native (Spark-on-K8s, operator), YARN, standalone. JVM driver + executors. Managed: Databricks, EMR, Dataproc, HDInsight. Kyuubi as multi-tenant gateway.

**Performance:** Best for large batch ETL and complex multi-join queries. Not optimized for sub-second interactive (seconds of task scheduling overhead). Comet/Gluten: significantly faster on compute-heavy workloads.

**Commercial:** Databricks (Delta-first, Iceberg interop), Cloudera (Ranger), AWS EMR, GCP Dataproc.

**Who uses it and why:**
- Apple, Netflix, Airbnb, Uber, Alibaba, Meta, LinkedIn — virtually every large data organization
- Chosen for: deepest Iceberg feature support, streaming + batch unification, largest ecosystem

**Weaknesses:** Not suitable for interactive ad-hoc queries (<5 second latency); JVM GC; resource-heavy; security (row/column) requires external systems; Spark is a compute framework, not a query service.

---

### 3. Apache Flink SQL
**License:** Apache 2.0 | **Runtime:** JVM | **Governance:** Apache Foundation

**Architecture:** Unified streaming + batch dataflow engine. Stateful stream processing with incremental checkpointing (ForSt/RocksDB). Flink 2.0 (March 2025). Disaggregated state storage in 2.0 for cloud-native.

**Iceberg support:**
- Streaming sink: exactly-once write to Iceberg, micro-commit on checkpoint
- Dynamic Iceberg Sink (2025): write to multiple tables dynamically, automatic schema + partition evolution without job restart
- CDC via Flink CDC 3.x → Iceberg: full row-level replication
- Batch read: full with predicate pushdown
- Incremental streaming read: new snapshot consumption
- MERGE INTO: **not supported** — biggest limitation
- Catalogs: HMS, REST, Glue, Hadoop, Nessie, JDBC
- **Flink 2.0 Iceberg support:** arriving with Iceberg 1.10.0 (pending as of March 2026)

**Security:** No data-level security in SQL layer; Kubernetes RBAC for operators; delegates to platform (Ranger etc.)

**Deployment:** K8s (Flink Kubernetes Operator), YARN, standalone. Managed: Confluent Platform (GA 2025), AWS Kinesis Data Analytics, Ververica Platform.

**Performance:** Best-in-class for real-time streaming ingestion into Iceberg with exactly-once semantics. Confluent Tableflow writes Iceberg natively.

**Commercial:** Ververica Platform, Confluent Platform for Flink, AWS KDA.

**Who uses it and why:**
- Alibaba, ByteDance, Meituan, Uber, Lyft, Netflix (streaming)
- Chosen for: only mature streaming SQL engine with exactly-once Iceberg write; CDC integration; dynamic multi-table sinks; Confluent ecosystem

**Weaknesses:** MERGE INTO not supported; high operational expertise requirement (checkpointing, watermarks, job management); interactive query latency is high; Flink 2.0 Iceberg connector pending.

---

### 4. DuckDB
**License:** MIT | **Runtime:** C++ | **Governance:** DuckDB Foundation

**Architecture:** Embedded, in-process single-node OLAP engine. Runs inside the application process. No server. Versions 1.4.0 (LTS, Sep 2025). WASM: runs in the browser.

**Iceberg support (via `iceberg` extension):**
- Read: full — partition pruning, predicate pushdown, time travel
- Write: INSERT, UPDATE, DELETE (v1.4+); requires Iceberg REST catalog (no direct S3 metadata write)
- REST catalog: any compliant REST endpoint with OAuth2 (Polaris, Nessie, S3 Tables, SageMaker Lakehouse)
- WASM: read AND write Iceberg REST from browser (announced Dec 2025 / Jan 2026) — unique capability
- Time travel: `AS OF` supported
- ⚠️ DuckLake: new MIT-licensed format by DuckDB team (May 2025) — simpler than Iceberg, metadata in SQL DB; separate from and not competing with Iceberg in the enterprise

**Security:** None built-in — embedded single-user model. MotherDuck adds team access controls.

**Deployment:** Embedded library (no server, no K8s operator). MotherDuck = managed multi-user cloud service.

**Performance:** Fastest single-node OLAP on local storage or S3. Surpassed ClickHouse on ClickBench Parquet in 2025. Peak memory <2.5 GB even on 2 TB datasets with partitioning. Ceiling: single-node only.

**Commercial:** MotherDuck (cloud service, collaborative, hybrid execution scale-out).

**Who uses it and why:**
- Data scientists, analysts, dbt developers, notebook users, embedded SaaS analytics
- Chosen for: zero infrastructure, MIT license permissive for embedding, fastest single-node performance, browser/WASM capability

**Weaknesses:** No distributed execution (hard single-node ceiling); no built-in security/RBAC; no streaming ingestion; MERGE INTO requires REST catalog not direct S3; not suitable for multi-terabyte interactive queries.

---

### 5. Apache DataFusion (+ Ballista)
**License:** Apache 2.0 | **Runtime:** Rust | **Governance:** Apache Foundation

**Architecture:**
- **DataFusion:** Embeddable query engine *library* in Rust. In-process, columnar vectorized, Arrow-native. Not a standalone server — embedded in applications.
- **Ballista:** Distributed scheduler/executor on DataFusion (v43.0.0, Feb 2025). First distributed write support in 43.0.
- **Comet:** Spark plugin replacing JVM physical plans with DataFusion execution (Dec 2025).

**Iceberg support (via `iceberg-rust` v0.8.0, Jan 2026):**
- Read: full — partition pruning, predicate pushdown, schema evolution
- Write: partitioned INSERT with parallel writer (0.8.0)
- Catalogs: REST (OAuth2), S3Tables, Glue, SQL catalog, Memory, FileIO (Hadoop-style)
- Time travel: snapshot-based reads
- `iceberg-datafusion` crate: `TableProvider`, `SchemaProvider`, `CatalogProvider` integration

**Security:** None built-in — security is the embedding application's responsibility (SQE uses this to build plan-rewrite-based enforcement).

**Deployment:** Library embedding. Ballista: native binaries, Docker, K8s. No official K8s operator. DataFusion 52.0.0 (Jan 2026), 121 contributors.

**Performance:** Fastest Parquet reader on ClickBench 2025 — first Rust engine at #1, faster than DuckDB and ClickHouse on same hardware.

**Commercial:** No official commercial distribution. Embedded by: InfluxDB 3.0, GlareDB, CeresDB, SDF, LanceDB, dozens of startups.

**Who uses it and why:**
- InfluxDB (InfluxData), LanceDB, Dask DataFusion backend, this project (SQE)
- Chosen for: building custom query engines; zero-GC Rust; fastest Parquet read; native Iceberg-rust integration; composable + extensible via traits

**Weaknesses:** Not a user-facing server product — requires application wrapping; Ballista less mature than Spark/Flink; security must be built by the embedding application; no end-user interface out of the box.

---

### 6. StarRocks
**License:** Apache 2.0 | **Runtime:** C++ (execution) + Java (FE) | **Governance:** Linux Foundation

**Architecture:** Shared-nothing MPP (native) + shared-data (compute-storage separation, NVMe cache). Vectorized C++ execution. Unified OLAP + data lake in one binary. StarRocks 4.0 (Oct 2025).

**Iceberg support (v4.0):**
- Read: full, vectorized, hidden partitioning, partition pruning
- Write: native Iceberg table writes (first-class in v4.0)
- Schema evolution: supported
- Time travel: supported
- Catalogs: HMS, REST (Polaris, Nessie), Glue, JDBC, Hadoop, DLF (Alibaba), GCS
- New Iceberg compaction API in 4.0
- **Iceberg v3:** planned for 2026

**Security:**
- Native RBAC: fine-grained on catalog/schema/table/column
- Column-level security: supported
- Row-level security: partial (access control, not full catalog-pushed filter)
- External catalog RBAC integration
- Ranger available via Cloudera deployments

**Deployment:** K8s (Helm, StarRocks Operator), Docker, bare metal. Shared-data mode for cloud-native elasticity.

**Performance:** Claims 6.93x faster than Trino on Iceberg catalog queries. ~60% faster TPC-DS year-over-year in v4.0. Sub-second ad-hoc on large Iceberg tables. No JVM in execution path.

**Commercial:** CelerData (cloud managed service).

**Who uses it and why:**
- Airbnb, Salesforce, Tencent, Meituan, JD.com, Xiaomi
- Chosen for: best interactive query performance on Iceberg without pre-aggregation; no JVM overhead; unified OLAP + lake engine; strong cloud-native deployment

**Weaknesses:** No streaming ingestion (batch only to Iceberg); write path to Iceberg less mature than read; Iceberg v3 not yet; heavy China enterprise adoption bias; limited Western enterprise reference customers vs Trino.

---

### 7. Apache Doris
**License:** Apache 2.0 | **Runtime:** C++ (BE) + Java (FE) | **Governance:** Apache Foundation

**Architecture:** MPP columnar database. Shared-nothing FE + BE. Real-time analytics + external lakehouse. Doris 3.0: compute-storage decoupled (cloud-native). Doris 3.1: enhanced materialized views and Iceberg/Paimon support.

**Iceberg support:**
- Read: native, vectorized, predicate pushdown
- Write: CREATE, INSERT INTO (since 2.1.6); MERGE INTO in progress — **not yet complete**
- Schema evolution: supported
- Time travel: supported
- Catalogs: HMS, Hadoop, REST, Glue, Dataproc Metastore, Alibaba DLF, JDBC
- Position + equality deletes: supported
- >1M files case: latency reduced 100s → 10s via async batch file shard fetching

**Security:**
- Built-in RBAC, column masking, row-level policy
- Ranger integration available

**Deployment:** K8s (Doris Operator, Helm), bare metal. Java FE, C++ BE.

**Performance:** 3-5x faster than Trino/Presto on TPC-H/TPC-DS (vendor benchmarks). Strong real-time ingestion via Flink/Kafka → Doris. Compute-storage decoupled in 3.0.

**Commercial:** VeloDB, SelectDB (cloud-managed, same founding team as Doris — causes roadmap confusion).

**Who uses it and why:**
- Meituan, JD.com, Xiaomi, NetEase, ByteDance
- Chosen for: unified real-time OLAP + lakehouse; built-in materialized views; sub-second freshness via streaming; strong in Chinese enterprise ecosystem

**Weaknesses:** MERGE INTO on Iceberg incomplete; primarily strong in Chinese ecosystem; VeloDB/SelectDB commercial split creates confusion; less mature Iceberg write vs read; Western enterprise references limited.

---

### 8. ClickHouse
**License:** Apache 2.0 ⚠️ *open-core — some features are cloud-only (SharedMergeTree, etc.)* | **Runtime:** C++ | **Governance:** ClickHouse Inc.

**Architecture:** Columnar OLAP database, shared-nothing MPP. MergeTree storage family (native). Iceberg is an "external table" style integration. ClickHouse 25.x (2025).

**Iceberg support (25.x):**
- Read: full — predicate pushdown, schema evolution, time travel
- Write: INSERT (25.8+), ALTER DELETE (25.8), ALTER UPDATE (25.10), distributed write (25.10)
- REST catalog: supported including Polaris
- AWS Glue catalog: beta (25.8+)
- Databricks Unity Catalog: beta (25.8+)
- DataLakeCatalog engine: open source
- ⚠️ Schema-change edge cases: ClickHouse was not designed for externally-changing schemas — Iceberg Table Engine has edge cases with column renames/type promotions
- **Iceberg v3:** not targeted for 2026

**Security:**
- Native RBAC: users, roles, privileges at database/table/column level — strongest out-of-the-box among all engines here
- Row-level security: row policies (WHERE clause per user/role) — built-in, no external system needed
- Column-level: access control native
- Kerberos, LDAP, SSL/TLS

**Deployment:** Single binary (clickhouse-server), Docker, K8s (Helm, Altinity/ClickHouse operators). ClickHouse Cloud: managed (proprietary SharedMergeTree). Self-hosted or cloud.

**Performance:** Best-in-class for high-throughput queries on its own MergeTree storage. For Iceberg external queries: competitive but secondary to native storage. DataFusion overtook it on Parquet read in 2025.

**Commercial:** ClickHouse Cloud (proprietary SharedMergeTree, some features cloud-only). Altinity: third-party managed ClickHouse support (Apache 2.0 only, no proprietary features).

**Who uses it and why:**
- Cloudflare, Uber, eBay, Yandex, ByteDance, Spotify, Deutsche Telekom, Contentsquare
- Chosen for: extreme throughput on native storage; best built-in row/column security; 10+ years production proven; sub-second on high-cardinality aggregations

**Weaknesses:** Iceberg is secondary — primary value is native MergeTree storage; cloud-only features create self-hosted friction; schema-change edge cases on Iceberg; no streaming; SQL dialect differences; Iceberg v3 not targeted.

---

### 9. Presto (Meta / PrestoDB)
**License:** Apache 2.0 | **Runtime:** Java + C++ (Prestissimo / Velox) | **Governance:** Presto Foundation / Linux Foundation

**Architecture:** Distributed MPP — coordinator + workers. Two paths:
- **Java engine:** original, JVM-based
- **Prestissimo / C++ native engine (Presto 2.0):** workers = C++ Velox execution. 2-4x faster than Java. GA at Meta in 2025.

**Iceberg support:**
- Read: full, predicate pushdown, partition pruning
- Write: INSERT + enhanced metadata handling in Prestissimo
- Time travel: supported
- Schema evolution: supported
- Catalogs: HMS, Iceberg REST, Glue, Raptor (Meta internal)

**Security:** File-based, LDAP, plugin authorizer. Less mature OPA integration than Trino. Meta's internal security is sophisticated but not open-sourced.

**Deployment:** JVM coordinator + C++ workers (Prestissimo). Docker, K8s. IBM/Ahana managed service.

**Performance:** Velox/Prestissimo: 2-4x faster than Java engine, 50% less capacity reduction at Meta fleet level. Best-in-class for Meta-specific Iceberg workloads.

**Commercial:** IBM/Ahana (managed PrestoDB). IBM is a major Presto Foundation contributor.

**Who uses it and why:**
- Meta (primary driver), Uber (contributed extensively), IBM, ByteDance
- Chosen for: Meta's production scale proof; Velox C++ native path; IBM enterprise backing; Presto Foundation governance

**Weaknesses:** Lower development velocity than Trino (~1/3 of Trino's PR rate); Meta's internal features not open-sourced; Prestissimo not feature-complete vs Java engine; smaller connector ecosystem than Trino.

---

### 10. Apache Impala
**License:** Apache 2.0 | **Runtime:** C++ (impalad) + Java (catalog/state store) | **Governance:** Apache Foundation

**Architecture:** Distributed MPP for HDFS/cloud storage. Daemon-based: impalad per node. Designed for low-latency queries on Hadoop. Tightly coupled to Cloudera ecosystem. Impala 4.5 (March 2025).

**Iceberg support (v4.5):**
- Read: Iceberg v1 and v2, Parquet
- Write: INSERT, DELETE, UPDATE (position-delete files)
- MERGE: preview in 4.5
- OPTIMIZE: supported (triggers compaction)
- Puffin column statistics: supported in 4.5
- Schema evolution: supported
- Time travel: supported
- Catalog: primarily HMS; REST catalog support **limited**

**Security:** Apache Ranger (primary — Cloudera deployments), column masking + row filtering via Ranger, Kerberos, LDAP, TLS.

**Deployment:** Tightly coupled to Cloudera CDP. Bare-metal Hadoop, limited K8s. Not cloud-native.

**Performance:** Very fast for short-scan interactive queries on co-located HDFS data. Less competitive on cloud object storage vs StarRocks/Trino.

**Commercial:** Cloudera (Impala is the query engine inside Cloudera Data Platform).

**Who uses it and why:**
- Cloudera enterprise customers with existing CDP/Hadoop deployments
- Chosen for: fastest interactive query on HDFS co-located data; deep Cloudera/Ranger integration; existing investment in Cloudera platform

**Weaknesses:** Tightly coupled to Cloudera/HMS — limited outside that ecosystem; limited REST catalog support; declining relevance as HDFS → cloud object storage migration continues; small independent community; MERGE is preview only.

---

### 11. Apache Drill
**License:** Apache 2.0 | **Runtime:** JVM | **Governance:** Apache Foundation

**Architecture:** Schema-free distributed MPP. Designed for self-describing data (JSON, Parquet, Avro) without pre-defined schema. Drillbit daemon per node. Latest: 1.22.0 (June 2025, maintenance release).

**Iceberg support:**
- Read: Parquet, Avro, ORC via Iceberg format plugin; auto-detects by `metadata/` folder
- Write: **not supported** — read-only engine for Iceberg
- REST catalog: **not supported** — HMS and Hadoop filesystem only
- Time travel: limited
- Drill Iceberg Metastore: built-in default metastore

**Security:** File-based, Kerberos, Drill impersonation. No column masking or row filtering for Iceberg.

**Commercial:** MapR (defunct). No active commercial distribution.

**Who uses it and why:**
- Legacy Hadoop deployments needing ad-hoc queries on raw heterogeneous files
- Chosen historically for: schema-free querying without DDL; JSON-heavy workloads; no other engine supported this pattern at the time

**Weaknesses:** No Iceberg write; no REST catalog; not competitive on modern benchmarks; limited to Hadoop-style deployments; declining community; last release is maintenance-only; not suitable for new lakehouse projects.

---

### 12. Dremio Community Edition
**License:** Apache 2.0 ⚠️ *open-core — row security and full column masking are Enterprise-only; some JDBC drivers bundled are non-OSS* | **Runtime:** JVM (Arrow-native) | **Governance:** Dremio Inc.

**Architecture:** Distributed lakehouse query engine. Coordinator + executors. Built natively on Arrow Flight SQL (primary wire protocol), Iceberg (storage), Polaris (catalog). Reflections = physically optimized Iceberg materializations on the lake. C3 (Columnar Cloud Cache) on local NVMe.

**Iceberg support:**
- Read: full — predicate pushdown, partition pruning, metadata caching
- Write: full DML — INSERT, UPDATE, DELETE, MERGE INTO, CTAS. Early driver of Iceberg write path.
- Time travel: supported
- Schema evolution: full
- Catalogs: Apache Polaris (first-class, donated by Dremio), Nessie, Glue, HMS, Unity Catalog — REST native
- Autonomous Reflections: auto-materialize query patterns as Iceberg tables, transparent rewrite

**Security:**
- RBAC: catalog/schema/table/column level
- Column masking: Enterprise only ⚠️
- Row-level security: Enterprise only ⚠️
- SSO/OIDC: Enterprise only for full feature set

**Deployment:** Docker, K8s (Helm). Community: local Docker/K8s. Dremio Cloud: fully managed.

**Performance:** 20x faster than Trino claim is **with Reflections** (materialized views) — not raw query. C3 cache eliminates repeated S3 round-trips. Without Reflections: competitive with Trino.

**Commercial:** Dremio Enterprise / Dremio Cloud — adds row security, full column masking, full SSO, audit, encrypted ODBC/Flight.

**Who uses it and why:**
- BNP Paribas, Regeneron, Dell, financial services
- Chosen for: Arrow Flight SQL as primary protocol; Autonomous Reflections for BI acceleration; donated Apache Polaris (foundational to Iceberg REST standard); data virtualization across S3+databases+Kafka

**Weaknesses:** Thin open-source community (primarily Dremio Inc.); row security and column masking require Enterprise; benchmark claims skewed by Reflections; community development velocity lower than Apache-governed engines.

---

### 13. Apache Kyuubi
**License:** Apache 2.0 | **Runtime:** JVM (gateway) | **Governance:** Apache Foundation

**Architecture:** Multi-tenant SQL gateway / serverless proxy over Spark, Flink, Hive, or Trino. Not a query engine — provisions isolated engine sessions per user on Kubernetes.

**Iceberg support:** Inherits 100% from underlying engine (Spark + Iceberg = full lakehouse).

**Security:** Multi-tenant isolation (dedicated Spark apps per user/team), Kerberos/LDAP auth, delegates authorization to Ranger (for Spark backend), OpenLineage injection (2025).

**Deployment:** K8s-native — provisions Spark apps as Pods via Spark-on-K8s. Helm charts.

**Commercial:** No prominent commercial offering.

**Who uses it and why:**
- NetEase (donated the project), organizations building self-service Spark SQL platforms on K8s
- Chosen for: multi-tenant Spark SQL without dedicated Databricks; serverless provisioning model; Flink + Spark + Hive in one interface

**Weaknesses:** Adds no Iceberg features itself; operational complexity of managing Kyuubi + Spark + catalog; primarily Spark-centric; China-heavy early adopter base; smaller Western community.

---

### 14. RisingWave
**License:** Apache 2.0 | **Runtime:** Rust | **Governance:** RisingWave Labs

**Architecture:** Cloud-native streaming database with disaggregated compute/storage. PostgreSQL-compatible SQL. Incremental computation, continuously maintained materialized views. Iceberg Table Engine (2025): creates Iceberg tables as native storage. Flink alternative.

**Iceberg support:**
- Iceberg Table Engine: create Iceberg tables inside RisingWave stored on S3 in Iceberg format
- Streaming sink: exactly-once into Iceberg, automatic compaction built-in
- Streaming source: incremental Iceberg reads
- Copy-on-Write mode (v2.6+): branch-based ingestion for high-frequency updates
- REST catalog: supported
- Schema evolution: automatic as records evolve
- Time travel: supported via Iceberg snapshots

**Security:** PostgreSQL-compatible auth (username/password, TLS). Row/column security immature.

**Deployment:** K8s-native (Helm, RisingWave Operator), Docker. Written in Rust. Managed: RisingWave Cloud.

**Performance:** Lower latency than Flink for streaming results (sub-second MV refresh). Not optimized for ad-hoc batch analytics. Vector search support (2025).

**Commercial:** RisingWave Cloud (managed service).

**Who uses it and why:**
- Early-stage adoption in real-time analytics where Flink is considered too operationally complex
- Chosen for: simpler than Flink (PostgreSQL SQL, no Java, no checkpoint tuning exposed); Rust-native; automatic compaction; Iceberg as native storage

**Weaknesses:** Less mature than Flink for complex streaming; smaller connector ecosystem; RBAC/security immature; not a batch analytics engine; community smaller than Flink.

---

## Feature Comparison Matrix

| Engine | License | Runtime | Iceberg Write | MERGE INTO | Time Travel | REST Catalog | Iceberg v3 | Streaming ingest | Built-in RBAC | Row security | Column masking | Arrow Flight SQL | K8s Native |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Trino | Apache 2.0 | JVM | ✅ Full | ✅ | ✅ | ✅ | ❌ (v2 only) | ❌ | Via OPA/Ranger | Via OPA | Via OPA | Via adapter | ✅ |
| Spark SQL | Apache 2.0 | JVM+Rust | ✅ Full | ✅ | ✅ | ✅ | ✅ (Spark 4+Iceberg 4) | ✅ (micro-batch) | Via Ranger/platform | Via Ranger | Via Ranger | ❌ | ✅ |
| Flink SQL | Apache 2.0 | JVM | ✅ (no MERGE) | ❌ | ⚠️ Limited | ✅ | ❌ (v2 only) | ✅ **Best** | ❌ | ❌ | ❌ | ❌ | ✅ |
| DuckDB | **MIT** | C++ | ✅ (REST req) | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| DataFusion | Apache 2.0 | **Rust** | ✅ (iceberg-rust) | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ (app builds) | ❌ (app builds) | ❌ (app builds) | ✅ (SQE builds this) | Via Ballista |
| StarRocks | Apache 2.0 | C++ | ✅ (v4.0) | ✅ | ✅ | ✅ | Planned 2026 | ❌ | ✅ Native | ⚠️ Partial | ✅ | ❌ | ✅ |
| Doris | Apache 2.0 | C++ | ⚠️ No MERGE | ❌ | ✅ | ✅ | ❌ | ❌ | ✅ Native | ✅ | ✅ | ❌ | ✅ |
| ClickHouse | Apache 2.0⚠️ | C++ | ✅ (25.8+) | ✅ (25.10) | ✅ | ✅ | ❌ | ⚠️ Micro-batch | ✅ **Best** | ✅ (row policies) | ✅ | ❌ | ✅ |
| Presto (Meta) | Apache 2.0 | Java+C++ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | Plugin-based | Plugin-based | Plugin-based | ❌ | ✅ |
| Impala | Apache 2.0 | C++ | ✅ (no MERGE*) | ⚠️ Preview | ✅ | ⚠️ Limited | ❌ | ❌ | Via Ranger | Via Ranger | Via Ranger | ❌ | ⚠️ Limited |
| Drill | Apache 2.0 | JVM | ❌ **None** | ❌ | ⚠️ Limited | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ⚠️ Limited |
| Dremio OSS | Apache 2.0⚠️ | JVM | ✅ Full | ✅ | ✅ | ✅ (Polaris native) | ❌ | ❌ | ⚠️ Partial | ❌ (Enterprise) | ❌ (Enterprise) | ✅ **Primary** | ✅ |
| Kyuubi | Apache 2.0 | JVM (proxy) | Inherits engine | Inherits | Inherits | Inherits | Inherits | Yes (Flink) | Delegates | Delegates | Delegates | ❌ | ✅ |
| RisingWave | Apache 2.0 | **Rust** | ✅ (streaming) | ✅ | ✅ | ✅ | ❌ | ✅ First-class | ⚠️ Limited | ⚠️ Limited | ❌ | ❌ | ✅ |

⚠️ = partial, caveated, or open-core
❌ = not supported / not applicable

---

## Scoring Table

*Scores 1–5. Weighted total out of 5.0.*

| Engine | Iceberg (22%) | Security (20%) | Ops burden (18%) | Performance (15%) | Ecosystem (13%) | Community (12%) | **Weighted Score** |
|---|---|---|---|---|---|---|---|
| **Trino** | 5 | 4 | 3 | 4 | 5 | 5 | **4.19** |
| **Spark SQL** | 5 | 3 | 2 | 3 | 5 | 5 | **3.75** |
| **Flink SQL** | 4 | 2 | 2 | 4 | 4 | 4 | **3.28** |
| **DuckDB** | 3 | 1 | 5 | 5 | 3 | 5 | **3.37** |
| **DataFusion** | 4 | 1 | 4 | 5 | 3 | 4 | **3.38** |
| **StarRocks** | 4 | 4 | 4 | 5 | 3 | 4 | **4.00** |
| **Doris** | 3 | 4 | 3 | 4 | 3 | 4 | **3.46** |
| **ClickHouse** | 3 | 5 | 4 | 4 | 4 | 5 | **3.90** |
| **Presto (Meta)** | 4 | 3 | 3 | 4 | 3 | 3 | **3.40** |
| **Impala** | 3 | 4 | 2 | 3 | 2 | 2 | **2.80** |
| **Drill** | 1 | 1 | 2 | 2 | 1 | 2 | **1.52** |
| **Dremio OSS** | 5 | 2 | 3 | 4 | 4 | 2 | **3.44** |
| **Kyuubi** | 4* | 3* | 2 | 3* | 3 | 3 | **3.06** |
| **RisingWave** | 4 | 2 | 4 | 3 | 2 | 3 | **3.12** |

*Kyuubi score inherited from Spark backend

### Score Rationale

**Trino (4.19) — highest overall**
- Iceberg 5/5: only engine with full read+write+MERGE+maintenance+OPA integration
- Security 4/5: OPA integration is production-grade; deducted 1 because OSS row/column security requires external OPA (not built-in to engine)
- Ops 3/5: JVM cluster management, tuning overhead; not single-binary
- Ecosystem 5/5: widest connector range, Starburst commercial, dbt native support, largest production community

**StarRocks (4.00) — best performance story**
- Iceberg 4/5: write first-class in v4.0, MERGE supported; deducted 1 for v3 gap
- Security 4/5: native RBAC + column masking built-in without external systems
- Ops 4/5: C++ execution (no JVM GC), K8s operator, shared-data mode for elasticity
- Performance 5/5: 6.93x Trino claim on Iceberg, sub-second interactive

**ClickHouse (3.90) — best native security**
- Iceberg 3/5: write added late (25.8), schema-change edge cases, secondary to native storage
- Security 5/5: strongest out-of-the-box RBAC + row policies + column access — no external system needed
- Community 5/5: 39K stars, monthly releases, massive user base

**Spark SQL (3.75) — reference implementation, not a service**
- Iceberg 5/5: the Iceberg reference engine, first mover on v3
- Security 3/5: zero built-in — fully delegated to Ranger/platform
- Ops 2/5: not a query service — needs cluster manager, not suitable for interactive use without Kyuubi/Trino on top

**DuckDB (3.37) — best for light workloads, MIT license**
- Iceberg 3/5: write requires REST catalog, no streaming, single-node ceiling
- Security 1/5: no RBAC — embedded single-user model
- Ops 5/5: zero infrastructure, single binary, runs anywhere including browser

**Drill (1.52) — do not use for new projects**
- No Iceberg write, no REST catalog, no streaming, no row/column security, declining community

---

## Use-Case Recommendations

### "I need to run ad-hoc SQL on a large Iceberg lake (BI, dashboards)"
**→ Trino** (OSS) or **StarRocks** (faster sub-second, less mature ecosystem)

### "I need real-time streaming into Iceberg with exactly-once"
**→ Flink SQL** (most mature, Confluent-managed option available)
**→ RisingWave** (simpler to operate, Rust, less mature)

### "I need batch ETL/transformation on Iceberg"
**→ Spark SQL** (deepest feature set, Iceberg v3 support first)
Consider Kyuubi on top for multi-tenant SQL gateway

### "I need zero infrastructure, local dev or embedded analytics"
**→ DuckDB** (MIT license, fastest single-node, WASM, MotherDuck for cloud scale)

### "I need the strongest built-in security without OPA/Ranger"
**→ ClickHouse** (best native row policies + column masking)
**→ StarRocks** (native RBAC + column-level, more Iceberg-centric)

### "I'm building a custom query engine on Iceberg"
**→ Apache DataFusion** (this project — SQE uses this)

### "I'm already on Cloudera"
**→ Impala** (stay with it; do not migrate to Impala if greenfield)

### "I need interactive query performance AND data lake federation"
**→ Trino** (best federation story) or **Dremio** (Reflections acceleration + Polaris native)

### "I need per-user identity passthrough to storage (zero-trust)"
**→ None of the above do this natively** — this is SQE's core differentiator

---

## Where SQE Fits

| Dimension | SQE | Closest competitor |
|---|---|---|
| License | Apache 2.0 | Trino (Apache 2.0) |
| Runtime | Rust (zero GC) | StarRocks (C++ execution), DataFusion |
| Per-user auth to storage | ✅ **Only engine** with this | ❌ All others use service account |
| Iceberg REST catalog | ✅ | Trino, Spark, StarRocks |
| Semantic layer (RDF/SPARQL/GQL) | ✅ Phase 6 | ❌ None |
| Vector search + Iceberg | ✅ Phase 6 (Lance) | ❌ None unified |
| Single binary, minimal ops | ✅ | DuckDB (single-node), RisingWave (Rust) |
| AI agent interface (CLI+REST+MCP) | ✅ Phase 6 | ❌ None purpose-built |
| Streaming ingestion | ❌ (not planned) | Flink SQL, RisingWave |
| Maturity / production deployments | ❌ New project | Trino, Spark |

**SQE's non-negotiable differentiation:** per-user OIDC identity propagation all the way to object storage. No other open-source engine does this. All others authenticate to the catalog and storage as a service account. This is the foundation for true zero-trust data access and is the core reason to use SQE in regulated environments (finance, healthcare, government).

**The SQE gap today:** streaming ingestion and production maturity. Intended complement: Flink SQL writes into Iceberg, SQE serves read queries with per-user identity enforcement.

---

## Key Industry Trends (2026 Perspective)

1. **Iceberg REST catalog is universal** — all engines support it; the catalog layer (Polaris, Nessie, Lakekeeper, Gravitino, Unity Catalog) is separating from the query layer
2. **Native (non-JVM) execution is taking over** — StarRocks, ClickHouse, DuckDB, DataFusion, RisingWave are C++/Rust; Spark is adding Comet/Gluten
3. **Iceberg write is now table stakes** — MERGE INTO is the differentiator (Trino, Spark, Dremio, ClickHouse 25.10, StarRocks)
4. **Security gap persists in OSS** — most engines delegate row/column security to external systems (Ranger, OPA, catalog); only ClickHouse and StarRocks have genuinely built-in row/column security
5. **Iceberg v3 momentum** — Spark 4.0 + Iceberg 4.0 are first; others follow in 2026
6. **Streaming lakehouse maturing** — Flink + Iceberg is production; Confluent Tableflow writes Iceberg natively; RisingWave emerging as simpler alternative
7. **AI + data lake integration is not solved** — no engine has a semantic/ontology layer, vector search integration, or agent-native interface — this is the Phase 6 opportunity for SQE

---

*Document generated: March 2026. Based on engine releases current at that date.*
*Sources: Apache project documentation, GitHub release notes, vendor blogs, OLake query engine matrix, ClickBench results.*
