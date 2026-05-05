# Public Iceberg Matrix Comparison

Side-by-side comparison of every Iceberg engine on the public scoreboard at [icebergmatrix.org](https://icebergmatrix.org). Source rubric: [Neuw84/iceberg-matrix](https://github.com/Neuw84/iceberg-matrix). SQE data lives in [`docs/iceberg-matrix-state.json`](./iceberg-matrix-state.json) and is rendered in detail in [`docs/iceberg-matrix.md`](./iceberg-matrix.md).

Each cell shows V2/V3 status. Glyphs:

- `F` full: feature works end-to-end with no significant caveats.
- `P` partial: works with caveats; see per-engine cell notes for details.
- `?` unknown: library primitives exist but no end-to-end verification.
- `.` none: not implemented, blocked upstream, or not applicable.

Cells like `./X` mean the feature is V3-only in the rubric (no V2 cell). Cells like `X/.` mean V2-only. Engines with no V3 column at all (e.g. `./.` row-wide) have not implemented V3 features yet.

## Scoreboard

| Rank | Engine | Score | Coverage |
|---:|---|---:|---:|
| 1 | EMR (7.12.0 Spark) | 180/189 | 95.2% |
| 2 | Glue (5.1) | 178/189 | 94.2% |
| 3 | OSS Spark (4.1) | 175/189 | 92.6% |
| 4 | Dataproc (2.3) | 174/189 | 92.1% |
| 5 | SQE (0.15.0) **(this engine)** | 165/189 | 87.3% |
| 6 | Managed Flink (1.20) | 157/189 | 83.1% |
| 7 | OSS Flink (2.2.0) | 153/189 | 81.0% |
| 8 | Doris (4.1) | 144/189 | 76.2% |
| 9 | Snowflake | 134/189 | 70.9% |
| 10 | PyIceberg (0.11.0) | 130/189 | 68.8% |
| 11 | Databricks (DBR 17.3) | 103/189 | 54.5% |
| 12 | DuckDB (1.5.2) | 88/189 | 46.6% |
| 13 | Synapse (3.5) | 78/189 | 41.3% |
| 14 | Daft | 77/189 | 40.7% |
| 15 | Kafka Connect | 74/189 | 39.2% |
| 16 | Fabric (1.3) | 72/189 | 38.1% |
| 17 | Athena SQL (v3) | 59/189 | 31.2% |
| 18 | Redshift (S3) | 49/189 | 25.9% |
| 19 | ClickHouse (26.1) | 46/189 | 24.3% |
| 20 | BigQuery | 40/189 | 21.2% |
| 21 | Data Firehose | 26/189 | 13.8% |

SQE sits at **165/189 (87.3%)**, fifth on the public scoreboard behind EMR Spark, AWS Glue Spark, OSS Spark, and Dataproc. SQE is the only entry in the top five that is not a Spark distribution.

---

## OSS engines comparison

SQE first, then Spark, Flink, PyIceberg, DuckDB, ClickHouse, Doris, Daft, and Kafka Connect (OSS streaming connector).

### Read & write fundamentals

| Feature | SQE | Spark | Flink | PyIce | DuckDB | CH | Doris | Daft | Kafka |
| --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Read Support | F/F | F/F | F/F | F/F | F/P | F/. | F/F | F/? | ./. |
| Write (INSERT) | F/F | F/F | F/F | F/F | F/P | ./. | F/F | F/? | F/F |
| Write (MERGE/UPDATE/DELETE) | F/F | F/F | P/P | P/P | P/? | ./. | P/P | ./? | ./. |
| Copy-on-Write | F/F | F/F | F/F | F/F | P/? | ./. | ./. | F/? | ./. |
| Merge-on-Read | F/F | F/F | F/F | P/P | F/? | F/. | F/F | P/? | ./. |
| Position Deletes | F/F | F/F | F/F | P/P | F/P | F/. | F/F | F/? | ./. |
| Equality Deletes | F/F | F/F | F/F | P/. | F/. | P/. | F/. | ./? | ./. |

### Schema & table evolution

| Feature | SQE | Spark | Flink | PyIce | DuckDB | CH | Doris | Daft | Kafka |
| --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Table Creation | F/F | F/F | F/F | F/F | F/? | ./. | F/F | F/? | P/P |
| Schema Evolution | F/F | F/F | P/P | F/F | F/P | P/. | F/F | F/? | P/P |
| Type Promotion / Widening | F/F | F/F | F/F | F/F | ./. | P/. | F/F | ./? | ./. |
| Column Default Values | ./F | ./P | ./? | ./. | ./. | ./. | ./P | ./. | ./. |
| Hidden Partitioning | F/F | F/F | P/P | F/F | F/P | F/. | F/F | F/? | P/P |
| Partition Evolution | F/F | F/F | P/P | F/F | F/P | ?/. | F/F | F/? | ./. |
| Multi-Argument Transforms | ./. | ./P | ./? | ./. | ./? | ./. | ./? | ./. | ./. |

### Time, lineage, history

| Feature | SQE | Spark | Flink | PyIce | DuckDB | CH | Doris | Daft | Kafka |
| --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Time Travel / Snapshots | F/F | F/F | F/F | F/F | F/P | F/. | F/F | P/? | ./. |
| Branching & Tagging | F/F | F/F | P/P | F/F | ./. | ./. | F/F | ./? | P/P |
| Lineage Tracking | ./. | ./? | ./? | ./. | ./. | ./? | ./P | ./. | ./. |
| Change Data Capture (CDC) | ./F | ./P | ./P | ./. | ./. | ./? | ./P | ./. | ./. |

### Statistics & maintenance

| Feature | SQE | Spark | Flink | PyIce | DuckDB | CH | Doris | Daft | Kafka |
| --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Statistics (Column Metrics) | F/F | F/F | F/F | F/F | F/P | P/. | F/F | F/? | P/P |
| Bloom Filters & Puffin | F/F | F/F | ?/? | ./. | ./. | ./. | ?/? | ./? | ./. |
| Table Maintenance | P/P | F/F | P/P | P/P | ./. | ./. | F/F | ./? | ./. |

### Catalog backends

| Feature | SQE | Spark | Flink | PyIce | DuckDB | CH | Doris | Daft | Kafka |
| --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Catalog Integration | F/F | F/F | F/F | F/F | F/P | F/. | F/F | F/? | F/F |
| REST Catalog | F/F | F/F | F/F | F/F | F/P | F/. | F/F | F/? | F/F |
| Polaris | F/F | F/F | F/F | F/F | F/P | P/. | P/P | P/? | P/P |
| Nessie | F/F | F/F | F/F | P/P | ./. | ./. | ?/? | P/? | F/F |
| Unity Catalog | F/F | F/F | P/P | P/P | P/? | P/. | P/P | P/? | ./. |
| Snowflake Horizon Catalog | P/P | F/P | F/P | F/P | F/? | P/? | P/P | F/? | P/P |
| Hive Metastore | F/F | F/F | F/F | F/F | ./. | ./. | F/F | F/? | F/F |
| AWS Glue Catalog | F/F | F/F | F/F | F/F | F/P | F/. | P/P | F/? | F/F |
| JDBC Catalog | F/F | F/F | F/F | ./. | ./. | ./. | P/P | ./. | P/P |
| Hadoop Catalog | P/P | F/F | F/F | ./. | ./. | ./. | F/F | ./. | F/F |

### V3 advanced types

| Feature | SQE | Spark | Flink | PyIce | DuckDB | CH | Doris | Daft | Kafka |
| --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Nanosecond Timestamps | ./F | ./. | ./? | ./F | ./. | ./. | ./. | ./. | ./. |
| Variant Type | ./. | ./F | ./? | ./. | ./. | ./? | ./? | ./. | ./. |
| Shredded Variant | ./. | ./P | ./? | ./. | ./. | ./? | ./? | ./. | ./. |
| Geometry / Geo Types | ./. | ./? | ./? | ./. | ./F | ./? | ./? | ./. | ./. |
| Vector / Embedding Type | ./. | ./? | ./? | ./. | ./. | ./? | ./? | ./. | ./. |

---

## Cloud-managed engines comparison

SQE first, then Snowflake, Databricks, AWS, GCP, and Azure offerings. EMR / Glue / Dataproc are Spark-managed so they share the same upstream matrix data as OSS Spark with vendor-specific caveats.

### Read & write fundamentals

| Feature | SQE | Snow | DBX | EMR | Glue | Athena | RS-S3 | MFlink | Fire | BQ | DProc | Fabric | Syn |
| --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Read Support | F/F | F/F | F/F | F/F | F/F | F/. | F/. | F/F | ./. | F/. | F/F | ?/? | P/? |
| Write (INSERT) | F/F | F/F | F/F | F/F | F/F | F/. | F/. | F/F | F/. | F/. | F/F | ?/? | P/? |
| Write (MERGE/UPDATE/DELETE) | F/F | F/F | F/F | F/F | F/F | F/. | F/. | P/P | F/. | F/. | F/F | ?/? | P/? |
| Copy-on-Write | F/F | F/F | F/F | F/F | F/F | P/. | F/. | F/F | P/. | F/. | F/F | ?/? | P/? |
| Merge-on-Read | F/F | P/F | ./F | F/F | F/F | F/. | F/. | F/F | P/. | P/. | F/F | ?/? | P/? |
| Position Deletes | F/F | P/F | ./. | F/F | F/F | F/. | F/. | F/F | ./. | P/. | F/F | ?/? | P/? |
| Equality Deletes | F/F | ./. | ./. | F/F | F/F | F/. | F/. | F/F | F/. | P/. | F/F | ?/? | P/? |

### Schema & table evolution

| Feature | SQE | Snow | DBX | EMR | Glue | Athena | RS-S3 | MFlink | Fire | BQ | DProc | Fabric | Syn |
| --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Table Creation | F/F | F/F | F/F | F/F | F/F | F/. | F/. | F/F | ./. | F/. | F/F | ?/? | P/? |
| Schema Evolution | F/F | F/F | F/F | F/F | F/F | F/. | F/. | P/P | P/. | F/. | F/F | ?/? | P/? |
| Type Promotion / Widening | F/F | F/F | ?/? | F/F | F/F | F/. | P/. | F/F | ./. | P/. | F/F | ?/? | P/? |
| Column Default Values | ./F | ./F | ./. | ./F | ./F | ./. | ./. | ./? | ./. | ./. | ./F | ?/? | ./? |
| Hidden Partitioning | F/F | P/F | P/P | F/F | F/F | F/. | F/. | P/P | F/. | P/. | F/F | ?/? | P/? |
| Partition Evolution | F/F | P/P | P/P | F/F | F/F | F/. | P/. | P/P | ./. | ./. | F/F | ?/? | P/? |
| Multi-Argument Transforms | ./. | ./? | ./. | ./F | ./F | ./. | ./. | ./? | ./. | ./. | ./F | ?/? | ./? |

### Time, lineage, history

| Feature | SQE | Snow | DBX | EMR | Glue | Athena | RS-S3 | MFlink | Fire | BQ | DProc | Fabric | Syn |
| --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Time Travel / Snapshots | F/F | F/F | F/F | F/F | F/F | F/. | F/. | F/F | ./. | F/. | F/F | ?/? | P/? |
| Branching & Tagging | F/F | ./. | ./. | F/F | F/F | P/. | ./. | P/P | ./. | ./. | F/F | ?/? | P/? |
| Lineage Tracking | ./. | ./F | ./F | ./F | ./F | ./. | ./. | ./? | ./. | ./. | ./? | ?/? | ./? |
| Change Data Capture (CDC) | ./F | ./F | ./. | ./F | ./F | ./. | ./. | F/F | ./. | ./. | ./? | ?/? | ./? |

### Statistics & maintenance

| Feature | SQE | Snow | DBX | EMR | Glue | Athena | RS-S3 | MFlink | Fire | BQ | DProc | Fabric | Syn |
| --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Statistics (Column Metrics) | F/F | F/F | ?/? | F/F | F/F | F/. | F/. | F/F | P/. | ?/. | F/F | ?/? | P/? |
| Bloom Filters & Puffin | F/F | ./. | ./. | F/F | F/F | P/. | ./. | ?/? | ./. | ?/. | F/F | ?/? | P/? |
| Table Maintenance | P/P | F/F | F/F | F/F | F/F | P/. | P/. | P/P | ./. | F/. | F/F | ?/? | P/? |

### Catalog backends

| Feature | SQE | Snow | DBX | EMR | Glue | Athena | RS-S3 | MFlink | Fire | BQ | DProc | Fabric | Syn |
| --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Catalog Integration | F/F | F/F | F/F | F/F | F/F | F/. | F/. | F/F | F/. | F/. | F/F | ?/? | P/? |
| REST Catalog | F/F | F/F | F/F | F/F | F/F | ./. | ./. | F/F | ./. | ?/. | F/F | ?/? | P/? |
| Polaris | F/F | F/F | ?/? | F/F | F/F | P/. | ./. | F/F | ./. | ./. | F/F | ?/? | ./. |
| Nessie | F/F | ./. | ./. | F/F | F/F | ./. | ./. | F/F | ./. | ./. | F/F | ?/? | P/? |
| Unity Catalog | F/F | P/P | F/F | P/P | P/P | P/. | ./. | P/P | ./. | ./. | P/P | ?/? | ./. |
| Snowflake Horizon Catalog | P/P | F/F | P/P | F/P | P/? | P/. | ?/. | F/P | ./. | ./. | F/P | P/. | ./. |
| Hive Metastore | F/F | ./. | P/P | F/F | F/F | ./. | ./. | F/F | ./. | ./. | F/F | ?/? | P/? |
| AWS Glue Catalog | F/F | F/F | P/P | F/F | F/F | F/. | F/. | F/F | F/. | ./. | P/P | ?/? | ./. |
| JDBC Catalog | F/F | ./. | ./. | F/F | F/F | ./. | ./. | F/F | ./. | ./. | F/F | ?/? | P/? |
| Hadoop Catalog | P/P | ./. | ./. | F/F | F/F | ./. | ./. | F/F | ./. | ./. | F/F | ?/? | P/? |

### V3 advanced types

| Feature | SQE | Snow | DBX | EMR | Glue | Athena | RS-S3 | MFlink | Fire | BQ | DProc | Fabric | Syn |
| --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Nanosecond Timestamps | ./F | ./F | ./. | ./F | ./F | ./. | ./. | ./? | ./. | ./. | ./F | ?/? | ./? |
| Variant Type | ./. | ./F | ./F | ./F | ./F | ./. | ./. | ./? | ./. | ./. | ./F | ?/? | ./? |
| Shredded Variant | ./. | ./? | ./? | ./? | ./? | ./. | ./. | ./? | ./. | ./. | ./? | ?/? | ./? |
| Geometry / Geo Types | ./. | ./F | ./. | ./? | ./? | ./. | ./. | ./? | ./. | ./. | ./? | ?/? | ./? |
| Vector / Embedding Type | ./. | ./? | ./? | ./? | ./? | ./. | ./. | ./? | ./. | ./. | ./? | ?/? | ./? |

---

## How to read the cells

A cell like `F/F` means full support on V2 and V3. `F/P` means full V2 with a known caveat on V3. `./F` means the feature is V3-only in the rubric. `./.` means neither version is supported (or not implemented yet). The exact caveats per engine live in the upstream platform JSONs at [`Neuw84/iceberg-matrix/src/data/platforms/`](https://github.com/Neuw84/iceberg-matrix/tree/main/src/data/platforms); SQE caveats live in [`docs/iceberg-matrix-state.json`](./iceberg-matrix-state.json).

## SQE specifics

Every SQE cell maps to a concrete file path or test name in the repo. The full per-cell evidence and notes live in [`docs/iceberg-matrix.md`](./iceberg-matrix.md). The matrix engineering history sits in chapters 16b ("The Matrix and the Quiet Bug") and 16c ("Following Through") of the ebook in `docs/ebook/`.

Regenerate this comparison: `python3 scripts/render-iceberg-matrix-compare.py`. The script fetches the latest platform JSONs from `Neuw84/iceberg-matrix` via `gh api` and joins them with our own `docs/iceberg-matrix-state.json`.
