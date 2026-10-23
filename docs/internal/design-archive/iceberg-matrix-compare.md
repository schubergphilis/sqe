# Public Iceberg Matrix Comparison

Side-by-side comparison of SQE with every engine on [icebergmatrix.org](https://icebergmatrix.org). Upstream data: [Neuw84/iceberg-matrix@d14d0f1341](https://github.com/Neuw84/iceberg-matrix/tree/d14d0f1341971cca8a8a218ca412d2cff28e3253) (MIT), fetched 2026-10-23. SQE data: [`docs/evidence/iceberg-matrix-state.json`](../../evidence/iceberg-matrix-state.json), rendered per cell in [`iceberg-matrix.md`](./iceberg-matrix.md).

Every engine is scored on the same 54 cells: one per feature and format version the rubric applies to. full=3, partial=2, unknown=1, none=0; max 162. SQE rates itself; the other engines are rated by the upstream maintainers and contributors.

Cells read `V2/V3`: `F` full, `P` partial, `?` unknown, `.` none, `-` the rubric does not score that version.

## Scoreboard

| Rank | Engine | Score | % |
|---:|---|---:|---:|
| 1 | **SQE (self-rated)** | 150/162 | 92.6 |
| 2 | AWS EMR (8.1 Spark) | 147/162 | 90.7 |
| 3 | Starburst (482-e) | 147/162 | 90.7 |
| 4 | Dremio / SAP (26.x) | 146/162 | 90.1 |
| 5 | AWS Glue (6.0) | 145/162 | 89.5 |
| 6 | AWS EMR (8.1 Spark) (S3 Tables) | 143/162 | 88.3 |
| 7 | AWS Glue (6.0) (S3 Tables) | 141/162 | 87.0 |
| 8 | Google Managed Spark (3.0.13) | 139/162 | 85.8 |
| 9 | Snowflake | 138/162 | 85.2 |
| 10 | Snowflake (external) | 138/162 | 85.2 |
| 11 | Spark (4.1) | 138/162 | 85.2 |
| 12 | Trino (483) | 130/162 | 80.2 |
| 13 | OSS Flink (2.3) | 126/162 | 77.8 |
| 14 | AWS Managed Flink (2.3) | 124/162 | 76.5 |
| 15 | AWS Managed Flink (2.3) (S3 Tables) | 124/162 | 76.5 |
| 16 | Doris (4.2) | 122/162 | 75.3 |
| 17 | DuckDB (1.5.5) | 116/162 | 71.6 |
| 18 | PyIceberg (0.12.0) | 113/162 | 69.8 |
| 19 | Databricks (DBR 18.2) | 110/162 | 67.9 |
| 20 | AWS Redshift (S3) | 95/162 | 58.6 |
| 21 | Oracle (26ai) | 95/162 | 58.6 |
| 22 | Alibaba Cloud Realtime Compute Flink (VVR 11.x) | 87/162 | 53.7 |
| 23 | Alibaba Cloud EMR Spark (EMR-5.21.0) | 78/162 | 48.1 |
| 24 | Daft | 70/162 | 43.2 |
| 25 | ClickHouse (26.9) | 67/162 | 41.4 |
| 26 | Google BigQuery | 60/162 | 37.0 |
| 27 | AWS Athena SQL (v3) | 54/162 | 33.3 |
| 28 | Microsoft Fabric (2.0) | 54/162 | 33.3 |
| 29 | AWS Athena SQL (v3) (S3 Tables) | 52/162 | 32.1 |
| 30 | AWS Aurora PostgreSQL (17.11) | 52/162 | 32.1 |
| 31 | AWS Aurora PostgreSQL (17.11) (S3 Tables) | 52/162 | 32.1 |
| 32 | AWS Redshift (S3) (S3 Tables) | 50/162 | 30.9 |
| 33 | Databend (1.2.725) | 48/162 | 29.6 |
| 34 | Alibaba Cloud Hologres (V4.2) | 46/162 | 28.4 |
| 35 | Spark (4.1) | 37/162 | 22.8 |
| 36 | Spark (4.1) | 35/162 | 21.6 |
| 37 | Alibaba Cloud MaxCompute | 34/162 | 21.0 |

---

## Open-source engines

### Row-level operations

| Feature | SQE | Spark | Trino | Flink | Doris | DuckDB | PyIce | Daft | CH | Dbend |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Position Deletes | F/- | F/- | F/- | F/- | F/- | F/- | ./- | ./- | F/- | ?/- |
| Equality Deletes | F/F | ./. | ./. | F/F | ./. | ./. | ./. | ./? | ./. | ?/. |
| Merge-on-Read | F/F | F/F | F/F | F/F | F/F | F/F | ./. | ./? | F/. | ?/. |
| Copy-on-Write | F/F | F/F | ./. | ./. | ./. | ./? | F/F | ./? | ./. | ./. |
| Lineage Tracking | -/F | -/F | -/F | -/F | -/P | -/F | -/. | -/. | -/F | -/. |
| Deletion Vectors | -/F | -/F | -/F | -/F | -/F | -/F | -/. | -/. | -/. | -/? |

### Table management

| Feature | SQE | Spark | Trino | Flink | Doris | DuckDB | PyIce | Daft | CH | Dbend |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Schema Evolution | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/? | P/F | ./. |
| Type Promotion / Widening | F/F | F/F | F/F | F/F | F/F | P/P | F/F | P/? | P/. | ./. |
| Column Default Values | -/F | -/. | -/. | -/. | -/F | -/F | -/P | -/. | -/. | -/? |
| Table Creation | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/? | F/F | P/? |
| Time Travel / Snapshots | F/F | F/F | F/F | F/F | F/F | F/F | F/F | P/? | F/. | ./. |
| Table Maintenance | F/F | F/F | F/F | P/P | F/F | ./. | P/P | ./? | P/. | ./. |
| Branching & Tagging | F/F | F/F | P/P | P/P | F/F | ./. | F/F | ./? | ./. | ./. |

### Partitioning

| Feature | SQE | Spark | Trino | Flink | Doris | DuckDB | PyIce | Daft | CH | Dbend |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Hidden Partitioning | F/F | F/F | F/F | P/P | F/F | F/F | F/F | F/? | F/F | ?/? |
| Partition Evolution | F/F | F/F | F/F | P/P | F/F | F/P | F/F | F/? | P/. | ./. |
| Multi-Argument Transforms | -/. | -/. | -/. | -/? | -/? | -/. | -/. | -/. | -/. | -/? |

### Read / write

| Feature | SQE | Spark | Trino | Flink | Doris | DuckDB | PyIce | Daft | CH | Dbend |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Read Support | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/? |
| Write (INSERT) | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/? | P/. | P/? |
| Write (MERGE/UPDATE/DELETE) | F/F | F/F | F/F | P/P | P/P | F/F | P/P | ./? | P/. | ./. |
| Catalog Integration | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/? | F/. | F/? |
| Statistics (Column Metrics) | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/? | P/. | P/? |
| Bloom Filters & Puffin | F/F | F/F | P/P | F/F | ?/? | ./. | ./. | ./? | P/. | ?/? |

### Catalog support

| Feature | SQE | Spark | Trino | Flink | Doris | DuckDB | PyIce | Daft | CH | Dbend |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| AWS Glue Catalog | F/F | F/F | F/F | F/F | P/P | F/F | F/F | F/? | F/. | F/? |
| REST Catalog | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/? | P/. | F/? |
| Unity Catalog | P/P | F/F | P/P | P/P | P/P | P/? | P/P | P/? | P/. | P/? |
| Snowflake Horizon Catalog | P/P | F/P | P/P | F/P | P/P | F/? | F/P | F/? | P/? | P/? |
| Google Lakehouse | P/P | P/P | P/P | P/P | P/P | P/P | P/P | P/? | P/? | P/? |

### V3 data types

| Feature | SQE | Spark | Trino | Flink | Doris | DuckDB | PyIce | Daft | CH | Dbend |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Variant Type | -/F | -/F | -/F | -/P | -/P | -/F | -/. | -/. | -/. | -/? |
| Shredded Variant | -/F | -/. | -/P | -/. | -/P | -/F | -/. | -/. | -/. | -/? |
| Geometry / Geo Types | -/. | -/. | -/F | -/. | -/. | -/P | -/. | -/. | -/P | -/? |
| Nanosecond Timestamps | -/F | -/. | -/F | -/F | -/. | -/F | -/F | -/P | -/. | -/. |
| Unknown Type | -/F | -/F | -/. | -/. | -/. | -/. | -/F | -/F | -/. | -/? |

---

## Managed and commercial engines

### Row-level operations

| Feature | SQE | Starb | EMR | Dremio | Glue | DProc | Snow | DBX | Athena | BQ | Fabric |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Position Deletes | F/- | F/- | F/- | F/- | F/- | F/- | F/- | ./- | F/- | P/- | ?/- |
| Equality Deletes | F/F | ./. | ./. | ./. | ./. | ./. | ./. | ./. | ./. | P/. | ?/? |
| Merge-on-Read | F/F | F/F | F/F | F/F | F/F | F/F | F/F | ./F | F/. | P/P | ?/? |
| Copy-on-Write | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/F | ./. | F/. | ?/? |
| Lineage Tracking | -/F | -/F | -/F | -/F | -/F | -/? | -/F | -/F | -/. | -/. | -/? |
| Deletion Vectors | -/F | -/F | -/F | -/F | -/F | -/F | -/F | -/F | -/. | -/P | -/? |

### Table management

| Feature | SQE | Starb | EMR | Dremio | Glue | DProc | Snow | DBX | Athena | BQ | Fabric |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Schema Evolution | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/. | F/. | ?/? |
| Type Promotion / Widening | F/F | F/F | F/P | F/P | F/P | F/F | F/F | F/P | F/. | P/. | ?/? |
| Column Default Values | -/F | -/F | -/F | -/F | -/F | -/F | -/F | -/P | -/. | -/. | -/? |
| Table Creation | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/. | F/. | ?/? |
| Time Travel / Snapshots | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/. | F/. | ?/? |
| Table Maintenance | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/F | P/. | F/. | ?/? |
| Branching & Tagging | F/F | F/F | F/F | F/F | F/F | F/F | ./. | ./. | P/. | ./. | ?/? |

### Partitioning

| Feature | SQE | Starb | EMR | Dremio | Glue | DProc | Snow | DBX | Athena | BQ | Fabric |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Hidden Partitioning | F/F | F/F | F/F | F/F | F/F | F/F | F/F | ./. | F/. | P/. | ?/? |
| Partition Evolution | F/F | F/F | F/F | F/F | F/F | F/F | F/F | P/P | F/. | ./. | ?/? |
| Multi-Argument Transforms | -/. | -/. | -/. | -/. | -/. | -/. | -/. | -/. | -/. | -/. | -/? |

### Read / write

| Feature | SQE | Starb | EMR | Dremio | Glue | DProc | Snow | DBX | Athena | BQ | Fabric |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Read Support | F/F | F/F | F/F | F/F | F/F | F/F | P/P | P/P | F/. | F/P | ?/? |
| Write (INSERT) | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/. | F/. | ?/? |
| Write (MERGE/UPDATE/DELETE) | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/. | F/. | ?/? |
| Catalog Integration | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/. | F/. | ?/? |
| Statistics (Column Metrics) | F/F | F/F | F/F | F/F | F/F | F/F | F/F | P/P | F/. | ?/. | ?/? |
| Bloom Filters & Puffin | F/F | F/F | F/F | F/F | F/F | F/F | ./. | ./. | P/. | ?/. | ?/? |

### Catalog support

| Feature | SQE | Starb | EMR | Dremio | Glue | DProc | Snow | DBX | Athena | BQ | Fabric |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| AWS Glue Catalog | F/F | F/F | F/F | F/F | F/F | P/P | F/F | P/P | F/. | P/. | ?/? |
| REST Catalog | F/F | F/F | F/F | F/F | F/F | F/F | F/F | F/F | ./. | P/P | ?/? |
| Unity Catalog | P/P | F/F | P/P | F/F | P/P | P/P | F/F | F/F | P/. | P/. | ?/? |
| Snowflake Horizon Catalog | P/P | P/P | F/P | F/F | P/? | F/P | F/F | P/P | P/. | P/. | P/. |
| Google Lakehouse | P/P | F/F | P/P | F/F | P/P | F/P | F/P | P/P | ?/? | F/P | ?/? |

### V3 data types

| Feature | SQE | Starb | EMR | Dremio | Glue | DProc | Snow | DBX | Athena | BQ | Fabric |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Variant Type | -/F | -/F | -/F | -/F | -/F | -/F | -/F | -/F | -/. | -/. | -/? |
| Shredded Variant | -/F | -/P | -/F | -/F | -/F | -/? | -/F | -/F | -/. | -/. | -/? |
| Geometry / Geo Types | -/. | -/F | -/F | -/. | -/F | -/? | -/F | -/F | -/. | -/. | -/? |
| Nanosecond Timestamps | -/F | -/F | -/F | -/F | -/F | -/F | -/F | -/. | -/. | -/. | -/? |
| Unknown Type | -/F | -/. | -/F | -/. | -/F | -/? | -/F | -/? | -/. | -/. | -/? |

---

Per-cell notes and caveats live in the upstream platform files under [`src/data/platforms/`](https://github.com/Neuw84/iceberg-matrix/tree/d14d0f1341971cca8a8a218ca412d2cff28e3253/src/data/platforms) and, for SQE, in the state file.

Regenerate: `python3 scripts/render-iceberg-matrix-compare.py`, then `python3 scripts/render-iceberg-matrix.py`.
