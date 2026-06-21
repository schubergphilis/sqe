# SQL Reference

A function-by-function and statement-by-statement reference for everything SQE can parse and execute. Every entry lists where the implementation lives so you can jump from "what does this do" to "where do I read the code".

The reference focuses on what ships in the running engine, not the SQL standard in the abstract. If a function is not listed here, it is not registered in our `SessionContext`.

## How to read these tables

Every page uses the same column shape. The first three columns describe the function in SQE; the four right columns describe how the same idea looks in other engines.

| Column | Meaning |
|---|---|
| **Function** | The name SQE accepts in SQL. Case-insensitive on the surface, lower-case canonical name. |
| **Origin** | Where the implementation comes from. See origins below. |
| **Notes** | One-line summary, return type, gotchas, link to source line. |
| **Trino** | The Trino-equivalent function name, or `-` if Trino has none. |
| **Snowflake** | The Snowflake-equivalent function name, or `-` if Snowflake has none. |
| **Spark SQL** | The Spark-equivalent function name, or `-` if Spark has none. |
| **DuckDB** | The DuckDB-equivalent function name, or `-` if DuckDB has none. |

## Origins

Every function in SQE has exactly one origin. Eight values appear:

| Origin tag | What it means | Where it lives |
|---|---|---|
| `datafusion-builtin` | Shipped automatically with `SessionContext::new()`. No SQE registration. | Upstream `datafusion-functions-*` crates. |
| `datafusion-functions-json` | DataFusion JSON helper crate, registered explicitly. | `datafusion_functions_json::register_all()` in `session_context.rs:361`. |
| `sqe-trino-functions` | Our Trino-compatibility crate. Adds Trino names for things DataFusion calls differently. | `crates/sqe-trino-functions/src/trino_functions.rs` and `trino_functions_ext.rs`. |
| `sqe-trino-functions (ext)` | Extended Trino aliases. Same crate, separate registration call. | `register_extended_trino_functions()` in the same crate. |
| `sqe-policy` | Security crate. Currently exposes one UDF (`sha256`) used by column masks. | `crates/sqe-policy/src/sha256_udf.rs`. |
| `sqe-catalog` | Iceberg catalog and TVF crate. Provides `read_*` and `table_*` table functions. | `crates/sqe-catalog/src/`. |
| `sqe-sql` | Parser extension. Statements pre-parsed before DataFusion sees them. | `crates/sqe-sql/src/`. |
| `sqe-coordinator` | Statement router. Handles statements that need catalog calls or auth before execution. | `crates/sqe-coordinator/src/query_handler.rs`, `catalog_ops.rs`. |

The two registration entry points are `crates/sqe-coordinator/src/session_context.rs` (cluster mode) and `crates/sqe-cli/src/embedded.rs` (single-binary mode). Both register the same UDFs / UDTFs in the same order, so a function works the same way in both personas.

## Pages

### Scalar functions

- [Conditional and null-handling](./conditional.md): `if`, `iff`, `case`, `coalesce`, `nullif`, `greatest`, `least`, `nvl`, `nvl2`, `typeof`, `try`.
- [String](./string.md): `concat`, `substring`, `trim`, `lower`, `upper`, regex, normalisation, `split`, `format`, hash digests.
- [Math](./math.md): trig, rounding, logs, exponents, sign, modular, base conversion.
- [Date and time](./datetime.md): timestamp construction, extraction, formatting, parsing, arithmetic, time-zone handling.
- [Array, map, struct](./array-map.md): the 40+ functions from `datafusion-functions-nested` plus Trino aggregate constructors (`map_agg`, `histogram`).
- [JSON](./json.md): two layered surfaces: Trino-named (`json_extract`, `json_parse`) and the `datafusion-functions-json` `json_get_*` family.
- [Encoding, hashing, URL](./encoding-url.md): base64, hex, `md5`, `sha224..512`, `url_extract_*`, `url_encode`, `url_decode`.

### Aggregate and window

- [Aggregate functions](./aggregate.md): `count`, `sum`, `avg`, statistical, regression, `array_agg`, `string_agg` / `listagg`, `histogram`, `map_agg`, approximation.
- [Window functions](./window.md): `row_number`, `rank`, `lag`, `lead`, `first_value`, frame syntax (`ROWS BETWEEN`, `RANGE BETWEEN`, `GROUPS BETWEEN`).

### Table-valued functions

- [Table-valued functions](./table-functions.md): file format (`read_parquet`, `read_csv`, `read_json`, `read_delta`), Iceberg metadata (`table_snapshots`, `table_history`, `table_files`, `table_partitions`, `table_manifests`, `table_refs`), generators (`generate_series`, `unnest`).

### Statements

- [DDL](./ddl.md): `CREATE`, `ALTER`, `DROP` for tables, schemas, views; partition evolution; branches and tags; column defaults.
- [DML](./dml.md): `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `COPY TO`, `TRUNCATE`, time travel (`FOR VERSION AS OF`, `FOR SYSTEM_TIME AS OF`, `FOR INCREMENTAL BETWEEN`), `SET WRITE_BRANCH`.
- [CALL procedures](./procedures.md): `system.rewrite_data_files`, `expire_snapshots`, `remove_orphan_files`, `rewrite_manifests`, `suggest_bloom_filter_columns`.
- [GRANT and REVOKE](./grant-revoke.md): SQE-specific security extensions. `GRANT MASKED WITH`, `GRANT ROWS WHERE`, `SHOW GRANTS`, `SHOW EFFECTIVE GRANTS`, `CHECK ACCESS`.
- [SHOW and EXPLAIN](./show-explain.md): metadata queries and plan inspection. `SHOW CATALOGS`, `SHOW STATS`, `EXPLAIN FULL`.
- [Operators](./operators.md): arithmetic, string, comparison, null tests, casting (`::`), set membership.

### Embedded CLI

- [Dot-commands](./dot-commands.md): `.help`, `.tables`, `.schema`, `.describe`, `.summarize`, `.timer`, `.read`, `.format`. Embedded CLI only.

## What is intentionally not in SQE

Some functions appear in the dialect comparison columns as missing. The reasoning:

- **`PIVOT`, `UNPIVOT`, `QUALIFY`, `ASOF JOIN`, FROM-first syntax**: DataFusion's parser does not accept them. Tracked upstream.
- **Lambda expressions, list comprehensions**: DataFusion has no AST node for closures.
- **Oracle / Snowflake `DECODE`**: name collides with DataFusion's binary `decode(input, encoding)` helper. `CASE WHEN` covers the use case.
- **`IIF` (T-SQL)**: covered by `if` (Trino) and `iff` (Snowflake), both registered.
- **`postgres_table_scanner`, `mysql_table_scanner`, `sqlite_scanner`**: out of scope. SQE is Iceberg-first; if you need a non-Iceberg engine, query it where it lives.
- **`spatial`, `vss`, `fts`, `excel`**: niche. Use a tool built for the job (PostGIS, a vector DB, an FTS engine).

The full DuckDB-comparison audit lives at [`duckdb-comparision.md`](../../../duckdb-comparision.md). The Trino-comparison audit lives at [`trino-compatibility.md`](../../../trino-compatibility.md).
