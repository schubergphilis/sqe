**STATUS 2026-10-17: `JSON` is a variant on V3 tables (`feat/json-variant-v3`, #849, stacked on !1459).**

`plan_create_table_schema` decides the format version before mapping `JSON`: a V3-only column, then a `format-version` / `format_version` option, then the configured default. On V3 the schema is rebuilt with `JSON` as variant at any depth. ALTER ADD COLUMN maps `JSON` from the table's version and refuses nested variants below V3 too. `variant_json_args` wraps every `json*` scalar UDF so a variant argument arrives as JSON text; without it `json_get_int(j, ...)` on a V3 JSON column failed at planning ("expected a string, got Struct"). Design and GIVEN/WHEN/THEN specs in `openspec/changes/iceberg-v3-variant/`.

Verified: unit tests (planner version sources and errors, wrapper over variant and text, shadowed-keys check), `variant_e2e` on the local stack including `json_column_is_a_variant_on_a_v3_table` and `json_functions_read_a_v3_json_column`.

Not verified: the embedded CLI (`sqe-cli`) does not wrap the JSON functions yet; Spark 4 interop; worker-path scans.

`default_table_format_version` now defaults to 3, so a plain `CREATE TABLE` is V3 and its `JSON` columns are variants. The opt-out for Trino < 481 / Spark < 4.0 / Iceberg < 1.10 readers is the setting or `WITH ('format-version' = '2')`. The quickstarts and `tests/sqe-ranger-test.toml` set 3; the Glue / S3 Tables quickstarts take the default. An unknown configured version now falls back to V3, not V2. `docker-compose.spark.yml` moves from Spark 4.2 + iceberg-spark-runtime-3.5 1.7.1 (no V3 reads) to Spark 4.1.3 + runtime-4.1 1.12.0; the Ranger quickstart's Spark 3.5 + Kyuubi image moves Iceberg 1.8.1 -> 1.12.0 and AWS SDK 2.24.6 -> 2.54.17. The Spark equality-delete interop test still creates its table as V2 on purpose. Neither Spark stack has been run with the bump yet.

**NEXT:**
- Test the Glue / S3 Tables quickstarts against V3 tables.
- MR3: `variant_get` / `parse_json` / `typeof` on variant, shredded reads via `unshred_variant`.
- Wrap the JSON functions in the embedded CLI session.
- Spark stack to iceberg-spark-runtime-4.0_2.13 1.10.x with an SQE-writes / Spark-reads variant test.
