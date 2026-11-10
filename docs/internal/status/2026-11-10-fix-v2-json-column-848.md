**STATUS 2026-11-10: V2 `JSON` columns stay JSON (`fix/v2-json-column-848`).**

A V2 table stores `JSON` as an Iceberg string, so `CAST(j AS JSON)` quoted it. The table property `sqe.json-columns` (sorted JSON array of top-level names, `sqe_core::json_columns`) now records the JSON columns; `IcebergTableProvider::arrow_schema_for` marks those string fields `sqe.json`, which the coordinator and worker scan paths carry through `projected_schema`. `to_json` / `CAST AS JSON` of a marked string re-emits it canonical (unparsable text becomes a JSON string); `JsonText` keeps the input string layout (Utf8View or Utf8); the Trino encoder types any marked string `json`.

Writes: CREATE (non-v3) records declared JSON columns, CTAS records the marked columns of the SELECT, CREATE LIKE copies the source property, `ctas_replace_is_in_place` requires an equal JSON set (otherwise staged). ALTER ADD / DROP / RENAME / SET DATA TYPE maintain the property in the same commit as the schema (`json_columns_update`). Pre-existing tables opt in with `SET DATA TYPE JSON`. That ALTER leaves a V2 schema unchanged, and the catalog reuses an equal schema and then rejects `SetCurrentSchema(-1)`; `alter_table_schema` now selects an equal existing schema by id, or commits only the property when the schema is already current.

Integration (`variant_e2e::cast_as_json_returns_a_v2_json_column`, local stack): CREATE/INSERT/INSERT SELECT/UPDATE/MERGE, UNION, CTAS, CREATE LIKE, RENAME, DROP + re-ADD as VARCHAR, legacy opt-in. All `json_column` and `cast_as_json` ITs pass.

Limits: DESCRIBE, SHOW CREATE TABLE and information_schema show `varchar`; nested JSON (inside struct/array) is not tracked; a hand-set malformed property is ignored.

**NEXT:** ITs for a row filter and a mask over a V2 JSON column and for the distributed scan (`make test-distributed`); consider reporting `json` in DESCRIBE.
