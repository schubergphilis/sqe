## 1. VARIANT type (!1459)

- [x] 1.1 `sqe_core::variant`: storage type, extension tag, shape detection, JSON text in / out
- [x] 1.2 `VARIANT` in CREATE TABLE, nested in STRUCT / ROW, makes the table v3
- [x] 1.3 `ALTER TABLE ADD COLUMN ... VARIANT`, refused below v3
- [x] 1.4 CTAS, `CREATE TABLE ... LIKE` and INSERT SELECT keep the variant type
- [x] 1.5 Writes parse JSON text; invalid JSON fails the statement
- [x] 1.6 Reads render JSON: Trino `json`, Flight `Utf8`, DESCRIBE / information_schema `variant`, SHOW CREATE `VARIANT`
- [x] 1.7 Compaction keeps variant columns (`apply_stamped_schema`)
- [x] 1.8 Integration tests: `variant_e2e` round trip, CTAS, v2 refusal, text conversion

## 2. JSON follows the table version

- [x] 2.1 `sql_type_to_arrow_for(.., json_as_variant)` through STRUCT and ROW
- [x] 2.2 `requested_format_version`: `format-version` / `format_version`, values 1-3
- [x] 2.3 `plan_create_table_schema`: v3 features, then request, then config default; contradictions refused
- [x] 2.4 ALTER ADD COLUMN JSON on a v3 table adds a variant, also nested
- [x] 2.5 Unit tests for each version source and the error cases
- [x] 2.6 Integration test `json_column_is_a_variant_on_a_v3_table`

## 3. JSON functions over a variant

- [x] 3.1 `VariantJsonArgs` wrapper for every `json*` scalar function
- [x] 3.2 Unit tests: variant and text arguments, idempotent re-wrap
- [x] 3.3 Integration test `json_functions_read_a_v3_json_column`
- [ ] 3.4 Same wrapping in the embedded CLI session (`sqe-cli`)

## 4. Follow-ups

- [ ] 4.1 `variant_get`, `parse_json`, `typeof` on variant
- [ ] 4.2 Read shredded variants (`unshred_variant`)
- [ ] 4.3 Nested variants over Flight as JSON text
- [ ] 4.4 Spark 4 / Iceberg 1.10 interop test (local Spark stack is Iceberg 1.7)
- [ ] 4.5 Worker-path scan of a variant table
- [x] 4.6 `default_table_format_version` defaults to 3; docs name the opt-out
