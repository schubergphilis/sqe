**STATUS 2026-10-19: Typed column DEFAULTs: no more CREATE TABLE panic; binary/fixed/UUID defaults applied on INSERT and MERGE (`fix/column-defaults-865`, closes #865 item 3).**

Found by probe before fixing: `default_to_iceberg_literal` (`crates/sqe-write/src/write_handler.rs`) turned any quoted DEFAULT on a non-string column into `PrimitiveLiteral::String`. Iceberg's `SerdeNestedField::from` then panics on `try_into_json`, so `CREATE TABLE t (d DATE DEFAULT '2024-01-02')` crashed the query (also TIMESTAMP, DECIMAL, BINARY, `BIGINT DEFAULT '5'`; the ALTER ADD COLUMN path shares the function). `DECIMAL DEFAULT 2` and `X'..'` were refused.

Fix:
- `typed_default_from_text` parses the text through `Literal::try_from_json` for the column's type (timestamps accept a space; timestamptz `Z` / no offset = UTC; decimals must fit precision and scale exactly). Every literal is checked with `try_into_json` before it reaches the metadata writer.
- `DefaultLiteral::Bytes` for `X'..'` (`crates/sqe-sql/src/v3_types.rs`).
- `omitted_column_sql` renders binary/fixed/UUID defaults as `arrow_cast(X'..', <arrow type>)`, so INSERT and MERGE INSERT apply them. That also removes the eager MERGE failure (`write_default_sqls` errored for every MERGE on a table with such a default).

SQL `UUID` maps to a string column and `FIXED(n)` is not creatable from SQL, so Iceberg UUID/fixed defaults only arrive on tables from other engines; covered by unit tests, not e2e.

Tests: `v3_e2e.rs::typed_defaults_apply_on_insert_merge_and_alter` (DATE, TIMESTAMP, DECIMAL text and integer, BINARY X'..', BIGINT '5' via INSERT and MERGE, ALTER ADD COLUMN DATE default read as initial_default by older rows, bad DATE default refused), `default_text_becomes_a_literal_of_the_column_type`, extended `omitted_column_sql_renders_each_default_as_its_typed_value`, `hex_default_is_extracted_as_bytes`. Gate: `FILTER=default` 6 passed, `FILTER=v3_e2e` 18 passed.

**NEXT:** close #865 once !1485 and this MR are merged.
