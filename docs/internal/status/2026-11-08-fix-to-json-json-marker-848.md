# 2026-11-08 fix/to-json-json-marker-848

Part of #848: `CAST(x AS JSON)` of a value that is already JSON.

## What changed

- `json_cast.rs`: a private field marker, `sqe.json = "true"`, on the Utf8
  result of `json_parse`, `json_extract`, `json_array_get` and `to_json`
  (`return_field_from_args`). `to_json` returns a marked input unchanged.
  Not the canonical `ARROW:extension:name = arrow.json`: that would turn
  the column into a JSON extension array for pyarrow / ADBC clients.
- `typeof` of a marked value is `json`.
- `__sqe_json_array_of_json(list)`: writes each element as the JSON text
  it is, NULL as `null`. `trino_compat` rewrites `CAST(ARRAY[e, ..] AS
  JSON)` to it when every element is a JSON-returning call or NULL (an
  array literal drops the element marker; overriding `make_array` would
  change the list `DataType` and every coercion and write check with it).
- Trino protocol (`sqe-trino-compat/src/protocol.rs`): `columns_for_schema`
  reports a marked column as `json`, as Trino does
  (`json_text_column_is_reported_as_json`).
- Book: `sql-reference/json.md` (cast section, known differences) and the
  `typeof` row in `conditional.md`.

## Probe

Throwaway test through `engine_context`: the marker reaches the consumer
directly, through a CTE, a subquery, a view, a WHERE and a UNION ALL;
`coalesce` and a `CASE` with a real condition drop it (and constant
folding of a literal `CASE`). Those keep the old behaviour (quoted).

## Tests

- `sqe-trino-functions`: `test_to_json_of_json_value_is_the_identity`,
  `test_typeof_json_value_is_json`, `test_json_array_of_json_embeds_elements`,
  the array-shape invariant (`column_input_test`) with a recipe for the new
  UDF. `cargo test -p sqe-trino-functions`: 128 + 195 passed.
- `sqe-sql`: `cast_of_array_of_json_embeds_the_elements`.
- Integration `cast_as_json_of_a_json_value_is_the_identity`
  (`variant_e2e.rs`): subquery alias, array literal, `typeof`, CTAS and
  INSERT of a marked column into a V2 table, UNION ALL with a plain column.
  Gate: `make test-integration FILTER=cast_as_json` -> 2 passed
  (`cast_as_json_embeds_a_v3_json_column`,
  `cast_as_json_of_a_json_value_is_the_identity`).

## Left open (#848)

- A stored V2 `JSON` column: SQE stores it as `VARCHAR` with no declared
  type, so `CAST(payload AS JSON)` still quotes it. Needs the declared
  type recorded (table property or sidecar) and set on the scan field,
  and kept through the policy `CASE WHEN <filter> THEN col END` guard.
  V3 `JSON` columns are variants and already embed (c520ead62).
- `CASE` / `coalesce` drop the marker; `ARRAY[payload]` and array columns
  are not embedded.

**NEXT:** #870 truncated string bounds, or #812 (b)
`system.runtime.query_history` over the audit JSONL.
