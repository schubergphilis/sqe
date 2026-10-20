# fix/json-cast-typeof-848-867

`to_json` (and so `CAST(x AS JSON)`, which the Trino rewrite turns into `to_json`) now writes a variant as its JSON value at any depth: a V3 `JSON` column, an array of them, a struct field holding one (#848). Before, it encoded the `metadata`/`value` binary storage. `typeof` returns the Trino type name the wire protocol reports (#867), `variant` for a variant.

Both share code with the server through `sqe-trino-protocol`: `arrow_to_trino_type` moved there from `sqe-trino-compat` (which re-exports it), and `variant_json` became public. `sqe-trino-functions` depends on `sqe-trino-protocol` (features `arrow`), which has no `sqe-*` dependencies, so the compile split is kept.

Gate: `make test-integration FILTER=variant_e2e` -> 9 passed, incl. `cast_as_json_embeds_a_v3_json_column` and the typeof assertion (`variant|integer`). Unit: `sqe-trino-functions` `to_json_embeds_a_variant`, `typeof_names_a_variant`, `typeof_returns_trino_type_names` (failed before the fix).

Left open (#848): a V2 JSON column is VARCHAR with no type marker, and `json_parse` returns Utf8, so `CAST(ARRAY[json_parse('{"a":1}')] AS JSON)` and a cast of a V2 JSON column still quote the text. Fixing that needs a JSON-typed value (an extension type on Utf8 or json_parse returning a variant). Documented in `sql-reference/json.md`.

Findings, not fixed:
- `information_schema.columns` maps types with a second mapper, `arrow_display_to_trino_type` (`info_schema_compat.rs`), which disagrees with `arrow_to_trino_type`: UInt8 -> smallint, UInt64 -> decimal(20,0), timestamp without `(6)`, bare `array`/`row`/`map`, variant -> `variant` vs `json`. Candidate follow-up issue.
- Literal typing differs from Trino: `typeof(42)` is `bigint` (Trino `integer`), `typeof('a')` is `varchar` (Trino `varchar(1)`). Documented.

**NEXT:** after merge close #867; keep #848 open for the V2/json_parse case or close it with the documented limitation.
