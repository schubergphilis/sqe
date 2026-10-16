**STATUS 2026-10-16: `CAST(x AS JSON)` writes real JSON (`fix/trino-json-cast`).**

`CAST(x AS JSON)` is rewritten to `to_json(x)`, and `to_json` fell back to a quoted display string for anything that was not a primitive, so `CAST(ARRAY[1, 2] AS JSON)` came back as `"[1, 2]"`. `to_json` now lives in `sqe-trino-functions/src/json_cast.rs` and walks the Arrow array: lists, structs (objects keyed by field name), maps (objects with stringified keys sorted by key text), decimals at their scale, doubles and reals in Java `Double.toString` notation with NaN and the infinities as strings, dates and timestamps as strings. NULL in is SQL NULL out.

Because SQE carries JSON as Utf8, `to_json` cannot tell a JSON value from a string, so the rewrite drops a cast whose operand is a JSON-returning function (`json_parse`, `json_extract`, `json_array_get`, or an inner cast). `json_parse` now sorts object keys (`preserve_order` is on workspace-wide, so it kept input order), and `JSON '<text>'` is read as `json_parse('<text>')` so literals come back canonical.

Side-by-side probe against Trino 476, 47 expressions: 39 identical. The other eight are documented in the book's JSON page or are parser gaps already on the list:

- an anonymous `ROW(1, 'x')` gives `c0`/`c1` keys, where Trino gives `""` (two expressions)
- a JSON value inside an array is encoded as a string, since there is no type marker on Utf8. The same applies to `CAST(col AS JSON)` on a stored `JSON` column (not in the probe), which is the case dbt models are likely to hit
- varbinary and time are written as quoted text, and `CAST(json AS VARCHAR)` is allowed; Trino raises an error for all of these (three expressions)
- the `CAST(.. AS ARRAY(INTEGER))` and `U&'..'` in `ARRAY[..]` parse errors (two expressions)

Malformed input to `json_parse` still returns the text `null`, where Trino raises an error. That behaviour change is left for its own MR.

**NEXT:** `fix/trino-typeof-and-float-literals`: `typeof` returns Trino type names, and exponent literals are typed as double. Then the session time zone. Open follow-up: `json_parse` should raise an error on malformed input, as Trino does.
