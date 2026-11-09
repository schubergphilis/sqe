# 2026-11-09 fix/json-marker-case-848

Part of #848: the JSON marker through `CASE` and `coalesce`.

## What changed

- `json_cast.rs`: `JsonMarkerRule` (analyzer, appended by
  `register_extended_trino_functions`, so after `type_coercion`; added once
  per context). DataFusion's `Expr::to_field` builds a `CASE` field without
  metadata, and `coalesce` / `nvl` / `ifnull` / `nullif` / `nvl2` return a
  plain field. When every result (THEN/ELSE, the coalesce arguments, the
  first `nullif` argument, the last two `nvl2` arguments) that is not a
  NULL literal or a cast NULL carries `sqe.json`, the expression is wrapped
  in `__sqe_json_text(x)`, an identity UDF whose return field has the
  marker. Bottom-up, so nested CASE/coalesce compose. Top-level names are
  kept with `NamePreserver`.
- Tests: `test_to_json_of_json_value_is_the_identity` (the old "CASE still
  quoted" case now embeds; CASE with ELSE, coalesce, nullif, through a
  subquery alias; a branch that is a plain string stays quoted),
  `test_typeof_json_value_is_json` (CASE with a NULL branch is `json`),
  the column-input recipe for `__sqe_json_text`, and the IT
  `cast_as_json_of_a_json_value_is_the_identity` (CASE and coalesce over a
  table).
- Book `sql-reference/json.md`: the known-differences entry.

## Left open (#848)

- A stored V2 `JSON` column has no marker: needs the declared type kept
  (table property or sidecar), set on the scan field and kept through the
  policy `CASE WHEN <filter> THEN col END` guard.
- `ARRAY[payload]` and array columns are not embedded.

**NEXT:** stored V2 `JSON` column marker (#848), or typed read of shredded
variants (#870).
