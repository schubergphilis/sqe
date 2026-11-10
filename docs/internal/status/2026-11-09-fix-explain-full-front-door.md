# 2026-11-09 fix/explain-full-front-door

Found while testing #870 (!1697): `EXPLAIN FULL SELECT .. WHERE d = 25e-1`
failed with `Cannot cast to Decimal128(30, 15). Overflowing on NaN` while
the same SELECT ran.

## Cause

`SqlFrontDoor::normalize` returns SQE-only grammar as soon as `dispatch`
classifies it. `EXPLAIN FULL` is SQE-only grammar, so its inner query kept
the user's text: no Trino-compat rewrites (`1e0` -> double, `FETCH FIRST`
-> `LIMIT`, ...), no identifier case folding, no `information_schema`
qualification. `Explainer::full` then planned that raw text.

## Fix

`normalize` runs the inner text of `StatementKind::ExplainFull` through
`normalize` itself, so it gets exactly the rewrites the query gets alone.

## Tests

- `front_door::tests::explain_full_normalizes_its_query` (every dialect).
- `test_explain_full` also explains a query with `FETCH FIRST` and `5e-1`.

**NEXT:** double vs decimal comparison coerces to decimal (Trino: double),
next MR.
