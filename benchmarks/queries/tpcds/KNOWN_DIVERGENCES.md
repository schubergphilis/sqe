# TPC-DS known divergences vs Trino

A handful of TPC-DS queries report `DIFF 0/1` against Trino at small scale
factors (SF0.1, SF1) when input is empty. This file enumerates them and
explains why they will keep reporting DIFF until DataFusion lands the fix.

## Affected queries

`q18`, `q27`, `q36`, `q67`, `q70`, `q86` — all six use `GROUP BY ROLLUP` or
`GROUPING` with a date predicate that filters the input down to zero rows
at small scale.

## Root cause

DataFusion emits zero rows for `GROUP BY ROLLUP (...)` when the input is
empty. Per SQL standard, ROLLUP must always emit the grand-total row
(with NULLs for the rolled-up grouping columns) regardless of whether the
input is empty. Trino does this. DataFusion does not.

Minimal repro (run against SQE):

```sql
SELECT SUM(x), y
FROM (SELECT 1 AS x, 'a' AS y WHERE 1=0)
GROUP BY ROLLUP(y);
```

- SQE / DataFusion: returns 0 rows.
- Trino: returns 1 row `(NULL, NULL)`.

## Upstream tracking

Filed as [apache/datafusion#21570](https://github.com/apache/datafusion/issues/21570)
on 2026-04-12. Assigned to jverhoeks + buraksenn. No PR yet.

Until that lands and SQE picks up a DataFusion release that includes the
fix, these six queries will continue to report DIFF at scale factors small
enough to produce an empty input. At SF10+ the input is never empty for
these queries, so they match cleanly.

## Why DIFF, not FAIL

The bench tool's comparison correctly counts rows in both responses. SQE
returns 0 rows, Trino returns 1 (the grand-total). The mismatch is real
and unavoidable until the upstream fix lands; the bench tool is right to
flag it.
