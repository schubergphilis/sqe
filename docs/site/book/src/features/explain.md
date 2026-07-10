# Query Plan Inspection (EXPLAIN)

SQE provides three variants of `EXPLAIN` for inspecting how queries are planned and executed.

## EXPLAIN

Returns the logical and physical query plan without executing the query.

```sql
EXPLAIN SELECT * FROM orders WHERE amount > 100;
```

**Output:** Two rows, `logical_plan` and `physical_plan`, each containing a
text representation of the plan tree. The plan shown is the **policy-enforced**
plan: any row filters or column masks applied by the security layer are visible.

## EXPLAIN ANALYZE

Executes the query and returns per-operator timing and row counts.

```sql
EXPLAIN ANALYZE
SELECT dept_id, COUNT(*), AVG(salary)
FROM employees
GROUP BY dept_id;
```

**Output columns:** `step`, `operation`, `output_rows`, `elapsed_ms`

Rows are ordered leaf-to-root (execution order). `output_rows` and `elapsed_ms`
are NULL for operators that do not expose DataFusion metrics.

## EXPLAIN FULL

Returns the plan enriched with Iceberg table statistics, without executing the query.

```sql
EXPLAIN FULL SELECT * FROM large_table WHERE region = 'EU';
```

**Output columns:** `step`, `operation`, `estimated_rows`, `estimated_bytes`,
`files_scanned`, `files_total`

For `IcebergScanExec` nodes, statistics come from the Iceberg snapshot summary
(fast, no data file reads). `estimated_rows` reflects the total rows in the
snapshot at plan time. `files_scanned` equals `files_total` because
predicate-pushdown to file level is not yet implemented.

For other operators (Filter, Aggregate, Sort) `estimated_rows` comes from
DataFusion's cardinality analysis where available; file columns are NULL.

## Notes

- All three variants apply policy enforcement. The plan reflects what will
  actually execute for the authenticated user.
- `EXPLAIN FULL` on non-Iceberg tables (e.g., `information_schema`) returns
  NULL for all statistics columns without error.
