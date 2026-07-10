--- qualify_top_per_partition
-- Canonical DuckDB QUALIFY: keep the top row per group by a window rank,
-- without a wrapping subquery. docs/duckdb-comparision.md previously listed
-- QUALIFY as unsupported; it is in fact handled by DataFusion's SQL planner
-- (verified on the DataFusion 54 build; the planner code is unchanged from 53.1).
WITH sales AS (
  SELECT 'A' AS region, 'Jan' AS mon, 100 AS amt UNION ALL
  SELECT 'A', 'Feb', 200                          UNION ALL
  SELECT 'B', 'Jan', 50                           UNION ALL
  SELECT 'B', 'Feb', 75
)
SELECT region, mon, amt
FROM sales
QUALIFY row_number() OVER (PARTITION BY region ORDER BY amt DESC) = 1
ORDER BY region;
--- expect
region | mon | amt
A | Feb | 200
B | Feb | 75

--- qualify_simple_filter
-- QUALIFY filtering on a window aggregate without an alias.
WITH t AS (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3)
SELECT n FROM t
QUALIFY row_number() OVER (ORDER BY n DESC) = 1;
--- expect
n
3
