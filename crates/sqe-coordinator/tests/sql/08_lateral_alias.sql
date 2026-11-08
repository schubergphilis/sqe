--- lateral_alias_parenthesized
-- Lateral column aliases (#878): a later select-list item names an earlier
-- alias; the alias's expression keeps its parentheses ((a + 1) * 2).
WITH t AS (SELECT 1 AS a UNION ALL SELECT 2)
SELECT a + 1 AS b, b * 2 AS c FROM t ORDER BY a;
--- expect
b | c
2 | 4
3 | 6

--- lateral_alias_from_column_wins
-- Databricks' rule: a column of the FROM relation beats an alias of the
-- same name.
WITH t AS (SELECT 10 AS price)
SELECT price * 2 AS price, price + 1 AS q FROM t;
--- expect
price | q
20 | 11

--- lateral_alias_over_aggregate
WITH t AS (
  SELECT 'x' AS g, 10 AS amt UNION ALL
  SELECT 'y', 20             UNION ALL
  SELECT 'x', 5
)
SELECT g, SUM(amt) AS s, s * 2 AS u FROM t GROUP BY g ORDER BY g;
--- expect
g | s | u
x | 15 | 30
y | 20 | 40
