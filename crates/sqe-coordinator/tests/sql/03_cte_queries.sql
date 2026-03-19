--- simple_cte
WITH nums AS (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3)
SELECT n FROM nums ORDER BY n;
--- expect
n
1
2
3

--- cte_with_filter
WITH data AS (SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob' UNION ALL SELECT 3, 'Carol')
SELECT id, name FROM data WHERE id > 1 ORDER BY id;
--- expect
id | name
2 | Bob
3 | Carol

--- cte_with_aggregation
WITH scores AS (
  SELECT 'Alice' AS name, 85 AS score UNION ALL
  SELECT 'Bob',           90            UNION ALL
  SELECT 'Carol',         78
)
SELECT COUNT(*) AS total, MAX(score) AS top FROM scores;
--- expect
total | top
3 | 90

--- multiple_ctes
WITH evens AS (
  SELECT 2 AS n UNION ALL SELECT 4 UNION ALL SELECT 6
),
odds AS (
  SELECT 1 AS n UNION ALL SELECT 3 UNION ALL SELECT 5
)
SELECT n FROM evens UNION ALL SELECT n FROM odds ORDER BY n;
--- expect
n
1
2
3
4
5
6

--- cte_self_reference_via_join
WITH employees AS (
  SELECT 1 AS id, 'Alice' AS name, CAST(NULL AS BIGINT) AS mgr_id UNION ALL
  SELECT 2, 'Bob',   1 UNION ALL
  SELECT 3, 'Carol', 1
)
SELECT e.name AS employee, m.name AS manager
FROM employees e
LEFT JOIN employees m ON e.mgr_id = m.id
ORDER BY e.id;
--- expect
employee | manager
Alice | NULL
Bob | Alice
Carol | Alice

--- subquery_in_where
WITH salaries AS (
  SELECT 'Alice' AS name, 90000 AS salary UNION ALL
  SELECT 'Bob',           85000            UNION ALL
  SELECT 'Carol',         70000
)
SELECT name FROM salaries
WHERE salary > (SELECT AVG(salary) FROM salaries)
ORDER BY name;
--- expect
name
Alice
Bob
