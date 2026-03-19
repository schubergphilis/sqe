--- count_star
WITH data AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3)
SELECT COUNT(*) AS cnt FROM data;
--- expect
cnt
3

--- sum_integers
WITH nums AS (SELECT 10 AS n UNION ALL SELECT 20 UNION ALL SELECT 30)
SELECT SUM(n) AS total FROM nums;
--- expect
total
60

--- avg_integers
WITH nums AS (SELECT 10 AS n UNION ALL SELECT 20 UNION ALL SELECT 30)
SELECT AVG(CAST(n AS DOUBLE)) AS avg_val FROM nums;
--- expect
avg_val
20.00

--- min_max
WITH nums AS (SELECT 5 AS n UNION ALL SELECT 1 UNION ALL SELECT 9 UNION ALL SELECT 3)
SELECT MIN(n) AS lo, MAX(n) AS hi FROM nums;
--- expect
lo | hi
1 | 9

--- group_by_count
WITH data AS (
  SELECT 'A' AS grp, 1 AS val UNION ALL
  SELECT 'A', 2               UNION ALL
  SELECT 'B', 3               UNION ALL
  SELECT 'B', 4               UNION ALL
  SELECT 'B', 5
)
SELECT grp, COUNT(*) AS cnt FROM data GROUP BY grp ORDER BY grp;
--- expect
grp | cnt
A | 2
B | 3

--- group_by_sum
WITH data AS (
  SELECT 'X' AS grp, 10 AS val UNION ALL
  SELECT 'X', 20               UNION ALL
  SELECT 'Y', 5
)
SELECT grp, SUM(val) AS total FROM data GROUP BY grp ORDER BY grp;
--- expect
grp | total
X | 30
Y | 5

--- having_filter
WITH data AS (
  SELECT 'A' AS grp, 100 AS val UNION ALL
  SELECT 'A', 200               UNION ALL
  SELECT 'B', 10                UNION ALL
  SELECT 'B', 20
)
SELECT grp, AVG(CAST(val AS DOUBLE)) AS avg_val
FROM data
GROUP BY grp
HAVING AVG(CAST(val AS DOUBLE)) > 50
ORDER BY grp;
--- expect
grp | avg_val
A | 150.00

--- count_distinct
WITH data AS (
  SELECT 'a' AS v UNION ALL
  SELECT 'b'      UNION ALL
  SELECT 'a'      UNION ALL
  SELECT 'c'
)
SELECT COUNT(DISTINCT v) AS cnt FROM data;
--- expect
cnt
3

--- count_nulls_excluded
WITH data AS (SELECT 1 AS n UNION ALL SELECT NULL UNION ALL SELECT 3)
SELECT COUNT(n) AS cnt FROM data;
--- expect
cnt
2
