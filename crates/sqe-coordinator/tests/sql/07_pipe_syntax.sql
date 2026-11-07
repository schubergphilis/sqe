--- pipe_where_aggregate_order
-- SQL pipe syntax (#851): the issue's query shape, FROM-first with pipe steps.
WITH orders AS (
  SELECT 1 AS customer_id, 500 AS amount, DATE '2025-01-10' AS order_date UNION ALL
  SELECT 1, 700, DATE '2025-02-01'                                      UNION ALL
  SELECT 2, 400, DATE '2024-12-31'                                      UNION ALL
  SELECT 2, 300, DATE '2025-03-05'                                      UNION ALL
  SELECT 3, 3000, DATE '2025-04-01'
)
FROM orders
|> WHERE order_date >= DATE '2025-01-01'
|> AGGREGATE SUM(amount) AS total_spent GROUP BY customer_id
|> WHERE total_spent > 1000
|> ORDER BY total_spent DESC;
--- expect
customer_id | total_spent
3 | 3000
1 | 1200

--- pipe_select_extend_limit
WITH t AS (SELECT 1 AS a UNION ALL SELECT 2 UNION ALL SELECT 3)
SELECT a FROM t
|> EXTEND a * 10 AS b
|> ORDER BY a
|> LIMIT 2;
--- expect
a | b
1 | 10
2 | 20

--- pipe_bitwise_or_untouched
SELECT 6 | 1 AS v, length('a|>b') AS s;
--- expect
v | s
7 | 4
