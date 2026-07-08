-- name: Cross-border corridors (7-day window)
-- timeout: 300s
-- Payment corridors: customer home country vs transaction country.
SELECT
    c.c_country  AS home_country,
    t.t_country  AS txn_country,
    COUNT(*)     AS txn_count,
    SUM(t.t_amount) AS volume
FROM
    transaction t
    JOIN account a ON t.t_a_id = a.a_id
    JOIN customer c ON a.a_c_id = c.c_id
WHERE
    t.t_day BETWEEN DATE '2026-06-05' AND DATE '2026-06-11'
    AND t.t_country <> c.c_country
GROUP BY
    c.c_country, t.t_country
ORDER BY
    volume DESC
LIMIT 100
