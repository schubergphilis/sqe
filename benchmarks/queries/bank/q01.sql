-- name: Daily settled volume (7-day window)
-- timeout: 300s
-- Window dates assume the default --start-date 2026-06-01; shift them to
-- match your run. Touches 7 day partitions, prunes the rest.
SELECT
    t_day,
    t_currency,
    COUNT(*)      AS txn_count,
    SUM(t_amount) AS total_amount,
    AVG(t_amount) AS avg_amount
FROM
    transaction
WHERE
    t_day BETWEEN DATE '2026-06-05' AND DATE '2026-06-11'
    AND t_status = 'settled'
GROUP BY
    t_day, t_currency
ORDER BY
    t_day, t_currency
