-- name: Failed payment monitoring (3-day window)
-- timeout: 300s
SELECT
    t_day,
    t_channel,
    t_country,
    COUNT(*)      AS failed_count,
    SUM(t_amount) AS failed_volume
FROM
    transaction
WHERE
    t_day BETWEEN DATE '2026-06-09' AND DATE '2026-06-11'
    AND t_status = 'rejected'
GROUP BY
    t_day, t_channel, t_country
ORDER BY
    failed_count DESC
LIMIT 200
