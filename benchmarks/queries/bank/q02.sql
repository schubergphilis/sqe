-- name: Channel mix trend (7-day window)
-- timeout: 300s
SELECT
    t_day,
    t_channel,
    COUNT(*)                                            AS txn_count,
    SUM(t_amount)                                       AS volume,
    COUNT(*) FILTER (WHERE t_status = 'rejected')       AS rejected,
    COUNT(*) FILTER (WHERE t_status = 'pending')        AS pending
FROM
    transaction
WHERE
    t_day BETWEEN DATE '2026-06-05' AND DATE '2026-06-11'
GROUP BY
    t_day, t_channel
ORDER BY
    t_day, volume DESC
