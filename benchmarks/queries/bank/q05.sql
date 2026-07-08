-- name: Single-day account range activity (1-day window)
-- timeout: 300s
-- Point-ish lookup: one day partition, narrow account-id range. Shows
-- partition pruning plus file-level min/max pruning on t_a_id (shards
-- write disjoint account ranges).
SELECT
    t_a_id,
    t_ts,
    t_amount,
    t_currency,
    t_direction,
    t_channel,
    t_status,
    t_description
FROM
    transaction
WHERE
    t_day = DATE '2026-06-10'
    AND t_a_id BETWEEN 1000 AND 1100
ORDER BY
    t_a_id, t_ts
LIMIT 1000
