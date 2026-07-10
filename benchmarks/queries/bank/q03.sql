-- name: High-velocity accounts (3-day window)
-- timeout: 300s
-- Fraud-style screen: accounts with unusually many outgoing transactions
-- in a short window. The window is anchored to the generator's start day
-- (default 2026-06-01): the first three trading days, covered by both the
-- scale-factor path and the iceberg-sink default 12-day run.
-- At extreme volumes (e.g. the 4 TB/day run with the default customer
-- count) the per-account baseline itself exceeds 100 debits per 3 days and
-- this screen stops being selective; scale --customers with volume so the
-- baseline stays below the threshold.
SELECT
    t_a_id,
    COUNT(*)      AS txn_count,
    SUM(t_amount) AS total_out,
    COUNT(DISTINCT t_counterparty_iban) AS distinct_counterparties
FROM
    transaction
WHERE
    t_day BETWEEN DATE '2026-06-01' AND DATE '2026-06-03'
    AND t_direction = 'debit'
GROUP BY
    t_a_id
HAVING
    COUNT(*) > 100
ORDER BY
    txn_count DESC
LIMIT 100
