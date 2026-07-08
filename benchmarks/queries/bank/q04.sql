-- name: Large transactions on flagged customers (7-day window)
-- timeout: 300s
-- KYC screening: large movements on accounts whose owner is politically
-- exposed, sanctioned, or high risk.
SELECT
    c.c_id,
    c.c_name,
    c.c_country,
    k.k_risk_rating,
    k.k_pep,
    k.k_sanctions_hit,
    COUNT(*)      AS large_txn_count,
    SUM(t.t_amount) AS large_txn_volume
FROM
    transaction t
    JOIN account a ON t.t_a_id = a.a_id
    JOIN customer c ON a.a_c_id = c.c_id
    JOIN kyc_profile k ON k.k_c_id = c.c_id
WHERE
    t.t_day BETWEEN DATE '2026-06-05' AND DATE '2026-06-11'
    AND t.t_amount > 25000
    AND (k.k_pep OR k.k_sanctions_hit OR k.k_risk_rating = 'high')
GROUP BY
    c.c_id, c.c_name, c.c_country, k.k_risk_rating, k.k_pep, k.k_sanctions_hit
ORDER BY
    large_txn_volume DESC
LIMIT 500
