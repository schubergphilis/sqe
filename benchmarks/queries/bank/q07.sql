-- name: Balance trend by account type (7-day window)
-- timeout: 300s
SELECT
    b.b_day,
    a.a_type,
    b.b_currency,
    COUNT(*)         AS accounts,
    AVG(b.b_balance) AS avg_balance,
    SUM(b.b_balance) AS total_balance
FROM
    account_balance b
    JOIN account a ON b.b_a_id = a.a_id
WHERE
    b.b_day BETWEEN DATE '2026-06-05' AND DATE '2026-06-11'
    AND a.a_status = 'open'
GROUP BY
    b.b_day, a.a_type, b.b_currency
ORDER BY
    b.b_day, a.a_type, b.b_currency
