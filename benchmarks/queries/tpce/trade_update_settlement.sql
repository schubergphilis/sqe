-- name: Trade Update — Correct settlement cash type
-- description: Write portion of Trade Update (Frame 2): fix settlement cash_type on completed trades
-- timeout: 30s

-- The Trade Update transaction (Frame 2) corrects settlement cash_type.
-- TPC-E spec changes "Cash Account" to "Cash" or vice versa.
UPDATE settlement
SET se_cash_type = CASE
        WHEN se_cash_type = 'Cash Account' THEN 'Cash'
        ELSE 'Cash Account'
    END
WHERE se_t_id IN (
    SELECT t.t_id
    FROM trade t
    WHERE t.t_st_id = 'CMPT'
    ORDER BY t.t_dts DESC
    LIMIT 20
);
