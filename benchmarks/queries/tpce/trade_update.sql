-- name: Trade Update
-- Read portion of the Trade Update transaction: retrieve completed trades
-- eligible for executor-name or settlement corrections.
-- Note: the full TPC-E Trade Update transaction modifies trade and settlement rows.

SELECT
    t.t_id                              AS trade_id,
    t.t_dts                             AS trade_date,
    t.t_exec_name                       AS executor,
    tt.tt_name                          AS trade_type,
    t.t_s_symb                          AS symbol,
    t.t_qty                             AS quantity,
    t.t_trade_price                     AS trade_price,
    se.se_cash_type                     AS settlement_type,
    se.se_cash_due_date                 AS due_date,
    se.se_amt                           AS settlement_amount,
    ct.ct_name                          AS cash_name,
    ct.ct_amt                           AS cash_amount
FROM
    trade            t
    JOIN trade_type     tt ON tt.tt_id   = t.t_tt_id
    JOIN settlement     se ON se.se_t_id = t.t_id
    LEFT JOIN cash_transaction ct ON ct.ct_t_id = t.t_id
WHERE
    t.t_st_id = 'CMPT'
ORDER BY
    t.t_dts DESC,
    t.t_id;
