-- name: Trade Lookup
-- Look up trades by various criteria: find completed trades for a set of
-- accounts with full settlement and cash transaction details.

SELECT
    t.t_id                              AS trade_id,
    t.t_dts                             AS trade_date,
    tt.tt_name                          AS trade_type,
    t.t_s_symb                          AS symbol,
    t.t_qty                             AS quantity,
    t.t_bid_price                       AS bid_price,
    t.t_trade_price                     AS trade_price,
    t.t_chrg                            AS charge,
    t.t_comm                            AS commission,
    t.t_tax                             AS tax,
    t.t_is_cash                         AS is_cash,
    se.se_cash_type                     AS settlement_type,
    se.se_cash_due_date                 AS settlement_date,
    se.se_amt                           AS settlement_amount,
    ct.ct_amt                           AS cash_amount,
    ct.ct_name                          AS cash_description
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
