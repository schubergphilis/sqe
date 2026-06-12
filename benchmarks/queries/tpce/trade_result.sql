-- name: Trade Result
-- Read portion of the Trade Result transaction: retrieve pending trades
-- and their holding impact for settlement computation.
-- Note: the full TPC-E Trade Result transaction updates holdings and inserts settlement.

SELECT
    t.t_id                              AS trade_id,
    t.t_dts                             AS trade_date,
    tt.tt_name                          AS trade_type,
    tt.tt_is_sell                       AS is_sell,
    t.t_s_symb                          AS symbol,
    t.t_qty                             AS quantity,
    t.t_trade_price                     AS trade_price,
    t.t_chrg                            AS charge,
    t.t_comm                            AS commission,
    t.t_tax                             AS tax,
    t.t_is_cash                         AS is_cash,
    h.h_qty                             AS current_holding_qty,
    h.h_price                           AS holding_price,
    hs.hs_qty                           AS summary_qty,
    t.t_qty * t.t_trade_price
        - t.t_chrg - t.t_comm - t.t_tax AS net_proceeds
FROM
    trade            t
    JOIN trade_type     tt ON tt.tt_id   = t.t_tt_id
    LEFT JOIN holding       h  ON h.h_ca_id  = t.t_ca_id
                               AND h.h_s_symb = t.t_s_symb
    LEFT JOIN holding_summary hs ON hs.hs_ca_id = t.t_ca_id
                                 AND hs.hs_s_symb = t.t_s_symb
WHERE
    t.t_st_id = 'PNDG'
ORDER BY
    t.t_dts,
    t.t_id
-- Cap the settlement worklist: without it this returns every pending
-- trade x holding row (21.6M at SF1), which is a result-transfer
-- stress test, not a query benchmark -- polling that over Trino's HTTP
-- protocol OOM-killed the comparison container twice. The sort key
-- (t_dts, t_id) is unique, so the top-N is deterministic.
LIMIT 1000;
