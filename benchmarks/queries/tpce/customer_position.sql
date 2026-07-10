-- name: Customer Position
-- Customer account positions: current holdings with market value and
-- the most recent completed trades per account.

SELECT
    ca.ca_id                            AS account_id,
    ca.ca_name                          AS account_name,
    ca.ca_bal                           AS cash_balance,
    hs.hs_s_symb                        AS symbol,
    hs.hs_qty                           AS qty_held,
    lt.lt_price                         AS last_price,
    hs.hs_qty * lt.lt_price             AS market_value,
    t.t_id                              AS last_trade_id,
    t.t_dts                             AS last_trade_date,
    t.t_trade_price                     AS last_trade_price
FROM
    customer_account ca
    JOIN holding_summary hs ON hs.hs_ca_id = ca.ca_id
    JOIN last_trade      lt ON lt.lt_s_symb = hs.hs_s_symb
    LEFT JOIN trade      t  ON t.t_ca_id    = ca.ca_id
                            AND t.t_st_id   = 'CMPT'
WHERE
    ca.ca_tax_st IN (0, 1)
ORDER BY
    ca.ca_id,
    market_value DESC;
