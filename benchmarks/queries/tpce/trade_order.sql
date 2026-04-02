-- name: Trade Order
-- Read portion of the Trade Order transaction: validate account, security,
-- and compute estimated charges before order submission.
-- Note: the full TPC-E Trade Order transaction inserts into trade and trade_request.

SELECT
    ca.ca_id                            AS account_id,
    ca.ca_name                          AS account_name,
    ca.ca_bal                           AS cash_balance,
    ca.ca_tax_st                        AS tax_status,
    c.c_f_name                          AS first_name,
    c.c_l_name                          AS last_name,
    c.c_tier                            AS customer_tier,
    s.s_symb                            AS symbol,
    s.s_name                            AS security_name,
    lt.lt_price                         AS last_price,
    cr.cr_rate                          AS commission_rate,
    ch.ch_chrg                          AS charge_amount
FROM
    customer_account ca
    JOIN customer        c   ON c.c_id      = ca.ca_c_id
    JOIN last_trade      lt  ON lt.lt_s_symb IS NOT NULL
    JOIN security        s   ON s.s_symb    = lt.lt_s_symb
    JOIN commission_rate cr  ON cr.cr_c_tier = c.c_tier
                             AND cr.cr_tt_id  = 'TMS'
                             AND cr.cr_ex_id  = s.s_ex_id
                             AND cr.cr_from_qty <= 100
                             AND cr.cr_to_qty   >= 100
    JOIN charge          ch  ON ch.ch_tt_id  = 'TMS'
ORDER BY
    ca.ca_id,
    s.s_symb;
