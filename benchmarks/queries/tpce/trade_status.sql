-- name: Trade Status
-- Recent trade status for customer accounts: pending and submitted trades
-- with current status history.

SELECT
    t.t_id                              AS trade_id,
    t.t_dts                             AS submitted_date,
    st.st_name                          AS status,
    tt.tt_name                          AS trade_type,
    t.t_s_symb                          AS symbol,
    t.t_qty                             AS quantity,
    t.t_bid_price                       AS bid_price,
    t.t_exec_name                       AS executor,
    ca.ca_name                          AS account_name,
    th.th_dts                           AS status_date,
    th.th_st_id                         AS history_status
FROM
    trade           t
    JOIN status_type   st ON st.st_id   = t.t_st_id
    JOIN trade_type    tt ON tt.tt_id   = t.t_tt_id
    JOIN customer_account ca ON ca.ca_id = t.t_ca_id
    JOIN trade_history th ON th.th_t_id = t.t_id
WHERE
    t.t_st_id IN ('PNDG', 'SBMT', 'ACTV')
ORDER BY
    t.t_dts DESC,
    t.t_id,
    th.th_dts;
