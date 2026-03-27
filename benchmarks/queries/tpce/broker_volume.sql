-- name: Broker Volume
-- Broker trading volume summary: total commissions and trade counts per broker,
-- broken down by trade type, for active brokers.

SELECT
    b.b_name                         AS broker_name,
    tt.tt_name                       AS tt_name,
    COUNT(t.t_id)                    AS trade_count,
    SUM(t.t_qty)                     AS total_qty,
    SUM(t.t_comm)                    AS total_commission,
    SUM(t.t_comm) / NULLIF(COUNT(t.t_id), 0) AS avg_commission
FROM
    trade        t
    JOIN broker     b  ON b.b_id    = t.t_ca_id
    JOIN trade_type tt ON tt.tt_id  = t.t_tt_id
WHERE
    b.b_st_id = 'ACTV'
GROUP BY
    b.b_name,
    tt.tt_name
ORDER BY
    total_commission DESC,
    broker_name,
    tt_name;
