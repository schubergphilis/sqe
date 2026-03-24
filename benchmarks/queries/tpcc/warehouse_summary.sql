-- name: Warehouse Summary
-- description: Aggregate statistics per warehouse: district count, order count, total revenue
-- type: read-only
-- timeout: 60s

SELECT
    w.w_id,
    w.w_name,
    w.w_state,
    w.w_tax,
    w.w_ytd,
    COUNT(DISTINCT d.d_id)  AS district_count,
    COUNT(DISTINCT o.o_id)  AS order_count,
    SUM(ol.ol_amount)       AS total_revenue,
    AVG(ol.ol_amount)       AS avg_order_line_amount
FROM
    warehouse w
    JOIN district d
        ON d.d_w_id = w.w_id
    JOIN orders o
        ON o.o_w_id = w.w_id
        AND o.o_d_id = d.d_id
    JOIN order_line ol
        ON ol.ol_w_id = o.o_w_id
        AND ol.ol_d_id = o.o_d_id
        AND ol.ol_o_id = o.o_id
GROUP BY
    w.w_id,
    w.w_name,
    w.w_state,
    w.w_tax,
    w.w_ytd
ORDER BY
    w.w_id;
