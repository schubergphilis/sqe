-- name: District Order Counts
-- description: Order counts and revenue breakdown by warehouse and district
-- type: read-only
-- timeout: 60s

SELECT
    o.o_w_id,
    o.o_d_id,
    d.d_name,
    COUNT(*)                                                    AS order_count,
    COUNT(CASE WHEN no_.no_o_id IS NOT NULL THEN 1 END)        AS pending_delivery,
    COUNT(CASE WHEN o.o_carrier_id IS NOT NULL THEN 1 END)     AS delivered,
    SUM(ol.ol_amount)                                           AS total_revenue,
    AVG(o.o_ol_cnt)                                             AS avg_lines_per_order,
    MIN(o.o_entry_d)                                            AS earliest_order,
    MAX(o.o_entry_d)                                            AS latest_order
FROM
    orders o
    JOIN district d
        ON d.d_w_id = o.o_w_id
        AND d.d_id = o.o_d_id
    JOIN order_line ol
        ON ol.ol_w_id = o.o_w_id
        AND ol.ol_d_id = o.o_d_id
        AND ol.ol_o_id = o.o_id
    LEFT JOIN new_order no_
        ON no_.no_w_id = o.o_w_id
        AND no_.no_d_id = o.o_d_id
        AND no_.no_o_id = o.o_id
GROUP BY
    o.o_w_id,
    o.o_d_id,
    d.d_name
ORDER BY
    o.o_w_id,
    o.o_d_id;
