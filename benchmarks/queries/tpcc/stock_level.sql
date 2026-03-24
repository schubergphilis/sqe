-- name: Stock Level Query
-- description: Count items below stock threshold among the last 20 orders in a district
-- type: read-only
-- timeout: 30s

SELECT COUNT(DISTINCT s.s_i_id) AS low_stock
FROM
    order_line ol
    JOIN stock s
        ON s.s_i_id = ol.ol_i_id
        AND s.s_w_id = ol.ol_w_id
WHERE
    ol.ol_w_id = 1
    AND ol.ol_d_id = 1
    AND ol.ol_o_id BETWEEN (
        SELECT d_next_o_id - 20
        FROM district
        WHERE d_w_id = 1 AND d_id = 1
    ) AND (
        SELECT d_next_o_id - 1
        FROM district
        WHERE d_w_id = 1 AND d_id = 1
    )
    AND s.s_quantity < 15;
