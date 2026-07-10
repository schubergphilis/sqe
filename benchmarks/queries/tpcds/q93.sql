-- name: Store customers with high net loss excluding certain return reasons
-- timeout: 60s
SELECT ss_customer_sk,
       SUM(act_sales) AS sumsales
FROM (
    SELECT ss_item_sk, ss_ticket_number, ss_customer_sk,
           CASE WHEN sr_return_quantity IS NOT NULL
                THEN (ss_quantity - sr_return_quantity) * ss_sales_price
                ELSE (ss_quantity * ss_sales_price)
           END AS act_sales
    FROM store_sales
    LEFT OUTER JOIN store_returns
        ON sr_item_sk    = ss_item_sk
       AND sr_ticket_number = ss_ticket_number
    WHERE sr_reason_sk IS NULL
       OR sr_reason_sk IN (
           SELECT r_reason_sk FROM reason
           WHERE r_reason_desc LIKE '%Found%'
       )
) t
GROUP BY ss_customer_sk
ORDER BY sumsales DESC, ss_customer_sk
LIMIT 100;
