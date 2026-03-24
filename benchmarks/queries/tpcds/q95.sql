-- name: Web orders with multiple warehouse splits and no returns
-- timeout: 60s
WITH ws_wh AS (
    SELECT ws1.ws_order_number, ws1.ws_warehouse_sk wh1,
           ws2.ws_warehouse_sk wh2
    FROM web_sales ws1, web_sales ws2
    WHERE ws1.ws_order_number = ws2.ws_order_number
      AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
)
SELECT COUNT(DISTINCT ws_order_number)    AS order_count,
       SUM(ws_ext_ship_cost)             AS total_shipping_cost,
       SUM(ws_net_profit)                AS total_net_profit
FROM web_sales, date_dim, customer_address, web_site
WHERE d_date BETWEEN DATE '1999-02-01' AND DATE '1999-04-02'
  AND ws_ship_date_sk    = d_date_sk
  AND ws_ship_addr_sk    = ca_address_sk
  AND ca_state           = 'IL'
  AND ws_web_site_sk     = web_site_sk
  AND web_company_name   = 'pri'
  AND ws_order_number    IN (SELECT ws_order_number FROM ws_wh)
  AND ws_order_number    NOT IN (
      SELECT wr_order_number
      FROM web_returns, ws_wh
      WHERE wr_order_number = ws_wh.ws_order_number
  )
ORDER BY order_count
LIMIT 100;
