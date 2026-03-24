-- name: Store net profit by item color and customer address
-- timeout: 60s
WITH ssales AS (
    SELECT c_last_name, c_first_name, s_store_name,
           ca_state, s_state, i_color, i_current_price,
           i_manager_id, i_units, i_size,
           SUM(ss_net_profit) AS netpaid
    FROM store_sales, store_returns, store, item, customer, customer_address
    WHERE ss_ticket_number   = sr_ticket_number
      AND ss_item_sk         = sr_item_sk
      AND ss_customer_sk     = c_customer_sk
      AND ss_item_sk         = i_item_sk
      AND ss_store_sk        = s_store_sk
      AND s_zip              = ca_zip
      AND c_current_addr_sk  = ca_address_sk
      AND i_color            = 'pale'
      AND i_current_price    BETWEEN 64 AND 64 + 10
      AND i_current_price    BETWEEN 64 + 1 AND 64 + 15
    GROUP BY c_last_name, c_first_name, s_store_name,
             ca_state, s_state, i_color, i_current_price,
             i_manager_id, i_units, i_size
)
SELECT c_last_name, c_first_name, s_store_name,
       SUM(netpaid) AS paid
FROM ssales
WHERE i_color = 'pale'
GROUP BY c_last_name, c_first_name, s_store_name
HAVING SUM(netpaid) > 0.05 * (SELECT AVG(netpaid) * 0.05 FROM ssales)
ORDER BY c_last_name, c_first_name, s_store_name;
