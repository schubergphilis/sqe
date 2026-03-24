-- name: Web sales return reasons by customer demographics
-- timeout: 60s
SELECT SUBSTR(r_reason_desc, 1, 20) AS reason,
       AVG(ws_quantity) AS avg_qty,
       AVG(wr_refunded_cash) AS avg_refunded,
       AVG(wr_fee) AS avg_fee
FROM web_sales, web_returns, web_page, customer_demographics cd1,
     customer_demographics cd2, customer_address, date_dim, reason
WHERE ws_web_page_sk            = wp_web_page_sk
  AND ws_item_sk                = wr_item_sk
  AND ws_order_number           = wr_order_number
  AND ws_sold_date_sk           = d_date_sk
  AND d_year                    = 2000
  AND cd1.cd_demo_sk            = wr_refunded_cdemo_sk
  AND cd2.cd_demo_sk            = wr_returning_cdemo_sk
  AND ca_address_sk             = wr_refunded_addr_sk
  AND r_reason_sk               = wr_reason_sk
  AND (
        (cd1.cd_marital_status  = 'M'
         AND cd1.cd_marital_status = cd2.cd_marital_status
         AND cd1.cd_education_status = 'Advanced Degree'
         AND cd1.cd_education_status = cd2.cd_education_status
         AND ca_country          = 'United States'
         AND ca_state            IN ('ND', 'WI', 'AL')
         AND ws_net_profit       BETWEEN 100 AND 200)
     OR (cd1.cd_marital_status  = 'S'
         AND cd1.cd_marital_status = cd2.cd_marital_status
         AND cd1.cd_education_status = 'College'
         AND cd1.cd_education_status = cd2.cd_education_status
         AND ca_country          = 'United States'
         AND ca_state            IN ('MD', 'IN', 'WA')
         AND ws_net_profit       BETWEEN 150 AND 300)
     OR (cd1.cd_marital_status  = 'D'
         AND cd1.cd_marital_status = cd2.cd_marital_status
         AND cd1.cd_education_status = '2 yr Degree'
         AND cd1.cd_education_status = cd2.cd_education_status
         AND ca_country          = 'United States'
         AND ca_state            IN ('WY', 'SD', 'HI')
         AND ws_net_profit       BETWEEN 50 AND 250)
  )
GROUP BY r_reason_desc
ORDER BY SUBSTR(r_reason_desc, 1, 20),
         avg_qty, avg_refunded, avg_fee
LIMIT 100;
