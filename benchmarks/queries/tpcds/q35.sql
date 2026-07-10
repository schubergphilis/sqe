-- name: Customer demographic profile for multi-channel buyers
-- timeout: 60s
SELECT ca_state, cd_gender, cd_marital_status,
       COUNT(*) cnt1,
       AVG(cd_dep_count) avg1,
       AVG(cd_dep_employed_count) avg2,
       AVG(cd_dep_college_count) avg3
FROM customer c, customer_address ca, customer_demographics
WHERE c.c_current_addr_sk    = ca.ca_address_sk
  AND cd_demo_sk             = c.c_current_cdemo_sk
  AND EXISTS (
      SELECT * FROM store_sales, date_dim
      WHERE c.c_customer_sk  = ss_customer_sk
        AND ss_sold_date_sk  = d_date_sk
        AND d_year           = 2002
        AND d_moy            BETWEEN 1 AND 4
  )
  AND (NOT EXISTS (
           SELECT * FROM web_sales, date_dim
           WHERE c.c_customer_sk = ws_bill_customer_sk
             AND ws_sold_date_sk = d_date_sk
             AND d_year          = 2002
             AND d_moy           BETWEEN 1 AND 4
       )
       OR NOT EXISTS (
           SELECT * FROM catalog_sales, date_dim
           WHERE c.c_customer_sk = cs_ship_customer_sk
             AND cs_sold_date_sk = d_date_sk
             AND d_year          = 2002
             AND d_moy           BETWEEN 1 AND 4
       ))
GROUP BY ca_state, cd_gender, cd_marital_status,
         cd_dep_count, cd_dep_employed_count, cd_dep_college_count
ORDER BY ca_state, cd_gender, cd_marital_status, cnt1
LIMIT 100;
