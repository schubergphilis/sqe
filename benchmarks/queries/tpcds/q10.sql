-- name: Customer demographics for those buying from multiple channels
-- timeout: 60s
SELECT cd_gender, cd_marital_status, cd_education_status,
       COUNT(*)                   AS cnt1,
       AVG(cd_purchase_estimate)  AS avg1,
       COUNT(cd_credit_rating)    AS cnt2,
       AVG(cd_dep_count)          AS avg3,
       AVG(cd_dep_employed_count) AS avg4,
       AVG(cd_dep_college_count)  AS avg5
FROM customer c, customer_address ca, customer_demographics
WHERE c.c_current_addr_sk   = ca.ca_address_sk
  AND ca.ca_county IN ('Rush County', 'Toole County', 'Jefferson County',
                       'Dona Ana County', 'La Porte County')
  AND cd_demo_sk = c.c_current_cdemo_sk
  AND EXISTS (
      SELECT * FROM store_sales, date_dim
      WHERE c.c_customer_sk = ss_customer_sk
        AND ss_sold_date_sk = d_date_sk
        AND d_year = 2002
        AND d_moy BETWEEN 1 AND 1 + 3
  )
  AND (EXISTS (
           SELECT * FROM web_sales, date_dim
           WHERE c.c_customer_sk = ws_bill_customer_sk
             AND ws_sold_date_sk = d_date_sk
             AND d_year = 2002
             AND d_moy BETWEEN 1 AND 1 + 3
       )
       OR EXISTS (
           SELECT * FROM catalog_sales, date_dim
           WHERE c.c_customer_sk = cs_ship_customer_sk
             AND cs_sold_date_sk = d_date_sk
             AND d_year = 2002
             AND d_moy BETWEEN 1 AND 1 + 3
       ))
GROUP BY cd_gender, cd_marital_status, cd_education_status,
         cd_purchase_estimate, cd_credit_rating, cd_dep_count,
         cd_dep_employed_count, cd_dep_college_count
ORDER BY cd_gender, cd_marital_status, cd_education_status,
         cd_purchase_estimate, cd_credit_rating, cd_dep_count,
         cd_dep_employed_count, cd_dep_college_count
LIMIT 100;
