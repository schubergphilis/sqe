-- name: Store sales by household and customer demographics
-- timeout: 60s
SELECT AVG(ss_quantity),
       AVG(ss_ext_sales_price),
       AVG(ss_ext_wholesale_cost),
       SUM(ss_ext_wholesale_cost)
FROM store_sales, store, customer_demographics, household_demographics,
     customer_address, date_dim
WHERE s_store_sk            = ss_store_sk
  AND ss_sold_date_sk       = d_date_sk
  AND d_year                = 2001
  AND ((ss_hdemo_sk         = hd_demo_sk
        AND cd_demo_sk      = ss_cdemo_sk
        AND cd_marital_status = 'U'
        AND cd_education_status = 'Advanced Degree'
        AND ss_sales_price  BETWEEN 100.00 AND 150.00
        AND hd_dep_count    = 3)
  OR   (ss_hdemo_sk         = hd_demo_sk
        AND cd_demo_sk      = ss_cdemo_sk
        AND cd_marital_status = 'M'
        AND cd_education_status = 'Primary'
        AND ss_sales_price  BETWEEN 50.00 AND 100.00
        AND hd_dep_count    = 1)
  OR   (ss_hdemo_sk         = hd_demo_sk
        AND cd_demo_sk      = ss_cdemo_sk
        AND cd_marital_status = 'D'
        AND cd_education_status = 'Secondary'
        AND ss_sales_price  BETWEEN 150.00 AND 200.00
        AND hd_dep_count    = 1))
  AND ((ss_addr_sk          = ca_address_sk
        AND ca_country       = 'United States'
        AND ca_state         IN ('CO', 'OH', 'TX')
        AND ss_net_profit    BETWEEN 100 AND 200)
  OR   (ss_addr_sk          = ca_address_sk
        AND ca_country       = 'United States'
        AND ca_state         IN ('OR', 'MN', 'KY')
        AND ss_net_profit    BETWEEN 150 AND 300)
  OR   (ss_addr_sk          = ca_address_sk
        AND ca_country       = 'United States'
        AND ca_state         IN ('VA', 'CA', 'MS')
        AND ss_net_profit    BETWEEN 50 AND 250));
