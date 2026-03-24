-- name: Customers who bought from catalog or web after switching from store
-- timeout: 60s
WITH my_customers AS (
    SELECT DISTINCT c_customer_sk, c_current_addr_sk
    FROM (
        SELECT cs_bill_customer_sk AS c_customer_sk, cs_item_sk
        FROM catalog_sales, date_dim
        WHERE cs_sold_date_sk = d_date_sk
          AND d_year          = 2000
          AND d_moy           = 7
        UNION ALL
        SELECT ws_bill_customer_sk AS c_customer_sk, ws_item_sk
        FROM web_sales, date_dim
        WHERE ws_sold_date_sk = d_date_sk
          AND d_year          = 2000
          AND d_moy           = 7
    ) cs_or_ws_sales,
    item, customer
    WHERE cs_item_sk        = i_item_sk
      AND i_category        = 'Women'
      AND i_class           = 'accessories'
      AND c_customer_sk     = cs_bill_customer_sk
),
my_revenue AS (
    SELECT c_customer_sk,
           SUM(ss_ext_sales_price) AS revenue
    FROM my_customers, store_sales, customer_address, store, date_dim
    WHERE c_current_addr_sk = ca_address_sk
      AND ca_county         = s_county
      AND ca_state          = s_state
      AND ss_sold_date_sk   = d_date_sk
      AND c_customer_sk     = ss_customer_sk
      AND d_month_seq       BETWEEN (
          SELECT DISTINCT d_month_seq + 1
          FROM date_dim
          WHERE d_year = 2000 AND d_moy = 7
          LIMIT 1
      ) AND (
          SELECT DISTINCT d_month_seq + 3
          FROM date_dim
          WHERE d_year = 2000 AND d_moy = 7
          LIMIT 1
      )
    GROUP BY c_customer_sk
),
segments AS (
    SELECT CAST(revenue / 50 AS INTEGER) AS segment
    FROM my_revenue
)
SELECT segment, COUNT(*) AS num_customers,
       segment * 50 AS segment_base
FROM segments
GROUP BY segment
ORDER BY segment, num_customers
LIMIT 100;
