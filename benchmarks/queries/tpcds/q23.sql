-- name: Cross-channel top items purchased by frequent store buyers
-- timeout: 300s
WITH frequent_ss_items AS (
    SELECT SUBSTR(i_item_desc, 1, 30) itemdesc,
           i_item_sk item_sk,
           d_date solddate,
           COUNT(*) cnt
    FROM store_sales, date_dim, item
    WHERE ss_sold_date_sk = d_date_sk
      AND ss_item_sk      = i_item_sk
      AND d_year          IN (2000, 2001, 2002, 2003)
    GROUP BY SUBSTR(i_item_desc, 1, 30), i_item_sk, d_date
    HAVING COUNT(*) > 4
),
max_store_sales AS (
    SELECT MAX(csales) tpcds_cmax
    FROM (
        SELECT c_customer_sk,
               SUM(ss_quantity * ss_sales_price) csales
        FROM store_sales, customer, date_dim
        WHERE ss_customer_sk = c_customer_sk
          AND ss_sold_date_sk = d_date_sk
          AND d_year          IN (2000, 2001, 2002, 2003)
        GROUP BY c_customer_sk
    ) x
),
best_ss_customer AS (
    SELECT c_customer_sk, SUM(ss_quantity * ss_sales_price) ssales
    FROM store_sales, customer
    WHERE ss_customer_sk = c_customer_sk
    GROUP BY c_customer_sk
    HAVING SUM(ss_quantity * ss_sales_price) > 0.95 * (SELECT tpcds_cmax FROM max_store_sales)
)
SELECT SUM(sales) AS total_sales
FROM (
    SELECT cs_quantity * cs_list_price AS sales
    FROM catalog_sales, date_dim
    WHERE cs_item_sk        IN (SELECT item_sk FROM frequent_ss_items)
      AND cs_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
      AND cs_sold_date_sk   = d_date_sk
      AND d_year            = 2000
      AND d_moy             BETWEEN 1 AND 1 + 2
    UNION ALL
    SELECT ws_quantity * ws_list_price
    FROM web_sales, date_dim
    WHERE ws_item_sk        IN (SELECT item_sk FROM frequent_ss_items)
      AND ws_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
      AND ws_sold_date_sk   = d_date_sk
      AND d_year            = 2000
      AND d_moy             BETWEEN 1 AND 1 + 2
) y
LIMIT 100;
