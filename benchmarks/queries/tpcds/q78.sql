-- name: Customers who bought in store but not catalog or web in same year
-- timeout: 120s
WITH ss AS (
    SELECT d_year, ss_item_sk item_sk, SUM(ss_ext_sales_price) AS ss_item_rev
    FROM store_sales, date_dim
    WHERE ss_sold_date_sk = d_date_sk
    GROUP BY d_year, ss_item_sk
),
cs AS (
    SELECT d_year, cs_item_sk item_sk, SUM(cs_ext_sales_price) AS cs_item_rev
    FROM catalog_sales, date_dim
    WHERE cs_sold_date_sk = d_date_sk
    GROUP BY d_year, cs_item_sk
),
ws AS (
    SELECT d_year, ws_item_sk item_sk, SUM(ws_ext_sales_price) AS ws_item_rev
    FROM web_sales, date_dim
    WHERE ws_sold_date_sk = d_date_sk
    GROUP BY d_year, ws_item_sk
)
SELECT ss.d_year AS ss_sold_year,
       ss.item_sk,
       ss.ss_item_rev,
       ss.ss_item_rev / (ss.ss_item_rev + cs.cs_item_rev + ws.ws_item_rev) / 3.0 * 100 AS ss_dev,
       cs.cs_item_rev,
       cs.cs_item_rev / (ss.ss_item_rev + cs.cs_item_rev + ws.ws_item_rev) / 3.0 * 100 AS cs_dev,
       ws.ws_item_rev,
       ws.ws_item_rev / (ss.ss_item_rev + cs.cs_item_rev + ws.ws_item_rev) / 3.0 * 100 AS ws_dev,
       (ss.ss_item_rev + cs.cs_item_rev + ws.ws_item_rev) / 3.0 AS average
FROM ss, cs, ws
WHERE ss.d_year  = cs.d_year
  AND ss.d_year  = ws.d_year
  AND ss.item_sk = cs.item_sk
  AND ss.item_sk = ws.item_sk
  AND ss.d_year  = 2000
  AND ss.ss_item_rev BETWEEN 0.9 * cs.cs_item_rev AND 1.1 * cs.cs_item_rev
  AND ss.ss_item_rev BETWEEN 0.9 * ws.ws_item_rev AND 1.1 * ws.ws_item_rev
  AND cs.cs_item_rev BETWEEN 0.9 * ws.ws_item_rev AND 1.1 * ws.ws_item_rev
ORDER BY ss.d_year, ss.ss_item_rev DESC, ss.item_sk
LIMIT 100;
