-- Semantics-preserving candidate for TPCBB q01. The original benchmark remains
-- in benchmarks/queries/tpcbb/q01.sql. Compare values and plans before adoption.
WITH clicks AS (
    SELECT wcs_item_sk, COUNT(wcs_sales_sk) AS click_count
    FROM web_clickstreams
    WHERE wcs_sales_sk IS NOT NULL
    GROUP BY wcs_item_sk
), sales AS (
    SELECT ss_item_sk,
           COUNT(*) AS sale_count,
           SUM(ss_net_paid) AS net_paid,
           COUNT(ss_net_paid) AS paid_count
    FROM store_sales
    GROUP BY ss_item_sk
)
SELECT i.i_item_id, i.i_item_desc, i.i_category, i.i_class,
       SUM(c.click_count * s.sale_count) AS click_sales_count,
       SUM(c.click_count * s.net_paid) AS total_net_revenue,
       SUM(c.click_count * s.net_paid)
         / NULLIF(SUM(c.click_count * s.paid_count), 0) AS avg_sale_amount
FROM clicks c
JOIN sales s ON s.ss_item_sk = c.wcs_item_sk
JOIN item i ON i.i_item_sk = c.wcs_item_sk
GROUP BY i.i_item_id, i.i_item_desc, i.i_category, i.i_class
ORDER BY total_net_revenue DESC
LIMIT 20;
