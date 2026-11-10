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
), weighted AS (
    SELECT i.i_item_id, i.i_item_desc, i.i_category, i.i_class,
           SUM(c.click_count * s.sale_count) AS click_sales_count,
           SUM(c.click_count * s.net_paid) AS net_paid,
           SUM(c.click_count * s.paid_count) AS paid_count
    FROM clicks c
    JOIN sales s ON s.ss_item_sk = c.wcs_item_sk
    JOIN item i ON i.i_item_sk = c.wcs_item_sk
    GROUP BY i.i_item_id, i.i_item_desc, i.i_category, i.i_class
), cents AS (
    SELECT *,
           CAST(net_paid * 100 AS DECIMAL(38, 0)) AS paid_cents,
           CAST(net_paid * 100 AS DECIMAL(38, 0)) % NULLIF(paid_count, 0) AS paid_rest
    FROM weighted
)
-- For a DECIMAL(7,2) ss_net_paid the original returns SUM as DECIMAL(38,2)
-- and AVG as DECIMAL(7,2), rounded half away from zero (Trino's types).
-- The average is the weighted sum over the weighted count. Dividing at the
-- division's scale and then casting to two decimals would round twice, and
-- from about 10^6 rows per group that is a cent off (0.0049999975 becomes
-- 0.005000, then 0.01). So the average divides in whole cents: the exact
-- quotient, plus one cent away from zero when the remainder is at least
-- half the count.
SELECT i_item_id, i_item_desc, i_category, i_class,
       click_sales_count,
       CAST(net_paid AS DECIMAL(38, 2)) AS total_net_revenue,
       CAST(((paid_cents - paid_rest) / paid_count
             + CASE WHEN 2 * ABS(paid_rest) >= paid_count
                    THEN CASE WHEN paid_cents < 0 THEN -1 ELSE 1 END
                    ELSE 0 END) / 100 AS DECIMAL(7, 2)) AS avg_sale_amount
FROM cents
ORDER BY total_net_revenue DESC
LIMIT 20;
