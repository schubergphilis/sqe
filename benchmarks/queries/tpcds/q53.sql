-- name: Items with monthly sales spike of more than 10% above average
-- timeout: 60s
SELECT *
FROM (
    SELECT i_manufact_id,
           SUM(CASE WHEN d_qoy = 1 THEN ss_sales_price ELSE 0 END) AS q1_price,
           SUM(CASE WHEN d_qoy = 2 THEN ss_sales_price ELSE 0 END) AS q2_price,
           SUM(CASE WHEN d_qoy = 3 THEN ss_sales_price ELSE 0 END) AS q3_price,
           SUM(CASE WHEN d_qoy = 4 THEN ss_sales_price ELSE 0 END) AS q4_price
    FROM item, store_sales, date_dim
    WHERE ss_item_sk   = i_item_sk
      AND ss_sold_date_sk = d_date_sk
      AND d_year       = 2000
      AND (i_category IN ('Books', 'Children', 'Electronics')
           AND i_class IN ('personal', 'portable', 'reference', 'self-help')
           AND i_brand IN ('scholaramalgamalg #14', 'scholaramalgamalg #7',
                           'exportiunivamalg #9', 'scholaramalgamalg #9'))
    GROUP BY i_manufact_id
) tmp1
WHERE ABS(q1_price - q2_price) / ((q1_price + q2_price) / 2) > 0.1
ORDER BY i_manufact_id
LIMIT 100;
