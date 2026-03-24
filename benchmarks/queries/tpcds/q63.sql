-- name: Items with monthly sales growth spike by brand and store
-- timeout: 60s
SELECT *
FROM (
    SELECT i_manager_id,
           SUM(ss_sales_price) AS sum_sales,
           AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manager_id) AS avg_monthly_sales
    FROM item, store_sales, date_dim, store
    WHERE ss_item_sk   = i_item_sk
      AND ss_sold_date_sk = d_date_sk
      AND ss_store_sk  = s_store_sk
      AND d_month_seq  IN (
          1200, 1201, 1202, 1203, 1204, 1205,
          1206, 1207, 1208, 1209, 1210, 1211
      )
      AND (i_category IN ('Books', 'Children', 'Electronics')
           AND i_class  IN ('personal', 'portable', 'reference', 'self-help')
           AND i_brand  IN ('scholaramalgamalg #14', 'scholaramalgamalg #7',
                            'exportiunivamalg #9', 'scholaramalgamalg #9'))
    GROUP BY i_manager_id
) tmp1
WHERE CASE WHEN avg_monthly_sales > 0
           THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
           ELSE NULL END > 0.1
ORDER BY i_manager_id, avg_monthly_sales, sum_sales
LIMIT 100;
