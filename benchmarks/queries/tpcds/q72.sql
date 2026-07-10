-- name: Catalog sales with inventory shortages and promotions
-- timeout: 60s
SELECT i_item_desc, w_warehouse_name, d1.d_week_seq,
       SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) AS no_promo,
       SUM(CASE WHEN p_promo_sk IS NOT NULL THEN 1 ELSE 0 END) AS promo,
       COUNT(*) AS total_cnt
FROM catalog_sales
JOIN inventory          ON cs_item_sk        = inv_item_sk
JOIN warehouse          ON w_warehouse_sk     = inv_warehouse_sk
JOIN item               ON i_item_sk          = cs_item_sk
JOIN customer_demographics ON cs_bill_cdemo_sk = cd_demo_sk
JOIN household_demographics ON cs_bill_hdemo_sk = hd_demo_sk
JOIN date_dim d1        ON cs_sold_date_sk    = d1.d_date_sk
JOIN date_dim d2        ON inv_date_sk        = d2.d_date_sk
JOIN date_dim d3        ON cs_ship_date_sk    = d3.d_date_sk
LEFT OUTER JOIN promotion ON cs_promo_sk      = p_promo_sk
WHERE d1.d_week_seq       = d2.d_week_seq
  AND i_current_price     BETWEEN 1.00 AND 1.00 + 1.00
  AND hd_buy_potential    = '501-1000'
  AND d1.d_year           = 1999
  AND cd_marital_status   = 'D'
  AND d3.d_date            > d1.d_date + INTERVAL '5' DAY
  AND inv_quantity_on_hand < cs_quantity
GROUP BY i_item_desc, w_warehouse_name, d1.d_week_seq
ORDER BY total_cnt DESC, i_item_desc, w_warehouse_name, d1.d_week_seq
LIMIT 100;
