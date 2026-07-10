-- name: Store day-of-week sales comparison year over year
-- timeout: 60s
WITH wss AS (
    SELECT d_week_seq,
           ss_store_sk,
           SUM(CASE WHEN d_day_name = 'Sunday'    THEN ss_sales_price ELSE NULL END) AS sun_sales,
           SUM(CASE WHEN d_day_name = 'Monday'    THEN ss_sales_price ELSE NULL END) AS mon_sales,
           SUM(CASE WHEN d_day_name = 'Tuesday'   THEN ss_sales_price ELSE NULL END) AS tue_sales,
           SUM(CASE WHEN d_day_name = 'Wednesday' THEN ss_sales_price ELSE NULL END) AS wed_sales,
           SUM(CASE WHEN d_day_name = 'Thursday'  THEN ss_sales_price ELSE NULL END) AS thu_sales,
           SUM(CASE WHEN d_day_name = 'Friday'    THEN ss_sales_price ELSE NULL END) AS fri_sales,
           SUM(CASE WHEN d_day_name = 'Saturday'  THEN ss_sales_price ELSE NULL END) AS sat_sales
    FROM store_sales, date_dim
    WHERE d_date_sk = ss_sold_date_sk
    GROUP BY d_week_seq, ss_store_sk
)
SELECT s_store_name1, s_store_id1, d_week_seq1,
       sun_sales1 / sun_sales2   AS sun_ratio,
       mon_sales1 / mon_sales2   AS mon_ratio,
       tue_sales1 / tue_sales2   AS tue_ratio,
       wed_sales1 / wed_sales2   AS wed_ratio,
       thu_sales1 / thu_sales2   AS thu_ratio,
       fri_sales1 / fri_sales2   AS fri_ratio,
       sat_sales1 / sat_sales2   AS sat_ratio
FROM (
    SELECT s_store_name AS s_store_name1, wss.d_week_seq AS d_week_seq1,
           s_store_id AS s_store_id1,
           sun_sales AS sun_sales1, mon_sales AS mon_sales1,
           tue_sales AS tue_sales1, wed_sales AS wed_sales1,
           thu_sales AS thu_sales1, fri_sales AS fri_sales1,
           sat_sales AS sat_sales1
    FROM wss, store, date_dim d
    WHERE d.d_week_seq = wss.d_week_seq
      AND ss_store_sk  = s_store_sk
      AND d.d_year     = 2001
) y,
(
    SELECT s_store_name AS s_store_name2, wss.d_week_seq AS d_week_seq2,
           s_store_id AS s_store_id2,
           sun_sales AS sun_sales2, mon_sales AS mon_sales2,
           tue_sales AS tue_sales2, wed_sales AS wed_sales2,
           thu_sales AS thu_sales2, fri_sales AS fri_sales2,
           sat_sales AS sat_sales2
    FROM wss, store, date_dim d
    WHERE d.d_week_seq = wss.d_week_seq
      AND ss_store_sk  = s_store_sk
      AND d.d_year     = 2002
) x
WHERE d_week_seq1 = d_week_seq2 - 52
  AND s_store_id1 = s_store_id2
ORDER BY s_store_name1, d_week_seq1
LIMIT 100;
