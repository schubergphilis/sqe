-- name: Seasonal sales patterns
-- description: Analyse store sales by month with year-over-year comparisons.
--              For each month compute current-year revenue and prior-year
--              revenue, then calculate the YoY growth percentage.
-- timeout: 120s
WITH monthly_revenue AS (
    SELECT
        d.d_year,
        d.d_moy                   AS month_of_year,
        i.i_category,
        SUM(ss.ss_net_paid)       AS monthly_revenue,
        COUNT(ss.ss_ticket_number) AS transaction_count
    FROM
        store_sales ss
        JOIN date_dim d ON d.d_date_sk = ss.ss_sold_date_sk
        JOIN item     i ON i.i_item_sk  = ss.ss_item_sk
    GROUP BY
        d.d_year,
        d.d_moy,
        i.i_category
)
SELECT
    cur.d_year,
    cur.month_of_year,
    cur.i_category,
    cur.monthly_revenue                          AS current_year_revenue,
    cur.transaction_count                        AS current_year_txns,
    prev.monthly_revenue                         AS prior_year_revenue,
    CASE
        WHEN prev.monthly_revenue > 0
        THEN ROUND(
                (cur.monthly_revenue - prev.monthly_revenue)
                    / prev.monthly_revenue * 100.0,
                2)
        ELSE NULL
    END                                          AS yoy_growth_pct
FROM
    monthly_revenue cur
    LEFT JOIN monthly_revenue prev
        ON  prev.d_year         = cur.d_year - 1
        AND prev.month_of_year  = cur.month_of_year
        AND prev.i_category     = cur.i_category
ORDER BY
    cur.i_category,
    cur.d_year,
    cur.month_of_year;
