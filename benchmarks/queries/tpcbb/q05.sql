-- name: Customer segmentation by purchase behavior
-- description: Segment customers by total spend across all channels into
--              quintile buckets (Low/Mid-Low/Mid/Mid-High/High) and count
--              the number of customers and average spend per segment.
-- timeout: 120s
WITH customer_spend AS (
    SELECT
        c.c_customer_sk,
        c.c_customer_id,
        cd.cd_gender,
        cd.cd_marital_status,
        cd.cd_education_status,
        COALESCE(SUM(ss.ss_net_paid), 0)
            + COALESCE(SUM(cs.cs_net_paid), 0)
            + COALESCE(SUM(ws.ws_net_paid), 0) AS total_spend
    FROM
        customer c
        JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
        LEFT JOIN store_sales    ss ON ss.ss_customer_sk   = c.c_customer_sk
        LEFT JOIN catalog_sales  cs ON cs.cs_bill_customer_sk = c.c_customer_sk
        LEFT JOIN web_sales      ws ON ws.ws_bill_customer_sk = c.c_customer_sk
    GROUP BY
        c.c_customer_sk,
        c.c_customer_id,
        cd.cd_gender,
        cd.cd_marital_status,
        cd.cd_education_status
),
spend_segments AS (
    SELECT
        *,
        CASE
            WHEN total_spend = 0                          THEN 'No Purchase'
            WHEN total_spend < 100                        THEN 'Low'
            WHEN total_spend < 500                        THEN 'Mid-Low'
            WHEN total_spend < 2000                       THEN 'Mid'
            WHEN total_spend < 10000                      THEN 'Mid-High'
            ELSE                                               'High'
        END AS spend_segment
    FROM customer_spend
)
SELECT
    spend_segment,
    cd_gender,
    cd_marital_status,
    cd_education_status,
    COUNT(*)                   AS customer_count,
    AVG(total_spend)           AS avg_spend,
    SUM(total_spend)           AS total_segment_spend
FROM spend_segments
GROUP BY
    spend_segment,
    cd_gender,
    cd_marital_status,
    cd_education_status
ORDER BY
    spend_segment,
    customer_count DESC;
