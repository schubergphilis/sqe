-- name: Customer segmentation by purchase behavior
-- description: Segment customers by total spend across all channels into
--              quintile buckets (Low/Mid-Low/Mid/Mid-High/High) and count
--              the number of customers and average spend per segment.
-- timeout: 120s
-- Each channel is summed per customer before the join. Joining the three
-- fact tables to customer directly multiplied every SUM by the customer's
-- row counts in the other two channels.
WITH store_spend AS (
    SELECT ss_customer_sk AS customer_sk, SUM(ss_net_paid) AS spend
    FROM store_sales
    WHERE ss_customer_sk IS NOT NULL
    GROUP BY ss_customer_sk
),
catalog_spend AS (
    SELECT cs_bill_customer_sk AS customer_sk, SUM(cs_net_paid) AS spend
    FROM catalog_sales
    WHERE cs_bill_customer_sk IS NOT NULL
    GROUP BY cs_bill_customer_sk
),
web_spend AS (
    SELECT ws_bill_customer_sk AS customer_sk, SUM(ws_net_paid) AS spend
    FROM web_sales
    WHERE ws_bill_customer_sk IS NOT NULL
    GROUP BY ws_bill_customer_sk
),
customer_spend AS (
    SELECT
        c.c_customer_sk,
        c.c_customer_id,
        cd.cd_gender,
        cd.cd_marital_status,
        cd.cd_education_status,
        COALESCE(ss.spend, 0)
            + COALESCE(cs.spend, 0)
            + COALESCE(ws.spend, 0) AS total_spend
    FROM
        customer c
        JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
        LEFT JOIN store_spend   ss ON ss.customer_sk = c.c_customer_sk
        LEFT JOIN catalog_spend cs ON cs.customer_sk = c.c_customer_sk
        LEFT JOIN web_spend     ws ON ws.customer_sk = c.c_customer_sk
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
