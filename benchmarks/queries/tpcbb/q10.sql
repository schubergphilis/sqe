-- name: Cross-channel customer behavior
-- description: Identify customers who are active across multiple sales channels
--              (store, catalog, web) and summarise their spend and web browsing
--              behaviour. Customers active in 2+ channels are multi-channel.
-- timeout: 180s
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
channel_activity AS (
    SELECT
        c.c_customer_sk,
        c.c_customer_id,
        CASE WHEN ss.customer_sk IS NOT NULL THEN 1 ELSE 0 END AS uses_store,
        CASE WHEN cs.customer_sk IS NOT NULL THEN 1 ELSE 0 END AS uses_catalog,
        CASE WHEN ws.customer_sk IS NOT NULL THEN 1 ELSE 0 END AS uses_web,
        COALESCE(ss.spend, 0) AS store_spend,
        COALESCE(cs.spend, 0) AS catalog_spend,
        COALESCE(ws.spend, 0) AS web_spend
    FROM
        customer c
        LEFT JOIN store_spend   ss ON ss.customer_sk = c.c_customer_sk
        LEFT JOIN catalog_spend cs ON cs.customer_sk = c.c_customer_sk
        LEFT JOIN web_spend     ws ON ws.customer_sk = c.c_customer_sk
),
web_activity AS (
    SELECT
        wcs.wcs_user_sk        AS customer_sk,
        COUNT(*)               AS total_clicks,
        COUNT(wcs.wcs_sales_sk) AS click_conversions,
        COUNT(DISTINCT wcs.wcs_item_sk) AS items_browsed
    FROM web_clickstreams wcs
    WHERE wcs.wcs_user_sk IS NOT NULL
    GROUP BY wcs.wcs_user_sk
)
SELECT
    ca.c_customer_id,
    (ca.uses_store + ca.uses_catalog + ca.uses_web) AS channels_used,
    ca.store_spend,
    ca.catalog_spend,
    ca.web_spend,
    ca.store_spend + ca.catalog_spend + ca.web_spend AS total_spend,
    COALESCE(wa.total_clicks, 0)                     AS web_clicks,
    COALESCE(wa.click_conversions, 0)                AS web_conversions,
    COALESCE(wa.items_browsed, 0)                    AS items_browsed
FROM
    channel_activity ca
    LEFT JOIN web_activity wa ON wa.customer_sk = ca.c_customer_sk
WHERE
    (ca.uses_store + ca.uses_catalog + ca.uses_web) >= 2
ORDER BY
    total_spend DESC
LIMIT 100;
