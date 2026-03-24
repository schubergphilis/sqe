-- name: Cross-channel customer behavior
-- description: Identify customers who are active across multiple sales channels
--              (store, catalog, web) and summarise their spend and web browsing
--              behaviour. Customers active in 2+ channels are multi-channel.
-- timeout: 180s
WITH channel_activity AS (
    SELECT
        c.c_customer_sk,
        c.c_customer_id,
        MAX(CASE WHEN ss.ss_customer_sk IS NOT NULL THEN 1 ELSE 0 END) AS uses_store,
        MAX(CASE WHEN cs.cs_bill_customer_sk IS NOT NULL THEN 1 ELSE 0 END) AS uses_catalog,
        MAX(CASE WHEN ws.ws_bill_customer_sk IS NOT NULL THEN 1 ELSE 0 END) AS uses_web,
        COALESCE(SUM(ss.ss_net_paid), 0)   AS store_spend,
        COALESCE(SUM(cs.cs_net_paid), 0)   AS catalog_spend,
        COALESCE(SUM(ws.ws_net_paid), 0)   AS web_spend
    FROM
        customer c
        LEFT JOIN store_sales   ss ON ss.ss_customer_sk       = c.c_customer_sk
        LEFT JOIN catalog_sales cs ON cs.cs_bill_customer_sk  = c.c_customer_sk
        LEFT JOIN web_sales     ws ON ws.ws_bill_customer_sk  = c.c_customer_sk
    GROUP BY
        c.c_customer_sk,
        c.c_customer_id
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
