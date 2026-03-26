-- name: Customer lifetime value
-- description: Compute total lifetime net revenue per customer across all
--              channels, partitioned by years active and customer demographics,
--              to rank and identify high-value customers.
-- timeout: 180s
WITH clv AS (
    SELECT
        c.c_customer_sk,
        c.c_customer_id,
        ca.ca_state,
        cd.cd_gender,
        cd.cd_education_status,
        MIN(d_ss.d_year)                      AS first_purchase_year,
        MAX(d_ss.d_year)                      AS last_purchase_year,
        COUNT(DISTINCT ss.ss_ticket_number)   AS store_transactions,
        COALESCE(SUM(ss.ss_net_paid), 0)      AS store_revenue,
        COUNT(DISTINCT cs.cs_order_number)    AS catalog_transactions,
        COALESCE(SUM(cs.cs_net_paid), 0)      AS catalog_revenue,
        COUNT(DISTINCT ws.ws_order_number)    AS web_transactions,
        COALESCE(SUM(ws.ws_net_paid), 0)      AS web_revenue
    FROM
        customer c
        LEFT JOIN customer_address      ca  ON ca.ca_address_sk  = c.c_current_addr_sk
        LEFT JOIN customer_demographics cd  ON cd.cd_demo_sk     = c.c_current_cdemo_sk
        LEFT JOIN store_sales           ss  ON ss.ss_customer_sk = c.c_customer_sk
        LEFT JOIN date_dim              d_ss ON d_ss.d_date_sk   = ss.ss_sold_date_sk
        LEFT JOIN catalog_sales         cs  ON cs.cs_bill_customer_sk = c.c_customer_sk
        LEFT JOIN web_sales             ws  ON ws.ws_bill_customer_sk = c.c_customer_sk
    GROUP BY
        c.c_customer_sk,
        c.c_customer_id,
        ca.ca_state,
        cd.cd_gender,
        cd.cd_education_status
)
SELECT * FROM (
    SELECT
        c_customer_id,
        ca_state,
        cd_gender,
        cd_education_status,
        first_purchase_year,
        last_purchase_year,
        (last_purchase_year - first_purchase_year + 1)        AS years_active,
        store_transactions + catalog_transactions
            + web_transactions                                AS total_transactions,
        store_revenue + catalog_revenue + web_revenue         AS total_lifetime_value
    FROM clv
) ranked
WHERE
    total_lifetime_value > 0
ORDER BY
    total_lifetime_value DESC
LIMIT 100;
