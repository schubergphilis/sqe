-- name: Customer lifetime value
-- description: Compute total lifetime net revenue per customer across all
--              channels, partitioned by years active and customer demographics,
--              to rank and identify high-value customers.
-- timeout: 180s
-- Each channel is aggregated per customer before the join. Joining the
-- three fact tables to customer directly multiplied every SUM by the
-- customer's row counts in the other two channels (the DISTINCT counts and
-- MIN/MAX were unaffected).
WITH store AS (
    SELECT
        ss.ss_customer_sk                   AS customer_sk,
        MIN(d.d_year)                       AS first_purchase_year,
        MAX(d.d_year)                       AS last_purchase_year,
        COUNT(DISTINCT ss.ss_ticket_number) AS transactions,
        SUM(ss.ss_net_paid)                 AS revenue
    FROM
        store_sales ss
        LEFT JOIN date_dim d ON d.d_date_sk = ss.ss_sold_date_sk
    WHERE ss.ss_customer_sk IS NOT NULL
    GROUP BY ss.ss_customer_sk
),
catalog AS (
    SELECT
        cs_bill_customer_sk             AS customer_sk,
        COUNT(DISTINCT cs_order_number) AS transactions,
        SUM(cs_net_paid)                AS revenue
    FROM catalog_sales
    WHERE cs_bill_customer_sk IS NOT NULL
    GROUP BY cs_bill_customer_sk
),
web AS (
    SELECT
        ws_bill_customer_sk             AS customer_sk,
        COUNT(DISTINCT ws_order_number) AS transactions,
        SUM(ws_net_paid)                AS revenue
    FROM web_sales
    WHERE ws_bill_customer_sk IS NOT NULL
    GROUP BY ws_bill_customer_sk
),
clv AS (
    SELECT
        c.c_customer_sk,
        c.c_customer_id,
        ca.ca_state,
        cd.cd_gender,
        cd.cd_education_status,
        s.first_purchase_year,
        s.last_purchase_year,
        COALESCE(s.transactions, 0)   AS store_transactions,
        COALESCE(s.revenue, 0)        AS store_revenue,
        COALESCE(cs.transactions, 0)  AS catalog_transactions,
        COALESCE(cs.revenue, 0)       AS catalog_revenue,
        COALESCE(w.transactions, 0)   AS web_transactions,
        COALESCE(w.revenue, 0)        AS web_revenue
    FROM
        customer c
        LEFT JOIN customer_address      ca ON ca.ca_address_sk = c.c_current_addr_sk
        LEFT JOIN customer_demographics cd ON cd.cd_demo_sk    = c.c_current_cdemo_sk
        LEFT JOIN store   s  ON s.customer_sk  = c.c_customer_sk
        LEFT JOIN catalog cs ON cs.customer_sk = c.c_customer_sk
        LEFT JOIN web     w  ON w.customer_sk  = c.c_customer_sk
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
