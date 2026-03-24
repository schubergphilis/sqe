-- name: Items with high return rate per category
-- description: Find item categories where the return rate (returns / sales)
--              exceeds 10%, ranked by return rate descending.
-- timeout: 120s
SELECT
    i.i_category,
    i.i_class,
    COUNT(sr.sr_item_sk)                               AS total_returns,
    COUNT(ss.ss_item_sk)                               AS total_sales,
    CAST(COUNT(sr.sr_item_sk) AS DOUBLE)
        / NULLIF(COUNT(ss.ss_item_sk), 0)              AS return_rate,
    SUM(sr.sr_return_amt)                              AS total_return_amount
FROM
    item          i
    JOIN store_sales    ss ON ss.ss_item_sk = i.i_item_sk
    LEFT JOIN store_returns sr ON sr.sr_item_sk = i.i_item_sk
                               AND sr.sr_ticket_number = ss.ss_ticket_number
GROUP BY
    i.i_category,
    i.i_class
HAVING
    CAST(COUNT(sr.sr_item_sk) AS DOUBLE)
        / NULLIF(COUNT(ss.ss_item_sk), 0) > 0.10
ORDER BY
    return_rate DESC,
    total_return_amount DESC;
