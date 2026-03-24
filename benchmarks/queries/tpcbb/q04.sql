-- name: Abandoned shopping carts
-- description: Find users who browsed items via web clickstreams but never
--              completed a purchase (wcs_sales_sk IS NULL), and count distinct
--              items browsed without buying, grouped by item category.
-- timeout: 120s
SELECT
    i.i_category,
    i.i_class,
    COUNT(DISTINCT wcs.wcs_user_sk)  AS unique_browsers,
    COUNT(*)                         AS total_browse_events,
    COUNT(DISTINCT wcs.wcs_item_sk)  AS unique_items_browsed
FROM
    web_clickstreams wcs
    JOIN item i ON i.i_item_sk = wcs.wcs_item_sk
WHERE
    wcs.wcs_sales_sk IS NULL
    AND wcs.wcs_user_sk IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM web_clickstreams wcs2
        WHERE wcs2.wcs_user_sk = wcs.wcs_user_sk
          AND wcs2.wcs_item_sk = wcs.wcs_item_sk
          AND wcs2.wcs_sales_sk IS NOT NULL
    )
GROUP BY
    i.i_category,
    i.i_class
ORDER BY
    unique_browsers DESC
LIMIT 25;
