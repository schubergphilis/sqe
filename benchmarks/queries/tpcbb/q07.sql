-- name: Product affinity analysis
-- description: Find pairs of items frequently viewed together in a single
--              user session (same wcs_user_sk and same click date) to
--              support recommendation engine seeding.
-- timeout: 180s
SELECT
    i1.i_item_id          AS item_a_id,
    i1.i_item_desc        AS item_a_desc,
    i1.i_category         AS category,
    i2.i_item_id          AS item_b_id,
    i2.i_item_desc        AS item_b_desc,
    COUNT(*)              AS co_view_count
FROM
    web_clickstreams wcs1
    JOIN web_clickstreams wcs2
         ON  wcs2.wcs_user_sk        = wcs1.wcs_user_sk
         AND wcs2.wcs_click_date_sk  = wcs1.wcs_click_date_sk
         AND wcs2.wcs_item_sk        > wcs1.wcs_item_sk  -- avoid duplicates
    JOIN item i1 ON i1.i_item_sk = wcs1.wcs_item_sk
    JOIN item i2 ON i2.i_item_sk = wcs2.wcs_item_sk
WHERE
    wcs1.wcs_user_sk IS NOT NULL
    AND i1.i_category = i2.i_category  -- same category affinities
GROUP BY
    i1.i_item_id,
    i1.i_item_desc,
    i1.i_category,
    i2.i_item_id,
    i2.i_item_desc
HAVING
    COUNT(*) >= 2
ORDER BY
    co_view_count DESC,
    item_a_id,
    item_b_id
LIMIT 50;
