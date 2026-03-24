-- name: Product review sentiment by category
-- description: Aggregate product review ratings by item category to understand
--              customer sentiment. Count reviews by star-rating band (1-2, 3,
--              4-5) per category and compute the weighted average rating.
-- timeout: 60s
SELECT
    i.i_category,
    i.i_class,
    COUNT(*)                                                   AS review_count,
    AVG(CAST(pr.pr_review_rating AS DOUBLE))                   AS avg_rating,
    SUM(CASE WHEN pr.pr_review_rating <= 2 THEN 1 ELSE 0 END)  AS low_ratings,
    SUM(CASE WHEN pr.pr_review_rating = 3  THEN 1 ELSE 0 END)  AS mid_ratings,
    SUM(CASE WHEN pr.pr_review_rating >= 4 THEN 1 ELSE 0 END)  AS high_ratings
FROM
    product_reviews pr
    JOIN item i ON i.i_item_sk = pr.pr_item_sk
GROUP BY
    i.i_category,
    i.i_class
ORDER BY
    avg_rating DESC,
    review_count DESC;
