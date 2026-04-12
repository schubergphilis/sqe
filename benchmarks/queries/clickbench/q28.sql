-- name: ClickBench Q28 — Top referer domains by average URL length
-- timeout: 30s
-- note: REGEXP_REPLACE is supported in DataFusion
SELECT
    REGEXP_REPLACE("Referer", '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS key,
    AVG(LENGTH("Referer")) AS l,
    COUNT(*) AS c,
    MIN("Referer")
FROM hits
WHERE "Referer" <> ''
GROUP BY REGEXP_REPLACE("Referer", '^https?://(?:www\.)?([^/]+)/.*$', '\1')
HAVING COUNT(*) > 100000
ORDER BY l DESC
LIMIT 25;
