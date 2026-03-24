-- name: ClickBench Q22 — Search phrases on Google title pages (non-telecom)
-- timeout: 30s
SELECT
    SearchPhrase,
    MIN(URL),
    MIN(Title),
    COUNT(*) AS c,
    COUNT(DISTINCT UserID)
FROM hits
WHERE Title LIKE '%Google%'
  AND URL NOT LIKE '%.google.%'
  AND SearchPhrase <> ''
GROUP BY SearchPhrase
ORDER BY c DESC
LIMIT 10;
