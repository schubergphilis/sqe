-- name: ClickBench Q15 — Top users by hit count
-- timeout: 30s
SELECT "UserID", COUNT(*)
FROM hits
GROUP BY "UserID"
ORDER BY COUNT(*) DESC
LIMIT 10;
