-- name: ClickBench Q30 — User agents by resolution width range
-- timeout: 30s
SELECT "UserAgent", COUNT(DISTINCT "UserID") AS u
FROM hits
WHERE "ResolutionWidth" >= 1024
GROUP BY "UserAgent"
ORDER BY u DESC
LIMIT 10;
