-- name: ClickBench Q41 — Not-bounce users by income bracket
-- timeout: 30s
SELECT "Income", COUNT(DISTINCT "UserID") AS u
FROM hits
WHERE "IsNotBounce" = 1
GROUP BY "Income"
ORDER BY u DESC;
