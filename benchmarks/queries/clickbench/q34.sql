-- name: ClickBench Q34 — Distinct users per browser country
-- timeout: 30s
SELECT "BrowserCountry", COUNT(DISTINCT "UserID") AS u
FROM hits
GROUP BY "BrowserCountry"
ORDER BY u DESC
LIMIT 10;
