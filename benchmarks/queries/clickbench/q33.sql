-- name: ClickBench Q33 — Distinct users per browser language
-- timeout: 30s
SELECT BrowserLanguage, COUNT(DISTINCT UserID) AS u
FROM hits
GROUP BY BrowserLanguage
ORDER BY u DESC
LIMIT 10;
