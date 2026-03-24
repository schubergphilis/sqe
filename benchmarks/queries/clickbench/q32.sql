-- name: ClickBench Q32 — OS distribution for mobile hits
-- timeout: 30s
SELECT OS, COUNT(*) AS c
FROM hits
WHERE IsMobile = 1
GROUP BY OS
ORDER BY c DESC
LIMIT 10;
