-- name: ClickBench Q37 — Download events by region
-- timeout: 30s
SELECT RegionID, COUNT(*) AS c
FROM hits
WHERE IsDownload = 1
GROUP BY RegionID
ORDER BY c DESC
LIMIT 10;
