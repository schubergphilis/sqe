-- name: ClickBench Q07 — Ad engine distribution
-- timeout: 30s
SELECT AdvEngineID, COUNT(*)
FROM hits
WHERE AdvEngineID <> 0
GROUP BY AdvEngineID
ORDER BY COUNT(*) DESC;
