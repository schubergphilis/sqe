-- name: ClickBench Q36 — Average timing metrics by counter
-- timeout: 30s
SELECT
    CounterID,
    AVG(SendTiming),
    AVG(DNSTiming),
    AVG(ConnectTiming),
    AVG(ResponseStartTiming),
    AVG(ResponseEndTiming),
    COUNT(*) AS c
FROM hits
WHERE SendTiming > 0
GROUP BY CounterID
ORDER BY c DESC
LIMIT 10;
