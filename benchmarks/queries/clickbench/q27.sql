-- name: ClickBench Q27 — Counters with long average URLs (HAVING filter)
-- timeout: 30s
SELECT "CounterID", AVG(LENGTH("URL")) AS l, COUNT(*) AS c
FROM hits
WHERE "URL" <> ''
GROUP BY "CounterID"
HAVING COUNT(*) > 100000
ORDER BY l DESC
LIMIT 25;
