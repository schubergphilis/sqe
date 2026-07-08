-- name: ClickBench Q27 — Counters with long average URLs (HAVING filter)
-- timeout: 30s
-- Result is empty below sf1000 by design. The `> 100000` threshold is the
-- canonical ClickBench query text, calibrated for the real ~100M-row dataset.
-- Our generator's hot CounterID holds exactly 1% of rows, so it only crosses
-- 100k at >= 100M rows (sf1000).
SELECT "CounterID", AVG(LENGTH("URL")) AS l, COUNT(*) AS c
FROM hits
WHERE "URL" <> ''
GROUP BY "CounterID"
HAVING COUNT(*) > 100000
ORDER BY l DESC
LIMIT 25;
