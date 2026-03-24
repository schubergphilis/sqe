-- name: ClickBench Q31 — Counters by JS + cookie enabled
-- timeout: 30s
SELECT CounterID, AVG(JavascriptEnable), AVG(CookieEnable), COUNT(*) AS c
FROM hits
GROUP BY CounterID
ORDER BY c DESC
LIMIT 10;
