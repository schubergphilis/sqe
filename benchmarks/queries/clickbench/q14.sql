-- name: ClickBench Q14 — Top search engine + phrase combos
-- timeout: 30s
SELECT "SearchEngineID", "SearchPhrase", COUNT(*) AS c
FROM hits
WHERE "SearchPhrase" <> ''
GROUP BY "SearchEngineID", "SearchPhrase"
ORDER BY c DESC
LIMIT 10;
