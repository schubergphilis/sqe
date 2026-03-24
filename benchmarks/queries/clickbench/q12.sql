-- name: ClickBench Q12 — Top search phrases by hit count
-- timeout: 30s
SELECT "SearchPhrase", COUNT(*) AS c
FROM hits
WHERE "SearchPhrase" <> ''
GROUP BY "SearchPhrase"
ORDER BY c DESC
LIMIT 10;
