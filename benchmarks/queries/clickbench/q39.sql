-- name: ClickBench Q39 — UTM source traffic analysis
-- timeout: 30s
SELECT "UTMSource", COUNT(*) AS c, COUNT(DISTINCT "UserID") AS u
FROM hits
WHERE "UTMSource" <> ''
GROUP BY "UTMSource"
ORDER BY c DESC
LIMIT 10;
