-- name: ClickBench Q13 — Top search phrases by distinct users
-- timeout: 30s
SELECT "SearchPhrase", COUNT(DISTINCT "UserID") AS u
FROM hits
WHERE "SearchPhrase" <> ''
GROUP BY "SearchPhrase"
ORDER BY u DESC
LIMIT 10;
