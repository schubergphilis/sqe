-- name: ClickBench Q26 — Search phrases ordered by time then phrase
-- timeout: 30s
SELECT "SearchPhrase"
FROM hits
WHERE "SearchPhrase" <> ''
ORDER BY "EventTime", "SearchPhrase"
LIMIT 10;
