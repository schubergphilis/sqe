-- name: ClickBench Q24 — Search phrases ordered by event time
-- timeout: 30s
SELECT SearchPhrase
FROM hits
WHERE SearchPhrase <> ''
ORDER BY EventTime
LIMIT 10;
