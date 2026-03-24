-- name: ClickBench Q25 — Search phrases ordered alphabetically
-- timeout: 30s
SELECT SearchPhrase
FROM hits
WHERE SearchPhrase <> ''
ORDER BY SearchPhrase
LIMIT 10;
