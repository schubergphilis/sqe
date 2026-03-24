-- name: ClickBench Q05 — Count distinct search phrases
-- timeout: 30s
SELECT COUNT(DISTINCT SearchPhrase) FROM hits;
