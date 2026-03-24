-- name: ClickBench Q17 — Top user + search phrase combos (unordered)
-- timeout: 30s
SELECT UserID, SearchPhrase, COUNT(*)
FROM hits
GROUP BY UserID, SearchPhrase
LIMIT 10;
