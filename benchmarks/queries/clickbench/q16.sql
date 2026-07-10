-- name: ClickBench Q16 — Top user + search phrase combos (ordered)
-- timeout: 30s
SELECT "UserID", "SearchPhrase", COUNT(*)
FROM hits
GROUP BY "UserID", "SearchPhrase"
ORDER BY COUNT(*) DESC
LIMIT 10;
