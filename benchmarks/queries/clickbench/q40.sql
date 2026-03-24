-- name: ClickBench Q40 — Hit color distribution
-- timeout: 30s
SELECT HitColor, COUNT(*) AS c
FROM hits
GROUP BY HitColor
ORDER BY c DESC;
