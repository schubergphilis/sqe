-- name: ClickBench Q38 — Age and sex distribution
-- timeout: 30s
SELECT "Age", "Sex", COUNT(*) AS c
FROM hits
WHERE "Age" > 0
GROUP BY "Age", "Sex"
ORDER BY c DESC
LIMIT 10;
