-- name: ClickBench Q42 — Top counters by distinct interested users
-- timeout: 30s
SELECT CounterID, COUNT(DISTINCT UserID) AS u, AVG(Interests) AS avg_interests
FROM hits
WHERE Interests > 0
GROUP BY CounterID
HAVING COUNT(DISTINCT UserID) > 10
ORDER BY u DESC
LIMIT 10;
