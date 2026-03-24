-- name: ClickBench Q35 — Social network action distribution
-- timeout: 30s
SELECT SocialNetwork, SocialAction, COUNT(*) AS c, COUNT(DISTINCT UserID) AS u
FROM hits
WHERE SocialNetwork <> ''
GROUP BY SocialNetwork, SocialAction
ORDER BY c DESC
LIMIT 10;
