-- name: ClickBench Q21 — Search phrases on google URLs
-- timeout: 30s
SELECT SearchPhrase, MIN(URL), COUNT(*) AS c
FROM hits
WHERE URL LIKE '%google%' AND SearchPhrase <> ''
GROUP BY SearchPhrase
ORDER BY c DESC
LIMIT 10;
