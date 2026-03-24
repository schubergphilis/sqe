-- name: ClickBench Q23 — All columns for google URLs, sorted by time
-- timeout: 60s
SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;
