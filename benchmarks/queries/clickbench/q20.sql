-- name: ClickBench Q20 — Count URLs containing google
-- timeout: 30s
SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';
