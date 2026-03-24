-- name: ClickBench Q01 — Count rows with ad engine
-- timeout: 30s
SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;
