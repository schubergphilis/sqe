-- name: ClickBench Q02 — Sum, count, avg aggregation
-- timeout: 30s
SELECT SUM("AdvEngineID"), COUNT(*), AVG("ResolutionWidth") FROM hits;
