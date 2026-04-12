-- name: ClickBench Q18 — User + minute + search phrase
-- timeout: 30s
-- note: EventTime is Int64 (unix timestamp seconds). Minute extraction uses
--       integer arithmetic: (EventTime / 60) % 60
SELECT
    "UserID",
    CAST("EventTime" / 60 % 60 AS INT) AS m,
    "SearchPhrase",
    COUNT(*)
FROM hits
GROUP BY "UserID", CAST("EventTime" / 60 % 60 AS INT), "SearchPhrase"
ORDER BY COUNT(*) DESC
LIMIT 10;
