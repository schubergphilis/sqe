**STATUS 2026-09-04 (later): architecture review filed, 102 issues, nothing fixed yet.**

`docs/internal/reviews/2026-09-04-architecture-review/` holds one file per
finding in a fixed template (scope, severity, category, effort, status,
problem with `path:line` evidence, why it matters, proposed change,
acceptance criteria) so an agent can pick a file up cold. 31 high, 62
medium, 9 low. The five to read first: 200 (policy rewriter never enters
expression subqueries, so `IN (SELECT ...)` reads governed tables raw), 202
(MERGE handlers apply no row filters to the target), 400 (time-travel
aliases bypass policy), 503 (Hive INSERT plans its source without the
enforcer), 601 (one shared secret flattens worker trust, rogue worker
receives every user's S3 credentials). Index with severities in the
directory README.
