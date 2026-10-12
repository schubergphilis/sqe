**STATUS 2026-09-05: security/scale review filed (7 findings) and fixed; 11 of 109 combined findings closed.**

`docs/internal/reviews/2026-09-05-security-scale-review/` adds seven
high-severity findings (900-906) on top of the September 4 review, and
`docs/internal/reviews/2026-09-05-combined-prioritized-findings.md` is the
combined P1/P2/P3 schedule for all 109. Branch
`fix/security-scale-review-2026-09-05` fixes all seven plus 105, 200, 203
and 206: URL-parsed HTTP destination gate, per-TVF scoped object-store
runtimes, result cache keyed on exact SQL bytes and full session scope and
disabled under any governing policy engine, bounded partitioned fanout by
default with pool-charged reservations, capped and pool-charged buffered
JSON decoding, subquery-aware policy rewriting, OAuth-only bearer
validation for MCP, Ranger cache key with groups, ASCII-only pre-scan
uppercasing. Each finding file ends with a Resolution section.
**NEXT:** the remaining 32 P1s in the combined index, starting with 202
(MERGE target row filters), 400 (time-travel aliases), 503 (Hive INSERT
enforcer) and 601/401/603 (worker trust); then a fingerprint-bound result
cache so governed deployments regain result reuse (902 follow-up).
