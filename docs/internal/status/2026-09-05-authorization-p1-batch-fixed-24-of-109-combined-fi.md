**STATUS 2026-09-05 (later): authorization P1 batch fixed; 24 of 109 combined findings closed.**

Branch `fix/authz-policy-enforcement-p1` closes the remaining P1s of the
authorization area (work package #653): one policy-key derivation shared by
read, write, Hive and information_schema paths (201); MERGE folds the
target's row filter and masks into the statement, PostgreSQL RLS semantics
(202); time-travel and incremental scans are renamed back to the user's
table before policy and audit (400); Hive INSERT enforces its source and
authorizes the target before writing (503); maintenance procedures are
tiered read-only / table-write / admin with no role-name heuristics (510);
column-tag properties are only read from fresh cache entries (303); a
twelve-test rewriter shape suite (211). Every finding now has a GitLab
issue (#543-#651) and the areas are grouped into eight `work-package`
umbrellas (#652-#659), see
`docs/internal/reviews/2026-09-05-gitlab-issue-map.md`.
**NEXT:** work package #652 (identity, credentials and transport trust:
601/401/603 worker trust, 604 quack_query, 204, 207, 702) and #655 memory
safety (701, 504, 403, 300, 301); then the fingerprint-bound result cache
(902 follow-up).
