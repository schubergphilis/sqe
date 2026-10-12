**STATUS 2026-09-06: SQL planning, protocol behaviour and result correctness batch (#656) implemented on `fix/sql-planning-protocol-p1`; 65 of 109 combined findings closed.**

Branch `fix/sql-planning-protocol-p1` (work package #656) closes 107,
311, 312, 404, 406, 605, 700, 703, 704, 708 and 710: `sqe-sql` tokenizes
a statement once, rejects multi-statement input with a position, and
dispatches every custom grammar on tokens instead of string prefixes;
one `SqlFrontDoor` normalizes SQL for the Trino, Flight and embedded
entry points (parse, Trino-shape rewrites on the AST, information_schema
qualification, anonymous column aliases) and the coordinator no longer
re-parses; metadata detection and `updateType` come from the AST;
Trino `SET SESSION` is an allow-list (`session_props.rs`) with
`INVALID_SESSION_PROPERTY` on anything else, PREPARE validates at
PREPARE time, EXECUTE binds through the shared comment-aware
`substitute_placeholders`, DEALLOCATE of an unknown name is `NOT_FOUND`,
and session headers appear only on the terminal response; Flight SQL
prepared statements are UUID handles over a session-scoped store that
plans once, types parameters, checks binds before execution and still
enforces policy per execution; one `post_planning.rs` pipeline owns the
ordered physical rules with per-rule fallback or fail-closed policy and
`sqe_plan_rule_failures_total`, shared by both execute paths and EXPLAIN;
views load the current version with `sqe > trino > spark` dialect
preference through typed REST requests inside the breaker and metrics,
CREATE OR REPLACE VIEW is a `replaceView` commit, and time-travel scans
declare and read the snapshot's schema; every extended Trino UDF runs
per row over columns (Utf8View included) and `at_timezone` declares the
type it returns; sqe-quack-wire bounds every allocation by the payload,
validates list/map offsets before slicing, has a `catch_unwind` client
boundary, three cargo-fuzz targets with seed corpora and 16 hostile-input
tests; the Trino compat layer has a wire-level golden test over the real
listener, proptests for the string rewriters, and a trino-cli
integration case (`make test-integration FILTER=trino_cli`).
Partial: the trino-cli golden is not yet recorded (needs the docker
stack: `UPDATE_GOLDENS=1 make test-integration FILTER=trino_cli`); the
fuzz targets compile but were not run (cargo-fuzz not installed);
`nested_row_cast.rs` still misses parentheses inside comments, literals
and quoted identifiers (ignored failing tests document it); EXPLAIN does
not print `DistributedScanExec`; MCP classifies through the classifier
directly rather than a `WireDialect`; 704's per-(table, column)
allow-list for result reshaping is replaced by AST gating only.
**NEXT:** work package #658 distributed worker reliability and test
infrastructure (402, 602, 609, 109, 411, 611, 807, 513, 313, 707, 705,
706), then the leftovers: 806 (#638), 205/212 (#653), 610 (#657) and the
#659 partials.
