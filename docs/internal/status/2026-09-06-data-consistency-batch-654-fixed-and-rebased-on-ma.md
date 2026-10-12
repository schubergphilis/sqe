**STATUS 2026-09-06: data consistency batch (#654) fixed and rebased on main; 42 of 109 combined findings closed.**

Branch `fix/data-consistency-p1` (work package #654) closes 302, 304,
407, 500, 501, 502, 505, 506, 507, 511, 514, 608 and the 902 follow-up:
all-token catalog cache eviction with a generation fence, writer-scoped
session invalidation, in-place CREATE OR REPLACE, one commit-retry helper
for every CoW and truncate path, MERGE BY SOURCE on empty sources, INSERT
arity at planning, `rows_affected` on every DML, spec-aware partition
pruning, `after_commit` invalidation at the commit site, a scheduler that
caches discovery and runs a bounded job pool, worker compactions tied to
the RPC with a timeout and upload cleanup, and a result cache bound to the
policy fingerprint. MRs: !941 (#653), !946 (#652), this branch (#654).
**NEXT:** work package #655 memory safety (701, 504, 403, 300, 301, 408,
308, 307, 806, 509, 811, 512), then #656 SQL planning and protocol.
