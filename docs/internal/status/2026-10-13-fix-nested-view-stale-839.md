**STATUS 2026-10-13: CREATE / DROP VIEW evict the result cache (`fix/nested-view-stale-839`, #839 part 1).**

`view_replace_nested_e2e` read the old view through `vrn_over_view` after `vrn_base` was replaced. The result cache keys on SQL text and evicts by `tables_touched`, which is taken after views are inlined, so no view name is ever in it. CREATE and DROP VIEW now call `invalidate_all()`, like GRANT and the maintenance procedures. The test fails without the change and passes with it. Views changed from another engine still wait for `ttl_secs`; the configuration page says so.

The other #839 failure (departments CTAS, "cast Decimal128(8,1) to Decimal128(7,1)") is a separate CTAS bug: the table schema is taken from the unanalyzed plan, where a mixed-precision decimal UNION still carries the first branch's type; after analysis it is Decimal(8,1). Fixed on its own branch.

**NEXT:** CTAS schema from the analyzed plan (#839 part 2); native audit log rotation; #803 when it recurs.
