**STATUS 2026-11-22: tag policies written in SQL are keyed to their catalog (`feat/catalog-scoped-tag-keys-919`, #919, openspec catalog-scoped-tag-policies 5.1-5.6).**

!1888 made a policy on `<catalog>.<tag>` govern its catalog only, but `CREATE POLICY ... ON TAG pii` still wrote a bare `pii` key that applies in every catalog. Under `catalog_scoped` the coordinator now qualifies a bare tag with the session catalog, `SET MASKING POLICY` strips the table catalog's own prefix so column tags stay bare, and the Ranger store refuses a `CREATE OR REPLACE` that would move a policy to another tag. Audit of every `resolve_effective_policy` caller found no path that drops the catalog. Legacy bare policies keep applying in every catalog, which only adds masks. One exception, a column literally tagged `c.pii` in another catalog, is #943; mask precedence across tags is #942.

**NEXT:** owner decision on #942 (most restrictive mask wins) before #943.
