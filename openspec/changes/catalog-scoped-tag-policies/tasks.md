## 1. Resolution

- [x] 1.1 `tag_scope::resolve_tags_in_catalog`: bare + `<catalog>.<tag>` lookup, catalog mask wins, filters ANDed, unmappable unioned; unit tests
- [x] 1.2 Rewriter scan path and `resolve_effective_policy` pass the table's catalog
- [x] 1.3 Coordinator callers (catalog TVFs, access control, Hive visibility and DDL, query handler) pass the catalog
- [x] 1.4 `table_files` / `table_partitions` key policy on the TVF's catalog argument, falling back to the session's

## 2. Projection

- [x] 2.1 `project_table_tags` writes both names under `catalog_scoped`; unit test

## 3. Verification

- [x] 3.1 Wiremock test with the real `RangerStore` through the rewriter: scoped, bare, and off
- [x] 3.2 Live Ranger: a dotted tag name imported by the projector, and a tag mask policy on `<catalog>.<tag>` created in Ranger Admin, masking one catalog only (`access_control_e2e::catalog_scoped_tag_policy_masks_its_catalog_only`)

## 4. Docs

- [x] 4.1 Book: fine-grained access control, tag policies under `catalog-scoped`

## 5. Policies written in SQL (#919 follow-through)

- [x] 5.1 `CREATE POLICY ... ON TAG <bare>` keyed to the session catalog under `catalog_scoped` (`tag_scope::policy_tag_in_catalog`); coordinator test `catalog_scoped_policy_ddl_test`
- [x] 5.2 `SET MASKING POLICY` strips the table catalog's own prefix (`tag_scope::column_tag_in_catalog`)
- [x] 5.3 `CREATE OR REPLACE POLICY` refuses to move a policy to another tag (`ranger_store::tag_policy_move`); unit test
- [x] 5.4 Unit test: a `c.pii` policy never applies in catalog `d` (`a_catalog_policy_never_reaches_another_catalog`)
- [x] 5.5 Live Ranger: `access_control_e2e::catalog_scoped_sql_tag_policy_is_keyed_to_the_session_catalog`
- [x] 5.6 Book: policies written in SQL, the legacy bare policy, the literal dotted-tag exception
- [ ] 5.7 #942 (mask precedence) and #943 (literal dotted column tags): owner decision first
