## 1. Policy store

- [x] 1.1 `PolicyStore::conflict_principals(table, namespace, tags) -> Result<Option<Vec<SessionUser>>>`, default `Ok(None)`
- [x] 1.2 Ranger: principals from matching resource mask/row-filter policies and from tag policies on the given tags, one synthetic user each, plus the empty (public) user; unit tests

## 2. Guard

- [x] 2.1 `TagConflictGuard::check(before, after)` (D2, D3, D4) in a new sqe-policy module; unit tests with a stub store: new conflict refused, existing conflict ignored, no tag added skips, store error refuses, `None` allows

## 3. Write paths

- [x] 3.1 `set_column_tags` takes `Option<&TagConflictGuard>`, runs it under the lock before the commit
- [x] 3.2 `set_table_properties_guarded` takes the lock and runs the guard when present and the statement writes `sqe.column-tags`
- [x] 3.3 Coordinator builds the guard under `refuse` with the tag gate's policy key and passes it on all three paths

## 4. Acceptance (live Ranger, `mask_precedence = refuse`)

Tests `refuse_at_write_*` in `crates/sqe-coordinator/tests/it/access_control_e2e.rs`.

- [x] 4.1 Resource `MASK_SHOW_LAST_4` + tag `MASK_NULL` for one role: SET TAGS and MODIFY COLUMN refused, tags unchanged
- [x] 4.2 Same mask from both: allowed
- [x] 4.3 Disjoint roles: allowed; mixed membership (bob) read refused (D1 limit)
- [x] 4.4 Row-filter variant refused
- [x] 4.5 Raw `sqe.column-tags` write refused
- [x] 4.6 UNSET on the conflicted column allowed
- [x] 4.7 `tag` mode allows the same write
- [x] 4.8 Control: with the guard not passed, 4.1 writes the tag (run by hand: `refuse_at_write_mask_conflict` fails with "write went through")
- [x] 4.9 Under `catalog_scoped` (#919), a `<catalog>.<tag>` policy is checked for its catalog only (`refuse_at_write_catalog_scoped_tag_policy`; unit tests in `tag_conflict_guard`)
