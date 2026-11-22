## 1. Config

- [x] 1.1 `TagRemoval { Same (default), RemoveTag, Admin }` and `AccessControlConfig.tag_removal` (alias `tag-removal`). The tag enums move to `config/tags.rs` to stay under the file budget. A parse test rejects other spellings.
- [x] 1.2 `sqe.toml.example` entry; the `TagAuthority` doc points at it

## 2. Policy

- [x] 2.1 `TablePrivilege::RemoveTag` (`remove_tag`)
- [x] 2.2 Ranger `all_grants` excludes `remove_tag`. A unit test covers: not from `all`, `alter` or `apply_tag`; granted by an item naming it.

## 3. Gate

- [x] 3.1 `removed_tag_columns(old, new)` with a unit test: a lost key is a removal, a value change is not
- [x] 3.2 SET/UNSET TAGS: columns with an `Unset` op need the removal right (D1, D2)
- [x] 3.3 Masking policy gate carries the real action (D1)
- [x] 3.4 Raw writes and CREATE OR REPLACE: removed columns need the removal right. `TagWriteAuth { changed, removed }` goes to `set_table_properties_guarded`, which refuses a removal outside `removed` (D3).
- [x] 3.5 Object tags: UNSET needs the removal right on the table or schema key; a catalog needs an admin (D5)
- [x] 3.6 An authorised removal is logged with the user, the table and the columns
- [x] 3.7 Coordinator tests: remove_tag and admin modes over the tag statements, the masking policy, a raw write, the default unchanged, and object tags

## 4. Acceptance

- [x] 4.1 The quickstart bootstrap adds `remove_tag` to the `hive` servicedef, idempotently
- [x] 4.2 Docs: access-control tutorial, `sqe.toml.example`
- [x] 4.3 Fail closed: a store that cannot answer for `remove_tag` refuses every untag (column, Databricks column form, masking policy, table) and passes the adds (coordinator test)
- [x] 4.4 Live Ranger: under `remove_tag`, `apply_tag` on ssn tags but cannot untag through `ALTER COLUMN ... UNSET TAGS`, `UNSET TAGS` or a raw `SET TBLPROPERTIES` (audited, mask kept); a value change on the kept key passes; an allow item naming `remove_tag` lets the untag through. Under `admin`, the steward's untag is refused and an admin's passes. The refusal texts the portal maps to `remove_tag` are pinned
