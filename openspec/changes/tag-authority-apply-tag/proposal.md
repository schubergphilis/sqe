# Tagging as a grant of its own (#917)

## Why

#911 lets a non-admin tag a column when Ranger grants them `alter` on it. Two gaps remain for the people a workspace lets apply tags:

- Tagging rides on `alter`. Everyone who may change a column's definition may also tag it, and so remove its mask. A steward cannot be given tagging without `alter`, and a writer cannot be given `alter` without tagging. Databricks keeps these apart with `APPLY TAG`, and the portal already writes Ranger policies with an `apply_tag` access type.
- Only `SET/UNSET TAGS` and `SET/UNSET MASKING POLICY` follow the delegated check. `SET TBLPROPERTIES ('sqe.column-tags' = ...)` and `CREATE OR REPLACE ... TBLPROPERTIES` stay admin-only, and a plain `CREATE TABLE ... TBLPROPERTIES` is not gated at all. A tool that writes the property directly is refused for a steward, and a plain `CREATE` can tag anything.

Building this also found that the 0.55 gate matched only raw property keys. `sqe.column-tags` written through Trino `extra_properties` passed it, so a non-admin could rewrite tags under the default `admin-role` (and so remove a mask).

## What Changes

- New setting `[access_control] tag_privilege = "alter" | "apply_tag"`, default `alter` (0.55 behaviour). It applies only under `tag_authority = "ranger-delegate"`.
- `TablePrivilege::ApplyTag`. The Ranger store evaluates `apply_tag` on a column with the same deny-first order as `alter`. It is granted only by an item that names it: `alter`, write and an allow item's `all` do not imply it. Narrowing items (deny items, allow exceptions) on `all` still cover it.
- Under `ranger-delegate`, every writer of `sqe.column-tags` uses the per-column check:
  - `SET/UNSET TAGS`, `SET/UNSET MASKING POLICY`: each column named (as in #911);
  - `ALTER TABLE ... SET TBLPROPERTIES` (and Trino `SET PROPERTIES extra_properties`): the columns whose tags differ between the live map and the new one, removed columns included, re-checked under the table's write lock;
  - `CREATE TABLE ... TBLPROPERTIES`: every column the map tags;
  - `CREATE OR REPLACE TABLE ... TBLPROPERTIES`: the columns that differ from the live table (a missing table counts as untagged).
- A value that is not a JSON tag map, a key spelled `sqe.column-tags` in another case, and overwriting a live value that does not parse stay admin-only.
- The gate reads the properties a statement will write (`TrinoTableOptions`), so `extra_properties` is covered in both modes.
- Under `admin-role` nothing else changes: the tag statements, `SET TBLPROPERTIES` and `CREATE OR REPLACE` need an admin role, and a plain `CREATE TABLE` is not gated.
- The quickstart's `bootstrap-ranger.sh` adds `apply_tag` to the `hive` servicedef, idempotently.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `tag-authority` (from `tag-authority-ranger-delegate`): the privilege becomes configurable, and the raw property writes join the delegated check. This supersedes D1 and D5 of that change.

## Impact

- `crates/sqe-policy-api`: `TablePrivilege::ApplyTag`.
- `crates/sqe-policy/src/ranger_store.rs`: `all` does not grant `apply_tag` on widening items.
- `crates/sqe-core/src/config/mod.rs`: `TagPrivilege`, `AccessControlConfig.tag_privilege`.
- `crates/sqe-write`: `tag_property_write`, `changed_tag_columns`, `set_table_properties_guarded`, `live_column_tags`.
- `crates/sqe-coordinator/src/query_handler/tag_authority.rs`: the gate (moved from `access_control.rs`).
- Quickstart bootstrap, book, `sqe.toml.example`.

Operators switching to `apply_tag` must add the access type to their `hive` servicedef first, or Ranger refuses the policies that grant it. Under `ranger-delegate`, `SET TBLPROPERTIES` and `CREATE OR REPLACE` widen from admin-only to the per-column check, and a plain `CREATE TABLE` that tags columns narrows from ungated to the same check.

## Not in this change

- Refusing a tag that no tag policy defines (request item 3). A follow-up.
- A `select` / `USE` check on the catalog and schema before a tag write. Done later as #941: see the requirement "A tag write needs USE on the schema first" in `specs/tag-authority/spec.md`.
