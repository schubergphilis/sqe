## Why

Under `[access_control] tag_authority = "ranger-delegate"`, anyone who may tag a column may also untag it (#927). Removing a tag removes the mask that the tag drives, so a steward who is trusted to add a `PII` tag can also lift the protection on every column they may tag. In Unity Catalog, removing a governed tag is a stronger right than applying one, and the tag parity matrix (R7, R11) tests for that.

## What changes

- New setting `[access_control] tag_removal`, used under `ranger-delegate`:
  - `same` (default): removing a tag needs the same right as adding one. This is today's behaviour.
  - `remove_tag`: removing a tag from a column also needs the custom Ranger access type `remove_tag` on that column. Nothing implies `remove_tag`: not `alter`, not `apply_tag`, not a write type, not `all`.
  - `admin`: only an admin may remove a tag.
- Every statement that removes a tag from a column takes this check: `UNSET TAGS`, `UNSET MASKING POLICY`, a `SET TBLPROPERTIES` / Trino `extra_properties` write of `sqe.column-tags` that drops a key from a column, and a `CREATE OR REPLACE TABLE` whose map drops a key from the live table. The re-check under the table lock carries the removed columns separately, so a column checked as an add cannot become a removal unchecked.
- `ALTER TABLE|SCHEMA ... UNSET TAGS` (object tags, #922) take the same check against the object's table-level grant. A catalog's object tags under `remove_tag` or `admin` need an admin.
- `TablePrivilege::RemoveTag` (`remove_tag`); the Ranger store's `all` excludes it as it excludes `apply_tag`.
- Each authorised removal is logged with the user, the table and the columns.
- The quickstart bootstrap adds `remove_tag` to the `hive` servicedef next to `apply_tag`.

## Out of scope

- Dropping a column or a table: the tags go with the object. #928 keeps Ranger's projection in step.
- A value change on a kept key (`'PII' = 'a'` to `'PII' = 'b'`) is not a removal. Masks resolve on tag keys only (the tag-values change); if that changes, this definition has to change with it.
- `admin-role` mode: everything is admin-only already, so the setting has no effect.
