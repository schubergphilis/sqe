### Added

- Tagging as a grant of its own: `[access_control] tag_privilege = "alter" | "apply_tag"` (#917)

    With `tag_authority = "ranger-delegate"` and `tag_privilege = "apply_tag"`, a user may change a column's tags when the Ranger `hive` access policies grant them `apply_tag` on that column. `apply_tag` is not implied by `alter`, write or an allow item's `all`; a deny item (including a deny on `all`) still wins. The `hive` service definition must carry an `apply_tag` access type (the quickstart bootstrap adds it). The default `alter` keeps the 0.55 behaviour, and `admin-role` is unchanged.

### Changed

- Under `tag_authority = "ranger-delegate"`, every statement that writes `sqe.column-tags` uses the same per-column check (#917)

    `ALTER TABLE ... SET TBLPROPERTIES` is checked against the columns whose tags differ between the live map and the new one (a removed column counts). `CREATE [OR REPLACE] TABLE ... TBLPROPERTIES` is checked on every column it tags (`OR REPLACE`: the columns that differ from the live table). These were admin-only (`SET TBLPROPERTIES`, `CREATE OR REPLACE`) or ungated (plain `CREATE`). A value that is not a JSON tag map, or a key spelled `sqe.column-tags` in another case, stays admin-only. `UNSET TBLPROPERTIES` does not parse, so it has no route to the tags. Refusals are `403 Forbidden` and logged.

### Security

- `sqe.column-tags` written through Trino `extra_properties` is now gated like the plain key (#917)

    The gate matched only the raw property keys, so `ALTER TABLE ... SET PROPERTIES extra_properties = MAP(ARRAY['sqe.column-tags'], ...)` or a `CREATE OR REPLACE TABLE ... WITH (extra_properties = ...)` from a non-admin could rewrite a table's tags and so remove a column's mask. The gate now reads the properties the statement will actually write.
