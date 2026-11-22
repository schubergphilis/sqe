## 1. Config

- [x] 1.1 `TagPrivilege { Alter (default), ApplyTag }`, `AccessControlConfig.tag_privilege` (alias `tag-privilege`); parse test rejects other spellings
- [x] 1.2 `sqe.toml.example` entry

## 2. Policy

- [x] 2.1 `TablePrivilege::ApplyTag` (`access_type() == "apply_tag"`)
- [x] 2.2 Ranger: `all` does not grant `apply_tag` on allow items / deny exceptions, still narrows on deny items / allow exceptions (D2); unit tests: only naming items grant, deny wins, deny on `all`, allow exception on `all`

## 3. Gate

- [x] 3.1 `tag_property_write` reads the property through `TrinoTableOptions` (D4); `changed_tag_columns` (D3); unit tests
- [x] 3.2 `gate_tag_property_statically` before the catalog preflight, `authorise_tag_property` after (D5)
- [x] 3.3 `set_table_properties_guarded`: per-table lock, reload, re-diff, refuse outside the authorised set (D6); sqlite test
- [x] 3.4 Coordinator tests: apply_tag gates the tag statements; plain CREATE checks every named column; malformed value / off-case key refused before the catalog; admin-role unchanged and sees `extra_properties`; live-map tests for SET TBLPROPERTIES, UNSET, CREATE OR REPLACE, a malformed live map, admin-role

## 4. Acceptance (live Ranger)

- [x] 4.1 Quickstart bootstrap adds `apply_tag` to the `hive` servicedef, idempotently
- [x] 4.2 `alter` + write without `apply_tag` refused on SET TAGS and SET TBLPROPERTIES, audited
- [x] 4.3 `apply_tag` on ssn: tag and untag through SET TAGS and SET TBLPROPERTIES; email refused, nothing written; INSERT refused
- [x] 4.4 #911 tests unchanged under the default `alter`

## 5. Docs

- [x] 5.1 Book access-control page: the raw property routes, `tag_privilege`, the servicedef step
- [x] 5.2 Changelog fragment, status doc

## 6. USE before a tag write (#941)

- [x] 6.1 `PolicyStore::grants_schema_use` (default false); Ranger: `select` reaching a table of the schema, schema-wide deny wins, no last-component allow; in-memory store
- [x] 6.2 Gate: checked first on column tag statements, raw `sqe.column-tags` writes, table and schema object tags; store error refuses
- [x] 6.3 Tests: Ranger unit (deny over allow, other schema, apply_tag alone, unscoped path); coordinator (every route, store error, catalog_scoped key, admin, admin-role)
- [ ] 6.4 Live Ranger run (`make test-access-control`) with a steward lacking `select`
