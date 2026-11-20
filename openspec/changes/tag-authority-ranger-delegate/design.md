# Design: column tags by Ranger grant

## D1. Access type: `alter` on the column

The Ranger `hive` service definition SQE uses is the stock one. It has no tag-specific access type, and adding one would mean a custom service definition that Polaris's embedded authorizer and every other `hive` client would also have to carry. `alter` is the access type Hive itself asks for when a column's definition changes, so it is the closest fit.

Consequence, stated in the book and the MR: with `tag_authority = "ranger-delegate"`, anyone holding `alter` on a column can untag it and so remove its mask. A blanket `alter` grant (for example `public` on `*`) becomes a blanket right to unmask. The default stays `admin-role` for this reason.

## D2. A column-level check, not the table-level one

`access_grants_table_type` (behind `grants_table_privilege`) only counts policies whose `column` resource covers the whole table (`column_resource_covers_table`). A grant on `column = ssn` would be ignored, so a column-scoped steward could never tag, which fails acceptance 1.

`access_grants_column_type` is a sibling with the same evaluation order:
- deny items first, across every matching policy, honouring deny exceptions, conditions and validity schedules exactly as the table check does;
- then allow items minus allow exceptions;
- nothing matching means denied.

The only difference is the column test. A policy matches when its database and table match (same `last_component_fallback` rule, off under `catalog_scoped`) and its `column` resource matches the column through `resource_matches` (wildcards, case-insensitive, `isExcludes`). A policy with no `column` resource matches every column. So a deny on `*` or on that column decides, and a deny on another column does not.

## D3. The resource checked is the resource written

The gate keys the check exactly as `SHOW EFFECTIVE POLICY` and the tag projector do: `resolve_policy_key_for_ident_scoped` with the catalog from a three-part name, else the session's catalog, the namespace from `resolve_table_ident` (which applies `USE` for a bare name), and the store's `catalog_scoped`. A bare `ALTER TABLE t SET TAGS` under `USE ws.finance` is checked against `ws.finance` / `finance`, the same table `set_column_tags` writes (the !1214 bare-name class).

A reference that cannot be keyed (catalog-scoped store, no catalog) is refused.

The build found that the write did not follow this rule. `set_column_tags` (and `load_column_tags`, behind `SHOW EFFECTIVE POLICY`) sent a bare name to the configured warehouse, ignoring the session's catalog (`USE CATALOG`, the Trino `catalog=` header). DML and `CREATE SCHEMA` honour it through `resolve_target_catalog`. Both tag paths now use `resolve_target_catalog` too, so the gate and the write agree. Several other `ALTER TABLE` paths in `catalog_ops.rs` have the same gap. They are a separate change, because they are not tag writes.

## D4. Gate shape

```
require_tag_authority(session, stmt):
  admin role                      -> allow
  tag_authority == admin-role     -> require_admin (unchanged message)
  no policy store                 -> require_admin
  for each column in stmt.ops:
     store.grants_column_privilege(user, table, ns, column, Alter)
        Ok(true)  -> next
        Ok(false) -> 403 "requires alter on <ns>.<table>.<column>"
        Err(e)    -> 403, logged (a store that cannot answer grants nothing)
```

The check runs before `reject_if_hive_table` and before any catalog write, like the admin gate today.

## D5. Sibling paths

- `SET/UNSET MASKING POLICY`: same write, same gate. A different gate on the second spelling would be a bypass or a needless refusal.
- `SET TBLPROPERTIES ('sqe.column-tags' = ...)`: stays admin-only. It replaces the whole map, so it cannot be checked per column without diffing against the stored map, and the property is an internal format.
- `CALL system.reproject_column_tags`: stays admin-only. It is a table-wide repair of the Ranger projection, not a tag change.

## D6. Audit

A refusal returns an error from `execute`, which already writes the failed-statement audit record (`record_failed_statement`) with the error text. No new audit path is needed; the e2e asserts the record exists.

## D7. Other backends

`PolicyStore::grants_column_privilege` defaults to `Ok(false)`. The in-memory store answers from its table-level registry (a table grant covers its columns), which lets the coordinator gate be unit tested without Ranger. Polaris grants and passthrough keep the default.
