## 1. Storage format

- [x] 1.1 `sqe_core::object_tags`: `TABLE_PROP_KEY`, `SCHEMA_PROP_KEY`, `parse`, `to_json`, `apply`; unit tests (round trip, replace value, unset, `[]`)

## 2. Parser

- [x] 2.1 `sqe_sql::object_tags`: `ObjectTagsStatement`, `TagObject`; `ALTER TABLE|SCHEMA|DATABASE|CATALOG ... SET|UNSET TAGS ('k' = 'v')`
- [x] 2.2 Mixed lists refused; native column forms unchanged (regression tests)
- [x] 2.3 `StatementKind::SetObjectTags` in the classifier and every exhaustive match

## 3. Vendor

- [x] 3.1 `RestCatalog::update_namespace` (`SQE PATCH (sqe#922)`), mock-server test, README patch row

## 4. Gate

- [x] 4.1 `require_object_tag_authority`: admin, `admin-role`, `ranger-delegate` table and database `*`; catalog refused
- [x] 4.2 `require_defined_tags` takes the catalog; object writes check the keys they add
- [x] 4.3 Raw `sqe.table-tags` writes admin-only (D5)

## 5. Writes

- [x] 5.1 Table: per-table lock, read, apply, `SetProperties`, evict every token
- [x] 5.2 Schema: per-namespace lock, `get_namespace`, apply, `update_namespace`
- [x] 5.3 Hive tables refused

## 6. Tests

- [x] 6.1 `tests/it`: admin writes, steward refused under `admin-role`, delegate table grant, column grant is not a table grant, schema needs `*`, undefined name refused, raw property refused
- [x] 6.2 Existing "tags on a table are not supported yet" assertions updated (none existed; catalog refusal asserted instead)

## 7. Docs

- [x] 7.1 Book: object tags, metadata only
- [x] 7.2 README roadmap, changelog fragment, status file

## 8. Catalog tags

- [x] 8.1 `SessionCatalog::update_catalog_property` / `catalog_properties`: Polaris management GET/PUT on the caller's token, full map, entity version, retry on 409; mock-server tests
- [x] 8.2 `CatalogOps::set_catalog_tags` writes `sqe.catalog-tags`; single-name target
- [x] 8.3 Gate: admin, `admin-role` admins only, `ranger-delegate` leaves the privilege to Polaris; `tag_names` for SET
- [x] 8.4 Book: catalog tags and the inheritance gap (#930)
- [x] 8.5 Live e2e on the Ranger compose stack: catalog admin writes, reader refused by Polaris

## 9. Readers

- [x] 9.1 `table_tags`: same table walk and policy check as `column_tags` (asked even without column tags; unreadable column tags leave the table out)
- [x] 9.2 `schema_tags`: `get_namespace` per listed or pinned namespace, `SHOW SCHEMAS` visibility
- [x] 9.3 `catalog_tags`: management API on the caller's token; forbidden, missing or not Polaris gives no rows
- [x] 9.4 Unit tests; sqlite `tests/it` read-after-write for table and schema tags
- [x] 9.5 Live e2e: catalog tags read back for a catalog admin, and what a reader sees (a reader of the catalog sees the same rows)
