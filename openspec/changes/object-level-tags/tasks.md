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
