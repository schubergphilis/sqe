**STATUS 2026-11-21: tags on tables and schemas (`feat/object-tags-922`, #922). Stacked on `feat/tag-names-ranger-tagdefs` (!1893).**

OpenSpec change: `openspec/changes/object-level-tags/`.

- Storage: `sqe_core::object_tags`. Table tags in the `sqe.table-tags` table property, schema tags in the `sqe.schema-tags` namespace property, JSON list in the column entry format (`["gold",{"owner":"finance"}]`). `[]` and a missing key both mean no tags. A malformed live value refuses the write instead of being wiped.
- Parser: `sqe_sql::object_tags` handles `ALTER TABLE|SCHEMA|DATABASE|CATALOG <name> SET|UNSET TAGS (...)`. On a table, a list of string literals is the object form and a list with no literals is the column form; a mixed list is refused. New `StatementKind::SetObjectTags`.
- Vendor: `RestCatalog::update_namespace` (`SQE PATCH (sqe#922)`), sends `removals: []`, mock-server test. Wrapped by `SessionCatalog::update_namespace`.
- Writes (`sqe-write/src/catalog_ops/object_tags.rs`): table under the per-table lock, `SetProperties`, then every token's cache entry is evicted; schema under a keyed namespace lock, `get_namespace` then `update_namespace`. Metadata only: no Ranger projection, no policy cache flush.
- Gate (`query_handler/tag_authority/object_tags.rs`): admins pass; `admin-role` requires admin; `ranger-delegate` needs `grants_table_privilege` on the table (column grants do not count) or on `<db>.*` for a schema, keyed by `resolve_policy_key_for_ident_scoped`; a store error grants nothing. `tag_names` checks SET keys after the privilege (admins exempt, as for column tags). Raw `sqe.table-tags` property writes are admin-only in every mode, any key case.
- Refused: catalog tags (until a storage decision), Hive tables, Hive roots.

Gates: `cargo test -p sqe-coordinator --test it grant_introspection_gate` (39 passed, 7 new), `cargo test -p sqe-sql --lib` (689), `cargo test -p sqe-core --lib object_tags` (4), `cargo test -p sqe-write --lib object_tags` (2), `cargo test -p sqe-write --lib tag_source`, vendor `iceberg-catalog-rest` update_namespace test, `cargo check-server`, clippy `-D warnings` on sqe-core, sqe-sql, sqe-catalog, sqe-write, sqe-coordinator. `cargo xtask budgets --check` fails on crate totals (sqe-coordinator, sqe-core, sqe-sql, plus pre-existing sqe-policy entries); no ceilings raised, pushed with `SKIP=budgets`. Not run: an e2e against a live Polaris (namespace property update) or the compose stack.

**NEXT:** wire the `table_tags` / `schema_tags` readers of !1890 to `sqe_core::object_tags`, add values to `SHOW TAGS`, rebase !1890 and mark it ready. Catalog tags wait on a storage decision.
