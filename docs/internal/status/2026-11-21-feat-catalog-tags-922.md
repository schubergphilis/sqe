**STATUS 2026-11-21: tags on catalogs (`feat/catalog-tags-922`, #922). Stacked on `feat/object-tags-922` (!1894).**

OpenSpec change: `openspec/changes/object-level-tags/` (section 8).

- Storage: the Polaris catalog property `sqe.catalog-tags` (`sqe_core::object_tags::CATALOG_PROP_KEY`), same JSON list as table and schema tags.
- Client (`sqe-catalog/src/rest_catalog/catalog_properties.rs`): `SessionCatalog::catalog_properties` and `update_catalog_property` call `GET/PUT {base}/api/management/v1/catalogs/{warehouse}` on the caller's token. Polaris replaces the whole property map on PUT, so the full map goes back with `currentEntityVersion`; a 409 re-reads and retries (3 attempts). Errors carry Polaris's own message (for a reader: "not authorized for op 'UPDATE_CATALOG'"). A catalog whose URL has no `/api/catalog`, or that signs with SigV4, is refused as not supported.
- Write: `CatalogOps::set_catalog_tags`; `ALTER CATALOG` takes exactly one name (`catalog_target`).
- Gate: admins pass; `admin-role` (or no policy store) requires admin; `ranger-delegate` has no query-service resource for a catalog, so the privilege is Polaris's call on the caller's token. `tag_names` checks the keys a SET adds. Hive catalogs stay refused.
- Book: catalog tags, and the parity gap with Unity Catalog (object tags do not inherit into policy); follow-up #930.

- Found by the live e2e: `BreakerFailureClass::from_http_status` classifies error statuses only, so a 2xx reads as `ServerError`. The first version fed every response to it, and five successful calls opened the caller's breaker for 30 s. A 2xx now records success; `successful_calls_never_open_the_breaker` covers it.

Gates: `cargo test -p sqe-catalog --lib catalog_properties` (6), `cargo test -p sqe-write --lib object_tags` (3), `cargo check-server`, `cargo test -p sqe-coordinator --test it -- object_tags catalog_tags` (9), clippy `-D warnings` on sqe-catalog, sqe-write, sqe-coordinator, and the live e2e `catalog_tags_are_written_through_polaris_on_the_callers_token` on the Ranger compose stack (carol writes, alice refused by Polaris, property restored). Budgets not checked; pushed with `SKIP=budgets`.

**NEXT:** after !1895 merges, rebase !1894 and this branch onto main; then wire the `table_tags` / `schema_tags` / `catalog_tags` readers of !1890 to these properties and add values to `SHOW TAGS`.
