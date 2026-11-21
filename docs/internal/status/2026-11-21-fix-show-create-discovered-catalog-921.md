**STATUS 2026-11-21: SHOW CREATE TABLE on a discovered catalog (`fix/show-create-discovered-catalog-921`).**

sqlparser 0.62 does not visit `Statement::ShowCreate { obj_name }` as a relation, so `extract_catalog_qualifiers` returned nothing for `SHOW CREATE TABLE cat.ns.t`, the polaris-auto pre-flight attached no catalog, and `show.rs` reported the table missing. A new `visit_named_relations` in `crates/sqe-sql/src/catalog_qualifiers.rs` visits the statement and then the `SHOW CREATE` name; the three readers (catalog qualifiers, schema qualifiers for the namespace refresh, `names_polaris_system_schema`) use it.

Tests: unit `show_create_table_three_part_extracts_catalog`; live `catalog_ops_show_create_table_on_a_fresh_context` (new), and `catalog_ops_ddl_uses_the_session_catalog` drops its seed-handler workaround and reads `SHOW CREATE TABLE` on the qualified name after a cache flush.

Gate: `make test-integration FILTER=catalog_ops_` -> 2 passed. With the fix reverted, the new live test fails (checked).

Not changed: `rename_catalog` (view bodies never hold a `SHOW CREATE`).

**NEXT:** none for #921.
