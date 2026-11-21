**STATUS 2026-11-21: information_schema tag views (`feat/information-schema-tag-views`, stacked on `feat/column-tag-values`).**

DataFusion answers every `information_schema.*` name with its built-in
provider, so the front door rewrites `cat.information_schema.<tag view>` into
the table function `sqe_information_schema_tags('<view>', 'cat') AS <view>`
(`trino_compat.rs`, next to the `t$snapshots` rewrite). A two-part name gets
the session catalog from `qualify_information_schema` first.

New `crates/sqe-catalog/src/tag_views.rs` holds the function and provider.
`column_tags` lists namespaces, lists tables, loads each one with the
caller's token (eight at a time) and reads `sqe.column-tags`. A not-found or
access-denied namespace or table is skipped; any other error fails the query.
A malformed property leaves the table out with a warning. Equality filters on
`schema_name` and `table_name` are pushed down as Inexact to narrow the
listing.

Visibility: never more than SHOW TAGS shows. The coordinator also passes an
access check that resolves the caller's effective policy (catalog-scoped key
`{catalog}.{namespace}` when `catalog_scoped`) and leaves out a table the
policy denies entirely. Embedded mode registers the function without one.

Wiring: the provider is known-untagged in `scan_column_tags`, audit names it
`cat.information_schema.<view>` so the result cache never stores it, and the
MCP read-only guard allows the function (it walks the rewritten statement).

Limits: `catalog_tags`, `schema_tags` and `table_tags` are empty until SQE
can tag those objects. Hive tables and Iceberg views are not listed. A
catalog the resolver does not know (one only discovered, not configured)
fails with catalog not found, as `t$snapshots` does. SHOW TAGS still lists
keys only. Under `catalog_scoped`, !1888 adds catalog-qualified tag names to
policy matching; the access check goes through the same resolver, so it picks
that up without change.

Tests: unit tests in `sqe-catalog` (`tag_views`), `sqe-sql` (rewrite and
front door), `sqe-write`, `sqe-mcp` and `sqe-coordinator` (`audit_resources`).
No compose stack. The live check is the Databricks parity test
`test_uc_r6_information_schema_tag_views`.

**NEXT:** catalog, schema and table tags to fill the three object views.
