**STATUS 2026-11-21: columns inherit their table's tags (`feat/tag-inheritance-table-924`, #924 / #930, openspec tag-inheritance 1.x).**

`sqe_core::object_tags::inherit_onto` merges `sqe.table-tags` into every column's own tags (own key wins). `tag_source_impl::effective_column_tags` feeds both enforcement tag sources (`scan_column_tags`, `CacheTagSource`), `SHOW STATS` and `SHOW EFFECTIVE POLICY` (`catalog_ops::load_column_tags`); `SHOW TAGS` stays on own tags; a malformed `sqe.table-tags` is tag state unknown, so the table is refused. Write authority and the Ranger tag projection stay on own tags (design D2, D5).

The IT harness gained `setup_handler_with_tag_store`, wired like production: provider tags for scans and a `CacheTagSource` over the handler's metadata cache for the DML policy probe. Without the cache source the UPDATE probe saw no tags and wrote the hidden row; production already wires it in `policy_wiring.rs`.

Gate: `make test-integration FILTER=tag_inheritance_924` -> 4 passed (mask + UNSET + SHOW EFFECTIVE POLICY, row filter on SELECT/UPDATE/DELETE/SHOW STATS, inherited conflict refused, malformed table tags refused).

Finding (confirmed by a probe, not fixed here): every MERGE into a row-filtered table fails, with a resource filter as well as a tag filter: "MERGE cardinality check failed to plan: No field named <table>.id". `write_predicates::unparse_predicates` renders the row filter with the table qualifier (masks get `unnormalize_col`, row filters do not). DELETE and UPDATE alias their scratch table to the table name, so it resolves; MERGE's target policy view and WITH CHECK do not. Fixed on the next branch; MERGE is left out of this MR's IT and docs until then.

Left open: schema and catalog inheritance with a cached parent read (tasks 2.x); Ranger projection of inherited tags for Spark parity (3.1); Ranger e2e (3.2).

**NEXT:** tag-inheritance MR 2: schema and catalog tags.
