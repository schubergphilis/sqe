**STATUS 2026-11-12: catalog-qualified metadata tables read the named catalog (`fix/metadata-table-catalog`, #317 follow-up).**

Found by the dbx-migrator writer probe (2026-11-11): over Flight SQL
`SELECT name FROM probe.s."rb$refs"` failed with "table_refs: failed to load
table 's.rb': TableNotFound" while the same query over Trino HTTP with
`X-Trino-Catalog: probe` worked. The `$` rewrite already passed the catalog as
the TVF's third argument; the coordinator built the six metadata TVFs with
`new` over the effective session catalog, which ignores it. The coordinator
now builds them with `with_catalog_resolver`, as embedded mode does, over the
catalogs the session built (config catalogs and the discovered warehouse).
No argument or the default's name is the effective catalog; an unknown name
is a planning error instead of a silent read from the default. Limit: the
`table_files` / `table_partitions` policy lookup is keyed by namespace and
table, not catalog, as before. Test: `metadata_tvf_reads_the_catalog_its_argument_names`
(session_context unit test).

**NEXT:** unchanged
