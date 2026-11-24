**STATUS 2026-11-24: DDL lineage hint builders moved to `sqe_lineage::ddl` (`refactor/lineage-ddl-hints`).**

Pure move, no behaviour change: `lineage_scope`, `lineage_table`, `lineage_lifecycle_hint` and the body of `lineage_rename_hint` left `query_handler.rs` for `crates/sqe-lineage/src/ddl.rs`. The coordinator re-exports them (`pub(crate) use`), so its callers and tests are untouched. The rename call site now runs `sqe_sql::metadata::rename_names` itself and passes the names in, so `sqe-lineage` gains no `sqe-sql` dependency. `extern crate self as sqe_lineage` keeps the moved bodies byte-identical. `sqe-coordinator` goes from 72796 to 72714 lines (`query_handler.rs` 10446 to 10364), which makes room for the view lineage events. Existing lineage tests ran unchanged.

**NEXT:** CREATE VIEW / DROP VIEW lineage events (view output with `datasetType: VIEW`, CREATE VIEW inputs and column lineage from its planned SELECT).
