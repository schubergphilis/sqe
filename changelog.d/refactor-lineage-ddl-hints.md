### Changed

- DDL lineage hint builders live in `sqe-lineage`

    `lineage_scope`, `lineage_table`, `lineage_lifecycle_hint` and `lineage_rename_hint` moved from the coordinator's query handler to `sqe_lineage::ddl`, unchanged apart from their visibility and the rename hint taking the old and new names. Lineage events are unchanged.
