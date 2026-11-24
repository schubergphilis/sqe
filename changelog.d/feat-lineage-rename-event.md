### Added

- OpenLineage event for ALTER TABLE ... RENAME TO

    A rename now emits a run whose output is the new table name, with `lifecycleStateChange` set to `RENAME` and the old name in the facet's `previousIdentifier` (`namespace`, `name`). A bare new name stays in the old table's schema. The event is recorded before the rename runs, so a failed rename's FAIL event names the table too. The facet field is optional and left out for every other change, so existing event shapes are unchanged.
