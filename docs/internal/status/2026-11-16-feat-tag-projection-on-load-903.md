**STATUS 2026-11-16: a tag committed outside SQE is projected into Ranger on the next load (`feat/tag-projection-on-load-903`, closes #903; ask 1 merged earlier).**

Ask 1 (an out-of-band tag masks the next SQE query) shipped in `feat/tag-refresh-on-load-903`. This branch is ask 2: Spark agrees with SQE.

How:

- `sqe-catalog/src/load_observer.rs`: a `TableLoadObserver` trait and a `LoadObserverSlot` (`Arc<OnceLock<..>>`) carried by `TableMetadataCache`, so every clone sees one observer.
- `SqeSchemaProvider::load_table_or_view` reports each successful table load with the planner's catalog name (`catalog_name`, else the warehouse), the name a catalog-scoped Ranger key carries (#899).
- `sqe-write/src/tag_load_observer.rs`: `TagLoadObserver`.
  - Sync fast path: metadata location unchanged since the last projection -> return.
  - Otherwise a spawned task takes `CatalogOps`' per-table lock (the one `SET TAG` holds), drops an observation older than the recorded one, projects `previous -> tags` with `project_table_tags`, and records the state only on success, so a failed Ranger call is retried on the next load.
  - Equal tags (a data commit) send nothing.
- Wired in `QueryHandler::with_table_cache` through `CatalogOps::load_tag_observer`, `None` when `project-tags` is off.

Known gaps, documented in the access-control tutorial:

- The memory is per process. After a restart the first load of a tagged table projects its tags once (idempotent add-or-update); a removal made while the coordinator was down stays in Ranger until `CALL system.reproject_column_tags`.
- `SET TAG` does not record into the observer; the next load sees the new location, finds the tags it already projected and converges without harm.
- The projection writes with the coordinator's Ranger credentials on behalf of whoever committed to Polaris. That client could already change what SQE masks (ask 1); this makes Spark agree, including on a removal.

Tests:

- Unit: `tag_load_observer::tests::*` (5) and `load_observer::tests::clones_share_the_slot_and_the_first_observer_stays`.
- e2e: `access_control_e2e::tag_from_out_of_band_commit_masks_the_next_query` now also polls Ranger's tag bundle for the `ssn` association after the raw commit, then removes the tag out of band and polls until the association is gone. Gate: `scripts/access-control-test.sh tag_from_out_of_band`.

**NEXT:** none on #903.
