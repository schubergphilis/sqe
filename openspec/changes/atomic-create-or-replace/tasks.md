## 1. Probes (confirm design assumptions A1-A4 before building on them)

- [ ] 1.1 Throwaway integration probe against Polaris: one raw commit carrying `AddSchema`, `SetCurrentSchema(-1)`, `AddPartitionSpec`, `SetDefaultSpec(-1)`, `SetProperties`, `AddSnapshot` and `SetSnapshotRef(main)`, with the Decision 4 requirements. Record whether it is accepted (A1)
- [ ] 1.2 Same probe for a shape seen before: does Polaris reuse the schema and spec ids, and does it need `SetCurrentSchema(<id>)` / `SetDefaultSpec(<id>)` in place of `-1` (A2, A3)? Record the assigned ids next to the predicted ones
- [ ] 1.3 Same probe on the embedded SQLite catalog through `TableCommit::from_parts`
- [ ] 1.4 Record which Polaris privileges the commit needs, compared with create + drop + rename (A4); update U5 in the design
- [ ] 1.5 Write the probe results into `design.md` (assumptions become facts or change the design), then delete the probe

## 2. Planner and commit

- [ ] 2.1 Decide the commit entry point: `commit_schema_update` as-is, or a sibling on `SessionCatalog` that takes snapshot updates (Decision 9, the open question)
- [ ] 2.2 Field id assignment by name with the promotion rule, nested paths included; unit tests for kept, promoted, incompatible and new columns
- [ ] 2.3 Partition field id reuse by (source id, transform) and equal-spec reuse; unit tests
- [ ] 2.4 Build the evolved `TableMetadata` and its `Table` view from the live table; emit the update list in Decision 3's order with explicit ids for reused schemas and specs; unit tests against the Polaris-rejected `-1` case
- [ ] 2.5 Requirements of Decision 4; the retry of Decision 5 (rebuild the snapshot only when every id is unchanged) and the error naming a concurrent schema change
- [ ] 2.6 Refusals of Decision 6 (format downgrade, `WRITE_BRANCH`) before any data is written
- [ ] 2.7 Sort order rule of Decision 8

## 3. Wire into CTAS

- [ ] 3.1 `handle_ctas_streaming_on`: replace the staged branch with the planner; write through the evolved table view; build the overwrite snapshot with the code `ctas_replace_in_place` uses (all live data and delete files removed)
- [ ] 3.2 Properties: `carry_live_properties` plus the recomputed `sqe.json-columns`, set in the same commit
- [ ] 3.3 Keep `refuse_replace_under_policy`, snapshot properties, `record_lineage_snapshot` and `after_commit` on the new path
- [ ] 3.4 Cleanup guard covers data files, manifests and the manifest list on every failure
- [ ] 3.5 Delete the `__sqe_replace_` branch, its warning and the swap block; grep for other `__sqe_replace_` references (embedded CLI, docs, tests)

## 4. Tests

- [ ] 4.1 Integration tests for every scenario in `specs/atomic-create-or-replace/spec.md` (`ctas_write_modes_e2e.rs` or a new `ctas_atomic_replace_e2e.rs`)
- [ ] 4.2 `information_schema_listing_race_e2e`: read the table being replaced (`moving`) too, and require the full column list on every read; drop the #892 note from its doc comment
- [ ] 4.3 Embedded CLI test: a schema-changing replace on a SQLite catalog keeps the UUID and history
- [ ] 4.4 The #896 test `ctas_replace_with_a_new_schema_keeps_properties_and_tags_896` still passes on the new path
- [ ] 4.5 Gate: `make test-integration FILTER=ctas_` and `FILTER=information_schema_listing_race`; `cargo test -p sqe-cli --bins`

## 5. Docs and after-work

- [ ] 5.1 CTAS reference page: remove the note that a schema-changing replace drops history; describe the kept UUID, history and grants, and the two refusals
- [ ] 5.2 `changelog.d/` entry, status fragment, close #892 with the gate output
