**STATUS 2026-10-23: copy-on-write MERGE reports the rows its clauses changed (`fix/merge-rows-affected`).**

Found by the dbt-sqe live probe (portal #42): MERGE `WHEN MATCHED THEN DELETE` reported `rows_affected` 2 for 1 deleted row.

Cause: `handle_merge` (CoW) in `crates/sqe-write/src/write_handler.rs` returned `total_rows` from `write_data_files_streaming_with_metrics`, the rows that survive the rewrite. Only the empty-source BY SOURCE case (#501) counted claimed rows. Unit tests pinned the old number. Merge-on-read (`handle_merge_equality`) already summed updated + deleted + inserted.

Fix:
- `merge_sql.rs`: `merge_select` takes a `SelectMode` (`Rewrite`, `WithCheck(rf)`, `ClaimedCount`); the claimed-row CASE now serves WITH CHECK and the new `build_merge_claimed_count_sql`, which counts claimed rows including DELETE rows (keep is FALSE).
- `handle_merge` runs that COUNT inside the retry closure (it reads the attempt's target relation) and returns it. The empty-source case keeps its cheaper join-free count.
- Unit tests updated to the claimed counts (m1 4 -> 2, m2 2 -> 1, m3 1 -> 3, m4 3 -> 2) plus `merge_mixed_clauses_count_claimed_rows` (delete, update, insert, one unclaimed match -> 3).

Cost: one more COUNT pass over the FULL OUTER JOIN per CoW MERGE, like the existing cardinality and WITH CHECK passes.

Gate:
- `make test-integration FILTER=merge_reports_rows_affected`: 1 passed (CoW and MoR: DELETE 1 -> 1, UPDATE + INSERT -> 2, ids asserted).
- `FILTER=merge` 11 passed, `FILTER=access_control_e2e` 63 passed.
- `RUST_MIN_STACK=33554432 cargo test -p sqe-write --features test-sqlite --lib`: 250 passed. Clippy clean.

**NEXT:** unchanged
