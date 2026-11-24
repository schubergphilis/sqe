**STATUS 2026-11-23: column lineage through a view names the right base columns (`fix/lineage-view-columns`).**

A CTAS from a view over a join reported `total <- items.id, items.label, orders.id` instead of `items.amount, orders.qty` (live lineage test, reproduced on main 312ae7927). SQE optimizes a view's body before wrapping it in a `ViewTable`, so the inlined base scans carry a pushed-down projection; `trace_table_scan` named the provider's full schema by position, shifting every column after the first one dropped. It now traces the scan's `projected_schema`.

Tests: `crates/sqe-lineage/tests/it/view_columns_test.rs` (failed on main with the live symptom, pass with the fix). Gates: `cargo test -p sqe-lineage` exit 0 (4 unit, 63 it); `cargo clippy -p sqe-lineage --all-targets -- -D warnings` exit 0.

The join keys (`items.id`, `orders.item_id`) still reach `total` as DIRECT/TRANSFORMATION; that is the next queued item (projections over a join re-tag INDIRECT/JOIN deps).

**NEXT:** join keys INDIRECT/JOIN in projections over a join (`extract/columns.rs`, `deps_for_refs`).
