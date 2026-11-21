**STATUS 2026-11-21: the #928 tag projection code moves into its own module, so the budgets job passes again (`refactor/tag-projection-budgets-928`).**

The #928 change (!1905) took `catalog_ops.rs` to 4162 lines (ceiling 3850), `query_handler.rs` to 10425 (ceiling 10400) and the sqe-write crate past 33000 lines, so the budgets job on main fails. This MR keeps the file ceilings and splits instead:

- `crates/sqe-write/src/catalog_ops/tag_projection.rs` (new): the projection helpers, the create and procedure-drop hooks, `reproject_column_tags` and its row helpers, and their unit tests, moved as-is. Only visibility (`pub(super)`) and imports change. `catalog_ops.rs` is 3635 lines.
- `query_handler/ddl.rs`: `run_maintenance_call` and `created_table_name` move here, with two helpers (`tags_before_create`, `project_created_tags`) that shorten the CREATE TABLE and CTAS arms. `query_handler.rs` is 10380 lines.
- `.config/budgets.json`: the sqe-write crate ceiling goes from 33000 to 34000 lines. The #928 projection is new behaviour (about 450 lines with tests), and the crate total does not shrink by moving code between its files.

Gate: `cargo xtask budgets --check` -> ok; `make test-integration FILTER=tag_projection_928` -> 6 passed; `cargo test -p sqe-write --lib -- tag union reproject` -> 61 passed; clippy on sqe-write and sqe-coordinator clean.

**NEXT:** #927 APPLY TAG access type (track 1.8).
