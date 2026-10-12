**STATUS 2026-09-10 (c): sqlite-rs is the default SQLite engine; no default build links libsqlite3 or sqlx (`feat/sqlite-rs-default`, on top of !981).**

`sql-sqlite-rs` replaces `sql-sqlite` in sqe-catalog's default set and is
unconditional on sqe-cli's sqe-catalog dependency (`sqlite-c` = the sqlx
+ C engine as an opt-in fallback). It is deliberately not a sqe-cli
default feature: the first version gated it on one and the release job,
which builds with `--no-default-features` like the Dockerfile and the
Makefile, hit the `compile_error!` (pipeline 1611344). `sqlite:` URLs on
the JDBC paths go to `SqliteRsCatalog` too: `TYPE jdbc` in `mount.rs`
(`build_jdbc` splits into a sqlite-rs branch and `build_jdbc_sqlx`) and
`CatalogBackend::Jdbc` in `build_backend_catalog` (guarded arm, same
`sqe-jdbc-session` scope as before). The `test-sqlite` features of
sqe-coordinator and sqe-write drop `sql-postgres`, since the write rig's
sqlite URL no longer needs sqlx. workspace-hack loses its hand pins on
`sqlx` (`sqlite`) and `libsqlite3-sys` (`bundled`) and the loader pin
goes to `rest` only; hakari no drift. `cargo tree -i libsqlite3-sys` and
`-i sqlx` match nothing for sqe-cli, sqe-coordinator, sqe-worker.
`maintenance.rs`'s race seam is now `cfg(all(test, feature =
"test-sqlite"))`, matching its only user, so `clippy --all-targets -D
warnings` is clean without the feature. Verified: mount_sqlite 5/5 and
sqlite_rs 4/4 on defaults, sqe-cli embedded 54/54 under `sqlite-rs` and
under `sqlite-c`, sqe-write `test-sqlite` write_handler 115/115 on
sqlite-rs, coordinator `test-sqlite` maintenance 36/36, clippy clean on
catalog/cli/write/coordinator/worker defaults and on sqe-catalog
`--all-features`. **NEXT:** Lab271 reply with the four defects (see (b));
re-vendor when #705 merges; delete `replace_row` when UPDATE binding is
fixed.
