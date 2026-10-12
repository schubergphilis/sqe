**STATUS 2026-09-10 (a): the `rusqlite` pin is gone (`chore/drop-rusqlite`), first of the sqlite-rs adoption MRs.**

`rusqlite 0.39` sat in sqe-catalog's `sql` feature with zero call sites.
The 2026-08-28 evaluation kept it as "the lever that turns on
libsqlite3-sys `bundled`"; that stopped being true when workspace-hack
began requesting `bundled` directly and `sqlx/sqlite` started implying
`sqlite-bundled`. `cargo tree -i libsqlite3-sys -e features` showed
workspace-hack as the sole `bundled` requester, `cargo hakari generate`
reports no drift, and the lockfile loses rusqlite plus its private
`hashlink`/`fallible-*` tail. The `links = "sqlite3"` double-owner
constraint that blocked #389 is gone with it. Verified: `cargo check -p
sqe-cli -p sqe-catalog --features sqe-catalog/sql-sqlite` clean, no C
rebuild. Rebased over (b) after !981 merged; the rusqlite lever comment
in sqe-catalog's `sql` feature is the only text this touches there.
