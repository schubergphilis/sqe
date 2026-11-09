**STATUS 2026-11-09: RUSTSEC-2026-0459 ignored in cargo-audit and cargo-deny (`fix/rustsec-2026-0459-libsqlite3`).**

`deps-check` started failing on main (pipeline 1705693) and on every MR once
the advisory DB picked up RUSTSEC-2026-0459: libsqlite3-sys < 0.39 bundles a C
SQLite with CVE-2026-119375. The only path is sqlx-sqlite 0.9.0, whose
`libsqlite3-sys = ">=0.30.1, <0.38.0"` blocks the fix, and sqlx 0.9.0 is the
latest release. `cargo tree -i libsqlite3-sys` finds nothing for default
`sqe-server` or `sqe-cli`; only the opt-in `sqe-cli/sqlite-c` and
`sqe-catalog/sql-sqlite` features reach it. The ignore sits in both
`.cargo/audit.toml` and `deny.toml` with review-by 2027-01-09.

**NEXT:** when sqlx ships a release accepting libsqlite3-sys >= 0.39, bump it and drop both ignore entries.
