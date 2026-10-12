**STATUS 2026-09-10 (f): `SqliteRsCatalog` serialises every call on its one connection (`fix/sqlite-rs-serialise-connection`); fixes the "cannot start a transaction within a transaction" seen on !985.**

The facade's worker serialises statements, not transactions. Two
concurrent `update_table` commits on the shared `Connection` interleaved
their `BEGIN`s (coordinator `maintenance_lease_test::concurrent_try_acquire_for_the_same_job_key_exactly_one_wins`
under `--all-features`, where `sql-sqlite-rs` wins the JDBC `sqlite:` arm),
and any autocommit statement from a third task during someone's
transaction would have run inside it. `SharedConn { conn, gate: Mutex }`
replaces the bare `Arc<Connection>`; `SharedConn::run` holds the gate for
the whole blocking closure, so every catalog call, single statement or
transaction, is exclusive. Regression test
`concurrent_commits_and_writes_on_one_connection_never_nest_transactions`
(8 concurrent property commits + 8 namespace creations on one connection,
multi-thread runtime; outcomes must be success or the retryable conflict,
end state consistent, oracle `integrity_check` ok). Verified: sqlite-rs
tests 6/6 x3 and the coordinator lease tests 9/9 x3 on sqlite-rs
(`--features test-sqlite,sqe-catalog/sql-sqlite-rs`), clippy clean.
