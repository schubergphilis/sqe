**STATUS 2026-10-29: embedded time travel (`feat/embedded-time-travel`).**

`sqe-cli --embedded` failed every `FOR {VERSION|TIMESTAMP|SYSTEM_TIME} AS OF`
clause in the parser ("Expected: one of UPDATE or SHARE"). `execute` now
strips the clauses with `sqe_sql::extract_time_travel_spec` before the front
door, loads each named table from its Iceberg catalog, resolves the clause
with `sqe_catalog::time_travel::resolve_version_snapshot` and registers an
`IcebergStaticTableProvider` pinned to that snapshot under a
`"default"."default".__sqe_ver_<n>` alias. The rewritten statement goes back
through `execute`, so an `INSERT ... SELECT ... FOR VERSION AS OF` still
reaches the Iceberg write path. The aliases are deregistered after the
statement, on success and on error.

The snapshot resolution (snapshot id, branch or tag, timestamp, the #821
transaction checks) moved out of the coordinator's `apply_version_spec`
into `sqe_catalog::time_travel`; timestamp parsing and
`replace_table_reference` moved to `sqe_sql::time_travel`. Coordinator
behaviour is unchanged.

Limits: the pinned read uses the snapshot's own schema; a one- or two-part
name resolves in the session default catalog, which is DataFusion's memory
catalog in embedded mode, so qualify with `iceberg.`.

**NEXT:** `CREATE VIEW` in Iceberg catalogs in embedded mode (REST-attached
catalogs first; check what the SQLite-backed warehouse catalog supports).
