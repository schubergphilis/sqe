**STATUS 2026-11-07: default NULL order documented and pinned (`docs/null-ordering-default-876`, #876).**

SQE keeps DataFusion's `nulls_max` default (Trino/PostgreSQL: NULL is the largest value, so `DESC`
puts it first). Confirmed on the embedded CLI and pinned by
`sqe-catalog` `session_tuning::null_ordering_default` (3 tests: bare ASC/DESC, explicit
`NULLS LAST`/`NULLS FIRST`, `ROW_NUMBER() OVER (ORDER BY a DESC)` top-1).

Chosen Databricks path: the migrator writes `NULLS LAST` on every `DESC` (and `NULLS FIRST` on
every `ASC`) that came from Databricks. No session switch was added. The only per-session store
SQE replays is the Trino `X-Trino-Session` header, so a switch would not reach Flight SQL / ADBC /
dbt-sqe, and `SET datafusion.*` is refused on every protocol. The `nulls_min` DataFusion value would
be the switch if one is ever wanted. It would need the session-context cache key to carry it
(`session_context.rs` `session_cache_key`).

Docs: `dml.md` (new "NULL order in ORDER BY" section; the session-control table now says plain
`SET` is refused and lists `SET TIME ZONE` and `SET SESSION`), `vs-other-engines.md` Spark note.

**NEXT:** dbx-migrator: emit `NULLS LAST` / `NULLS FIRST` on Databricks sorts that have no null
clause, and stop stripping an explicit one.
