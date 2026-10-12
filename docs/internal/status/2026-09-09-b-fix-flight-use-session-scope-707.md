**STATUS 2026-09-09 (b): finding 707 (#627) closed on `fix/flight-use-session-scope-707`: `USE` over Flight SQL scopes the session, and the bench Flight client stops qualifying table names.**

`USE [catalog.]schema` was a logged no-op in the engine; only the Trino
layer honoured it, by handing the scope back as `X-Trino-Set-*` headers the
client echoes. Flight SQL has no such channel, so the bench's Flight client
still rewrote every query with the tokenizer qualifier. Now the handler
mutates the stored session the same way `SET WRITE_BRANCH` does
(`SessionManager::set_default_scope`, copy-and-reinsert), so every later
request on the same bearer token, on any connection, resolves unqualified
names in the new scope; `USE schema` keeps the catalog, a three-part target
is a syntax error. `current_schema()` reads the same field, so it is the
observable. The first test of the new setter hung: `set_write_branch`
held the DashMap read guard while inserting into the same shard, a
deadlock that `SET WRITE_BRANCH` over Flight had in production and no
test exercised; both setters now go through `update_session`, which
clones the entry and drops the guard before the insert. `FlightSqlBenchClient::with_schema` sends one `USE` lazily on
the first query (`tokio::sync::OnceCell`) and reports `default_schema()`,
which switches `QualifyTables::Auto` off; the `run` verb builds one scoped
client per suite (`[golden.]<ns>`), `create_client` applies
`[catalog.]schema` for `test`/`run`. A tokenless Flight client keeps
qualifying (no session to scope). Tests: `session_manager` unit,
`parse_use_target` unit, `flight_sql_smoke::use_over_flight_scopes_later_statements_on_the_same_token`
(fresh channel, same token; catalog.schema form; bad target; other session
untouched). Docs: sql-reference/dml.md session-control rows,
benchmarks/queries/README.md. Not re-run: `sqe-bench compare tpch` 22/22
parity on a live stack (needs the docker Trino rig).
**NEXT:** #659 partials (508 and 108 are deletions), 807 item 3 (docker
runner), the #4xx duplicate close (109 issues; the assistant's tooling is
not allowed to close issues, so this is a `glab api` loop run by hand).
