**STATUS 2026-10-31: coordinator SHOW CREATE VIEW (`feat/coordinator-show-create-view`).**

`handle_show_create_table` printed a view's columns as a `CREATE TABLE`
(the column path succeeds on a view). It now looks the name up as a view
first (`show_create_view_body`: `resolve_table_ident` + `show_catalog` +
`load_view_sql`, the same resolution the table path uses):

- view + `SHOW CREATE VIEW`: `CREATE VIEW <name> AS\n<body>`, column
  `Create View`, body verbatim (the coordinator stores the body as written;
  an embedded-created body names the warehouse, which the coordinator
  resolves too).
- view + `SHOW CREATE TABLE`: `SemanticError` "is a view, not a table".
- table + `SHOW CREATE VIEW`: `SemanticError` "is a table, not a view".
- A catalog without a `SessionCatalog` (ATTACHed) or a non-REST backend
  holds no views; a view lookup error falls through to the table path for
  `SHOW CREATE TABLE` and fails `SHOW CREATE VIEW`.

Policy: `deny_hidden_table` already runs on the name in dispatch, so a
hidden view is refused before its body loads.

MCP `get_table_ddl` sends `SHOW CREATE TABLE`; on the "is a view" error it
reruns as `SHOW CREATE VIEW` (matched on the raw error, since
`client_error_text` redacts). Unit test with a scripted executor.

Verified: `make test-integration FILTER=test_create_and_drop_view` (adds
SHOW CREATE VIEW + both wrong-kind errors), sqe-mcp lib 71.

**NEXT:** OAuth2 attaches in embedded mode still refuse views. The raw
view calls (`authenticate_raw_request`, three call sites incl. the ETag
spawn) need the token the vendored client exchanged; check in
`auth/oauth2.rs` whether `authenticate` refreshes on its own or only
`on_unauthorized` does, then expose it as an SQE PATCH and key the circuit
breaker on the credential instead of the empty bearer.
