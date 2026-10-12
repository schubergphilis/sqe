**STATUS 2026-08-28: MCP endpoint over Streamable HTTP, OAuth resource-server model, uncommitted on `main`.**

SQE serves `/mcp` (protocol revision 2025-11-25) from the coordinator binary, off by default behind a new `[mcp]` block. New crate `sqe-mcp`, wired by `crates/sqe-coordinator/src/mcp_server.rs` either onto the existing web listener (`enabled = true, port = 0`) or onto a dedicated port with its own TLS.

The design decision worth writing down is what SQE refuses to be. It is a resource server only: it validates the caller's bearer through the existing auth chain, reuses the same per-user `Session` as Flight SQL and Trino HTTP, and forwards that bearer to the source, so `user bearer -> MCP -> Session -> catalog/storage` holds with no MCP service account and no token exchange. Discovery and registration stay with the authorization server; SQE publishes RFC 9728 protected-resource metadata at both well-known paths and answers a missing or wrong token with a `WWW-Authenticate` challenge naming the scope.

Two gates carry the security story. `execute_sql_readonly` is enforced by `sqe-sql`'s classifier as an ALLOW-list (query and `SHOW` kinds, one statement per call), so a statement kind added later is refused by default rather than accidentally permitted. `execute_sql_change` is absent from `tools/list` unless `allow_writes = true`, and then needs the write scope, an SQE role in `write_roles`, and source authorization independently -- the quickstart proves the third one is not redundant by giving a reader the OAuth write scope and watching Polaris deny it anyway.

`quickstart/mcp/` layers onto the `polaris-keycloak-user-token` stack rather than cloning it: 14 checks passed, 0 failed on 2026-08-28 against live Keycloak, Polaris and RustFS, plus secret-free Codex and Claude Code templates. Responses are bounded (1000 rows / 2 MiB, explicit `truncated`), browser callers need an exact `Origin` allow-list entry, and bearer validation and query execution ride the coordinator's existing rate limiters.

**Mode-2 auth landed on top.** `[mcp.auth] mode` is `oauth` (default,
unchanged), `static`, or `anonymous`. A catalog reached with engine
credentials has no user tokens, and MCP could not serve it at all: the scope
check reads JWT claims and the validator demanded an authorization server.
Static mode matches the bearer against `[[mcp.auth.principals]]` (token from
`token_env`, else `token_b64`, resolved once at startup, constant-time
compare) and adopts that principal's user and roles. The comparison lives in
`sqe-mcp` rather than the auth chain ON PURPOSE: a no-IdP chain typically
ends in the `anonymous` provider, which accepts any credential, so
delegating would let a WRONG token authenticate. That is the one test worth
keeping (`static_mode_rejects_an_unknown_token_despite_a_permissive_auth_chain`).
Anonymous mode is refused when any IdP provider is configured, under
`production_mode`, and with `allow_writes = true`. Neither mode forwards the
MCP bearer upstream (empty catalog token, same as the anonymous SQL path),
and neither mounts the `/.well-known` routes. 354 sqe-core + 17 sqe-mcp
tests green, strict clippy clean.

One real gap found while writing the docs: the Helm coordinator Service publishes Flight SQL, Trino HTTP and metrics, and NOT `healthPort`, so a shared-listener `/mcp` (and the web UI on the same port) is unreachable through the shipped chart. Written down in [Kubernetes & Helm](docs/site/book/src/deployment/kubernetes.md) rather than silently fixed, since it is a chart change with its own ingress question.

**NEXT: commit this on a feature branch and open an MR** (nothing here is committed yet), then decide whether MCP resources/prompts and an SSE streaming response are worth having. Docs and CHANGELOG are updated; the marketing site sync (`/update-sqeweb`) has not been run.

---
