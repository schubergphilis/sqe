---
title: "The MCP server that never holds a token"
description: "SQE now speaks Model Context Protocol over Streamable HTTP, and it does it as an OAuth resource server: no service account, no token minting, no credential proxy. The caller's bearer becomes an SQE session and travels to Polaris and S3 unchanged. Read tools are gated on one scope, the read-only SQL tool is enforced by SQE's own parser rather than a keyword regex, and the write tool needs three independent authorizations that do not substitute for one another. The runnable quickstart proves it with 14 checks against live Keycloak, Polaris and RustFS, including the one that matters most: a reader holding the OAuth write scope is still denied by the source."
pubDate: "2026-08-28"
author: "Jacob Verhoeks"
tags:
  - "mcp"
  - "oauth"
  - "security"
  - "identity"
  - "claude-code"
  - "codex"
---

Most MCP servers for databases hold a credential. You give the server a connection string or a service account, the model asks for rows, and the server answers with whatever that account can read. The identity of the person driving the model is a header at best, and a comment at worst.

SQE's MCP server holds nothing.

The endpoint is an OAuth protected resource. It validates the caller's access token with SQE's existing auth chain, turns that identity into the same per-user session Flight SQL and Trino HTTP use, and forwards the caller's own bearer to the catalog and storage layer. There is no MCP service account and no shared root credential. A tool call lands in the same `QueryHandler` the Flight SQL path uses, so the Polaris grants that gate a user's `SELECT` from the CLI gate the model's query by construction, and the Ranger plan rewriting sits below the protocol rather than beside it. The MCP quickstart exercises the Polaris gate; the Ranger masking suite still drives Flight SQL, so treat the mask half as reasoned rather than measured through `/mcp`.

## The invariant

One line describes the whole design:

```text
user bearer -> MCP endpoint -> SQE Session -> catalog/storage source
```

Nothing in that chain mints, exchanges, caches on behalf of, or upgrades a credential. The MCP layer is a protocol adapter with an authorization gate, and the gate only decides whether the caller may reach the MCP surface at all.

## What "OAuth compatible" actually means

The MCP authorization spec (revision 2025-11-25) splits the roles cleanly, and SQE takes exactly one of them: resource server.

SQE publishes RFC 9728 protected-resource metadata at `/.well-known/oauth-protected-resource` and at the resource-scoped `/.well-known/oauth-protected-resource/mcp`. An unauthenticated call gets a `401` with a `WWW-Authenticate: Bearer` challenge that carries `resource_metadata` and the scope the caller is missing. From there the client follows the metadata to the authorization server, reads its RFC 8414 or OpenID Connect discovery document, and runs Authorization Code with PKCE using the RFC 8707 `resource` parameter so the token it gets back is audience-bound to the SQE endpoint.

SQE validates that token. It never issues one.

The practical consequence is that any OIDC provider works, because SQE is not integrating with a provider so much as declining to be one. Keycloak in the quickstart, Auth0, Okta, Entra: the configuration is a JWKS URL, an issuer, and an audience equal to the canonical resource URI.

```toml
[mcp]
enabled = true
resource_uri = "https://sqe.example.com/mcp"
authorization_servers = ["https://idp.example.com/realms/data"]
allowed_origins = []
allow_writes = false

[[auth.providers]]
type = "bearer_token"
jwks_url = "https://idp.example.com/realms/data/protocol/openid-connect/certs"
issuer = "https://idp.example.com/realms/data"
audience = "https://sqe.example.com/mcp"
```

The whole block defaults to off. With `enabled = true` and no port, `/mcp` mounts on the coordinator's existing web listener; a nonzero `port` gives it a dedicated listener with its own network policy and its own TLS.

## Nine read tools, and a parser instead of a regex

After the read scope validates, the model gets `execute_sql_readonly`, `list_catalogs`, `list_schemas`, `list_tables`, `describe_table`, `get_table_ddl`, `get_table_stats`, `explain_query`, and `whoami`. The last one returns identity metadata and never a token.

`execute_sql_readonly` is where the interesting decision sits. The easy implementation is a keyword filter: reject anything matching `INSERT|UPDATE|DELETE|DROP`. That filter fails on the first statement a determined caller writes, and it fails silently, because a filter that does not understand SQL cannot tell you what it let through.

SQE already owns a SQL parser and a statement classifier, so the tool uses them, and it uses them as an allow-list rather than a deny-list. Exactly one statement per call, and the classified statement kind must be a query or one of the `SHOW` forms. Everything else (DDL, DML, `CALL` procedures, `GRANT`, secret handling, session mutation) is refused by not being on the list, before the query handler ever sees the text. A statement kind added to SQE next month is refused by default rather than accidentally allowed.

Bearer validation and query execution run under the coordinator's existing rate limiters, the same ones guarding Flight SQL, so a chatty agent is throttled by the machinery already in place rather than by a second implementation written for MCP.

Responses are bounded too: `max_result_rows` defaults to 1000, `max_response_bytes` to 2 MiB, and a truncated result says so in the payload rather than quietly handing the model a partial answer it will treat as complete.

## Three gates that do not substitute for each other

`execute_sql_change` does not appear in `tools/list` unless `allow_writes = true`. Turning it on is not the same as granting it. A call needs all three of:

1. an access token carrying both the read scope and the configured write scope;
2. an SQE identity holding at least one role in `write_roles` (default `data_writer`);
3. authorization from the source catalog and storage for that exact user and that exact statement.

The third gate is the one that makes the other two boring. OAuth scopes bound the MCP surface. They grant no catalog, table, row, column or storage access, ever. A model that somehow obtains a token with every scope SQE advertises still reads exactly what its human can read.

## The evidence

`quickstart/mcp/` layers the MCP configuration over the existing `polaris-keycloak-user-token` stack: real Keycloak users, real Polaris, real RustFS, no mocks. `./run.sh` captured this on 2026-08-28:

```text
==> checking OAuth protected-resource metadata
  ok resource identifier
  ok read scope advertised
==> checking authentication challenges
  ok missing bearer returns 401
  ok challenge points to RFC 9728 metadata
  ok invalid bearer rejected
==> checking MCP initialization and tools
  ok protocol version
  ok read-only SQL tool
  ok metadata tools
==> checking per-user identity and governed reads
  ok admin identity reaches MCP
  ok source data read through SQE
  ok write rejected by read-only parser gate
==> checking the opt-in write gate
  ok write tool advertised
  ok writer role can change source data
  ok reader denied despite OAuth write scope

==> checks: 14 passed, 0 failed
```

Two of those lines carry the argument. `write rejected by read-only parser gate` is the classifier refusing a mutation inside the read-only tool. `reader denied despite OAuth write scope` is `testuser` holding `sqe:mcp:write`, being advertised the write tool, calling it, and getting refused because Polaris does not care what an OAuth scope says about a user's ambitions.

A server that authenticates as a service account cannot produce that second line. Its reader and its writer are the same principal.

## Connecting a model

The quickstart ships secret-free templates for both clients we use daily. Codex reads the token from an environment variable through `bearer_token_env_var` and uses the `writes` approval mode, so read-annotated tools run and write-capable tools prompt. Claude Code takes a project `.mcp.json` pointing at the same endpoint.

```bash
cd quickstart/mcp
cp .env.example .env
./test-static.sh
./run.sh --check

export SQE_MCP_TOKEN="$(./mint-token.sh adminuser adminuser123)"
./setup-client.sh codex   # or: ./setup-client.sh claude
```

Because the token is a real user token, the assistant's view of the lakehouse is that user's view. Ask it for a table it holds no grant on and it gets the same "not found" a human gets. Ask it for a masked column and it should read the mask, because the rewrite happens on the LogicalPlan below every protocol; that path is shared code rather than a second implementation, though the end-to-end assertion for it currently runs over Flight SQL.

## What we did not build

No dynamic credential issuance. No token exchange. No server-side session store: the endpoint is stateless, `tools/list` works without a prior `initialize`, and a `GET /mcp` answers `405` with the reason in the body rather than opening an event stream nobody would feed. No wildcard origins: a request carrying an `Origin` header is rejected unless the exact origin is listed, which closes DNS rebinding for browser-based clients while leaving native clients (which send no `Origin`) alone.

And no writes by default. For an analytics assistant, `allow_writes = false` is the right setting and should stay that way. Automation that genuinely needs to write belongs in a separate deployment with narrowly scoped source roles, so the blast radius is a configuration boundary rather than a prompt.

The full reference is [Model Context Protocol](../book/src/deployment/mcp.md) in the book, and the runnable stack is in [`quickstart/mcp/`](https://github.com/schubergphilis/sqe/tree/main/quickstart/mcp/).
