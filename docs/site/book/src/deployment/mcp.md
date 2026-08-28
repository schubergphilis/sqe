# Model Context Protocol

SQE can expose an MCP Streamable HTTP endpoint from the main coordinator
binary. It is disabled by default. The endpoint is a protected OAuth resource,
not an authorization server and not a credential proxy.

## Identity and authorization

The invariant is:

```text
user bearer -> MCP endpoint -> SQE Session -> catalog/storage source
```

The MCP client sends `Authorization: Bearer <user-access-token>` on every
request. MCP passes it to SQE's normal auth chain. A successfully validated
identity becomes the same per-user `Session` used by Flight SQL and Trino HTTP;
if the auth provider does not return a separate catalog token, the original
bearer is placed in the session and forwarded to the source. There is no MCP
service account and no shared root credential.

Source grants and SQE policy remain authoritative. OAuth scopes only limit the
MCP surface; they never grant catalog, table, row, column, or storage access.

Use a validating `bearer_token` provider for MCP. Configure its `audience` to
the canonical `mcp.resource_uri`. Do not use `bearer_passthrough` for an
internet-facing endpoint.

## Configuration

```toml
[mcp]
enabled = true
# port = 8081 # optional dedicated listener; omit/zero to share health/web port
resource_uri = "https://sqe.example.com/mcp"
authorization_servers = ["https://idp.example.com/realms/data"]
allowed_origins = [] # add exact browser origins only when browser MCP clients need them
read_scope = "sqe:mcp:read"
write_scope = "sqe:mcp:write"
allow_writes = false
write_roles = ["data_writer"]
max_result_rows = 1000
max_response_bytes = 2097152

[[auth.providers]]
type = "bearer_token"
jwks_url = "https://idp.example.com/realms/data/protocol/openid-connect/certs"
issuer = "https://idp.example.com/realms/data"
audience = "https://sqe.example.com/mcp"
```

The endpoint is `POST https://sqe.example.com/mcp`. In the main `sqe-server`
binary, `enabled = true` with `port = 0` merges the MCP routes into the existing
health/web listener (`metrics.prometheus_port + 1`). Set a nonzero `port` for a
dedicated listener and independent network policy. The legacy
`sqe-coordinator` binary supports the dedicated-listener form only.

SQE publishes RFC 9728
metadata at both:

- `/.well-known/oauth-protected-resource`
- `/.well-known/oauth-protected-resource/mcp`

Unauthorized responses include a `WWW-Authenticate: Bearer` challenge with
`resource_metadata` and the required scope. The OAuth authorization server
must expose RFC 8414 or OpenID Connect discovery metadata. MCP clients use
Authorization Code with PKCE and the RFC 8707 `resource` parameter; SQE only
validates the resulting access token.

To prevent DNS-rebinding attacks, requests carrying an `Origin` header are
rejected unless the exact origin appears in `allowed_origins`. Native MCP
clients normally omit `Origin` and are unaffected. Avoid wildcard origins.

On Kubernetes, the shipped Helm Service publishes Flight SQL, Trino HTTP and
metrics only. `healthPort` is a container port used by the probes, so a
shared-listener `/mcp` is not reachable through that Service as it stands. Give
MCP a dedicated `port` and a Service port for it, or add a Service port for
`healthPort`. See [Kubernetes & Helm](./kubernetes.md#service).

A dedicated MCP listener uses `[coordinator.tls]`. The shared internal web
listener is plaintext, as before, so terminate TLS at the ingress/reverse proxy.
In either mode, the public `resource_uri` must use HTTPS in production.

## Tools

Read tools are always available after the `sqe:mcp:read` scope is validated:

- `execute_sql_readonly`
- `list_catalogs`
- `list_schemas`
- `list_tables`
- `describe_table`
- `get_table_ddl`
- `get_table_stats`
- `explain_query`
- `whoami` (identity metadata only; never tokens)

`execute_sql_readonly` is enforced with SQE's SQL parser/classifier, not a
keyword regular expression. Exactly one statement is accepted. DDL, DML,
maintenance procedures, grants, secrets, and session mutations are rejected
before reaching the query handler.

`execute_sql_change` is not advertised unless `allow_writes = true`. A call
then needs all three independent authorizations:

1. the OAuth token contains both the read scope and `write_scope`;
2. the SQE identity has at least one role in `write_roles`;
3. SQE and the source catalog/storage authorize that exact user and statement.

Keep `allow_writes = false` for analytics assistants. For automation requiring
writes, use a separate deployment/configuration and narrowly scoped source
roles where operational isolation is required.

Results are structured JSON and text content. `max_result_rows` and
`max_response_bytes` bound model context consumption; `truncated` reports when
the result was cut. Prefer aggregation and explicit `LIMIT` clauses.

## Design references

The design follows the MCP 2025-11-25 authorization requirements (OAuth 2.1,
RFC 9728 protected-resource metadata, RFC 8707 resource indicators, discovery,
and incremental scope challenges), Snowflake's read-only-by-default SQL tool,
and Databricks' on-behalf-of-user governed SQL model:

- <https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization>
- <https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-mcp>
- <https://docs.databricks.com/aws/en/agents/mcp-tools/databricks-sql>
