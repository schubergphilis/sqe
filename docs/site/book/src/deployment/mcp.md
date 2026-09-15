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
# write_catalogs = ["iceberg"] # catalogs the change tool may target; see "Catalog scoping"
# default_catalog = "iceberg"  # lets agents use two-part names
# default_schema = "analytics"
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
dedicated listener and independent network policy.

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

## Deployments with no identity provider

The section above assumes every caller carries a user token. A catalog reached
with engine credentials (AWS Glue, S3 Tables, an attached warehouse, an
auth-less Nessie) has no such tokens: SQE runs those queries under one identity
and the source authorizes the engine, not the person. `[mcp.auth] mode` covers
that shape.

For the simplest case, an anonymous read-only MCP endpoint over a Glue catalog,
`sqe-server` runs flag-only with no config file at all:

```bash
sqe-server --glue-catalog glue --allow-s3-prefix s3://lake/ \
  --mcp-anonymous --mcp-port 8098
```

See [Run SQE as an MCP server](../getting-started/mcp-server.md) for that path.
The rest of this section is the config-file form for static and OAuth auth.

```toml
[mcp]
enabled      = true
resource_uri = "http://sqe.internal:9091/mcp"
allow_writes = true

[mcp.auth]
mode = "static"                       # "oauth" (default) | "static" | "anonymous"

[[mcp.auth.principals]]
user      = "mcp-reader"
roles     = ["reader"]
token_env = "SQE_MCP_READ_TOKEN"      # preferred

[[mcp.auth.principals]]
user      = "mcp-writer"
roles     = ["data_writer"]
# nosemgrep: opt.aikido.semgrep-rules.generic.secrets.gitleaks.generic-api-key -- placeholder value
token_b64 = "c3FlLXdyaXRlci10b2tlbi0x"
default_catalog = "iceberg"           # this principal's sessions resolve two-part names here
default_schema  = "staging"
```

A caller presents `Authorization: Bearer <token>`; SQE matches it against the
configured principals in constant time and adopts that principal's user name
and roles. `token_env` wins over `token_b64` when both are set. Base64 is
encoding, not protection: anyone who can read the config can read the token, so
use `token_env` outside development. Tokens resolve once at startup, and a
token under 16 characters, an unset `token_env`, malformed base64, or two
principals sharing a token all fail the config check rather than the first
request.

Static mode carries no OAuth scopes, because SQE is both the issuer and the
verifier of these tokens. The three write gates become two: the principal's
roles must intersect `write_roles`, and the source must authorize the
statement. `enabled = true` with `allow_writes = true` and no principal holding
a writer role is a startup error, since the write tool would be advertised and
unusable.

`mode = "anonymous"` accepts every caller as one fixed identity:

```toml
[mcp.auth]
mode = "anonymous"

[mcp.auth.anonymous]
user  = "mcp-anonymous"
roles = ["reader"]
```

Three conditions refuse it at startup:

- any `auth.providers` entry that authenticates against an identity provider
  (`oidc_password`, `client_credentials`, `client_credentials_passthrough`,
  `token_exchange`, `bearer_token`, `bearer_passthrough`). A deployment that
  can authenticate real callers has no reason to skip it. `api_key`, `mtls` and
  `aws_iam` are local credential checks and do not block anonymous mode.
- `production_mode`, matching the existing rule that refuses the `anonymous`
  auth provider there.
- `allow_writes = true`. An anonymous caller is nobody to attribute a write to.

`[mcp.auth] mode` governs `/mcp` only. The SQL path keeps whatever
`auth.providers` does, so the natural mode-2 configuration pairs an
`anonymous` provider (every Flight SQL and Trino caller is one identity, as the
`aws-glue` and `aws-s3-tables` quickstarts already do) with
`mcp.auth.mode = "static"` (an MCP caller must present a configured token).
The two lines look contradictory and are not: the anonymous provider describes
the SQL door, the MCP mode describes the MCP door.

The reverse pairing is refused. `mcp.auth.mode = "oauth"` with an `anonymous`
auth provider fails at startup, because OAuth mode reads scopes out of the
bearer's payload and leaves signature verification to the chain: a provider
that verifies nothing would accept an unsigned token carrying the read scope,
and in a deployment reached with engine credentials that token would read real
data. `bearer_passthrough` is a weaker case (the source still validates the
token it receives) and is refused only under `production_mode`, but do not use
it for an internet-facing MCP endpoint.

In both modes the OAuth machinery stands down: the two
`/.well-known/oauth-protected-resource` routes are not mounted, and a `401`
carries a bare `WWW-Authenticate: Bearer` with no `resource_metadata`.
`authorization_servers` is required only in `oauth` mode. `resource_uri` stays
required in all three.

The MCP bearer is a credential for SQE, never forwarded upstream: the session
built for a static or anonymous caller carries an empty catalog token, the same
as the anonymous SQL path, and the source is reached with the engine's
configured credentials.

One consequence worth stating plainly. In `oauth` mode the source is the last
line of defense, and a caller holding every scope SQE advertises still reads
only what the catalog grants that user. In these two modes **there is no such
last line**: the source serves whatever the engine's credentials can reach. The
gates that were defense in depth become load-bearing, so keep `allow_writes`
off unless a specific principal needs it, issue one token per caller so the
audit log names something useful, and put the endpoint on a network only its
callers can reach.

## Tools

Read tools are always available after the `sqe:mcp:read` scope is validated:

- `execute_sql_readonly`
- `list_catalogs`
- `list_schemas`
- `list_tables`
- `describe_table`
- `get_table_ddl`
- `get_table_stats`
- `search_tables` (walks `SHOW SCHEMAS` and `SHOW TABLES` for one catalog and
  matches a LIKE pattern server-side; a bare word is a case-insensitive
  substring match; the catalog defaults to the session's; a schema the caller
  may not list is skipped)
- `sample_rows` (`SELECT * ... LIMIT n` as the caller, default 10, at most 100;
  row filters and masks apply)
- `explain_query`
- `whoami` (identity metadata only; never tokens)
- `refresh_metadata` (drops cached catalog metadata for a `table`, a `schema`,
  or the session's catalog view; `CALL system.refresh_catalog_cache(...)`
  underneath, so it needs no privilege and touches no other user's view;
  one refresh per scope per 10 s, a repeat inside the window is a no-op that
  says so; not annotated read-only, so clients that auto-approve reads still
  ask)

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

## Catalog scoping

Two settings decide which catalogs an agent works in.

`default_catalog` and `default_schema` are set on every MCP session, so agents
can write `SELECT * FROM orders` or `staging.orders` instead of qualifying
every name. In static mode a principal's own `default_catalog` and
`default_schema` win over the endpoint-wide values, which pins one agent to
one warehouse. The same defaults drive `information_schema` qualification and
the write gate below. A `default_schema` without a catalog to resolve in is a
startup error.

`write_catalogs` is the allowlist for `execute_sql_change`. When set, the
statement's target must resolve to a listed catalog, compared without regard
to case. Targets are what the parser exposes: the table of an `INSERT`,
`UPDATE`, `DELETE`, `MERGE`, `CREATE TABLE`, `CREATE TABLE AS`, `ALTER TABLE`,
`DROP`, `TRUNCATE` or `COMMENT`, the view of `CREATE VIEW`, the schema of
`CREATE SCHEMA` or `DROP SCHEMA`. Sources are reads, so an
`INSERT INTO iceberg.t SELECT * FROM lake.raw` is a write to `iceberg` only;
policy enforcement covers the read side as it does everywhere. An unqualified
target resolves through the session default catalog. While the gate is active,
a target that cannot be resolved is refused, and so is a statement kind with no
parser-visible target: `CALL` procedures, grants, policy DDL. Run those over
Flight SQL or Trino, as the user, not from an agent.

Hive external tables (`[[hive.roots]]`) get one extra rule, whether or not
`write_catalogs` is set. Iceberg writes are authorised twice: SQE's policy, and
Polaris vending credentials for that user. A Hive root has no per-user
credentials; SQE reads and writes it with its own storage credentials, so an
agent writing there acts as the engine. That needs an explicit opt-in: the
Hive root's catalog name must appear in `write_catalogs`, and SQE logs a
warning at startup when it does. With no `write_catalogs` and no Hive roots,
the change tool behaves as before.

## Hive tables over MCP

Hive external tables are visible through every read tool, with the same
row filters and column masks as over Flight SQL: the manifest's policy key is
`<root>.<database>`, and the read tools go through the same query handler.
Two things differ from an interactive session.

- Only `[[hive.roots]]` catalogs are reachable. `ATTACH ... TYPE hive_external`
  is a session statement, and an MCP session lives for one request, so an
  attached catalog is gone before the next call. Configure roots in the server
  config instead.
- `get_table_stats` reads Iceberg snapshot metadata and returns a tool error
  for a Hive table. `describe_table`, `get_table_ddl` and `execute_sql_readonly`
  work on both.

## Audit attribution

Every tool call runs as the authenticated user, and the audit record says so
in `actor.user`. It also says which agent asked. The session's client label,
the same field Trino HTTP fills from `X-Trino-Source`, is set to:

```text
mcp:<tool>:<client name>/<client version>
```

The client name and version come from the `clientInfo` the MCP client sends on
`initialize`. SQE answers `initialize` with an `Mcp-Session-Id` header; a
client that echoes it on later requests gets that label on every call. The id
is advisory. An unknown id, one from before a restart, or one issued to a
different identity is ignored, never rejected, and the label falls back to the
request's `User-Agent`, then to the bare `mcp:<tool>` form. Labels are reduced
to printable ASCII and cut at 64 bytes before they reach a log line.

In the OCSF export the label is `actor.app_name`; in `system.runtime.queries`
and the dashboard it is the `source` column. `whoami` returns the label the
server resolved for the current request as `client`.

The authentication rate limit is spent per `(peer IP, credential)` tuple, the
same tuple the Trino and Basic paths use. The credential half is a SHA-256
fingerprint of the bearer, so agents behind one proxy address get separate
budgets and a token-rotating attacker does not.

## Design references

The design follows the MCP 2025-11-25 authorization requirements (OAuth 2.1,
RFC 9728 protected-resource metadata, RFC 8707 resource indicators, discovery,
and incremental scope challenges), Snowflake's read-only-by-default SQL tool,
and Databricks' on-behalf-of-user governed SQL model:

- <https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization>
- <https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-mcp>
- <https://docs.databricks.com/aws/en/agents/mcp-tools/databricks-sql>
