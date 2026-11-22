# Security and trust model

This page states SQE's trust boundaries plainly: what is gated per user, what is not, and what a compromised component can reach. It describes the model as documented and shipping today, and marks the gaps. Where a control is designed but not yet built, it says so and links the limitation. For the SQL surface of access control, see [GRANT and REVOKE](../sql-reference/grant-revoke.md). For the runtime controls (TLS, rate limiting, timeouts, error sanitization), see [Security & Policy](security.md).

## Identity: the user's token is the credential

SQE has no service account. A client authenticates with a username and password (or a pre-minted JWT, or one of the other configured providers), and the coordinator exchanges that for an OIDC token. The token becomes the credential for the rest of the query. There is no second, broader identity that the engine falls back to for the catalog. See [Authentication Flow](auth-flow.md).

In client-credentials mode (the lightweight test stack, where the catalog itself issues tokens) the username from the handshake is informational, used for session labelling and audit. The catalog requests still carry the service token. In OIDC password-grant mode, the token is the user's own.

## Signed catalog scope: narrowing one client's sessions

An orchestrating service can bind a workspace to some catalogs, each read-write or read-only, and launch jobs (dbt, Python clients) that connect to SQE with the person's own token. The job's code owns that connection, so a plain header cannot narrow it: the code could drop the header. `[session.catalog_scope]` closes that gap for one OAuth client. It is off unless configured.

The orchestrator mints the job a token issued to its runs client (`sub` is the person, `azp` is the runs client). It also signs a **catalog scope** for that token: a compact JWS (ES256) from a configured issuer, with these claims:

| Claim | Meaning |
|---|---|
| `sub` | Must equal the bearer's `sub` |
| `jti` | Must equal the bearer's `jti`, so the scope is bound to one token |
| `catalogs` | Object of catalog name to `read_write` or `read_only` |
| `exp` | Expiry; a leeway of `leeway_secs` (default 30) applies |

The client sends it in the `x-sqe-catalog-scope` HTTP header on the Trino path, or as Flight SQL call metadata under the same key. The name is configurable with `header_name`: an orchestrator that already sends the scope under its own header name sets `header_name` to that name, and the Flight SQL metadata key follows it. SQE verifies the signature against the issuer's JWKS and checks the binding and the expiry. A session of that client without a valid scope is refused with the reason, and the refusal is written to the audit log. The refusal also holds on protocols that cannot carry the header (MCP, Quack): such a session never gets a query context.

A scoped session:

- sees only the listed catalogs. Any other catalog answers "catalog not found", the same as one that does not exist;
- lists only them in `SHOW CATALOGS`, and can only select them with `USE`, `SET CATALOG` or `X-Trino-Catalog`;
- cannot run `ATTACH` / `DETACH`, `CREATE` / `DROP SECRET`, `SHOW SECRETS`, `GRANT` / `REVOKE` / `DENY`, policy DDL or tag and masking-policy statements;
- cannot write into a `read_only` catalog. The catalog layer refuses every commit there, as a backstop under the statement checks.

A refused statement is audited with the reason as its `error_type`: `catalog_not_in_workspace` for a catalog outside the scope, `catalog_read_only` for a write into a `read_only` one. The client still sees "Catalog 'x' not found" for the first, so a hidden catalog looks exactly like a missing one. The second answers "Catalog 'x' is read-only in this session (catalog_read_only)".

### Modes

`mode` picks how strictly the scope binds. It is server configuration only, so neither a job's code nor a client can change it.

| Session of the configured client | `binding` (default) | `union` | `off` |
|---|---|---|---|
| no scope | refused, audited | refused, audited | allowed, unscoped |
| bad, expired or other-token scope | refused, audited | refused, audited | header ignored |
| issuer keys unreachable | refused | refused | not fetched |
| valid scope | listed catalogs only, `read_only` enforced | verified and audited; every catalog the person can see | header ignored |

Sessions of other clients are unchanged in every mode. Without a `[session.catalog_scope]` section the feature is off. `binding` and `union` refuse to start without `client_id`, `issuer` and `jwks_url`. The coordinator logs the active mode at startup, as a warning when a configured section sets `union` or `off`.

`union` still requires a valid scope on the doors that can carry one (Trino HTTP, Flight SQL), but it does not narrow the session. Because no session is bound, MCP and Quack sessions of the client are not refused at context build in `union`, as they are in `binding`. Use `union` while an orchestrator rolls out scopes, and `binding` once every job sends one.

The person stays the identity end to end. The bearer goes to Polaris and S3 exactly as it would without a scope, so grants still decide access inside the scope. The scope only narrows what the user can reach in this session; it never widens it. Tokens of every other client are unchanged.

```toml
[session.catalog_scope]
client_id = "orchestrator-runs"        # azp of the job tokens
issuer = "https://orchestrator.example.com"  # iss of the scope JWS
jwks_url = "https://orchestrator.example.com/.well-known/jwks.json"
# header_name = "x-sqe-catalog-scope"
# leeway_secs = 30
# mode = "binding"                     # binding | union | off
```

## The metadata boundary: gated per user

Catalog operations carry the user's bearer token. Listing namespaces, loading a table, committing a snapshot: all go to the catalog as `Authorization: Bearer <user token>`, and the catalog enforces what that user may see and do. `SHOW TABLES` returns only tables the user can access. A write commits as the user. The metadata path is gated per user.

## The read data path: FileIO can refuse the shared key

Writes use per-table credentials vended by the catalog. INSERT, MERGE, and DELETE go through the loaded table's file IO, which carries the credentials the catalog returned for that table.

Iceberg FileIO used to fall back to the static `[storage]` key whenever Polaris did not vend credentials. That is now a switch. `[catalog] require_vended_credentials = true` omits those keys from FileIO and sets `s3.disable-config-load` / `s3.disable-ec2-metadata`, so a table load that does not return STS or remote signing cannot read objects as the coordinator, the pod IAM role, `AWS_*` env, or IMDSv2. `production_mode` refuses to start unless every REST catalog has the flag on (issue #395). Leave it off only for single-tenant and local stacks that still need the fallback.

Distributed workers are a separate path. `ScanTask` still ships the configured `[storage]` key on the Flight ticket. Per-user vending on that ticket is designed and not yet built. See [S3 Credential Vending](../design-notes/s3vending.md) and [Limitations](../reference/limitations.md#read-path-s3-access-and-require_vended_credentials).

The consequence to hold in mind: with the flag on, FileIO is fail-closed without vended credentials. With the flag off, or on the worker ticket, a user who can load table metadata can have files read with the engine key. Scope that key to the minimum the engine needs.

## Policy enforcement: off by default

SQE parses a fine-grained access-control SQL surface: column masks (`GRANT ... MASKED WITH`), row filters (`GRANT ... ROWS WHERE`), `SHOW EFFECTIVE GRANTS`, and `CHECK ACCESS`. The enforcement model is plan rewriting: filters and masks are injected into the logical plan before DataFusion's optimizer runs, so the optimizer cannot push a user predicate through a mask to probe raw values. The design follows the PostgreSQL row-level-security model, with no information leakage.

The enforcement is off by default: the default `[policy] engine = "passthrough"` returns plans unmodified. Two enforcers are shipped and wired, and you turn enforcement on by selecting one. `ranger` reads row-filter and column-mask policies from an Apache Ranger `query` service and feeds the plan rewriter; `in-memory` keeps grants in a hash map for dev and tests. Because the Ranger backend reads the same `query` service-def that Apache Spark reads through its Kyuubi authorization plugin, one policy enforces byte-identically in SQE and in Spark. See [Fine-grained access control](../features/fine-grained-access-control.md) for the how-to, [Spark / Ranger Parity](../design-notes/sqe-spark-ranger-parity.md) for the cross-engine result, and [GRANT and REVOKE](../sql-reference/grant-revoke.md) for the engine config.

## The TVF object-store boundary

The file-format table-valued functions (`read_parquet`, `read_csv`, `read_json`) read external files directly. They can address local filesystem paths and arbitrary object-store URLs. When inline credentials are omitted, they fall back to the configured `[storage]` credentials. Inline credentials are passed as SQL literals; SQE's audit logger redacts values matching `access_key`, `secret_key`, and `session_token` patterns. See [read_parquet TVF](../features/read-parquet.md).

The boundary to reason about: a SQL-capable user can direct these functions at whatever the engine's storage identity can reach. The trust boundary is the engine's storage credentials and host filesystem access, not the user's. Scope the engine's storage credentials minimally and run it with least-privilege filesystem access, the same posture you would take for any service that reads paths supplied at query time.

## What a compromised worker can reach

Workers are stateless. They hold no catalog state and no persistent credentials. The coordinator ships a worker a secured plan fragment plus the user's bearer token for the query, and the worker executes against storage. A worker reads data files for the queries it is assigned. It does not hold the long-lived OIDC client secret, and it does not have its own standing identity to the catalog.

In distributed mode the coordinator and workers share a secret that authenticates worker registration and the credential push. The engine refuses to start when the coordinator URL or worker URLs are set with an empty worker secret. See [Worker Secret](../deployment/kubernetes.md#worker-secret-distributed-mode).

A compromised worker can therefore reach the storage reachable with the credentials pushed to it for the queries it runs. `[coordinator] distributed_credentials` picks what a scan ticket carries. `vended` ships only the per-user, table-scoped credentials the catalog vended, and refuses to distribute without them. `auto` (the default) uses vended credentials when the table has them and falls back to the static `[storage]` key; `static` always ships that key; `worker_default` ships none and lets each worker use its own credential chain. With `static`, or `auto` on a table without vended credentials, a compromised worker that observes the key sees the same broad storage reach the coordinator has. `production_mode` rejects `auto`. FileIO on the coordinator can refuse the shared key (`require_vended_credentials`).

## Transport

Flight SQL connections can run with TLS, and optional mTLS adds client-certificate verification. TLS is off unless both a certificate and key are configured. See [TLS in Configuration](../deployment/configuration.md#tls) and [Security & Policy](security.md#tls-encryption). Run with TLS in any deployment where the network between client and coordinator, or coordinator and workers, is not already trusted.

## Summary of boundaries

| Path | Gated per user today | Notes |
|---|---|---|
| Catalog metadata (list, load, commit) | Yes | User bearer token to the catalog |
| Write data path (INSERT / MERGE / DELETE) | Yes | Per-table vended credentials |
| Read data path (FileIO) | Optional | `require_vended_credentials` omits the static key; production_mode requires it |
| Read data path (worker `ScanTask`) | Optional | `distributed_credentials = "vended"` ships only per-user vended credentials |
| Row filters / column masks | Off by default; available | Ranger + in-memory enforcers shipped; enable with `[policy] engine`. Ranger shares the policy with Spark/Kyuubi |
| TVF file access | Engine identity, not user | Scope storage creds and filesystem access minimally |
| Catalogs of one client's sessions | Optional | `[session.catalog_scope]`: a signed scope narrows the session to listed catalogs, read-only where marked |
| Worker reach | Limited by pushed creds | Bounded by the static read key unless `distributed_credentials = "vended"` |
