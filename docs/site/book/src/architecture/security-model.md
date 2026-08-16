# Security and trust model

This page states SQE's trust boundaries plainly: what is gated per user, what is not, and what a compromised component can reach. It describes the model as documented and shipping today, and marks the gaps. Where a control is designed but not yet built, it says so and links the limitation. For the SQL surface of access control, see [GRANT and REVOKE](../sql-reference/grant-revoke.md). For the runtime controls (TLS, rate limiting, timeouts, error sanitization), see [Security & Policy](security.md).

## Identity: the user's token is the credential

SQE has no service account. A client authenticates with a username and password (or a pre-minted JWT, or one of the other configured providers), and the coordinator exchanges that for an OIDC token. The token becomes the credential for the rest of the query. There is no second, broader identity that the engine falls back to for the catalog. See [Authentication Flow](auth-flow.md).

In client-credentials mode (the lightweight test stack, where the catalog itself issues tokens) the username from the handshake is informational, used for session labelling and audit. The catalog requests still carry the service token. In OIDC password-grant mode, the token is the user's own.

## The metadata boundary: gated per user

Catalog operations carry the user's bearer token. Listing namespaces, loading a table, committing a snapshot: all go to the catalog as `Authorization: Bearer <user token>`, and the catalog enforces what that user may see and do. `SHOW TABLES` returns only tables the user can access. A write commits as the user. The metadata path is gated per user.

## The read data path: FileIO can refuse the shared key

Writes use per-table credentials vended by the catalog. INSERT, MERGE, and DELETE go through the loaded table's file IO, which carries the credentials the catalog returned for that table.

Iceberg FileIO used to fall back to the static `[storage]` key whenever Polaris did not vend credentials. That is now a switch. `[catalog] require_vended_credentials = true` omits those keys from FileIO, so a table load that does not return STS or remote signing cannot read objects as the coordinator. `production_mode` refuses to start unless every REST catalog has the flag on (issue #395). Leave it off only for single-tenant and local stacks that still need the fallback.

Distributed workers are a separate path. `ScanTask` still ships the configured `[storage]` key on the Flight ticket. Per-user vending on that ticket is designed and not yet built. See [S3 Credential Vending](../design-notes/s3vending.md) and [Limitations](../reference/limitations.md#read-path-s3-access-and-require_vended_credentials).

The consequence to hold in mind: with the flag on, FileIO is fail-closed without vended credentials. With the flag off, or on the worker ticket, a user who can load table metadata can have files read with the engine key. Scope that key to the minimum the engine needs.

## Policy enforcement: off by default

SQE parses a fine-grained access-control SQL surface: column masks (`GRANT ... MASKED WITH`), row filters (`GRANT ... ROWS WHERE`), `SHOW EFFECTIVE GRANTS`, and `CHECK ACCESS`. The enforcement model is plan rewriting: filters and masks are injected into the logical plan before DataFusion's optimizer runs, so the optimizer cannot push a user predicate through a mask to probe raw values. The design follows the PostgreSQL row-level-security model, with no information leakage.

The enforcement is off by default: the default `[policy] engine = "passthrough"` returns plans unmodified. Two enforcers are shipped and wired, and you turn enforcement on by selecting one. `ranger` reads row-filter and column-mask policies from an Apache Ranger `query` service and feeds the plan rewriter; `in-memory` keeps grants in a hash map for dev and tests. The `opa` and `cedar` engines are defined in config but not yet wired (selecting them errors today). Because the Ranger backend reads the same `query` service-def that Apache Spark reads through its Kyuubi authorization plugin, one policy enforces byte-identically in SQE and in Spark. See [Fine-grained access control](../features/fine-grained-access-control.md) for the how-to, [Spark / Ranger Parity](../design-notes/sqe-spark-ranger-parity.md) for the cross-engine result, and [GRANT and REVOKE](../sql-reference/grant-revoke.md) for the engine config.

## The TVF object-store boundary

The file-format table-valued functions (`read_parquet`, `read_csv`, `read_json`, `read_delta`) read external files directly. They can address local filesystem paths and arbitrary object-store URLs. When inline credentials are omitted, they fall back to the configured `[storage]` credentials. Inline credentials are passed as SQL literals; SQE's audit logger redacts values matching `access_key`, `secret_key`, and `session_token` patterns. See [read_parquet TVF](../features/read-parquet.md).

The boundary to reason about: a SQL-capable user can direct these functions at whatever the engine's storage identity can reach. The trust boundary is the engine's storage credentials and host filesystem access, not the user's. Scope the engine's storage credentials minimally and run it with least-privilege filesystem access, the same posture you would take for any service that reads paths supplied at query time.

## What a compromised worker can reach

Workers are stateless. They hold no catalog state and no persistent credentials. The coordinator ships a worker a secured plan fragment plus the user's bearer token for the query, and the worker executes against storage. A worker reads data files for the queries it is assigned. It does not hold the long-lived OIDC client secret, and it does not have its own standing identity to the catalog.

In distributed mode the coordinator and workers share a secret that authenticates worker registration and the credential push. The engine refuses to start when the coordinator URL or worker URLs are set with an empty worker secret. See [Worker Secret](../deployment/kubernetes.md#worker-secret-distributed-mode).

A compromised worker can therefore reach the storage reachable with the credentials pushed to it for the queries it runs. The worker ticket still carries the static `[storage]` key, so a compromised worker that observes that key sees the same broad storage reach the coordinator has. Per-user, per-table vended credentials on `ScanTask` would narrow that blast radius; they are not yet built for the worker read path. FileIO on the coordinator can already refuse the shared key (`require_vended_credentials`).

## Transport

Flight SQL connections can run with TLS, and optional mTLS adds client-certificate verification. TLS is off unless both a certificate and key are configured. See [TLS in Configuration](../deployment/configuration.md#tls) and [Security & Policy](security.md#tls-encryption). Run with TLS in any deployment where the network between client and coordinator, or coordinator and workers, is not already trusted.

## Summary of boundaries

| Path | Gated per user today | Notes |
|---|---|---|
| Catalog metadata (list, load, commit) | Yes | User bearer token to the catalog |
| Write data path (INSERT / MERGE / DELETE) | Yes | Per-table vended credentials |
| Read data path (FileIO) | Optional | `require_vended_credentials` omits the static key; production_mode requires it |
| Read data path (worker `ScanTask`) | No | Static `[storage]` key; per-user vending not yet built |
| Row filters / column masks | Off by default; available | Ranger + in-memory enforcers shipped; enable with `[policy] engine`. Ranger shares the policy with Spark/Kyuubi |
| TVF file access | Engine identity, not user | Scope storage creds and filesystem access minimally |
| Worker reach | Limited by pushed creds | Today bounded by the static read key |
