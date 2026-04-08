## Why

SQE is being open-sourced under Apache 2.0. The codebase was built for a specific internal deployment (Keycloak + Polaris + MinIO). Before public release it needs:
1. Vendor-neutral language — "Keycloak" and "MinIO" must become generic OAuth2/OIDC and S3-compatible storage
2. Production-grade security controls absent from an internal-only MVP: rate limiting, TLS enforcement, session timeouts, query timeouts, and audit logging
3. Observable and operable: health endpoints, graceful shutdown, startup config validation

MinIO was removed from the supported stack — it went commercially closed-source (BSL licence). S3-compatible alternatives (Ceph, SeaweedFS, Cloudflare R2, Garage) remain supported via the generic S3 driver.

## What Changes

- **Rename**: all `keycloak`/`Keycloak` identifiers → `oidc` / `OAuth2` in code, config keys, docs, and the auth module name
- **Remove**: MinIO-specific documentation, config examples, and quickstart references; replace with generic S3-compatible language
- **Rate limiting**: per-connection and per-user query rate limits (configurable, defaults off)
- **TLS enforcement**: require TLS on Flight SQL listener; config flag for dev-mode plaintext
- **Query timeouts**: hard wall-clock limit per query, configurable per-role
- **Session timeouts**: idle and absolute session lifetime limits
- **Query cancellation**: client-side cancel propagated through coordinator → workers
- **Audit log**: structured JSON log line per query: user, query text hash, tables accessed, rows returned, duration, outcome
- **Startup validation**: fail-fast on invalid or missing required config (no silent defaults for auth endpoints)
- **Health endpoints**: `/healthz/live` and `/healthz/ready` on a separate admin HTTP port
- **Error sanitisation**: internal error details (stack traces, catalog URLs) stripped from client-visible errors in production mode

## Capabilities

### New Capabilities
- `rate-limiting`: per-user and global query rate limits via token bucket
- `tls`: TLS on Flight SQL listener; mTLS optional
- `query-timeout`: wall-clock timeout per query with configurable per-role override
- `session-lifecycle`: idle timeout + absolute max session age
- `query-cancellation`: client-initiated cancel propagated to workers
- `audit-log`: structured per-query audit log (JSON, to stdout or file)
- `health-endpoints`: `/healthz/live` + `/healthz/ready` on admin HTTP port
- `startup-validation`: required config fields validated at boot

### Modified Capabilities
- `auth-passthrough` → renamed to `auth-oidc`; all Keycloak-specific naming removed
- Docs and config examples: MinIO removed, generic S3-compatible wording throughout

## Impact

- Config key renames: `keycloak.*` → `auth.oidc.*` (breaking change, migration note in changelog)
- Docker quickstart: MinIO service removed from `docker-compose.yml`; replaced with comment pointing to S3-compatible alternatives
- No behaviour changes to the query path for existing users with a compliant OIDC provider and S3-compatible storage

## Rollback

Config renames are the only breaking change. A one-version deprecation period (old keys accepted with warning) allows smooth migration.
