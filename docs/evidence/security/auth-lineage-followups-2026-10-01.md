# Authentication and lineage follow-ups — 2026-10-01

Base: main `19cdbfb37d9c8b6972a8f0b0ea34bbf5406a0579`, after !1230 merged.

## #760 — unsupported mTLS identity provider

The current Flight/Trino/MCP credential adapters never populate a verified `client_cert_cn`. The provider therefore fails closed but cannot perform the configured authentication. Configuration validation and the auth-chain factory now reject any explicit `mtls` provider, including mixed chains, with a startup error explaining the missing transport identity extraction. Factory validation runs before constructing other providers.

This concerns the SQL identity provider, not transport certificate verification. Configuring a client CA still verifies client certificates; it does not provide a mapped SQL identity. The standalone provider remains available for direct tests or future verified-transport integrations. Request headers must never substitute for certificate verification.

## #761 — unnecessary session bearers in lineage

Removed `UserCtx.bearer` and its four coordinator producers. The observer boundary now carries the username without a session credential. Removed the unused `AuthMode::UserToken` sink variant. Enabled OpenLineage rejects `user_token` and unknown modes rather than silently substituting a static API key. Supported collector modes remain `none` and `bearer`; bearer requires a nonblank dedicated collector API key. Previous `user_token` configurations must migrate explicitly to one of those modes. Disabled OpenLineage remains dormant.

## #795 — collector credential Debug disclosure

Both `AuthMode` and `HttpConfig` previously derived Debug around raw credential Strings. Formatting either could reveal the collector bearer, despite the top-level configuration redaction. This is a conditional logging disclosure, not proof that a default production log path emitted it. The sink now holds the credential in `SecretString`; it exposes the credential only when creating the Authorization header. A canary test covers both Debug boundaries, and the HTTP integration test verifies authenticated delivery.

## Validation

- Auth unit suite: 285 passed.
- Core unit suite: 465 passed, including mixed mTLS-provider refusal, invalid lineage modes and blank collector credentials.
- Lineage unit suite: 3 passed.
- Lineage integration suite: 54 passed, including static collector authentication and event/spool behavior.
- Clippy across core, auth, lineage, coordinator and server (including tests), with warnings denied: passed.
- `git diff --check`: passed.

Local HTTP mock tests require loopback access. Builds used a temporary SDK-aware linker wrapper for the workstation; repository build configuration was not changed. Existing dirty files in the primary workspace were preserved by using an isolated worktree.
