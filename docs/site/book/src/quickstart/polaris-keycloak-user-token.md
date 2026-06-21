---
slug: polaris-keycloak-user-token
title: "Polaris + Keycloak (user token)"
description: "Run SQE against Apache Polaris where clients bring a pre-minted Keycloak bearer token. SQE validates the token against the realm JWKS and passes it through to Polaris. No client secret, no password grant."
---

# Polaris + Keycloak (user token)

The bring-your-own-token path. An upstream application or identity provider has
already authenticated the user and holds their bearer token. The client sends
that token to SQE; SQE validates it (signature, issuer, expiry) against the
realm's public JWKS endpoint and passes it through to Polaris. SQE never sees a
password and holds no client secret.

## How it works

- The Docker stack is identical to the [client credentials quickstart](./polaris-keycloak-client-id.md):
  Keycloak, Polaris, RustFS, and SQE on one network.
- The only difference is SQE's auth provider: `bearer_token` instead of
  `oidc_password`. SQE fetches the realm's signing keys once from the JWKS
  endpoint, then verifies every incoming token locally — no call to Keycloak's
  token endpoint, no client secret.
- `run.sh` mints a token from Keycloak's public client (standing in for an
  upstream app) and queries SQE with `--token`.
- Polaris enforces RBAC as before: the token's `preferred_username` maps to a
  principal with its own role grants.

## What it demonstrates

- SQE as a pure token validator: it verifies the JWT signature and claims, then
  passes the token to the catalog.
- A valid token with write access completes the full create/write/read round-trip.
- A read-only token is allowed to read but denied a write by Polaris RBAC (403).
- A malformed token is rejected by SQE's JWKS validation before reaching the
  catalog.

**Status:** validated (2026-06-06).

## Run it

Full config, `docker compose`, queries, and captured output are in the repo:

**→ [quickstart/polaris-keycloak-user-token/](https://github.com/schubergphilis/sqe/tree/main/quickstart/polaris-keycloak-user-token/)**

```bash
cd quickstart/polaris-keycloak-user-token
cp .env.example .env
./run.sh
```
