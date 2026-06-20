---
slug: polaris-keycloak-client-id
title: "Polaris + Keycloak (client credentials)"
description: "Run SQE against Apache Polaris with Keycloak as the identity provider. SQE holds a confidential client and exchanges each user's username + password for a bearer token via the OIDC password grant, then passes that token through to Polaris."
---

# Polaris + Keycloak (client credentials)

Run SQE against an Apache Polaris catalog where Keycloak issues the identities.
A user connects to SQE with a username and password; SQE exchanges those for the
user's bearer token via its own confidential client (the OIDC Resource Owner
Password Credentials grant), then passes the token straight through to Polaris.
Polaris decides what the user can see — no service account, no shared credential.

## How it works

- **Keycloak** acts as the identity provider. An `iceberg` realm holds a
  confidential client (`sqe-client`) and three test users with different role
  levels.
- **SQE** uses the `oidc_password` auth provider: on login it posts the user's
  credentials plus its own client secret to Keycloak's token endpoint and
  receives the user's bearer token.
- **Polaris** is federated to Keycloak — it validates the token SQE forwards
  (issuer, signature, audience) and maps the token's `preferred_username` to a
  Polaris principal with its own RBAC roles.
- **RustFS** provides S3-compatible warehouse storage. A one-shot `bucket-init`
  container creates the warehouse bucket on startup.
- Every query runs as the authenticated user. The user never contacts Keycloak
  directly.

## What it demonstrates

- SQE minting a user's token from username + password via OIDC password grant.
- Token passthrough to Polaris: Polaris enforces catalog-level RBAC per principal.
- Multi-user isolation: `adminuser` (write access) and `testuser` (read-only)
  running the same queries with different results.
- Full create/write/read round-trip: `CREATE SCHEMA` → `CREATE TABLE` → `INSERT`
  → `SELECT … GROUP BY`.
- Role-level access control validated in two layers: the demo path and the
  integration test suite.

**Status:** validated (2026-06-06).

## Run it

Full config, `docker compose`, queries, and captured output are in the repo:

**→ [quickstart/polaris-keycloak-client-id/](https://github.com/schubergphilis/sqe/tree/main/quickstart/polaris-keycloak-client-id/)**

```bash
cd quickstart/polaris-keycloak-client-id
cp .env.example .env
./run.sh
```
