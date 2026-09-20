---
slug: mcp-ranger-okta
title: "MCP with Ranger and Okta on external data"
description: "SQE and Apache Ranger only. Okta users sign in from Claude Code or Codex through the browser, Ranger policies per Okta group decide which Glue tables they see, and every setting is an environment variable ready for Fargate."
---

# MCP with Ranger and Okta on external data

This quickstart is the MCP endpoint against a real identity provider and real
data. Okta issues the access tokens, SQE verifies them, Apache Ranger holds the
grants, and the tables are whatever the AWS Glue Data Catalog lists, read from
S3 with the engine's own credentials. There is no Keycloak, no Polaris and no
object store in the compose file.

```text
Claude Code / Codex --(browser login)--> Okta --access token--> SQE /mcp
SQE: token -> Session(user, groups) -> Ranger access policies -> Glue catalog -> S3
```

Nobody handles a token. The MCP client gets a `401` from SQE, follows the
protected-resource metadata to Okta, opens the browser, and the user signs in
with whatever the org policy demands. From then on the model queries the
lakehouse as that user. Two Okta groups drive the two Ranger policies the
bootstrap writes: `datalake_readonly` sees every table except the ones listed
as sensitive, `datalake_sensitive_ro` sees those too.

```bash
cd quickstart/mcp-ranger-okta
cp .env.example .env                      # Okta issuer + client id, Glue databases, sensitive tables
eval "$(aws configure export-credentials --format env)"
./run.sh                                  # Ranger + SQE, protocol checks, writes .mcp.json
claude                                    # /mcp -> sqe -> Authenticate
```

The Okta objects (custom authorization server, scopes, `groups` claim, access
policy, native PKCE app, groups) ship as Terraform in `okta/` and as a
click-by-click list in the README. The same README explains how the login
works, how to hand the container AWS credentials on a laptop and on Fargate,
and the three gotchas that look like Ranger problems but are not.

For the complete configuration, Terraform, compose file and troubleshooting,
see [`quickstart/mcp-ranger-okta/`](https://github.com/schubergphilis/sqe/tree/main/quickstart/mcp-ranger-okta/).
