---
title: "MCP and OAuth with Okta: the browser is the only login"
description: "We pointed SQE's MCP endpoint at an Okta tenant and asked Claude Code to query a Glue Data Catalog as a real person. The result is the Atlassian-plugin experience for a lakehouse: no token to paste, no client secret, no service account. Okta issues the access token in the browser, SQE verifies it, and Apache Ranger decides which tables that person's groups may read. Three Okta settings decide whether it works at all, and two configuration slips look exactly like authorization failures without being one. This is what we set, what broke, and what the System Log told us."
pubDate: "2026-09-18"
author: "Jacob Verhoeks"
tags:
  - "mcp"
  - "oauth"
  - "okta"
  - "ranger"
  - "identity"
  - "claude-code"
---

The Atlassian MCP plugin gets one thing right. You add it, it opens a browser, you sign in where you always sign in, and from then on the model sees Jira the way you see Jira. No API token, no personal access token pasted into a config file, nothing to rotate.

We wanted the same for a lakehouse. The ingredients were already there: SQE's MCP endpoint is an OAuth resource server ([the server that never holds a token](./2026-08-28-the-mcp-server-that-never-holds-a-token.md)), Apache Ranger holds table grants per group, and the data sits in S3 behind an AWS Glue Data Catalog. What was missing was a real identity provider that a company already runs. We picked Okta.

The result is one directory: [`quickstart/mcp-ranger-okta/`](https://github.com/schubergphilis/sqe/tree/main/quickstart/mcp-ranger-okta/). Two containers, SQE and Ranger. No Keycloak, no Polaris, no local object store. This post is about the identity half, because that is where the day went.

## What the user does

```bash
cd quickstart/mcp-ranger-okta
./run.sh
claude
```

In Claude Code: `/mcp`, pick `sqe`, choose Authenticate. A browser tab opens on Okta. Sign in, approve, the tab closes. Ask for `whoami` and the answer is your login and your Okta groups. Ask for `SHOW SCHEMAS FROM glue` and you get the databases your groups may see.

That is the whole user-facing procedure. No step in it involves copying a string.

## What actually happens

SQE never talks to Okta during login. It publishes RFC 9728 metadata at `/.well-known/oauth-protected-resource/mcp`, naming Okta as the authorization server, and answers an unauthenticated call with `401` and a `WWW-Authenticate: Bearer` challenge. The client does the rest: reads Okta's discovery document, runs Authorization Code with PKCE, catches the redirect on a loopback port, exchanges the code for an access token, and refreshes it when it expires.

```text
Claude Code --(browser)--> Okta /authorize --code--> localhost:8400/callback
Claude Code --code + PKCE verifier--> Okta /token --access token--> Claude Code
Claude Code --Bearer--> SQE /mcp --verify JWKS, iss, aud--> Session(user, groups) --> Ranger --> Glue --> S3
```

There is no client secret anywhere in that diagram. The Okta app is a native, public client with PKCE required and client authentication set to none. A secret would identify the app, not the person, and a token minted with it would carry no groups. Every table would come back not found.

The only Okta value a user ever sees is the client id, and it is not a secret. Okta refuses anonymous dynamic client registration, so Claude Code is told which app to be: `oauth.clientId` in the project's `.mcp.json`, which `run.sh` writes from `.env`. Codex takes the same id in its config.

## Three Okta settings that decide everything

**A custom authorization server.** The org authorization server that every tenant has issues opaque access tokens. SQE cannot verify those; it needs a JWT with a JWKS behind it. A custom server (Security > API > Authorization Servers) issues JWTs, and its issuer is `https://<org>.okta.com/oauth2/<server id>`. That string is both `authorization_servers` on the MCP side and `issuer` on the token verifier.

**Audience and claims.** The server's audience must equal SQE's `mcp.resource_uri`, character for character; a mismatch is a `401` with nothing else to go on. The scopes are `sqe:mcp:read`, marked default so clients that ask for nothing still get it, and `sqe:mcp:write`. And a `groups` claim goes on the access token, value type Groups, filtered by a regex such as `^datalake_.*`. SQE reads the user from `sub` and the roles and groups from that claim. The groups are what Ranger matches.

**An access policy with the right grant.** A custom authorization server issues nothing until a policy says it may. Without one, the login succeeds and Okta then answers `access_denied` with "Policy evaluation failed". We created the policy with a single rule for the device grant, because that is what we tested first from a shell, and moved on.

Then Claude Code, which uses Authorization Code, got this after a successful sign-in:

> You are not allowed to access this app. To request access, contact an admin.

Every admin reads that as a missing app assignment. The assignment was there. The System Log had the real reason: `app.oauth2.as.authorize FAILURE no_matching_policy`. The rule listed Device Authorization and nothing else, so the authorization code request matched no rule, and Okta rendered the generic denial. Adding Authorization Code to the rule fixed it in one API call.

The lesson is not "add the grant". The lesson is that Okta's user-facing denial messages are deliberately uninformative, and the System Log is the only honest source. Filter it on the app and read `outcome.reason`.

## What Ranger gets

The bearer token becomes an SQE session with the user's login and their `datalake_*` groups. SQE downloads the Ranger policies for its Hive service and decides access itself: a Glue table is readable when a policy grants `select` to the user, one of their roles or one of their groups, and reads as not found otherwise. The quickstart bootstrap writes two policies from two Okta groups.

| Okta group | Sees |
|---|---|
| `datalake_readonly` | every table except the ones listed as sensitive (explicit deny) |
| `datalake_sensitive_ro` | every table, sensitive ones included |

Group membership is managed in Okta, where HR and IT already manage it. Ranger holds the table rules. SQE joins the two on every call and neither system has to know about the other.

## Two slips that looked like Ranger

The login worked, the metadata tools listed 135 databases and 600 tables, and every `SELECT` failed. The first instinct was a missing grant. It was configuration, twice.

The quickstart's `sqe.toml` had a placeholder Iceberg catalog URL, put there to satisfy a validator. SQE registered the placeholder as the default catalog and tried to list its namespaces while building every SQL session. `SHOW` statements do not build that session, so the metadata tools kept working and the failure read as authorization. The fix is an empty `[catalog]` table, which the config test now asserts.

The second was quieter. The `search_tables` tool runs one `SHOW TABLES` per database, and each one counted against the per-user rate limit of 120 a minute. With 135 databases the tail was skipped, logged at debug level, and the model was handed a partial listing that looked complete. The quickstart raises the limit above the database count; the tool should count once per call and say what it skipped, and that is now an issue.

Neither slip was Okta's fault and neither was Ranger's. Both were only visible because the engine logs every policy decision with the user, the table and the counts of masks, filters and restricted columns. Read those lines before you touch a policy.

## Credentials, region, Fargate

The one credential in the system is the engine's own AWS identity for Glue and S3, and it lives nowhere in the repository. On a laptop, `aws sso login` and `eval "$(aws configure export-credentials --format env)"` put session credentials in the shell, and Compose passes them into the container. On Fargate the variables stay unset and the task role is picked up by the SDK chain. The same compose command and the same `SQE_*` variables describe both; a task definition is a transcription.

Session credentials expire, and expired credentials do not fail loudly. Reads return empty or hang. The README says so twice.

## What we did not build

No token minting script. The first version of this quickstart had one, so the smoke test could call `whoami` from a shell. It was the last place a human touched a token, and it went.

No broker. SQE could register clients dynamically, front Okta with its own `/authorize` and issue its own tokens, which would remove the client id from the client configuration. For one Okta app and clients that accept a pre-registered id, the extra moving part buys nothing yet. If a chat frontend or a fleet of clients needs zero-configuration onboarding, that is the next step, and it is a design note before it is code.

The reference is [Model Context Protocol](../book/src/deployment/mcp.md) in the book, now with an Okta section, and the runnable stack with its Terraform is [`quickstart/mcp-ranger-okta/`](https://github.com/schubergphilis/sqe/tree/main/quickstart/mcp-ranger-okta/).
