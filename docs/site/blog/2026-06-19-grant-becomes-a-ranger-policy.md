---
title: "When GRANT becomes a Ranger policy"
description: "SQE got a second access-control backend: write GRANT/REVOKE as Apache Ranger policy and let Polaris enforce it. The protocol took an afternoon. The identity model took the week, because Polaris federation does not work the way the token suggests."
pubDate: "2026-06-19"
author: "Jacob Verhoeks"
tags:
  - "ranger"
  - "polaris"
  - "security"
  - "iceberg"
  - "access-control"
---

*June 19, 2026*

SQE already enforces fine-grained policy by rewriting the query plan. Row filters become filter nodes, column masks become projection expressions, denied columns vanish from the schema. The engine is the boundary. It writes the rule and it applies it.

Some shops do not want the engine to be the boundary. They run Apache Ranger already. They have a Ranger admin, a Ranger audit trail, an LDAP sync feeding Ranger roles. For them the question is not how SQE enforces policy. It is whether SQE can write into the governance system they already operate, and let that system decide.

So we built a second access-control backend. Set `access_control.backend = "ranger"` and a SQL `GRANT` becomes an Apache Ranger policy. Then SQE steps back, and Apache Polaris does the enforcing.

## Two halves, two owners

The path splits cleanly, and the two halves belong to different processes.

```
SQE  --GRANT/REVOKE-->  Ranger Admin        (policies stored here)
SQE  --query+token-->   Polaris  --check-->  Ranger    (enforcement)
```

The write half is SQE. `GRANT SELECT ON sales_wh.sales.orders TO ROLE "analyst"` is an HTTP call to the Ranger Admin REST API. `REVOKE` is the inverse. `SHOW GRANTS` reads the policies back. The backend speaks Ranger's grant protocol and has no other opinion.

The enforcement half is Polaris. SQE asks Polaris to load a table, carrying the user's Keycloak bearer token. Polaris resolves the principal, asks its embedded Ranger authorizer, and either returns the metadata or refuses. SQE is not in the loop. It never filters a row on this path. The doc-comment on the backend says it plainly: enforcement is delegated to Polaris 1.5's embedded Ranger authorizer; this backend only writes and reads Ranger policies.

The deployment assumption is Polaris 1.5 with `polaris.authorization.type=ranger`, talking to Ranger 2.8 through the new embedded authorizer API. Older Ranger does not speak it.

## The protocol was the easy part

The grant mapping took an afternoon. One wrinkle, then done.

A single SQL privilege expands to the full explicit set of Ranger access types the matching Polaris operations check. `GRANT SELECT` writes three access types (`table-data-read`, `table-properties-read`, `table-list`), not one. `GRANT INSERT` writes twenty-three, the full snapshot and schema and properties commit set.

The reason is that the Polaris embedded authorizer ignores service-def implied-grants. A service-def can declare that `table-data-write` implies the commit verbs, and the authorizer reads right past the declaration. So SQE expands each privilege to every access type the operations actually check. A read loads the table then reads files. A write loads the table then commits a snapshot, which fans out into many fine-grained operations, each its own check.

That is the whole protocol surprise. Then I went to test it end to end, and it failed.

## The identity model took the week

Polaris reads the principal from the Keycloak token: the principal name is `preferred_username`. That looks like ordinary OIDC, where the token carries the identity and the catalog trusts it. The reading is wrong in two ways, and each one cost a debugging session.

Principals must pre-exist in Polaris. Federation resolves an existing principal entity. It does not create one. A token for a user Polaris has never heard of is rejected with 401, "Failed to resolve principal." The token is a lookup key, not an identity source. Every user has to be pre-created as a Polaris principal first.

I went looking for the config flag that turns this off. There is none. I confirmed it against the Polaris source: `DefaultAuthenticator` is the only authenticator in Polaris 1.5, and it always looks the principal up in the metastore, in every auth mode including `external`. Removing per-user provisioning would take a custom `Authenticator` bean, which means forking Polaris auth. The token proves who you are. It does not enrol you.

Roles do not come from the token either. The Keycloak token carries realm roles, and Polaris drops them, because they lack the `PRINCIPAL_ROLE:` prefix Polaris expects. Polaris principal-roles do not help, because the 1.5 Ranger authorizer leaves principal-role management unmapped, so creating one is always denied. The mapping that works is Ranger role membership: Polaris sends the username to Ranger, Ranger resolves the roles from its own store, fed by usersync in production. In the quickstart we set it by hand.

```
analyst   -> alice, bob, carol
engineer  -> bob, carol
sqe_admin -> carol
```

Groups never arrive. Polaris does not forward them to Ranger without usersync, so the backend supports `USER` and `ROLE` grantees only and rejects `GROUP` with `NotImplemented` rather than writing a policy that would never match.

## The bug that has no error

Then the worst failure of the week. The `GRANT` succeeded. `SHOW GRANTS` listed the policy. The query still returned "not found."

Nothing in SQE was wrong. The cause was the `root` resource. Ranger's `polaris` service has a hierarchy, `root -> catalog -> namespace -> table`, and every authorization request Polaris sends includes a `root` value. A policy SQE writes must match that, `root` included. SQE was writing policies with no `root`, Polaris was checking with `root` set, and the two never matched. A granted user stayed denied, silently.

The fix was four characters in a TOML file.

```toml
[access_control.ranger]
service-name = "polaris"
# Polaris includes the `root` resource in every authorization request, so every
# Ranger policy SQE writes must carry a matching `root` value. "*" matches the
# realm Polaris sends. Without it, GRANTs succeed but enforcement never matches.
realm = "*"
```

I found it in the Ranger Admin audit tab, watching the access request arrive with a resource our policy did not carry. An hour of reading backend code that was correct, then one minute of reading the audit log that was not lying. A tighter realm string can replace the star once you confirm the exact value Polaris sends, but a wrong narrow value is invisible and a star is at least debuggable.

Two unrelated things called "realm" sit in the same `sqe.toml`. The Keycloak realm in the `[auth]` `token_url` is the OIDC realm. The `[access_control.ranger] realm` is the Polaris `root` resource. They have nothing to do with each other, and conflating them costs time.

## Deny by omission survives the change of enforcer

One thing did carry over from the plan-rewrite path: a denied load is not a permission error. SQE turns a Polaris denial into "table not found." A user without the grant does not learn the table exists and is off-limits. They learn nothing.

A 403 tells the user a table is there. "Not found" tells them nothing. The quickstart test treats an explicit 403 and a "not found" as the same denial, because a user should never be able to tell the two apart. It does not matter that Polaris makes the call now instead of SQE's rewriter. The rule is the same.

## One more honest edge

SQE reads Parquet from S3 with its own credentials. Once a user can load a table's metadata, they can read the data, because SQE already holds the keys. So the Polaris `table-data-read` check, the one that gates vended credentials, never fires for this deployment. The effective read gate is `LOAD_TABLE`, specifically `table-properties-read`.

The quickstart uses that. The baseline traverse set every user gets omits `table-properties-read` on purpose, so `GRANT SELECT` is the statement that actually lets a member read a table and `REVOKE` is what takes it away. In a deployment that vends per-user S3 credentials through Polaris, the gate would sit one level lower. The behavior is the same, granted users read and ungranted users do not, but the check that enforces it differs, and anyone debugging a permission needs to know which one is live.

This path is coarse. It answers one question: may this user load this table. No row filtering, no column masking, no tags. Those live on a separate Ranger service that SQE reads and enforces itself, and that is the next post, where the same policy you write once enforces byte-for-byte identically in Apache Spark.
