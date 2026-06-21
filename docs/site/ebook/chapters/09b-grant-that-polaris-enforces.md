# A GRANT That Polaris Enforces {#sec:ranger-catalog}

> The engine writes the policy. It does not enforce it.
> A different process decides who gets in.

Chapter 9 put the security boundary inside the query plan. Row filters become `Filter` nodes, column masks become projection expressions, denied columns vanish from the schema. SQE rewrites the plan, and SQE enforces what it wrote. The engine is the policy boundary.

There is a second model where the engine is not the boundary at all.

In this one, SQE translates a `GRANT` into a catalog policy, sends it to Apache Ranger, and then steps back. When a query arrives, Apache Polaris asks Ranger whether this user may load this table. Polaris decides. SQE never filters a single row. It writes the rule and surfaces the denial, nothing more.

We built both. This chapter is the catalog-based one, and the reason it exists is that some shops already run Ranger. They have a Ranger admin, a Ranger audit trail, an LDAP sync feeding Ranger roles. For them the question is not "how does SQE enforce policy" but "can SQE write into the governance system we already operate." The answer is a backend that turns SQL `GRANT` / `REVOKE` into Ranger policy and lets Polaris do the enforcing.

The mechanism is simple. The identity model behind it is not. Most of this chapter is the identity model, because that is where the days went.


## Two halves, two owners

The catalog path splits into a write half and an enforcement half, and they belong to different processes.

```
SQE  --GRANT/REVOKE-->  Ranger Admin        (policies stored here)
SQE  --query+token-->   Polaris  --check-->  Ranger    (enforcement)
```

The write half is SQE. A `GRANT SELECT ON sales_wh.sales.orders TO ROLE "analyst"` becomes an HTTP call to the Ranger Admin REST API. `REVOKE` becomes the inverse call. `SHOW GRANTS` reads the policies back. SQE speaks Ranger's grant protocol and otherwise has no opinion about the result.

The enforcement half is Polaris. SQE asks Polaris to load a table, carrying the user's Keycloak bearer token. Polaris resolves the principal, asks its embedded Ranger authorizer for a decision, and either returns the table metadata or refuses. The check runs entirely inside Polaris. SQE is not in the loop.

The selector is one line of config. `access_control.backend = "ranger"` in `sqe.toml` routes `GRANT` / `REVOKE` / `SHOW GRANTS` through the Ranger backend (`RangerGrantBackend` in `crates/sqe-policy/src/grants/ranger.rs`). The doc-comment on that file states the contract without ceremony: enforcement is delegated to Polaris 1.5's embedded Ranger authorizer; this backend only writes and reads Ranger policies.

The deployment assumption is Polaris 1.5 running with `polaris.authorization.type=ranger`. That is the embedded authorizer, still marked Beta in the 1.5 line, talking to Apache Ranger 2.8 through the new authorizer API. Older Ranger does not speak it. The version pins are real and we hit every one of them.


## The same denial as chapter 9

The catalog path keeps the deny-by-omission rule from chapter 9, and it keeps it for the same reason.

A denied load does not surface as a permission error. SQE turns a Polaris load denial into "table not found." A user without the grant does not learn that the table exists and is forbidden. They learn nothing. The table is invisible, exactly as a masked column was invisible in chapter 9 and a denied column simply was not in the schema.

::: {.sovereignty}
**Sovereignty principle:** Deny by omission survives the change of enforcer. It does not matter that Polaris makes the decision now instead of SQE's plan rewriter. A 403 tells the user a table is there and off-limits. "Not found" tells them nothing. The quickstart test treats both an explicit 403 and a "not found" as the same denial, precisely because a user should never be able to tell the two apart.
:::


## The identity model is the whole problem

The grant protocol took an afternoon. The identity model took the rest of the week, because Polaris federation does not work the way the token suggests it should.

Polaris reads the principal from the Keycloak token. The principal name is `preferred_username`. So far this looks like ordinary OIDC: the token carries the identity, the catalog trusts the token. That reading is wrong in two specific ways, and each one cost a debugging session.

**Principals must pre-exist in Polaris.** Federation resolves an existing principal entity. It does not create one. A token for a user Polaris has never heard of is rejected with 401, "Failed to resolve principal." The token is a lookup key, not an identity source. Every user has to be pre-created as a Polaris principal before a single query will run. The quickstart bootstrap creates `alice`, `bob`, `carol`, `dave` for exactly this reason.

We went looking for a way around this. There is none in config.

::: {.deadend}
**Dead end: eliminating per-user provisioning.** We assumed `external` authentication mode would let Polaris trust the token's identity without a pre-created principal. It does not. We confirmed it against the Polaris source: `DefaultAuthenticator` is the only authenticator in Polaris 1.5, and it always looks the principal up in the metastore, in every mode. Removing per-user provisioning is not a config flag. It would take a custom `Authenticator` bean, which means forking Polaris auth. We wrote it up in `docs/polaris-principal-provisioning.md` and moved on. The token proves who you are. It does not enrol you.
:::

**Roles do not come from the token.** This was the second surprise. The Keycloak token carries realm roles, and the obvious assumption is that Polaris reads them. It does not. Polaris drops them during authentication because they lack the `PRINCIPAL_ROLE:` prefix Polaris expects. We tried Polaris principal-roles next, and that path is also closed: the 1.5 Ranger authorizer leaves principal-role management operations unmapped, so creating or assigning them is always denied.

The mapping that works is Ranger role membership. Polaris sends the username to Ranger, and Ranger resolves that user's roles from its own role store. In production that store is fed by Ranger usersync from LDAP, AD, or SCIM. In the quickstart we set it explicitly:

```
analyst   -> alice, bob, carol
engineer  -> bob, carol
sqe_admin -> carol
```

This is the sharp divergence from SQE's own fine-grained path, and it is worth holding onto for the next chapter. On the catalog path, roles live in Ranger and Polaris resolves them. SQE's session roles, which it reads from the token's `realm_access.roles`, do not enter into the catalog decision at all.

**Groups never arrive.** Polaris does not forward groups to Ranger unless Ranger usersync runs. So the backend supports `USER` and `ROLE` grantees only. `GRANT ... TO GROUP ...` is rejected with `NotImplemented` rather than written as a policy that would never match.


## The realm that has to be a star

The setting that breaks silently is `realm`.

Ranger's `polaris` service-def has a resource hierarchy: `root -> catalog -> namespace -> table`. The `root` level carries a realm or context value, and every authorization request Polaris sends includes it. A policy SQE writes must match the resource Polaris checks against, `root` included. If SQE writes a policy at `{catalog:*}` with no `root`, Polaris sends a request with `root` set, the two never match, and a freshly granted user is still denied.

The value that matches for this stack is `"*"`. SQE controls it through `[access_control.ranger] realm` in `sqe.toml`. Every policy SQE writes then carries `root = *`, which matches whatever realm Polaris sends.

```toml
[access_control]
backend = "ranger"
url = "http://ranger-admin:6080"

[access_control.ranger]
service-name = "polaris"
admin-user = "admin"
admin-password = "rangerR0cks!"
# Polaris includes the `root` resource in every authorization request, so every
# Ranger policy SQE writes must carry a matching `root` value. "*" matches the
# realm Polaris sends (verified against this stack). Without it, GRANTs succeed
# but enforcement silently never matches.
realm = "*"
```

The failure mode is the worst kind: the `GRANT` succeeds, `SHOW GRANTS` lists the policy, and the query still fails. Nothing in SQE is wrong. The policy simply does not match the request. A tighter realm string can replace the star once you confirm the exact value Polaris sends, which you find in the Ranger Admin audit tab or in `docker compose logs polaris`. We left it at `"*"` in the quickstart because a wrong narrow value is invisible and a star is at least debuggable.

::: {.fieldreport}
**Field report:** The first end-to-end test failed with the grant present and correct. `SHOW GRANTS` returned the policy. The query returned "not found." We spent an hour reading the backend code before reading the Ranger audit log, which showed the access request arriving with a `root` resource our policy did not carry. The fix was four characters in a TOML file. The diagnosis was the realm-resource hierarchy nobody had documented for the embedded authorizer.
:::

Two unrelated things called "realm" sit in the same `sqe.toml`, and confusing them costs time. The Keycloak realm (`iceberg-ranger`, inside the `[auth]` `token_url`) is the OIDC realm. The `[access_control.ranger] realm = "*"` is the Polaris `root` resource value. They have nothing to do with each other.


## One privilege, many access types

The grant mapping has a wrinkle that looks like over-engineering until you read the Polaris source.

SQL privileges map to Ranger access types in `map_sql_to_ranger_access`. The wrinkle is that a single SQL privilege expands to the full explicit set of access types the corresponding Polaris operations check.

| SQL privilege | Ranger access types | Resource level |
|---|---|---|
| `SELECT` | `table-data-read`, `table-properties-read`, `table-list` | table |
| `INSERT` | `table-data-write` plus the full snapshot/schema/properties commit set (22 types) | table |
| `DROP` | `table-drop` | table |
| `CREATE TABLE` | `table-create` | namespace |
| `USAGE` | `namespace-list`, `namespace-properties-read` | namespace |
| `CREATE SCHEMA` / `CREATE` | `namespace-create` | catalog |
| `ALL` / `ALL PRIVILEGES` | `catalog-content-manage` | catalog |

A `SELECT` writes three access types, not one. An `INSERT` writes twenty-three. The reason is that the Polaris embedded authorizer does not honor service-def implied-grants. A service-def can declare that `table-data-write` implies the commit verbs, and the authorizer ignores the declaration. So SQE expands each privilege to every access type the operations will actually check. A read loads the table then reads files, which is three checks. A write loads the table then commits a snapshot, which fans out into snapshot, schema, sort-order, partition-spec, and properties commit operations, every one a separate check.

Unknown privileges pass through lowercased. An operator who knows the native Ranger access types can name them directly in a `GRANT`, and SQE will write them verbatim.

Names that flow from SQL into the JSON resource map are validated. `validate_identifier` rejects empty values and anything containing `/ ? # %` backslash, whitespace, or control characters. A grant with no catalog is also rejected, because the backend requires the full `catalog.namespace.table` form.


## The read gate is LOAD_TABLE, not credentials

There is a subtlety in this deployment that changes which grant is the meaningful one.

SQE reads Parquet from S3 with its own configured credentials. Once a user can load a table's metadata, they can read its data, because SQE already holds the S3 keys. The Polaris `table-data-read` check, the one that gates vended credentials, never fires for this deployment. SQE does not ask Polaris for credentials, so Polaris never gets to withhold them.

The effective read gate is `LOAD_TABLE`, specifically `table-properties-read`. The quickstart uses that fact deliberately. The baseline traverse set every authenticated user gets (`catalog-list`, `catalog-properties-read`, `namespace-list`, `namespace-properties-read`, `table-list`) omits `table-properties-read` on purpose. So `GRANT SELECT` is the statement that actually lets a member load and read a table, and `REVOKE` is what takes it away. The grant is the visible gate because the deployment is built to make it the gate.

This is honest about a limitation. In a deployment that vends per-user S3 credentials through Polaris, the `table-data-read` check would matter and the gate would sit one level lower. SQE's direct-S3 model moves the gate up to table load. The behavior is the same either way (granted users read, ungranted users do not), but the check that enforces it differs, and an operator debugging a permission needs to know which one is live.


## What the catalog path will not do

The catalog path answers one question. May this user load this table? It does not answer the questions chapter 9 was about.

- No row filtering. It cannot restrict a query to a subset of rows.
- No column masking. It cannot redact or null a column's values.
- No tag-based policy. The `polaris` service-def declares no `rowFilterDef` and no `dataMaskDef`.

Those constructs do not exist on the `polaris` service, and the Polaris authorizer reads only a boolean allow or deny. Fine-grained control lives on a separate path, enforced by SQE's own plan rewriter against a separate Ranger service. The two paths are independent, both apply, and a query must pass both. Revoking the coarse `SELECT` still denies the query at Polaris before any fine-grained check runs.

That separate path is the next chapter. It is where Ranger becomes a third policy backend alongside the OPA and Cedar enforcers from chapter 9, and where the same policy you write once enforces identically in SQE and in Apache Spark.

::: {.ailog}
**AI Logbook:** The grant-protocol mapping and the identifier validation came together quickly with the AI working from the Ranger REST shapes. The identity model did not. Every assumption the AI and I shared about token-carried roles and federated principals turned out to be wrong against Polaris 1.5, and the only way through was reading the Polaris source directly: `DefaultAuthenticator`, the dropped realm roles, the unmapped principal-role operations. The AI was useful for the mechanical translation and useless as an oracle for an undocumented Beta authorizer. We tested every claim in this chapter against a live Polaris-Ranger-Keycloak stack before writing it down.
:::
