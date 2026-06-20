# One Policy, Two Engines {#sec:ranger-fine-grained}

> The mask is written once, in Ranger.
> SQE and Spark read the same rule and return the same byte.

Chapter 9 built the plan-rewrite enforcer and wired in two backends: OPA with Rego, and Cedar. The previous chapter added a third way to spend a `GRANT`, where Polaris enforces and SQE only writes. This chapter closes the loop with a third enforcement backend that does what chapter 9 described, against Apache Ranger, and then proves something the OPA and Cedar backends could never prove: that the exact same policy enforces identically in a completely different engine.

The setup is the one from chapter 9. SQE downloads policies, rewrites the `LogicalPlan` before the optimizer runs, injects row filters as `Filter` nodes, replaces masked columns with masking expressions, and drops restricted columns from the schema. The mechanics are the same. What changes is the source of the policy and who else can read it.

The source is the `hive` Ranger service. The same `hive` service Apache Spark reads through its Kyuubi authorization plugin. That sharing is the whole point of this chapter, and it is the reason we picked the `hive` service-def over inventing a new one.


## Two Ranger services, two config blocks

The catalog path and the fine-grained path both talk to Ranger, and operators confuse them constantly, so it is worth pinning the difference down before anything else.

| | Catalog path | Fine-grained path |
|---|---|---|
| Config block | `[access_control] backend = "ranger"` | `[policy] engine = "ranger"` |
| Ranger service | `polaris` | `hive` (plus a linked `tag` service) |
| Granularity | catalog / namespace / table allow-deny | row filters, column masks, restricted columns, tag masks |
| Authored via | SQL `GRANT` / `REVOKE` | Ranger UI or REST, plus `SET TBLPROPERTIES` for tags |
| Enforced by | Polaris embedded authorizer | SQE `PolicyPlanRewriter` |
| Does SQE filter? | No | Yes, it rewrites the plan |
| Shared with Spark? | No, the `polaris` service is Polaris-specific | Yes, the `hive` service Kyuubi reads |
| Identity matching | Ranger role membership, resolved by Polaris | token roles, matched directly |

Both gates apply to every query. The Polaris catalog gate runs first. Then SQE's rewrite runs on the loaded plan. Revoke the coarse `SELECT` and the query dies at Polaris before any mask is ever computed.

The activation is `[policy] engine = "ranger"`, which swaps the PassthroughEnforcer from chapter 9 for `RangerStore` (`crates/sqe-policy/src/ranger_store.rs`). The rewriter is the same `PolicyPlanRewriter` the OPA and Cedar backends use.

```toml
[policy]
engine = "ranger"

[policy.ranger]
url = "http://ranger-admin:6080"
service-name = "hive"
admin-user = "admin"
# Set via SQE_POLICY__RANGER__ADMIN_PASSWORD rather than in the file.
admin-password = ""
timeout-secs = 5
cache-ttl-secs = 30
```

Why this lives in SQE and not Polaris: the `polaris` service-def from the previous chapter declares no `rowFilterDef` and no `dataMaskDef`, and the Polaris authorizer reads only a boolean. It cannot enforce a row filter or a column mask even though the Ranger engine can compute one. Fine-grained enforcement has to happen in the query engine, which is precisely the chapter 9 thesis arriving from a different direction.


## Download, resolve, rewrite

The flow is three steps.

```
Ranger Admin  --download bundle-->  RangerStore (resolve)  -->  ResolvedPolicy
ResolvedPolicy  -->  PolicyPlanRewriter  -->  rewritten LogicalPlan  -->  optimizer
```

The download hits one endpoint, `GET /service/plugins/policies/download/hive`, with HTTP basic auth. The response is the full `ServicePolicies` bundle: the resource policies, each tagged with a `policyType` (0 = access, 1 = DATAMASK, 2 = ROWFILTER), plus an optional nested `tagPolicies` block. This is the exact bundle the JVM Ranger plugin downloads, which is the mechanical reason the policy set is shared with Spark. The flat public-v2 `/api/policy` endpoint returns resource-only data and is not enough, so SQE uses the plugin download path.

Resolution returns a small struct:

```rust
ResolvedPolicy {
    row_filters: Vec<Expr>,
    column_masks: HashMap<String, MaskType>,
    restricted_columns: Vec<String>,
}
```

It is keyed on the user plus the user's token roles. A policy item applies if its `users` list contains the username, or its `roles` list intersects the user's token roles (`item_matches`). Here is the divergence from the catalog path that the previous chapter flagged. SQE's session roles come from the token's `realm_access.roles`, and SQE matches those roles directly. It does not depend on Ranger role membership the way Polaris does. Same Ranger server, two different identity resolutions, because one decision happens inside Polaris and the other happens inside SQE.

Resource matching compares the policy's `database` and `table` values against the target. Exact match and bare `*` are supported; Ranger glob patterns like `orders*` are not matched in this version. The namespace flattens to a Hive `database` name by taking the last dotted component, so schema `sales_wh.sales` becomes database `sales`. That truncation is a sharp edge worth knowing, and it matches the write path's keying so the two stay consistent.

The rewrite is the chapter 9 mechanism. Row filters inject as `Filter` nodes above the scan, and user predicates push through them. Column masks replace the column reference in the projection with the masking expression, and user predicates cannot push through the expression boundary, so a `WHERE ssn = '...'` probe evaluates against the masked value, never the raw one. Restricted columns leave the projection entirely. The masking expression keeps the column's Arrow type, so a nullify on a BIGINT emits a typed Int64 NULL rather than a Utf8 NULL, and no downstream join or filter can coerce both sides to strings and leak a masked row.


## Fail closed, everywhere

Every uncertain path denies rather than leaks. The rule is not a feature flag; it is the default at every branch.

- A table that cannot be mapped to a policy key gets a `lit(false)` row filter. Deny all rows.
- A resolution error (transport, parse, breaker open) gets a `lit(false)` row filter for that table.
- An unparseable row-filter expression becomes `lit(false)` rather than being silently dropped.
- An unsupported mask type restricts the column rather than returning it raw.
- An unmappable tag, with no resource mask on the column, restricts the column.

The download is guarded by a circuit breaker. Repeated Ranger failures trip it, and an open breaker returns an error, which the rewriter treats as deny-all. Resolved policies cache in a moka TTL cache keyed by username, namespace, table, and the sorted role list, and the cache invalidates when table properties change.

::: {.sovereignty}
**Sovereignty principle:** Fail closed is not pessimism. It is the only safe default when the policy source can be unreachable. A governance system that returns raw data when it cannot reach its policy server has no governance at all; it has a network dependency masquerading as one. SQE denies on doubt, every time, and pays for it with a `lit(false)` instead of a leak.
:::


## The mask vocabulary

SQE implements the complete Ranger Hive built-in mask set. `map_mask` translates each Ranger `dataMaskType` string into an SQE `MaskType`, and the character-class transformer is a DataFusion UDF.

| Ranger `dataMaskType` | Effect |
|---|---|
| `MASK_NULL` | Replace the value with a typed NULL. |
| `MASK_HASH` | HMAC-SHA256 hex digest, or plain SHA-256 with no mask key. |
| `MASK` | Full redact: uppercase to `X`, lowercase to `x`, digit to `n`; punctuation kept. |
| `MASK_SHOW_LAST_4` | Show the last 4 characters; mask the rest with `x`. |
| `MASK_SHOW_FIRST_4` | Show the first 4 characters; mask the rest with `x`. |
| `MASK_DATE_SHOW_YEAR` | Truncate a date to its year; month and day zeroed. |
| `CUSTOM` | An arbitrary SQL expression with `{col}` as the column placeholder. |
| `MASK_NONE` | An explicit exemption. Placed first in Ranger, it carves an exception. |

The character conventions match the Hive serviceDef transformer templates exactly, down to Unicode-scalar counting. For `111-11-1111` with `MASK_SHOW_LAST_4` the output is `xxx-xx-1111`. Hold onto that string; it is about to appear in two engines at once.


## Masking that depends on the role

Snowflake has conditional masking policies: the value a user sees depends on the user's role, evaluated at query time. SQE gets the same behavior, and it gets it in a way that survives distribution.

Five session-context UDFs carry the session identity (`crates/sqe-policy/src/session_udf.rs`):

| Function | Returns |
|---|---|
| `current_user()` | the session username |
| `is_role_in_session(role)` | true if `role` is in the session's token roles |
| `current_available_roles()` | the role set as a sorted JSON array string |
| `current_database()` | the session database, or NULL |
| `current_schema()` | the session schema, or NULL |

Each one bakes the session identity in at construction and is marked `Volatility::Immutable`. DataFusion const-folds the call to a literal during logical optimization on the coordinator. The folded literal is what ships to workers; the function call never crosses the wire. That const-fold is what makes role-conditional masking distribution-safe. A worker never has to know who the user is, because the coordinator already resolved the identity into a constant before the fragment left.

These functions work in user SQL and inside Ranger-authored policy expressions, both row filters and `CUSTOM` mask templates. So a row filter like `is_role_in_session('engineer') OR region = current_user()` resolves per session, folds to a constant, and enforces the same on a single node or across a cluster.

One honest limitation. `RangerStore` builds its resolution identity without the session warehouse, so inside a Ranger policy expression `current_database()` and `current_schema()` fold to NULL while the other three resolve correctly. In ordinary user SQL all five resolve fully. That is the documented MVP behavior, not a bug we are hiding.


## Tags, and where they live

The last piece is tag-based masking: a rule like "any column tagged `PII` is masked show-last-4," applied without naming the column. It splits into two halves that we chose to store in two different places, and the split is deliberate.

The mask-per-tag RULE lives in Ranger as a `tag`-service policy, returned in the download bundle's `tagPolicies` block. Shared with Spark, like every other Ranger policy.

The tag-to-column ASSOCIATION lives in the Iceberg table property `sqe.column-tags`, a JSON object mapping column to tags:

```sql
ALTER TABLE sales_wh.sales.orders
  SET TBLPROPERTIES ('sqe.column-tags' = '{"ssn":["PII"],"amount":["FINANCIAL"]}');
```

The write goes through a Polaris `updateProperties` commit, after which SQE invalidates the table and the policy cache so the new tags show up on the next query without waiting for the TTL.

Storing associations as a table property, rather than in the Ranger or Atlas tag store, wins on four counts. The associations cover federated catalogs Polaris cannot gate. They need no Atlas or tagsync deployment. They travel with the data through clone, replicate, and rename. And SQE already reads the table metadata on every scan, so reading one more property is free. The full rationale is in `docs/ranger-tag-storage-decision.md`.

The merge has a locked precedence contract, because tags and resource policies can both touch the same column. Restricted columns always win; a tag cannot un-restrict a column. A resource mask wins over a tag mask. Tag row filters are ANDed with resource row filters, most restrictive. Within a column, the first matching tag in stored order wins, deterministically. An unmappable tag whose column has no resource mask restricts the column, fail-closed.


## The byte that proves it

Here is the result the OPA and Cedar backends could never give us.

Take the same Ranger `hive`-service policy on the same Polaris catalog. Run the same query as the same user against SQE and against standard Apache Spark. The masked output is byte-exact identical.

```sql
SELECT id, ssn FROM sales_wh.sales.orders
```

Run as `bob`, both engines return the same masked SSNs:

```
xxx-xx-1111
xxx-xx-2222
xxx-xx-3333
```

Three of three rows, byte-exact, across two engines built by two different communities in two different languages. This was validated live in MR !386.

The results agree because both engines apply the mask through their own plan-rewrite layer, not a shared runtime. SQE rewrites the `LogicalPlan` in its `PolicyPlanRewriter`. Spark rewrites its logical plan through Kyuubi's Spark Authz plugin (`RangerSparkExtension`). Both read the same `hive` service-def: the same policy items, the same `dataMaskType` strings, the same transformer templates. SQE reimplements the Hive char-class transformer faithfully, and because both engines start from the same policy and apply the same semantics, the masked values match. One policy set, two engines, the same answer.

::: {.fieldreport}
**Field report:** The first parity run did not produce `xxx-xx-1111` in Spark. It failed outright, because Spark could not resolve the Hive mask UDF the transformer template names. The fix was a Spark setting, `spark.sql.catalogImplementation=hive`, which keeps function resolution inside the built-in Hive registry where the mask function lives. With that set, Spark and SQE agreed on every byte. The lesson is that "byte-exact" is a claim you earn by running both engines on the same input, not one you assert from reading two codebases.
:::


## What parity does not cover

The parity claim has edges, and stating them is part of telling the truth about it.

Parity is validated for RESOURCE policies: named-column masks and named-table or named-column row filters on the `hive` service. Those are the policies both engines resolve by table and column name.

Tag-based masking is NOT cross-compared. The two engines source the tag-to-column association from different places. Spark Authz reads it from the Ranger or Atlas tag store. SQE reads it from the Iceberg `sqe.column-tags` property. The mask-per-tag rule is shared, both engines read `tagPolicies`, but the association source differs, so tag parity would require an Iceberg-to-Ranger tag sync that mirrors the property into the Ranger tag store. That sync is optional and is not part of this result.

Spark 4 is not feasible off the shelf for this parity. The `kyuubi-spark-authz_2.13` artifact is unpublished, so a Spark 4 stack on Scala 2.13 would need Kyuubi built from source. The validated matrix is Spark 3.5.4, the Iceberg Spark runtime `1.8.1`, Kyuubi Spark Authz `1.11.1`, against Ranger 2.8 and Polaris 1.5.0.

The payoff stands inside those edges. A data governance team writes a masking policy once, in the Ranger console they already operate. Spark enforces it. SQE enforces it. The SSN that arrives at an analyst's client reads `xxx-xx-1111` no matter which engine ran the query. The policy is the contract, and two independent engines honor it to the byte. That is what a shared service-def buys, and it is the strongest argument for putting fine-grained enforcement in the query plan instead of anywhere else.

::: {.ailog}
**AI Logbook:** The mask vocabulary and the precedence contract came together fast, because they are mechanical translations of a documented Ranger transformer set, and the AI is good at mechanical translation with a reference in hand. The parity result was the opposite. It was not something the AI could assert, only something we could observe: stand up Spark next to SQE, run the same query as the same user, and diff the bytes. The first run failed on the Spark UDF resolution, and no amount of reasoning about the two codebases would have predicted the `catalogImplementation` setting. We found it by running the thing and reading the Spark error, which is the only way these claims are ever worth anything.
:::
