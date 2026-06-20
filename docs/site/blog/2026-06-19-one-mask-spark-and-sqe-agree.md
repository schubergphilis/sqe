---
title: "One mask, and Spark and SQE agree to the byte"
description: "SQE's fine-grained Ranger backend does row filters, column masks, role-conditional masking, and tags. The kicker: the same Ranger policy on the same Polaris catalog produces byte-exact identical masked output in SQE and in standard Apache Spark. We proved it live, and the first run failed for a reason no amount of reading could have predicted."
pubDate: "2026-06-19"
author: "Jacob Verhoeks"
tags:
  - "ranger"
  - "spark"
  - "security"
  - "iceberg"
  - "data-masking"
---

*June 19, 2026*

The last post was about the coarse path: SQE writes a `GRANT` to Apache Ranger, Polaris enforces it, may this user load this table. One question, one boolean answer.

This post is the path that answers the harder questions. Which rows may the user see. Which columns. Should the SSN arrive raw or as `xxx-xx-1111`. SQE enforces those itself, by rewriting the query plan, against a separate Ranger service. And at the end there is a result the engine's other policy backends could never give me: the same Ranger policy produces byte-exact identical output in SQE and in standard Apache Spark.

## A third backend for the same rewriter

SQE has had plan-rewrite policy enforcement for a while, with OPA and Cedar as the policy sources. Set `policy.engine = "ranger"` and the source becomes the `hive` Ranger service. The rewriter is unchanged: it downloads the policies, rewrites the `LogicalPlan` before the optimizer runs, injects row filters as filter nodes, replaces masked columns with masking expressions, and drops restricted columns from the schema.

```toml
[policy]
engine = "ranger"

[policy.ranger]
url = "http://ranger-admin:6080"
service-name = "hive"
```

Why the `hive` service and not a new SQE-specific one? Because Apache Spark reads the `hive` service too, through its Kyuubi authorization plugin. Sharing the service-def is the entire point, and it is what makes the parity result at the end possible.

Why does this live in SQE and not Polaris? Because the `polaris` service-def from the last post declares no `rowFilterDef` and no `dataMaskDef`, and the Polaris authorizer reads only a boolean. It cannot compute a mask even though the Ranger engine can. Fine-grained enforcement has to happen in the query engine.

Note the identity divergence from the coarse path. SQE matches policy items against the user's token roles directly, read from `realm_access.roles`. It does not depend on Ranger role membership the way Polaris does on the coarse path. Same Ranger server, two different identity resolutions, because one decision happens inside Polaris and the other inside SQE.

## The full Hive mask set, fail-closed

SQE implements the complete Ranger Hive built-in mask vocabulary. `MASK_NULL` nullifies. `MASK_HASH` emits an HMAC-SHA256 digest. `MASK_SHOW_LAST_4` shows the last four characters and masks the rest. `MASK_DATE_SHOW_YEAR` truncates a date to its year. `CUSTOM` runs an arbitrary SQL expression with `{col}` as the placeholder. The character conventions match the Hive transformer templates down to Unicode-scalar counting, so `111-11-1111` under `MASK_SHOW_LAST_4` becomes `xxx-xx-1111`.

Every uncertain branch denies. A table that cannot be mapped to a policy gets a `lit(false)` row filter. A resolution error gets `lit(false)`. An unparseable row filter becomes `lit(false)` rather than being dropped. An unsupported mask type restricts the column rather than returning it raw. The download sits behind a circuit breaker, and an open breaker is treated as deny-all. A governance system that returns raw data when it cannot reach its policy server has no governance at all.

## Masking that depends on the role, distribution-safe

Snowflake has conditional masking: the value a user sees depends on their role at query time. SQE gets the same behavior through five session-context UDFs, `current_user()`, `is_role_in_session(role)`, `current_available_roles()`, and two more. Each one bakes the session identity in at construction and is marked immutable, so DataFusion const-folds the call to a literal during optimization on the coordinator.

The folded literal is what ships to workers. The function call never crosses the wire. That const-fold is what makes role-conditional masking work across a distributed cluster: a worker never has to know who the user is, because the coordinator already turned the identity into a constant before the fragment left. A Ranger row filter like `is_role_in_session('engineer') OR region = current_user()` resolves per session and enforces the same on one node or fifty.

## Tags, stored where the data is

Tag-based masking, "any column tagged `PII` is masked show-last-4," splits into two halves stored in two places, on purpose.

The mask-per-tag rule lives in Ranger as a `tag`-service policy, shared with Spark like everything else. The tag-to-column association lives in the Iceberg table property `sqe.column-tags`, authored with `SET TAGS`:

```sql
ALTER TABLE sales_wh.sales.orders
  SET TAGS (ssn = ('PII'), amount = ('FINANCIAL'));
```

`SET TAGS` writes the `sqe.column-tags` property for you and merges, so it changes only the columns you name. The Snowflake form works too: `MODIFY COLUMN ssn SET TAG PII = 'true'`, where the tag name is the label and the value is ignored.

Storing associations as a table property beats the Ranger or Atlas tag store on four counts. They cover federated catalogs Polaris cannot gate. They need no Atlas or tagsync deployment. They travel with the data through clone, replicate, and rename. And SQE already reads the table metadata on every scan, so the read is free.

## The byte that proves it

Here is the result. Same Ranger `hive`-service policy. Same Polaris catalog. Same query, same user, two engines.

```sql
SELECT id, ssn FROM sales_wh.sales.orders
```

Run as `bob`, both SQE and standard Apache Spark return the same masked SSNs:

```
xxx-xx-1111
xxx-xx-2222
xxx-xx-3333
```

Three of three rows, byte-exact, across two engines built by two communities in two languages. Validated live in MR !386.

The results agree because both engines apply the mask through their own plan-rewrite layer, not a shared runtime. SQE rewrites the `LogicalPlan` in its `PolicyPlanRewriter`. Spark rewrites through Kyuubi's Spark Authz plugin. Both read the same `hive` service-def, the same policy items, the same `dataMaskType` strings, the same transformer templates. SQE reimplements the Hive char-class transformer faithfully, and because both engines start from the same policy and apply the same semantics, the output matches.

## The first run failed

The parity run did not produce `xxx-xx-1111` in Spark the first time. It failed outright. Spark could not resolve the Hive mask UDF the transformer template names.

The fix was one Spark setting, `spark.sql.catalogImplementation=hive`, which keeps function resolution inside the built-in Hive registry where the mask function lives. With that set, every byte agreed. No amount of reading the two codebases would have predicted that line. "Byte-exact" is a claim you earn by running both engines on the same input, not one you assert from inspecting source.

## Where the parity ends

The claim has edges, and naming them is part of the claim.

Parity is validated for resource policies: named-column masks and named row filters on the `hive` service. Tag-based masking is not cross-compared, because the two engines source the tag-to-column association differently. Spark reads it from the Ranger or Atlas tag store; SQE reads it from the `sqe.column-tags` property. The mask-per-tag rule is shared, the association source is not, so tag parity would need an Iceberg-to-Ranger sync that is optional and not part of this result.

Spark 4 is not feasible off the shelf, because the `kyuubi-spark-authz_2.13` artifact is unpublished and a Scala 2.13 stack would need Kyuubi built from source. The validated matrix is Spark 3.5.4, Iceberg Spark runtime 1.8.1, Kyuubi Spark Authz 1.11.1, Ranger 2.8, Polaris 1.5.0.

Inside those edges the payoff stands. A governance team writes a masking policy once, in the Ranger console they already run. Spark enforces it. SQE enforces it. The SSN that reaches an analyst's client reads `xxx-xx-1111` no matter which engine ran the query. The policy is the contract, and two independent engines honor it to the byte. That is the strongest argument I have for putting fine-grained enforcement in the query plan, and nowhere else.
