---
title: "Snowflake's governance model on open Iceberg"
description: "Snowflake gives you masking policies, row access policies, object tags, and a GRANT model. SQE gives you the same primitives on Apache Ranger and open Iceberg, enforced by plan rewrite and shared across engines. Here is the mapping, the one real edge SQE has, and the gaps we have not closed yet."
pubDate: "2026-06-19"
author: "Jacob Verhoeks"
tags:
  - "ranger"
  - "snowflake"
  - "security"
  - "iceberg"
  - "data-masking"
---

*June 19, 2026*

The two previous posts built the machinery. A SQL `GRANT` becomes an Apache Ranger policy that Polaris enforces. A column mask written once in Ranger produces byte-exact identical output in SQE and in standard Apache Spark. Both posts answered "how." This one answers "compared to what."

The honest comparison is Snowflake. Snowflake set the bar for data governance that a SQL person can read: masking policies, row access policies, object tags, a privilege model built on `GRANT`. We did not invent a vocabulary. We mapped Snowflake's onto open infrastructure, then made it enforce in more than one engine.

## The mapping

SQE governance is two layers. The catalog layer answers "may this user load this table" through `GRANT`/`REVOKE` written to Ranger and enforced by Polaris. The fine-grained layer answers "which rows, which columns, masked how" through SQE rewriting the query plan against the `hive` Ranger service. Snowflake folds both into native DDL. The primitives line up almost one to one.

| Snowflake | SQE |
|---|---|
| `GRANT`/`REVOKE` on objects | `GRANT`/`REVOKE`, written to the `polaris` Ranger service, enforced by Polaris |
| Role hierarchy, secondary roles, ownership, FUTURE grants | flat token roles from `realm_access.roles` |
| `CREATE MASKING POLICY` (CASE body, role-conditional) | built-in mask types + `CUSTOM` SQL + `is_role_in_session()` |
| `CREATE ROW ACCESS POLICY` (boolean per row, mapping tables) | row filter as an arbitrary SQL `filterExpr`, injected as a `Filter` node |
| Object tags + tag-based masking with propagation | Iceberg `sqe.column-tags` property + Ranger `tagPolicies` (no propagation yet) |
| `IS_ROLE_IN_SESSION`, `CURRENT_USER`, `CURRENT_AVAILABLE_ROLES` | the same five session-context UDFs, const-folded to literals |

Column masking is the closest match. Snowflake's masking policy is a CASE expression that returns the raw value for privileged roles and a transform otherwise, and it can reference the session role and other columns. SQE's `CUSTOM` mask is the same: an arbitrary SQL expression with `{col}` as the placeholder, and `is_role_in_session('engineer')` resolves the same conditional. For the common cases, show last four, hash, null, redact, SQE ships those as named mask types so you do not hand-write the CASE every time.

Row access is the same mechanism on both sides. Snowflake evaluates a boolean per row, often against a mapping table keyed on the session role. SQE injects the row filter as a `Filter` node above the scan, and a user predicate pushes through it the same way a `WHERE` clause would. A Ranger filter like `is_role_in_session('engineer') OR region = current_user()` is exactly the row-access-policy idiom, written in Ranger instead of Snowflake DDL.

## The deliberate difference

Snowflake keeps policy inside Snowflake. The policy is a native DDL object, stored in the account, enforced by the Snowflake engine, visible to nothing else. One vendor owns the rule, the store, and the enforcement. That is clean, and it is also a lock.

SQE splits those apart on purpose. The policy lives in Apache Ranger, an open and pluggable governance system a security team can run, audit, and feed from LDAP. Enforcement happens by rewriting the query plan, which means any engine that can read the same Ranger service and rewrite its own plan honors the same rule. The policy is a contract, not a feature of one engine.

That split is the whole reason a shop with a Ranger admin and a Ranger audit trail can adopt SQE without moving governance into the engine. They write the policy where they already write policy. SQE reads it.

## The edge

Snowflake masking is Snowflake-only. A masking policy you author in Snowflake protects data Snowflake serves and nothing else. The moment a second engine touches the same table, the policy is gone unless you re-implement it.

SQE's Ranger policy is not engine-only. The same `hive`-service mask, on the same Polaris catalog, enforces byte-identically in SQE and in standard Apache Spark through Kyuubi's authorization plugin. We proved it live: `bob` running `SELECT id, ssn FROM sales_wh.sales.orders` gets `xxx-xx-1111` in both engines, three of three rows, validated in MR !386. Two engines, two communities, two languages, the same masked byte. The previous post earned that claim by running both, not by inspecting source.

There is one more place SQE goes past Snowflake. Snowflake's masking policy can return NULL, but the column still exists in the result schema. SQE has column restriction: a denied column is dropped from the projection entirely, invisible, not an error, the PostgreSQL RLS model. A user who cannot see `ssn` does not learn the column is there. Mask-to-NULL tells the user a protected column exists. Restriction tells them nothing.

## How a query flows

Walk one `SELECT` through both layers.

```sql
SELECT id, ssn, region FROM sales_wh.sales.orders WHERE region = 'EU'
```

```
SELECT -> Polaris catalog gate (Ranger polaris service): may bob load orders?
       -> SQE PolicyPlanRewriter (Ranger hive service):
            inject Filter (row filter)  above the TableScan
            swap ssn -> mask expression  in the projection
            drop restricted columns      from the projection
       -> DataFusion optimizer -> execution
```

The catalog gate runs first, inside Polaris. If `bob` lacks the coarse `SELECT` grant, the query stops there and SQE never sees a plan to rewrite. Once the table loads, SQE's rewriter takes over. It resolves the policy for `bob` and his token roles, injects the row filter as a `Filter` node, replaces the `ssn` reference in the projection with the masking expression aliased back to `ssn`, and drops any restricted column from the schema. The optimizer then runs on the already-secured plan.

For a tagged column the rewriter does one extra join. It reads the column-to-tag map from the Iceberg `sqe.column-tags` property, reads the mask-per-tag rule from the Ranger `tagPolicies` block, and applies the mask the tag points at. A resource mask on the same column wins over a tag mask, and a tag that maps to no known mask fails closed by restricting the column. Tags add restrictions; they never remove one.

## What we have not closed

The mapping is honest about its edges, and naming them is part of the claim.

Role hierarchy is the big one. `SessionUser.roles` is a flat list from the token, with no inheritance and no secondary-role notion. Snowflake's `IS_ROLE_IN_SESSION` walks an activated role graph; ours matches a flat set. The richer model belongs in `sqe-auth`, and it is the prerequisite for a faithful `current_role()` and secondary roles.

Tag parity stops at the association. Both engines read the mask-per-tag rule from Ranger, but Spark sources the tag-to-column association from the Ranger or Atlas tag store while SQE reads it from the Iceberg property. Closing the gap needs an Iceberg-to-Ranger tag sync so Spark honors the same associations SQE does. That sync is optional and not built.

Two more sit on the list. There is no tag propagation when a column is derived from a tagged one. Policies are not named reusable objects the way Snowflake's `CREATE MASKING POLICY` is; a Ranger policy item is the unit. And the catalog layer has no FUTURE grants, so a `GRANT` covers tables that exist, not tables yet created.

None of these block the result that matters. A governance team writes a masking policy once, in a Ranger console they already run, against an open Iceberg catalog they own. SQE enforces it. Spark enforces it. The SSN reads `xxx-xx-1111` no matter which engine ran the query. Snowflake gives you that inside Snowflake. We give you the same primitives without the wall around them.
