---
title: "The tag travels with the table"
description: "Tag-based masking is two systems wearing one name: a rule that says what a tag means, and an association that says which columns carry it. We put the rule in Apache Ranger, where Spark can share it, and the association in the Iceberg table metadata, where it survives rename, replication, and federated catalogs that no policy store can see. Here is the split, the precedence contract, and the identity bug the tests now pin down."
pubDate: "2026-07-05"
author: "Jacob Verhoeks"
tags:
  - "security"
  - "ranger"
  - "iceberg"
  - "governance"
---

*July 5, 2026*

A masking policy per column per table does not scale. Fifty tables with an `ssn` column means fifty policies, and the fifty-first table ships unmasked because nobody wrote policy fifty-one. The model that scales is the one Snowflake sold to every governance team: tag the column `PII` once, write one rule that says what `PII` means, and let the engine connect the two. We wanted that on open Iceberg, enforced by SQE's plan rewriter.

The first design decision is the one most tag systems get wrong. A tag system is two systems wearing one name.

## Two halves, two stores

Half one is the **rule**: "any column tagged `PII` is masked show-last-4." Half two is the **association**: "column `sales.customers.ssn` carries the tag `PII`." They have different owners, different change rates, and different blast radii. The rule changes when the governance team changes its mind. The association changes when a data engineer adds a column. Conflating them into one store is the usual mistake, and it forces both audiences through one door.

The standard Ranger answer stores both halves on the Ranger side: rules as tag-service policies, associations in the Ranger tag store, fed from Apache Atlas by a sync daemon. We rejected that for the associations, for four reasons.

First, federated catalogs. SQE's plan rewrite is the only fine-grained layer that covers tables in external catalogs that Polaris cannot gate, and a Ranger tag store keyed by exact resource names would leave those tables untagged. Second, there is no Atlas here. A Polaris lakehouse has no Atlas and no tagsync, so a Ranger-side association store sits empty unless something new pushes into it. Third, Ranger associations are keyed by name, so they break on rename and do not follow a clone or a replica. Fourth, SQE already reads the table metadata on every scan. Reading one more property costs nothing.

So the association lives in the data. One Iceberg table property, `sqe.column-tags`, a JSON map of column to tags:

```
sqe.column-tags = {"ssn": ["PII"], "amount": ["FINANCIAL"]}
```

The rule stays in Ranger, in the `tagPolicies` block of the same policy bundle SQE already downloads for row filters and column masks. Spark and Kyuubi read that same bundle, so the mask-per-tag rule remains shared across engines. Only the source of truth for which columns carry which tag moves to the table metadata, where it travels with the data through every rename and replication.

## Authoring without JSON

Nobody should hand-write a JSON blob into `SET TBLPROPERTIES`. The DDL does it:

```sql
ALTER TABLE sales.customers
  SET TAGS (ssn = ('PII'), amount = ('FINANCIAL'));

ALTER TABLE sales.customers UNSET TAGS (amount);
```

`SET TAGS` merges. Only the columns you name change, tags within a column are unioned and deduped, and everything else stays put. The Snowflake form works too, for muscle memory: `ALTER TABLE t MODIFY COLUMN ssn SET TAG PII = 'true'`, where the tag name is the label and the assigned value is ignored.

## Enforcement is a join with a locked precedence

At query time the rewriter joins the two halves. For each scan it reads the column-to-tag map from the table metadata, asks the policy store which mask each tag resolves to for the user's roles, and merges the result into the same `ResolvedPolicy` that resource-level masks and row filters already populate. The tag path added no new enforcement machinery. It added a second source feeding the machinery that shipped with the resource policies.

The merge follows a precedence contract we treat as locked, because every rule in it is a security decision:

- A restricted column stays restricted. No tag can un-restrict it.
- A resource mask on a column beats a tag mask on the same column. The named policy is more specific than the label.
- Tag row filters are ANDed with resource row filters. The result is the most restrictive combination.
- A tag that maps to no known mask fails closed: the column is dropped from the output, not returned raw.

The last rule is the one worth stealing. The lazy failure mode for an unmappable tag is a warning in a log nobody reads, while the column sails through unmasked. If a column's only protection is a tag the engine cannot interpret, silence must mean denial. The summary of the whole contract fits in one line: tags add restrictions, they never remove one.

## The bug the tests pin down

The one real bug in this build was not in the masking. It was in the table identity.

An Iceberg table is a catalog, a namespace path, and a name, and the namespace path can be multi-level: `sales_wh.emea.sales.customers` has the namespace `["emea", "sales"]`. Ranger's `hive` service model flattens that to a single `database` resource, and SQE maps the last path component into it. That flattening is fine for Ranger, which is why the habit formed. It is fatal for a tag lookup, because `["emea", "sales"]`, `["sales"]`, and `["emea.sales"]` are three different tables, and a lookup with the wrong identity reads another table's tags or nobody's.

The `TagSource` trait therefore takes the full identity: catalog, the namespace as a vector of components, and the table name. And because this leak had crept in more than once, one of the rewriter integration tests is a capture assertion: a fake tag source records exactly what the rewriter asked for, and the test fails unless the request was `["ns1", "ns2"]`. Not the truncated `["ns2"]`. Not the joined `["ns1.ns2"]`. The test encodes the two wrong shapes we actually wrote, so neither can come back.

Three more executable tests prove the pipeline end to end: a column tagged `PII` with a Nullify rule comes back all NULL; a resource-level Redact beats a tag-level Hash on the same column; and a column tagged `SECRET`, a tag with no mask rule, disappears from the output entirely.

## What this does not do yet

Spark does not see these associations. Kyuubi's Ranger plugin reads tag-to-column links from the Ranger or Atlas tag store, not from Iceberg properties, so cross-engine tag enforcement needs a one-way sync that mirrors `sqe.column-tags` into Ranger. The sync is deliberately optional: SQE never depends on it, and for federated catalogs the property is the only option anyway. It is also not built yet.

Two smaller gaps sit behind it. There is no `SHOW TAGS` read-back, so today you inspect the table property. And tags do not propagate: a column derived from a tagged column in a CTAS starts life untagged.

The shape of the decision is what we would defend. Rules belong where policy admins live, and Ranger already owns that. Associations belong where the data lives, because the data outlives every store that merely points at it. Tag a column once, and the tag is still there after the rename, in the replica, and in the catalog your policy store has never heard of.
