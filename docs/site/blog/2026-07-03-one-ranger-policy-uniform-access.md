---
title: "One Ranger policy, uniform access across SQE and Spark"
description: "A lakehouse rarely has one query engine. Spark writes the tables, SQE serves the interactive queries, and both touch the same Iceberg data. The usual failure is policy drift: each engine has its own access-control plugin, the same masking intent gets translated twice, and the two translations disagree. We avoided that by not translating twice. SQE and Spark read the same Apache Ranger hive service, so one policy written once enforces the same way in both. The SSN reads xxx-xx-1111 whichever engine ran the query."
pubDate: "2026-07-03"
author: "Jacob Verhoeks"
tags:
  - "security"
  - "ranger"
  - "spark"
  - "governance"
---

*July 3, 2026*

A lakehouse rarely runs on one engine. Spark builds the tables in a nightly job. SQE serves the interactive queries and the dashboards. Both read the same Iceberg data on the same object store. That is the point of an open lakehouse: the data is not trapped inside one engine's walls.

Access control is where that openness usually breaks.

The reason is drift. Each engine enforces fine-grained policy through its own plugin, and each plugin is a separate translation of the same intent. You write "mask the SSN to the last four digits" once for Spark and again for the other engine, in two consoles or two policy languages, and now you maintain two things that are supposed to say the same sentence. Over time they stop saying it. A column tightened in one place stays open in the other. The mask that returns `xxx-xx-1111` in Spark returns the raw value somewhere else, and nobody notices until an audit does.

The fix is not a shared runtime. Engines will never share a runtime. The fix is a shared policy source.

## One service, two readers

SQE and Apache Spark both read the same Apache Ranger service: the `hive` service-def. Not a copy, not a synced mirror. The same service.

Spark reads it through the Kyuubi Spark-Authz plugin, which downloads the policy bundle and rewrites Spark's logical plan before execution. SQE reads the same bundle from the same Ranger Admin endpoint and rewrites its own `LogicalPlan` in the coordinator, injecting row filters and column masks above the table scan before DataFusion optimizes. Two independent codebases, no shared library between them. What they share is the policy.

We picked the `hive` service on purpose, over inventing an SQE-specific one, precisely because Spark already reads it. Sharing the service-def is the whole design. A new service-def would have been cleaner to reason about and useless in practice, because the entire value is that the governance team does not learn a second thing.

## What the one policy covers

The `hive` service carries three policy types, and SQE honors all three:

- **Row filters.** A boolean SQL expression attached to a table for a user or role. SQE parses it and injects it as a filter above the scan, so a user sees only the rows the expression admits.
- **Column masks.** The full Ranger `hive` built-in mask set: full redaction, show-first-4, show-last-4, hash, nullify, date-to-year, and custom SQL expressions. SQE reimplements the Hive character-class transformer faithfully, counting by Unicode scalar the way Hive does.
- **Role-conditional logic.** Row filters and custom masks can call `current_user()`, `current_role()`, and `is_role_in_session()`. SQE const-folds them per session before the plan is distributed, so the same policy shows a column to an auditor and masks it for an analyst.

The mask precedence, the fail-closed behavior on an unmappable rule, the character conventions: all of it tracks the Hive service-def, because that is the contract both engines read.

## Configure each engine to read it

On the SQE side, point the fine-grained policy engine at the `hive` service:

```toml
[policy]
engine = "ranger"

[policy.ranger]
url = "http://ranger-admin:6080"
service-name = "hive"
```

On the Spark side, the Kyuubi `RangerSparkExtension` reads the same service through the shaded Kyuubi Spark-Authz plugin, against the same Ranger Admin. One Spark setting matters for the parity to hold: `spark.sql.catalogImplementation=hive`, so Spark resolves the masking UDFs the way the transformer templates expect. We did not predict that one. We found it by standing Spark up next to SQE and reading the first error.

Two config blocks, two engines, one `hive` service behind both.

## The result: write once, enforce everywhere

Take one Ranger masking policy on one Polaris catalog. Run the same query as the same user against SQE and against standard Apache Spark. The masked output is byte-exact identical. A social security number arrives as `xxx-xx-1111` from both, down to the character. We proved it live, and the first run failed for a reason no amount of reading the two codebases would have predicted. That story is in [One mask, Spark and SQE agree](2026-06-19-one-mask-spark-and-sqe-agree.md).

The two engines agree not because one calls the other, but because both start from the same policy and apply the same semantics through their own plan-rewrite layer. The policy is the contract. Each engine honors the contract independently.

## Why this is the governance win

A data governance team writes a masking policy once, in the Ranger console they already operate. Spark enforces it on the write and batch side. SQE enforces it on the interactive and dashboard side. Adding SQE next to an existing Spark stack adds a query engine, not a second access-control surface to keep in sync. There is one place to author policy, one place to audit it, and one answer to "who can see this column," regardless of which engine served the row.

That is what an open lakehouse should feel like. The data is shared, and so is the rule that governs it.

## The honest edges

Tag-based masking is not fully cross-compared. The mask-per-tag rule is shared, both engines read the Ranger `tagPolicies`, but the tag-to-column association is sourced differently: Spark reads it from the Ranger or Atlas tag store, SQE reads it from the Iceberg `sqe.column-tags` table property. Full tag parity would need an Iceberg-to-Ranger tag sync, which is optional and not built.

The validated matrix is Spark 3.5.4 with the Iceberg Spark runtime and Kyuubi Spark-Authz 1.11.1, against Ranger 2.8 and Polaris. Spark 4 on Scala 2.13 is not feasible off the shelf yet, because the Kyuubi Spark-Authz artifact for it is unpublished.

Inside those edges, the result stands. One policy, two engines, the same answer to the byte. The SQE docs cover the full setup under Fine-grained access control, and the validated Spark parity matrix under the Spark / Ranger Parity design note.
