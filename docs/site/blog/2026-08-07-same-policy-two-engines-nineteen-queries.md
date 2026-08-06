---
title: "Same policy, two engines, nineteen queries"
description: "We wrote one Ranger policy set and ran the same SQL through SQE and through Spark, then compared the output cell by cell. Most of it matches byte for byte, including tag-based masking. Four cases do not, and each one is a different reason: Kyuubi ignores the mask transformer our engine honors, the two disagree about whether a resource mask or a tag mask wins, renaming a column breaks each engine in a different direction, and adding one to a masked table stops SQE dead. Here is the full table, with the query, both outputs, and what causes each divergence."
pubDate: "2026-08-07"
author: "Jacob Verhoeks"
tags:
  - "security"
  - "ranger"
  - "spark"
  - "testing"
  - "governance"
---

*August 7, 2026*

One policy set. Two engines. The same SQL.

That is the whole promise of putting access control in Apache Ranger instead of in the
engine, and until this month we had never checked it properly. We had a demo that
compared one column mask across SQE and Spark and called it parity.

Nineteen assertions later, most of the promise holds. Four cases break it, in four
different ways, and the differences are more instructive than the matches.

## How to read the table

Every row is a real query run against a live stack: Apache Polaris 1.7, Apache Ranger
2.8, Keycloak 26.5, and Spark 3.5.9 with Kyuubi Authz 1.11.1. The fixture table holds
three rows. `bob` is in the Ranger role `engineer`, `alice` is in `analyst` only,
`dave` holds no role, `carol` is the admin.

Both engines read the SAME Ranger services. Object-level decisions come from Polaris on
the `polaris` service; row filters and masks come from a service we call `query`, plus
its attached `tag` service.

Cells marked **not measured** are exactly that. I am not going to fill a comparison
table with things I did not run.

## Object level: who may load the table at all

| # | Query, as | SQE | Spark | Same |
|---|---|---|---|---|
| 1 | `SELECT region FROM ac.orders`, alice, no grant | denied, surfaced as `table not found` | denied, `ForbiddenException ... op 'LOAD_TABLE'` | outcome yes, message no |
| 2 | same, bob, after `GRANT SELECT ... TO ROLE engineer` | 3 rows | 3 rows | yes |
| 3 | same, alice, after that grant to `engineer` only | denied | denied, `LOAD_TABLE` | yes |
| 4 | same, dave, after `GRANT SELECT ... TO USER dave` | 3 rows | 3 rows | yes |
| 5 | same, bob, after `REVOKE SELECT ... FROM ROLE engineer` | denied | denied, `LOAD_TABLE` | yes |
| 6 | same, bob, with a Ranger DENY item on `engineer` | denied | denied, `LOAD_TABLE` | yes |
| 7 | same, alice, with that DENY on `engineer` | 3 rows | 3 rows | yes |
| 8 | `SELECT count(*)` on a second table, bob, after `GRANT SELECT ON ALL TABLES IN SCHEMA` | 1 row | 1 row | yes |
| 9 | `INSERT INTO ac.orders VALUES (...)`, bob, holding SELECT only | not measured side by side | denied, `ForbiddenException ... op 'ADD_TABLE_SNAPSHOT'`, row count unchanged | n/a |

Row 1 is the one to notice. Both engines refuse, but SQE reports "table not found"
while Spark names the principal and the operation. SQE follows the Polaris
information-hiding model deliberately: a denied object is invisible rather than
forbidden. Spark surfaces Polaris's own message, which tells the caller the object
exists and which operation was refused. Neither is wrong. They are different postures,
and an auditor reading logs from both engines needs to know that.

Row 9 has an honest gap. I measured the write denial through Spark and never ran the
identical statement through SQE in the same fixture state, so I am not claiming a
comparison. What I can say is that the refusal lands at the snapshot COMMIT, not at the
load, so the data files were already staged when it was refused.

## Fine-grained: what you see once you are in

The fine-grained tier is where one policy governing two engines gets tested properly.
All of these run as `bob`, who is in the masked role.

| # | Policy and query | SQE | Spark | Same |
|---|---|---|---|---|
| 10 | CUSTOM mask `concat('xxx-xx-', substr({col},8,4))` on `ssn`; `SELECT id, ssn` | `xxx-xx-1111` `xxx-xx-2222` `xxx-xx-3333` | identical | **yes, byte for byte** |
| 11 | same policy, as alice (not in the masked role) | `111-11-1111` raw | `111-11-1111` raw | yes |
| 12 | row filter `region = 'EU'`; `SELECT id, region` | 2 rows: `1 EU`, `3 EU` | identical | yes |
| 13 | same row filter, as alice (unfiltered) | 3 rows | 3 rows | yes |
| 14 | tag mask on tag `pii`, `SET TAG` through SQL, projector on; `SELECT id, ssn` | `xxx-xx-1111` ... | identical | **yes, byte for byte** |
| 15 | then `UNSET TAG`; same query | `111-11-1111` raw | `111-11-1111` raw | yes |
| 16 | **named** mask type `MASK_SHOW_LAST_4` on `ssn` | `xxx-xx-1111` | `nnnUnnU1111` | **no** |
| 17 | resource mask AND tag mask on `ssn` at once | `RES-1111` | `xxx-xx-1111` | **no** |
| 18 | tag mask with the projector OFF | `xxx-xx-1111` | `111-11-1111` raw | **no** |
| 19 | `RENAME COLUMN ssn TO tax_id` on a tagged column; `SELECT id, tax_id` | one column returned, `tax_id` DROPPED, no error | `111-11-1111` raw | **no** |
| 20 | `ADD COLUMN nickname` beside a masked `ssn`; `SELECT id, ssn, nickname` | query FAILS, plan/scan schema mismatch | not measured | n/a |

Row 14 is the headline. A tag applied with `ALTER TABLE ... MODIFY COLUMN ssn SET TAG
pii = 'true'`, one mask rule written once against that tag, and both engines render the
same bytes. That took a projector to achieve, and row 18 is what it looks like without
one.

## The four divergences, and why each happens

### 16: the named mask type

Ranger lets you name a mask type instead of writing an expression. `MASK_SHOW_LAST_4`
is supposed to show the last four characters and replace the rest.

SQE reads the servicedef's transformer and renders `xxx-xx-1111`. Kyuubi ignores the
transformer entirely and applies its own substitution rules, one character class at a
time: digits become `n`, separators become `U`. You get `nnnUnnU1111`.

Both hide the raw value. Both show the last four. The bytes differ, so any consumer
comparing values across engines, or any test asserting an exact string, breaks.

The fix is not to use named types across engines. A CUSTOM transformer written in
portable standard SQL, `concat('xxx-xx-', substr({col},8,4))`, uses only functions that
exist as built-ins in both DataFusion and Spark, so each engine injects the same
expression verbatim and rows 10 and 14 come out identical. **Portability comes from
writing the expression yourself, not from Ranger's vocabulary.**

### 17: resource mask versus tag mask

Put both kinds of mask on one column and the engines pick different winners. SQE
applies the resource mask. Kyuubi applies the tag mask.

Stock `RangerBasePlugin` evaluates tag policies before resource policies. Kyuubi is
following Ranger's own ordering. **We are the ones who differ**, and we had that
precedence pinned as intended behaviour on our side without knowing it disagreed with
every other engine that uses the standard plugin.

Nothing leaks: both cells are masked. But whichever mask is weaker becomes the
effective one for whoever picked that engine, so the same column is governed differently
depending on how it is read. My view is that we move to tag-first and match Ranger.
Being the odd one out on a security-relevant evaluation order is the kind of difference
that surprises people at the worst possible time. It is a behaviour change to a shipped
rule, so it gets taken deliberately.

### 18: the tag with no projection

Tag associations live in the Iceberg table property `sqe.column-tags`. We chose that so
tags travel with the table, which is a real sovereignty property: copy the table
somewhere else and its classifications come along.

Kyuubi reads Ranger's tag store and cannot read Iceberg properties. So the association
is invisible to it, no tag matches, and the mask never fires.

Row 18 is that state: the column is masked in SQE and raw in Spark. It is the worst
shape a governance gap can take, because the engine you demo with protects the data and
the engine someone else uses does not.

Closing it means writing the association into Ranger's tag store as well, on every
`SET TAG`. The Iceberg property stays the source of truth and Ranger holds a projection.
When the projection fails we roll the property back, because a tag that exists in one
store and not the other is exactly row 18 again, and the statement would have reported
success.

### 19: the renamed column

`sqe.column-tags` is keyed by column NAME, and no schema-change path rewrites it. So
after `RENAME COLUMN ssn TO tax_id` the association names a column that no longer
exists.

I predicted, when I wrote the test, that the mask would stop applying in both engines
and the data would come back raw in both. That was wrong, and the test caught me:

```
SELECT id, tax_id FROM ac.orders ORDER BY id
  SQE:   [["1"], ["2"], ["3"]]
  Spark: [["1","111-11-1111"], ["2","222-22-2222"], ["3","333-33-3333"]]
```

SQE returns ONE column. It drops `tax_id` from the result, silently, with no error, for
a query that explicitly projected it. Spark returns the raw value.

So the stricter engine hides a column you asked for and the other hands over unmasked
data, and neither says anything is wrong. I do not yet know the mechanism on the SQE
side. I know exactly what it does, which is enough to write down and not enough to fix
confidently.

A query returning fewer columns than it projected, without an error, is a defect
independent of access control.

## Row 20, the one that is not a divergence

Adding a column to a table that already has a column mask breaks SQE outright:

```
PhysicalExpr Column references column 'nickname' at index 2 (zero-based)
but input schema only has 2 columns: ["id", "ssn"]
```

The rewritten plan and the scan schema disagree. It fails closed, so nothing leaks, but
a governed table becomes unqueryable after a routine `ADD COLUMN`. I never got a Spark
number for it, because the comparison helper checks SQE first and never reached the
other engine.

Two questions about DDL, two defects. We had simply never asked what a schema change
does to a policy, and that is the most uncomfortable finding in this whole exercise:
the gaps were not in the hard parts. They were in the ordinary ones.

## What the table is actually worth

Fifteen of nineteen comparisons match, including every row filter and every mask
written as a portable expression. Tag-based masking matches once the association is
projected. That is a working multi-engine governance story, and it is not a story
Databricks or Snowflake tells, because in both of those the engine IS the policy
authority and there is only ever one engine to ask.

The four that do not match are the price. Two are cosmetic-but-breaking (a mask
vocabulary and a precedence order). Two are real defects we now have written down.

If you are evaluating this pattern, the useful question is not whether the engines agree.
It is whether you can enumerate where they do not. That is what the table is for, and it
is why every cell that says **not measured** says it instead of guessing.
