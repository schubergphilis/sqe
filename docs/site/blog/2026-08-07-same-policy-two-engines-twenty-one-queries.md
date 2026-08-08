---
title: "Same policy, two engines, twenty-one queries"
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
different ways, and the differences are more instructive than the matches. The tables
below give the SQL and the full output from both engines for each one.

## How to read the tables

Every row is a real query against a live stack: Apache Polaris 1.7, Apache Ranger 2.8,
Keycloak 26.5, and Spark 3.5.9 with Kyuubi Authz 1.11.1. The output columns are what the
engines printed, not a summary of them.

The fixture is one table, seeded identically every run:

```sql
CREATE TABLE sales_wh.ac.orders (
  id BIGINT, region VARCHAR, amount DOUBLE,
  ssn VARCHAR, email VARCHAR, signed_on DATE);

INSERT INTO sales_wh.ac.orders VALUES
  (1,'EU',10.0,'111-11-1111','a@x',DATE '2021-05-04'),
  (2,'US',20.0,'222-22-2222','b@x',DATE '2022-06-05'),
  (3,'EU',30.0,'333-33-3333','c@x',DATE '2023-07-06');
```

SQE addresses it as `sales_wh.ac.orders`. Spark reaches the same table through its own
registered catalog, so the SQL there reads `acwh.ac.orders`. Same warehouse, same
Iceberg table, same Polaris catalog. Only the catalog alias differs, and the tables
below show each engine's statement as it actually ran.

Identities: `bob` is in the Ranger role `engineer`, `alice` is in `analyst` only, `dave`
holds no role, `carol` is the admin. Both engines read the SAME Ranger services:
object-level from Polaris on the `polaris` service, masks and filters from a service
called `query` plus its attached `tag` service.

Cells marked **not measured** are exactly that.

## Object level: who may load the table at all

| # | Setup | SQE statement and output | Spark statement and output | Same |
|---|---|---|---|---|
| 1 | none | `SELECT region FROM sales_wh.ac.orders` as alice<br>error: denied. The SELECT path follows the information-hiding model and reports the table as absent rather than forbidden | `SELECT region FROM acwh.ac.orders` as alice<br>`ForbiddenException: Forbidden: Principal 'alice' is not authorized for op 'LOAD_TABLE'` | outcome yes, message no |
| 2 | `GRANT SELECT ON sales_wh.ac.orders TO ROLE "engineer"` | `SELECT count(*) FROM sales_wh.ac.orders` as bob<br>`3` | `SELECT count(*) FROM acwh.ac.orders` as bob<br>`3` | yes |
| 3 | same grant, `engineer` only | `SELECT count(*) ...` as alice<br>error: denied | `SELECT count(*) ...` as alice<br>`ForbiddenException ... op 'LOAD_TABLE'` | yes |
| 4 | `GRANT SELECT ON sales_wh.ac.orders TO USER "dave"` | `SELECT count(*) ...` as dave<br>`3` | `SELECT count(*) ...` as dave<br>`3` | yes |
| 5 | grant, then `REVOKE SELECT ON sales_wh.ac.orders FROM ROLE "engineer"` | `SELECT count(*) ...` as bob<br>error: denied | `SELECT count(*) ...` as bob<br>`ForbiddenException ... op 'LOAD_TABLE'` | yes |
| 6 | grant to `analyst` and `engineer`, then a Ranger DENY item on `engineer` | `SELECT count(*) ...` as bob<br>error: denied | `SELECT count(*) ...` as bob<br>`ForbiddenException ... op 'LOAD_TABLE'` | yes |
| 7 | same DENY on `engineer` | `SELECT count(*) ...` as alice<br>`3` | `SELECT count(*) ...` as alice<br>`3` | yes |
| 8 | `GRANT SELECT ON ALL TABLES IN SCHEMA sales_wh.ac TO ROLE "engineer"` | `SELECT count(*) FROM sales_wh.ac.orders_extra` as bob<br>`1` | `SELECT count(*) FROM acwh.ac.orders_extra` as bob<br>`1` | yes |
| 9 | `GRANT SELECT` only (no INSERT) | not measured side by side | `INSERT INTO acwh.ac.orders VALUES (99,'EU',1.0,'999-99-9999','z@x',DATE '2024-01-01')` as bob<br>`ForbiddenException: Forbidden: Principal 'bob' is not authorized for op 'ADD_TABLE_SNAPSHOT'`<br>row count before and after: `3` | n/a |
| 10 | the blanket `policyType-0` defer item REMOVED from the frontend service | not applicable, SQE ignores `policyType-0` | `SELECT id FROM acwh.ac.orders` as bob<br>`AccessControlException: Permission denied: user [bob] does not have [select] privilege on [ac/orders/id]` | **no** |

Row 1 is worth pausing on. Both refuse. SQE reports the table as absent, because a
denied object should be invisible rather than forbidden. Spark passes Polaris's own
message through, which tells the caller the object exists and names the operation. An
auditor reading logs from both engines needs to know they are looking at two postures,
not two bugs.

Row 9 is an honest hole. I measured the write refusal through Spark and never ran the
identical `INSERT` through SQE in the same fixture state, so there is no comparison to
claim. What is certain is that the refusal lands at the snapshot COMMIT rather than the
load, so the data files were staged before it was refused.

Row 10 is the tier-composition trap: with the defer item gone, Kyuubi refuses before
Polaris is ever consulted, on a table Polaris would have allowed.

## Fine-grained: what you see once you are in

All as `bob`, who is in the masked role. Policies are written once, into the service both
engines read.

| # | Policy | SQE statement and output | Spark statement and output | Same |
|---|---|---|---|---|
| 11 | column mask on `ssn`, CUSTOM `concat('xxx-xx-', substr({col},8,4))` | `SELECT id, ssn FROM sales_wh.ac.orders ORDER BY id`<br>`1  xxx-xx-1111`<br>`2  xxx-xx-2222`<br>`3  xxx-xx-3333` | `SELECT id, ssn FROM acwh.ac.orders ORDER BY id`<br>`1  xxx-xx-1111`<br>`2  xxx-xx-2222`<br>`3  xxx-xx-3333` | **yes, byte for byte** |
| 12 | same mask, run as alice (not in `engineer`) | `SELECT id, ssn ...`<br>`1  111-11-1111`<br>`2  222-22-2222`<br>`3  333-33-3333` | same statement<br>`1  111-11-1111`<br>`2  222-22-2222`<br>`3  333-33-3333` | yes |
| 13 | row filter `region = 'EU'` | `SELECT id, region FROM sales_wh.ac.orders ORDER BY id`<br>`1  EU`<br>`3  EU` | `SELECT id, region FROM acwh.ac.orders ORDER BY id`<br>`1  EU`<br>`3  EU` | yes |
| 14 | same row filter, as alice (unfiltered) | `SELECT id, region ...`<br>3 rows | same statement<br>3 rows | yes |
| 15 | tag mask on tag `pii` (`hive:CUSTOM`), applied with `ALTER TABLE sales_wh.ac.orders MODIFY COLUMN ssn SET TAG pii = 'true'`, projector ON | `SELECT id, ssn ...`<br>`1  xxx-xx-1111`<br>`2  xxx-xx-2222`<br>`3  xxx-xx-3333` | same statement<br>`1  xxx-xx-1111`<br>`2  xxx-xx-2222`<br>`3  xxx-xx-3333` | **yes, byte for byte** |
| 16 | then `ALTER TABLE sales_wh.ac.orders MODIFY COLUMN ssn UNSET TAG pii` | `SELECT id, ssn ...`<br>`1  111-11-1111`<br>`2  222-22-2222`<br>`3  333-33-3333` | same statement<br>`1  111-11-1111`<br>`2  222-22-2222`<br>`3  333-33-3333` | yes |
| 17 | **named** mask type `MASK_SHOW_LAST_4` on `ssn` | `SELECT id, ssn ...`<br>`1  xxx-xx-1111`<br>`2  xxx-xx-2222`<br>`3  xxx-xx-3333` | same statement<br>`1  nnnUnnU1111`<br>`2  nnnUnnU2222`<br>`3  nnnUnnU3333` | **no** |
| 18 | resource mask `concat('RES-', substr({col},8,4))` AND tag mask on `ssn` at once | `SELECT id, ssn ...`<br>`1  RES-1111`<br>`2  RES-2222`<br>`3  RES-3333` | same statement<br>`1  xxx-xx-1111`<br>`2  xxx-xx-2222`<br>`3  xxx-xx-3333` | **no** |
| 19 | tag mask, projector OFF (`project-tags = false`) | `SELECT id, ssn ...`<br>`1  xxx-xx-1111`<br>`2  xxx-xx-2222`<br>`3  xxx-xx-3333` | same statement<br>`1  111-11-1111`<br>`2  222-22-2222`<br>`3  333-33-3333` | **no** |
| 20 | tag mask, then `ALTER TABLE sales_wh.ac.orders RENAME COLUMN ssn TO tax_id` | `SELECT id, tax_id FROM sales_wh.ac.orders ORDER BY id`<br>`1`<br>`2`<br>`3`<br>one column, `tax_id` absent, no error | `SELECT id, tax_id FROM acwh.ac.orders ORDER BY id`<br>`1  111-11-1111`<br>`2  222-22-2222`<br>`3  333-33-3333` | **no** |
| 21 | column mask on `ssn`, then `ALTER TABLE sales_wh.ac.orders ADD COLUMN nickname VARCHAR` | `SELECT id, ssn, nickname FROM sales_wh.ac.orders ORDER BY id`<br>`Internal error: PhysicalExpr Column references column 'nickname' at index 2 (zero-based) but input schema only has 2 columns: ["id", "ssn"]` | not measured | n/a |

Row 15 is the headline. One tag applied through SQL, one mask rule written once against
that tag, and both engines render the same bytes. That needed a projector to achieve,
and row 19 is what it looks like without one.

## The four divergences, and why each happens

### Row 17: the named mask type

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
expression verbatim and rows 11 and 15 come out identical. **Portability comes from
writing the expression yourself, not from Ranger's vocabulary.**

### Row 18: resource mask versus tag mask

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

### Row 19: the tag with no projection

Tag associations live in the Iceberg table property `sqe.column-tags`. We chose that so
tags travel with the table, which is a real sovereignty property: copy the table
somewhere else and its classifications come along.

Kyuubi reads Ranger's tag store and cannot read Iceberg properties. So the association
is invisible to it, no tag matches, and the mask never fires.

Row 19 is that state: the column is masked in SQE and raw in Spark. It is the worst
shape a governance gap can take, because the engine you demo with protects the data and
the engine someone else uses does not.

Closing it means writing the association into Ranger's tag store as well, on every
`SET TAG`. The Iceberg property stays the source of truth and Ranger holds a projection.
When the projection fails we roll the property back, because a tag that exists in one
store and not the other is exactly row 19 again, and the statement would have reported
success.

### Row 20: the renamed column

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

## Row 21, the one that is not a divergence

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

Sixteen of the twenty-one rows match, including every row filter and every mask written
as a portable expression. Tag-based masking matches once the association is
projected. That is a working multi-engine governance story, and it is not a story
Databricks or Snowflake tells, because in both of those the engine IS the policy
authority and there is only ever one engine to ask.

Five do not match, and two of those carry no comparison at all: one because I did not
run it, one because the SQE side dies first. Of the real divergences, two are
cosmetic-but-breaking (a mask vocabulary and a precedence order), and two are defects we
now have written down.

If you are evaluating this pattern, the useful question is not whether the engines agree.
It is whether you can enumerate where they do not. That is what the table is for, and it
is why every cell that says **not measured** says it instead of guessing.
