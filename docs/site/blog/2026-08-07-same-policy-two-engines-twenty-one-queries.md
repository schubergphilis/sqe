---
title: "Same policy, two engines, twenty-one queries"
description: "We wrote one Ranger policy set and ran the same SQL through SQE and through Spark, then compared the output cell by cell. Fifteen of the nineteen comparable rows match, including tag-based masking. Four do not, and each is a different reason: Kyuubi ignores the mask transformer our engine honors, the two disagree about whether a resource mask or a tag mask wins, a tag that is not projected is invisible to Spark, and a frontend tier can refuse before the one you think is deciding. The row worth acting on first is one where both engines AGREE and both are wrong: a rename silently unmasks a column. Full table, with the query and both outputs."
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

Twenty-one assertions later, most of the promise holds. Four cases break it, in four
different ways, and the differences are more instructive than the matches. The tables
below give the SQL and the full output from both engines for each one.

One row is an agreement that is worse than a divergence: both engines get it wrong
identically. That one cost me a published correction, and it is the last section.

## How to read the tables

Every row is a real query against a live stack: Apache Polaris 1.7, Apache Ranger 2.8,
Keycloak 26.5, and Spark 3.5.9 with Kyuubi Authz 1.11.1. The output columns are what the
engines printed, not a summary of them: each cell was transcribed from a captured run,
not reconstructed from what a test asserts. That distinction is not pedantry. The first
version of this post got row 20 wrong precisely because the claim was derived from a
test's logic instead of copied from output, and I caught the same slip a second time
while correcting it.

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
| 20 | tag mask, then `ALTER TABLE sales_wh.ac.orders RENAME COLUMN ssn TO tax_id` | `SELECT id, tax_id FROM sales_wh.ac.orders ORDER BY id`<br>`1  111-11-1111`<br>`2  222-22-2222`<br>`3  333-33-3333` | `SELECT id, tax_id FROM acwh.ac.orders ORDER BY id`<br>`1  111-11-1111`<br>`2  222-22-2222`<br>`3  333-33-3333` | yes, and both WRONG |
| 21 | column mask on `ssn`, then `ALTER TABLE sales_wh.ac.orders ADD COLUMN nickname VARCHAR` | `SELECT id, ssn, nickname FROM sales_wh.ac.orders ORDER BY id`<br>`1  xxx-xx-1111  NULL`<br>`2  xxx-xx-2222  NULL`<br>`3  xxx-xx-3333  NULL` | **not measured** | n/a |

Row 15 is the headline. One tag applied through SQL, one mask rule written once against
that tag, and both engines render the same bytes. That needed a projector to achieve,
and row 19 is what it looks like without one.

## The four divergences, and the one shared defect

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
exists, no tag matches, and the mask stops applying:

```
SELECT id, tax_id FROM ac.orders ORDER BY id
  SQE:   [["1","111-11-1111"], ["2","222-22-2222"], ["3","333-33-3333"]]
  Spark: [["1","111-11-1111"], ["2","222-22-2222"], ["3","333-33-3333"]]
```

Both engines hand back the raw value. A routine rename unmasks a governed column and
neither engine says anything is wrong.

I have to correct something here, because the first version of this post got it
wrong and the way it got it wrong is the interesting part.

I originally reported that the engines broke DIFFERENTLY: that SQE silently dropped
`tax_id` from the result while Spark returned it raw, and I wrote several paragraphs
about the stricter engine hiding a column while the other leaked data. That
measurement was real. It was a measurement of a different bug.

Underneath sat a scan defect with nothing to do with access control. SQE's
small-file read path resolved a query's projection against each data FILE's parquet
column names rather than against Iceberg field ids. A renamed column matches no name
in a file written before the rename, and the miss was silently discarded. So the
column vanished from the result, and it vanished whether or not any policy existed.

The control that settled it: run the same DDL and the same query with NO policy at
all. Both cases reproduced, with `masks=0 filters=0 restricted=0` in the log. Two
findings I had filed as access-control defects were one scan bug.

Worth being blunt: both engines returning the value raw is what I predicted before
writing the original test. I abandoned the prediction because the measurement
disagreed with it, which is normally the right instinct. Here the measurement was
of a layer I had not thought to suspect.

### Row 21: adding a column

Adding a column to a table that already has a column mask used to break SQE outright:

```
PhysicalExpr Column references column 'nickname' at index 2
but input schema only has 2 columns: ["id", "ssn"]
```

Same root cause. The scan dropped `nickname`, because no file written before the
`ALTER` contains it, and the mask projection sitting above the scan then indexed
past the end of a batch that had quietly become narrower. The plan rewriter was
never wrong. It was the first consumer to notice.

Fixed the same way: resolve projected columns to Iceberg field ids, and backfill a
typed NULL for a field id a file genuinely does not carry. The masked table is
queryable again, `ssn` is still masked, and `nickname` reads as NULL.

I did not run the identical statement through Spark in the same fixture state, so
that cell says **not measured** rather than guessing that it matches.

### The worst one, which no row shows

Chasing those two turned up a third symptom of the same defect that is worse than
either, and it never appeared in this table because no governance test would think
to look for it:

```
SELECT classified FROM scratch.ev        -- after RENAME secret TO classified
  +------------+
  | classified |
  | 7          |     <- these are `id`'s values
  | 8          |
```

When NO projected name matched the file, the empty index list was treated as
`COUNT(*)`, which reads parquet column 0 for a row count. The real `COUNT(*)` flag
is computed separately, and was false, so that raw first column went out as the
query's data. A query asking for one column received a different column's values,
under the name it asked for, with no error.

The gate on that path is "no delete files and every file under 3 MB", which is every
small and freshly-created table and no merge-on-read table. That is why benchmarks
never saw it and the access-control fixture hit it on every run.

## Every assertion, and why only some of them compare

The twenty-one rows above are the cases where the same statement can be put through
both engines. The suite is larger than that: 31 SQE cases, 10 Spark object-level
cases, 10 cross-engine parity cases. Most of the SQE-only ones are not oversights,
they are things Spark structurally cannot be asked. Here is the whole inventory, so
"we tested access control" can be checked rather than believed.

### Covered by both engines (the rows above)

| SQE case | Spark case | Rows |
|---|---|---|
| `denied_before_any_grant` | `spark_denied_before_any_grant` | 1, 3 |
| `grant_select_to_role_enables_exact_rows` | `spark_grant_select_to_role_enables_exact_rows` | 2 |
| `role_grant_and_user_grant_both_apply` | `spark_role_grant_and_user_grant_both_apply` | 4 |
| `revoke_disables_access` | `spark_revoke_disables_access` | 5 |
| `ranger_deny_overrides_allow` | `spark_ranger_deny_overrides_allow` | 6, 7 |
| `all_tables_in_schema_grant_covers_the_namespace` | `spark_all_tables_in_schema_grant_covers_the_namespace` | 8 |
| `write_privileges_are_separate_from_read` | `spark_write_privileges_are_separate_from_read` | 9 |
| `resource_column_masks_apply_to_engineer_only` | `column_mask_is_byte_identical_across_engines`, `an_unmasked_role_is_unmasked_in_both_engines` | 11, 12 |
| `resource_row_filter_restricts_rows` | `row_filter_returns_identical_rows_across_engines` | 13, 14 |
| `tag_column_mask_applies_from_iceberg_property` | `tag_column_mask_is_byte_identical_across_engines`, `unset_tag_stops_masking_in_both_engines` | 15, 16, 19 |
| `remaining_mask_types_apply_live` | `a_named_mask_type_is_not_byte_portable` | 17 |
| `resource_mask_beats_tag_mask_live` | `resource_and_tag_mask_precedence_diverges_across_engines` | 18 |

### SQE only, and the reason

| SQE case | Why it has no Spark counterpart |
|---|---|
| `tag_row_filter_restricts_rows` | Kyuubi Spark 3.5 throws `MISSING_ATTRIBUTES` on a row filter over a column the query does not project (Kyuubi #6889). A Spark assertion here would be measuring their bug, not the policy. |
| `hash_mask_is_keyed_hmac` | The HMAC key is SQE's, held engine-side. Nothing for Spark to agree or disagree with. |
| `unmappable_tag_mask_fails_closed` | A mask type SQE cannot map must restrict the column rather than return it raw. Fail-closed on an unsupported type is an engine-internal contract. |
| `unknown_tag_state_denies` | Same shape: what SQE does when it cannot resolve the tag state at all. |
| `ranger_outage_fails_closed` | Ranger is taken away and SQE must deny rather than pass through. Kyuubi's behaviour under the same outage is Kyuubi's design, not a parity claim. |
| `cache_ttl_bounds_policy_staleness` | Bounds how long SQE may serve a stale policy. The two engines refresh on independent schedules by design. |
| `show_grants_lists_both_roles`, `check_access_reflects_user_grants`, `show_schemas_describes_the_catalog_it_names` | SQE SQL surface. Spark has no equivalent statement. |
| `sql_deny_blocks_a_granted_read_and_revoke_clears_it` | `DENY` is an SQE SQL extension. The resulting Ranger deny item IS cross-engine, and rows 6 and 7 cover that half. |
| `a_non_admin_cannot_grant_under_the_default_gate`, `a_delegated_owner_grants_on_their_own_table_without_an_admin_role`, `deny_still_requires_an_admin_role_under_ranger_delegate` | Who may WRITE policy. Spark writes none. |
| `one_table_grant_writes_the_namespace_it_needs`, `revoking_write_leaves_an_independent_read_grant_intact` | Assertions about the Ranger policies SQE authors, checked against Ranger directly rather than through a query. |
| `insert_does_not_confer_storage_relocation` | `INSERT` must not carry `table-full-metadata-relocation`. A privilege-expansion claim about SQE's own grant mapping. |
| `ranger_wiring_smoke_carol_can_query`, `fixture_round_trip_creates_services_and_policies`, `capture_live_tag_bundle` | Fixture and wiring guards. They fail loudly when the stack is misconfigured, so a green suite means something. |

### Spark only, and the reason

| Spark case | Why SQE has no counterpart |
|---|---|
| `object_denial_survives_the_frontend_defer_policy` (row 10) | SQE ignores `policyType-0` on the frontend service entirely. The defer item exists because Kyuubi default-denies without it. There is nothing to assert on the SQE side. |
| `mismatched_identity_reveals_the_two_tier_trust_split` | Deliberately hands the two tiers different identities: Polaris verifies a JWT signature, Kyuubi trusts an asserted OS username. SQE has one identity per session. |
| `a_service_account_catalog_in_the_session_defeats_per_user_identity` | A leftover root-credentialed catalog alias in the Spark session. SQE has no equivalent alias mechanism. |
| `a_failed_projection_rolls_back_the_tag` | The tag projector writes the Iceberg property AND Ranger's tag store. When the second write fails the first is rolled back, because a tag in one store and not the other is row 19 again with the statement reporting success. |

## What the table is actually worth

Of twenty-one rows, two carry no comparison at all: one because I never ran the
identical statement through both engines, one because I did not measure the Spark
side. Of the nineteen that do compare, fifteen agree, including every row filter and
every mask written as a portable expression, and including tag-based masking once the
association is projected. Row 1 agrees on the outcome and differs only in the message.

Four diverge: a mask vocabulary, a precedence order, an unprojected tag, and a tier
that answers before the one you think is deciding.

That is a working multi-engine governance story, and it is not a story Databricks or
Snowflake tells, because in both of those the engine IS the policy authority and
there is only ever one engine to ask.

The row I would actually act on first is none of the four. It is row 20, where both
engines agree and both are wrong: a rename silently unmasks a column. Agreement is
what this whole exercise was set up to look for, and it turns out agreement is not
the same as correctness. A cross-engine comparison finds the places two
implementations disagree. It is structurally blind to the places they share an
assumption, and "the tag association is keyed by column name" is exactly that kind
of shared assumption.

If you are evaluating this pattern, the useful question is not whether the engines
agree. It is whether you can enumerate where they do not, and whether you have some
other check for the things they would get wrong together. That is what the table is
for, and it is why every cell that says **not measured** says it instead of guessing.
