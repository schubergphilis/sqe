---
title: "Every ACL we can write, and which system enforces it"
description: "A working catalogue of the access control SQE and Spark share through Apache Ranger: object grants at the catalog, five column masks, row filters, tags that span tables, four personas built from the same primitives, and the two places the engines still render differently. Everything here is a line from a run that finished 43 of 43, on an EU bank fixture rather than a toy table. Includes the reason there are two enforcement points rather than one, and what changed when we moved every privilege write onto an authenticated endpoint."
pubDate: "2026-08-14"
author: "Jacob Verhoeks"
tags:
  - "security"
  - "ranger"
  - "polaris"
  - "spark"
  - "iceberg"
  - "access-control"
---

An earlier post, [One policy model, two engines](/blog/2026-08-10-one-policy-model-sqe-and-spark/), derived the model: one Ranger, two gates, and the order they answer in. That derivation still holds and this post does not repeat it. What follows is the catalogue: every kind of access control we can actually write today, which system enforces each one, and what each looks like when two different engines read the same table.

The fixture changed since then, and it matters. A three-row table called `orders` with a column named `ssn` proves the mechanics work. It does not tell you whether the mechanics survive a real governance model. So the demo now runs on an EU retail bank: a twelve-row customer register and a twenty-four-row payment ledger, with national identifiers, IBANs, dates of birth, nationality, residency, PEP flags, risk scores, cross-border counterparties and AML alerts.

Every number and every rendered value below is copied from a run that finished 43 of 43 with exit 0.

## Why two enforcement points and not one

Polaris decides whether you may load a table. The query engine decides what you see once you have loaded it. That split is not an implementation accident, and it is the thing to understand before the catalogue makes sense.

A catalog can answer "may this principal open this table" because it knows the table exists and who is asking. It cannot answer "may this principal see the `national_id` column, and only for EU-resident customers", because that answer is not a yes or no. It is a rewrite of the query. Row filters and column masks are transformations of a plan, so they belong where plans are built.

The consequence worth stating plainly: a table is reachable only if BOTH gates agree, and they fail differently. The object gate refuses to hand over the table. The data gate hands it over with columns nulled and rows removed. When a grant is missing, you get nothing. When a mask applies, you get a result set that is smaller or blanker than the raw table, with no error at all.

Both gates read the same Ranger. Object grants live in the `polaris` service, fine-grained rules in a service named `query`. One server, two service instances, one place to look when someone asks who can see what.

## Gate one: object grants

`GRANT` and `REVOKE` are SQL in SQE, and they write Ranger policies. Nothing SQE-specific happens at this tier: Polaris runs its own Ranger plugin against the OIDC identity, so the same grant governs Spark.

```sql
GRANT SELECT ON sales_wh.acparity.customers TO ROLE "analyst";
GRANT INSERT ON sales_wh.acparity.customers TO ROLE "analyst";
GRANT SELECT ON VIEW sales_wh.acparity.customers_eu TO ROLE "analyst";
REVOKE ALL PRIVILEGES ON sales_wh.acparity.customers FROM ROLE "analyst";
```

Three properties of this tier are worth knowing before you rely on it.

**A grant writes more than one policy.** A table-level policy is inert on its own, because SQE resolves a table only through a namespace whose metadata it could read. So one `GRANT SELECT ON a.b.c` writes catalog-level `namespace-list`, namespace-level `namespace-properties-read`, and then the table. That widens namespace-NAME visibility across the catalog, which is a real tradeoff: separate catalogs are the answer when namespace names are themselves sensitive.

**`REVOKE SELECT` does not always stop reads.** `table-data-write` expands to include `table-data-read`, because a writer that cannot read its own table is useless. A principal holding INSERT keeps reading after `REVOKE SELECT`, and the statement reports success. `REVOKE ALL PRIVILEGES` exists for exactly that reason: it reads the access types the grantee actually holds and removes them, instead of planning them from a privilege name.

**Denial is invisible, not forbidden.** A user with no grant sees `table 'sales_wh.acparity.customers' not found`. That is deliberate. An explicit "permission denied" tells a prober that the table exists, which is itself a disclosure. Spark answers the same question with `Principal 'dave' is not authorized for op 'LOAD_TABLE'`, because Polaris speaks its own error to the Iceberg client.

## Gate two: the five column masks

Masks are Ranger data-mask policies, authored as SQL, applied by rewriting the plan above the scan. Five kinds are in the demo, on five columns of one table, all targeting the role `engineer`:

```sql
CREATE OR REPLACE POLICY "risk-null" ON TABLE sales_wh.acparity.customers
  COLUMN MASK MASK_NULL TO ROLE engineer ON COLUMN risk_score;

CREATE OR REPLACE POLICY "nid-last4" ON TABLE sales_wh.acparity.customers
  COLUMN MASK MASK_SHOW_LAST_4 TO ROLE engineer ON COLUMN national_id;

CREATE OR REPLACE POLICY "iban-hash" ON TABLE sales_wh.acparity.customers
  COLUMN MASK MASK_HASH TO ROLE engineer ON COLUMN iban;

CREATE OR REPLACE POLICY "dob-year" ON TABLE sales_wh.acparity.customers
  COLUMN MASK MASK_DATE_SHOW_YEAR TO ROLE engineer ON COLUMN dob;

CREATE OR REPLACE POLICY "name-mask" ON TABLE sales_wh.acparity.customers
  COLUMN MASK MASK TO ROLE engineer ON COLUMN full_name;
```

Read as a governance statement rather than a feature list, that set says: the analyst may not see the internal risk score at all, may confirm the last four digits of an identifier without learning it, may join on an account without reading the account number, may know a customer's age bracket without their birthday, and may not read a name.

Here is what one row looks like to a role with no mask policy, and to `engineer`:

```
no mask policy names her (alice)
 | cust_id | full_name      | national_id | dob        | iban                 | risk_score |
 | 1       | Sanne de Vries | 184729103   | 1978-03-14 | NL91ABNA0417164300   | 12         |

engineer (bob)
 | cust_id | full_name      | national_id | dob        | iban                             | risk_score |
 | 1       | Xxxxx xx Xxxxx | xxxxx9103   | 1978-01-01 | 701502320c05f830e08c...5998      |            |
```

The masks compose with each other and drop no rows. `MASK_HASH` is the one to think about: it produces a stable pseudonym, so a masked IBAN still joins and still counts, which is what makes hashing useful rather than merely destructive.

## Gate two: row filters

A row filter is a predicate injected above the scan. In the demo it reads as GDPR data residency:

```sql
CREATE OR REPLACE POLICY "eu-rows" ON TABLE sales_wh.acparity.customers
  ROW FILTER TO ROLE engineer USING (residency_region = 'EU');
```

Seven of twelve customers are EU-resident, so `engineer` sees seven rows and every other role sees twelve. The filter and the masks apply together: `engineer` gets seven rows with five columns protected, in one plan.

Filters also work on a second table with different semantics. The audit role has no masks at all and one restriction, a retention window on the ledger:

```sql
CREATE OR REPLACE POLICY "retention-rows" ON TABLE sales_wh.acparity.payments
  ROW FILTER TO ROLE auditor USING (booked_at >= DATE '2019-01-01');
```

Eighteen of twenty-four payments fall inside the window. The register is untouched, which is the point: a filter is scoped to the table its policy names, and nothing leaks sideways.

## Tags: one rule, many columns, two tables

Column masks name a column. Tags name a meaning, and the rule follows the meaning wherever it is attached. Associations live in the Iceberg table properties under `sqe.column-tags`, so they travel with the table, and SQE projects them into Ranger's tag store so Spark sees them too.

```sql
ALTER TABLE sales_wh.acparity.customers
  SET TAGS (phone = ('ACPARITY_ACCOUNT'), nationality = ('ACPARITY_SPI'),
            national_id = ('ACPARITY_IDENTITY'), full_name = ('ACPARITY_IDENTITY'));

ALTER TABLE sales_wh.acparity.payments
  SET TAGS (counterparty_iban = ('ACPARITY_ACCOUNT'));

CREATE OR REPLACE POLICY "account-tag" ON TAG ACPARITY_ACCOUNT
  COLUMN MASK CUSTOM TO ROLE fraud_analyst USING ('REDACTED');
```

One policy, written once, covers `customers.phone` and `payments.counterparty_iban`. That is the property a per-column mask cannot give you, and it is why the tag vocabulary carries a reason rather than a location: `ACPARITY_ACCOUNT` for account identifiers, `ACPARITY_SPI` for article 9 special-category data, `ACPARITY_IDENTITY` for direct identifiers.

Three behaviours we test because each one could plausibly go the other way. A tag with no policy is inert, in both engines. A tag whose mask type Ranger does not understand fails closed, restricting the column rather than passing it through. And a column carrying both a resource mask and a tag mask resolves to the tag mask, matching the order Spark's plugin implements, configurable through `policy.mask-precedence`.

One constraint shapes the vocabulary: Ranger allows one policy per resource signature, so a single tag cannot carry two different masks for two different roles. Hence separate tags per reason rather than one `PII` tag with per-role variants.

## The same primitives, four personas

Nothing above is a feature for a role. The interesting part is that four different governance postures come out of the same three primitives, and the demo runs one query as each of them. The SQL never changes. Only who asked.

```sql
SELECT c.cust_id, c.full_name, c.national_id, c.residency_region, c.risk_score,
       p.booked_at, p.counterparty_iban, p.counterparty_country
FROM sales_wh.acparity.customers c
JOIN sales_wh.acparity.payments p ON p.cust_id = c.cust_id
WHERE p.amount_eur > 5000
ORDER BY c.cust_id, p.pay_id;
```

**analyst (alice)** reads six rows, unmasked. No policy names her.

**engineer (bob)** reads four rows, EU residents only, with name, identifier, IBAN, date of birth and risk score protected.

**fraud desk (erin)** reads all six rows including the non-EU ones, with `full_name` and `national_id` nulled and the counterparty account `REDACTED`, and with `risk_score` visible. Data minimisation: every jurisdiction, no identity, the signal she needs to do the work.

**audit (frank)** reads the register unmasked, and four of six payment rows, the two pre-2019 ones removed by the retention window.

**admin (carol)** reads the same four masked rows Bob does. She holds `sqe_admin`, and also `engineer`. Being an administrator at the object gate is not an exemption from the data gate.

That last one was a mistake in our own demo before we ran it. The panel labelled her "every row, every column", and the run said otherwise.

## Everything above governs Spark too

The demo asserts every one of these through both engines and compares full result sets, cell by cell. That is the claim worth making precisely: not "Spark also has access control", but "the same policy produces the same rows and the same masked values in a different engine".

Spark reaches the same two gates by different routes. Its Iceberg REST catalog gets a per-user Keycloak token, so Polaris authorizes the human rather than a service account. Its data gate is Kyuubi's `RangerSparkExtension` reading the same `query` service. No SQE code participates in a Spark query.

Two divergences survive, and we assert both rather than skipping them.

**Named masks render differently.** SQE follows the Hive servicedef transformer; Kyuubi applies its own character classes.

```
SQE:    1 | xxxxx9103        Spark:  1 | nnnnn9103
        2 | xxxxx0214                2 | nnnnn0214
```

Same protected digits, different filler. `MASK_HASH` is the same story with a different algorithm: SQE emits sha256, Kyuubi md5, so we assert digest length and the absence of the raw value rather than a fixed digest.

**A row filter reading a tag-masked column disagrees.** Tag the column the filter reads, and SQE evaluates the filter against stored values then masks what survives, while Kyuubi injects its masking projection below the row filter, compares `residency_region` against the masked literal, and matches nothing. Two rows against zero. We assert the counts on both sides, because an empty result and an error look identical from the outside.

Everything else is byte-identical, including tag masks once the association is projected.

## Who granted this, and when

`SHOW GRANTS` answers the auditor's question:

```
| privilege             | resource                    | grantee | granted_by | granted_at           |
| table-data-read       | sales_wh.acparity.customers | analyst | carol      | 2026-08-14T11:51:26Z |
```

That column was empty until recently, and the reason is worth recording. Ranger stores policy provenance, but the endpoint we read it from omits the fields, and the endpoint we used to WRITE through recorded the SQL caller only as a side effect of its own authorization model. Moving privilege writes onto Ranger's authenticated policy API meant `createdBy` became SQE's service identity instead of the human. So the caller is now recorded deliberately, in a policy label, and `SHOW GRANTS` prefers it.

`CHECK ACCESS` answers the other half, and it answers it without running a query:

```sql
CHECK ACCESS SELECT ON sales_wh.acparity.customers FOR USER "alice";
-- true, Allowed via ROLE 'analyst'
CHECK ACCESS SELECT ON sales_wh.acparity.customers FOR USER "dave";
-- false, No matching grant
```

A denial and a slow policy refresh look the same from a query result. They do not look the same to `CHECK ACCESS`, which is why it belongs in any script that asserts a denial.

## Authority: who may grant

The default is an admin role: an operator with `admin_roles` may write grants, and everyone else may not. That is one bit of authority for a whole deployment, which is the wrong shape for a lake with many owners.

The alternative is per-object. Set `grant_authority = "ranger-delegate"` and SQE stops applying its own role check, handing the decision to Ranger, which authorizes the named grantor against `delegateAdmin` on that resource. `GRANT ... WITH GRANT OPTION` is what sets that flag. The check is strict in a way worth measuring: a grantor holding delegate admin for `table-data-read` is refused when the request names `table-data-write`, and delegate admin on a table does not confer it on the catalog above.

Two properties of that mode to design around. `delegateAdmin` does not cascade upward, so a table-level owner cannot write the catalog and namespace traversal policies a grant plan needs, and those levels have to be granted by someone who holds them. And Ranger lets a delegate pass grant-option onward for types they hold, so if you want non-transferable ownership the engine has to gate the flag itself.

## What we do not have

Column-level deny is not authorable through this surface. SQE can restrict a column, but only as a fail-closed outcome when a mask type is unmappable, so "this role cannot see that this column exists" is not something you can write today.

`MASK_NONE` as a break-glass exemption does not work either. Masks from every matching policy are unioned, so lifting a mask for one role needs Ranger evaluation-order priorities that SQE does not implement.

And renaming or dropping a column used to remove its protection silently, because tag associations are keyed by column name. Rename and drop now rewrite `sqe.column-tags` in the same commit as the schema change. The lesson generalises past that bug: a parity suite where both engines agree is not a suite that proves both are right.

## Run it

```bash
scripts/access-control-parity-demo.sh
```

Forty-three cross-engine comparisons, each printing the SQL, both engines' full output, and a verdict. It brings up Polaris, Ranger, Keycloak, an S3-compatible store, SQE and Spark, and leaves the tables in place so you can query them yourself. `AC_PARITY_SECTIONS="3,8,9"` runs the comparisons in selected sections only, which is the difference between minutes and a coffee break, because every Spark probe starts a JVM.

The parts that assert are separate from the parts that show. Panels print one query as five identities through SQE alone, because a JVM start costs seven seconds and an SQE query costs two hundred milliseconds. No panel can fail the run, so none of them can produce a false green.
