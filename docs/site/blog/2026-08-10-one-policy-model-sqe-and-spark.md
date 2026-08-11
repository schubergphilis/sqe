---
title: "One policy model, two engines: access control across SQE and Spark"
description: "A complete, executable account of how SQE and Spark query the same Iceberg table under the same Apache Ranger policies: Polaris object grants, row filters, column masks, tags, precedence, writes, views, and the differences that remain."
pubDate: "2026-08-10"
author: "Jacob Verhoeks"
tags:
  - "security"
  - "ranger"
  - "spark"
  - "polaris"
  - "iceberg"
  - "access-control"
---

*August 10, 2026*

A lakehouse stops being open the moment its security model depends on which engine
reads the table.

That is an easy failure to create. Spark runs the pipelines, another SQL engine serves
the dashboards, and both read the same Iceberg table. If each engine has a separate
policy language and a separate copy of every rule, those copies eventually drift. An
SSN is masked in one engine and raw in the other. A regional filter is updated for the
batch path but not for the interactive path. Both systems can pass their own tests and
the platform can still be wrong.

We wanted a stronger property for SQE: write a security rule once, store it in Apache
Ranger, then prove what happens by running the same query through SQE and Apache Spark
against the same data and user identity.

The result is not a claim of perfect byte-for-byte compatibility. Most portable rules
do agree exactly. Some Ranger features have engine-specific semantics, and pretending
otherwise would be more dangerous than documenting the difference. The useful result
is an executable contract: object access, writes, row filters, column masks, tags,
policy composition, precedence, views, and revocation are all exercised in one live
demo.

## First: there are not two Ranger servers

The quickstart runs one Ranger Admin 2.8 instance. Inside it are separate service
instances for separate kinds of decisions:

| Ranger service | Service definition | Decision | Enforcer |
|---|---|---|---|
| `polaris` | custom Polaris definition | May this principal discover, load, read, or modify this catalog object? | Apache Polaris |
| `query` | Ranger's built-in `hive` definition | Which rows and column values may this query return? | SQE or Kyuubi's Spark extension |
| `tag` | Ranger's built-in tag definition, linked to `query` | Which mask or filter follows a classification such as `PII` or `GEO`? | SQE or Kyuubi's Spark extension |

People often call the first two “the two Rangers.” Operationally they are one Ranger
server and two authorization planes. The linked `tag` service is the classification
half of the fine-grained plane, not another server to deploy.

That separation is necessary. The Polaris service definition understands catalogs,
namespaces, tables, views, snapshots, and metadata operations. It answers boolean
object-access questions. It does not define row filters or column masks. Ranger's Hive
service definition does define those fine-grained policies, and both query engines
already know how to consume them.

```text
                                  one Ranger Admin
                               ┌─────────────────────┐
                               │ polaris service     │
                               │ query + tag service │
                               └──────────┬──────────┘
                                          │ policies
         end-user token                   │
client ───────────────► SQE ─────────► Polaris ─────────► Iceberg metadata
                         │                 │
                         │ rewrites plan   └─ object gate (`polaris`)
                         └─────────────────── data gate (`query` + `tag`)

client ───────────────► Spark ─────────► Polaris ─────────► Iceberg metadata
         end-user token    │                 │
                           │ Kyuubi rewrites └─ object gate (`polaris`)
                           └─────────────────── data gate (`query` + `tag`)
```

The engines share data and policy, but not execution code. SQE rewrites a DataFusion
logical plan. Spark is rewritten by `RangerSparkExtension` from Kyuubi Authz. Agreement
between them is therefore meaningful: two independent implementations interpreted the
same policy the same way.

## The object gate belongs to Polaris

Both engines reach the same Iceberg REST catalog in Polaris. Every invocation carries
the user's own Keycloak bearer token. Polaris resolves the principal and asks Ranger's
`polaris` service whether the requested catalog operation is allowed.

SQE provides the SQL management surface:

```sql
GRANT SELECT ON sales_wh.acparity.orders TO ROLE "analyst";
GRANT INSERT ON sales_wh.acparity.orders TO ROLE "analyst";
REVOKE INSERT ON sales_wh.acparity.orders FROM ROLE "analyst";
REVOKE SELECT ON sales_wh.acparity.orders FROM ROLE "analyst";
```

These statements are not an SQE-local ACL. SQE translates them into Ranger policies on
the `polaris` service. Polaris enforces those policies for SQE and Spark alike.

A SQL privilege expands to the Polaris access types required by the operation. SELECT
needs discovery and metadata access before files can be read. INSERT needs the snapshot,
schema, property, and commit operations that can participate in an Iceberg write. This
is why SELECT and INSERT are tested independently: permission to read a table must not
silently authorize a snapshot commit.

The identity path matters here. The Spark catalog is deliberately not configured with
a root credential. The demo obtains a token for Alice, Bob, Carol, or Dave and passes it
to Spark's Iceberg REST catalog for that process. Token refresh is disabled because an
OAuth exchange could replace the end-user identity with a service identity. A parity
test that reaches Polaris as root proves nothing about user authorization.

The denial text differs. SQE follows an information-hiding posture and can surface a
denied table as not found. Spark commonly passes through Polaris's explicit
`LOAD_TABLE` 403. The messages are different; the security result is the same.

## The data gate belongs to the query engine

Once Polaris permits a table load, the `query` service answers a different question:
what may this user see inside that table?

SQE downloads Ranger's Hive-shaped policy bundle and injects filters and projections
before DataFusion optimization. Spark's Kyuubi extension performs the corresponding
rewrite in Spark. The table scan never hands an unfiltered logical result to the rest
of the query plan.

We manage these rules through SQL as well:

```sql
CREATE OR REPLACE POLICY "amount-null"
  ON TABLE sales_wh.acparity.orders
  COLUMN MASK MASK_NULL TO ROLE engineer ON COLUMN amount;

CREATE OR REPLACE POLICY "ssn-last-four"
  ON TABLE sales_wh.acparity.orders
  COLUMN MASK MASK_SHOW_LAST_4 TO ROLE engineer ON COLUMN ssn;

CREATE OR REPLACE POLICY "eu-only"
  ON TABLE sales_wh.acparity.orders
  ROW FILTER TO ROLE engineer USING (region = 'EU');
```

SQE parses and validates this Databricks-inspired administrative SQL, then writes the
corresponding Ranger policy. Spark does not have an equivalent SQL command for
creating Ranger policies. That is a management-surface difference, not an enforcement
difference: after SQE writes the rule, both engines consume it.

This distinction also prevents a misleading demo. The policy creation statement is
printed as `SQL (SQE/carol)`. Every data query is then printed twice, once under SQE and
once under Spark. Nothing is hidden behind an unexplained Ranger REST POST.

## Tags live with the table and are projected for Spark

A resource policy names a concrete column. A tag policy says something broader, such
as “mask every column classified as PII.” The rule belongs in Ranger, but the
association between a column and a classification is table metadata:

```sql
ALTER TABLE sales_wh.acparity.orders
  SET TAGS (phone = ('PII'));

CREATE OR REPLACE POLICY "pii-redaction"
  ON TAG PII
  COLUMN MASK CUSTOM TO ROLE engineer USING ('XX');
```

SQE stores the association in the Iceberg property `sqe.column-tags`. That makes the
classification travel with the table instead of trapping it inside a query engine.
SQE can read that property directly. Kyuubi cannot, because the standard Ranger plugin
looks in Ranger's tag store. With `project-tags = true`, SQE therefore projects every
SET or UNSET into Ranger as part of the DDL operation. The Iceberg property remains the
source of truth; the Ranger copy is the interoperability projection Spark needs.

Projection failure is not harmless. If Iceberg says PII while Ranger does not, SQE
masks and Spark returns raw data. The implementation rolls the table-property change
back when projection fails, so a successful statement cannot knowingly leave the two
engines in that split state.

## The fixture and the four identities

The parity demo creates one six-column Iceberg table and three rows:

```sql
CREATE TABLE sales_wh.acparity.orders (
  id BIGINT,
  region VARCHAR,
  amount DOUBLE,
  ssn VARCHAR,
  email VARCHAR,
  phone VARCHAR
);

INSERT INTO sales_wh.acparity.orders VALUES
  (1, 'EU', 10.0, '111-11-1111', 'a@x', '555-0001'),
  (2, 'US', 20.0, '222-22-2222', 'b@x', '555-0002'),
  (3, 'EU', 30.0, '333-33-3333', 'c@x', '555-0003');
```

Each column carries exactly one kind of rule. `region` is read by the row filter,
`amount`, `ssn` and `email` carry resource masks, and `phone` carries the tag mask.
Keeping them separate is deliberate: overlapping two rules on one column is where the
engines stop agreeing, and each overlap gets its own section later.

Carol is the administrator. Alice belongs to `analyst`. Bob belongs to `engineer`.
Dave belongs to neither demo role. These identities provide a control beside every
policy claim:

| Identity | Purpose in the demo |
|---|---|
| Carol | Creates the fixture and authors policy |
| Alice / `analyst` | Granted and deliberately unmasked control |
| Bob / `engineer` + `analyst` | Filtered and masked subject |
| Dave / no role | Proof that role membership is required |

Bob holding two roles is deliberate, and the closing REVOKE check is where it bites.
Removing the `engineer` grant does not lock Bob out, because the `analyst` grant from
section one still reaches him. Revoking analyst SELECT is not enough either, and this
is the part worth writing down: the grant profile expands `table-data-write` to include
`table-data-read`, so the INSERT granted earlier keeps conferring read. `REVOKE SELECT`
reports success and the rows still come back.

That is the right behaviour. A writer that cannot read its own table is useless, and
Polaris asks Ranger for the exact access type an operation needs rather than applying
subsumption at query time, so the expansion has to happen when the grant is written.
The consequence for anyone closing a gate: revoke every privilege that implies read,
not the one named SELECT. Measured one statement at a time, engineer SELECT gone leaves
three rows, analyst SELECT gone still leaves them, and only analyst INSERT gone produces
the 403.

Controls matter. “Table not found” is not proof of a denial unless the same table is
shown to exist for an authorized user. A masked result is not proof of role scoping
unless a user outside that role sees the raw control value. The script establishes
both.

## What the complete parity demo proves

The older demo compared a small subset of the SQE-only transcript. The consolidated
script now carries every behavior family over:

| Area | Assertion through SQE | Assertion through Spark |
|---|---:|---:|
| Denial before GRANT and allow after GRANT | yes | yes |
| Role member versus user outside the role | yes | yes |
| SELECT separate from INSERT | yes | yes |
| Successful SQE write visible cross-engine | yes | yes |
| Successful Spark write visible cross-engine | yes | yes |
| `MASK_NULL` | yes | yes |
| `MASK_SHOW_LAST_4` | yes | yes, with documented rendering difference |
| `MASK_HASH` protects all values | yes | yes; keyed bytes and digest length are not a parity promise |
| Resource row filter | yes | yes |
| Tag mask and tag projection | yes | yes |
| Filter + three resource masks + tag mask in one plan | yes | yes |
| Resource-mask/tag-mask precedence | yes | yes, identical since `mask-precedence` defaults to `tag` |
| Row filter reading a tag-masked column | yes | yes, with documented ordering difference |
| Tag with no policy is inert | yes | yes |
| Invalid CUSTOM policy is rejected | SQL management check | no Spark policy DDL |
| `SHOW GRANTS` and `CHECK ACCESS` | SQL management check | no Spark equivalent |
| View grant type and base-table authorization | yes | query enforcement checked where supported |
| Denial after REVOKE | yes | yes |

Successful writes are important here. The demo first proves that SELECT does not imply
INSERT in either engine. It then grants INSERT, commits a row through SQE, and verifies
both engines see four rows. Carol deletes that row and both return to three. The same
sequence is repeated with the commit made through Spark. This catches both authorization
errors and stale cross-user table snapshots.

The combined policy query is intentionally compact:

```sql
SELECT
  count(*) AS rows_seen,
  sum(CASE WHEN phone = 'XX' THEN 1 ELSE 0 END) AS tag_masked,
  sum(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS amount_nullified,
  sum(CASE WHEN substr(ssn, 8, 4) IN ('1111', '3333')
            AND ssn NOT IN ('111-11-1111', '333-33-3333')
           THEN 1 ELSE 0 END) AS ssn_masked,
  sum(CASE WHEN length(email) >= 32
            AND email NOT IN ('a@x', 'b@x', 'c@x')
           THEN 1 ELSE 0 END) AS email_hashed
FROM sales_wh.acparity.orders;
```

For Bob, every count must be two. The row filter removes the US row, while four masks
transform the two surviving rows. Composition is proven in one logical plan instead of
proving each policy only in isolation.

The email predicate asks whether the address was replaced by something hash-shaped, not
whether it is a specific length. SQE emits a 64-character sha256 digest and Kyuubi emits
a 32-character md5 digest, so a fixed length would assert the digest algorithm rather
than the protection.

## Where SQE and Spark intentionally do not claim byte parity

Shared policy does not automatically mean identical semantics. Three differences showed
up. One we closed, two we expose in the transcript rather than smoothing over.

First, Ranger's named `MASK_SHOW_LAST_4` is not byte-portable. SQE honors the Hive
service definition's transformer and renders `111-11-1111` as `xxx-xx-1111`. Kyuubi
uses its own character-class substitutions and renders `nnnUnnU1111`. Both hide the
same characters and preserve the same final four digits, but the strings differ.

When identical output matters, use a portable CUSTOM expression:

```sql
USING (concat('xxx-xx-', substr({col}, 8, 4)))
```

Both engines inject that expression and return identical bytes.

Second was a resource mask and a tag mask on the same column, and this one we closed
rather than documented. SQE chose the resource-specific mask on a most-specific-rule
reading; Kyuubi, following the standard Ranger plugin order, chose the tag mask. Neither
returned the raw SSN, so nothing leaked, but the same policy set rendered two different
values and that is not a policy model, it is two.

`policy.mask-precedence` now decides it, and the default is `tag`. Matching the plugin
order costs SQE the narrower reading and buys one answer per policy across both engines,
which is the property the whole exercise is about. `resource` restores the old behaviour
for anyone who wants the specific rule to win, and the parity demo diverges again when
they set it.

The choice is genuinely arguable, which is why it is a setting. A resource policy names
the column outright, so "most specific wins" is a defensible reading of intent. What
settles it is that Ranger is the shared store: a rule authored once should not mean two
things depending on which engine reads it.

Third, and the sharpest of the three, a row filter that reads a tag-masked column does
not compose the same way. SQE evaluates the filter against stored values and masks the
rows that survive. Kyuubi injects its masking projection *below* its row-filter marker,
so the filter compares the mask literal instead of the stored value. Tag `region` with
`PII` while `region = 'EU'` is the active row filter and the result is stark:

| Engine | Rows Bob sees |
|---|---:|
| SQE | 2, with `region` rendered as `XX` |
| Spark / Kyuubi | 0 |

Nothing leaks, so this is a correctness difference rather than a security hole, and it is
the reason the demo fixture keeps the filtered column and the tag-masked column apart.
Section 5b of the script then re-creates the collision on purpose and asserts the two row
counts, because a difference that is only discovered by accident tends to be rediscovered
by accident. The count is asserted rather than the empty result set: an empty result is
also what a failed query produces, and 0 is not.

`MASK_HASH` deserves a separate warning. SQE's hash is a keyed HMAC using an engine-held
key, and Kyuubi's is an unkeyed md5. The security property under test is that every value
is replaced by a digest and no row disappears. Those digest bytes and their length are not
a cross-engine compatibility contract. A keyed hash should not be weakened merely to make
it equal to another runtime's unkeyed function.

These are precisely the cases where a vague “Ranger compatible” label is insufficient.
The policy source is shared; the behavioral contract still has to name the semantics.

## Management SQL versus Spark SQL

Spark has SQL for querying and modifying Iceberg tables. It does not provide a native
SQL grammar for creating Ranger masks, row filters, or Polaris grants. Kyuubi Authz is
an enforcement plugin, not a Ranger policy administration interface.

SQE deliberately fills that administration gap with one SQL surface:

```sql
SHOW GRANTS ON sales_wh.acparity.orders;

CHECK ACCESS SELECT ON sales_wh.acparity.orders FOR USER "alice";

CREATE OR REPLACE POLICY "email-hash"
  ON TABLE sales_wh.acparity.orders
  COLUMN MASK MASK_HASH TO ROLE engineer ON COLUMN email;

DROP POLICY IF EXISTS "email-hash";
```

This is close in spirit to Databricks governance SQL: administrators describe grants,
filters, masks, and tags in SQL next to the data object. The backing store is Ranger,
not a proprietary metastore, and the syntax should be described as inspired by that
style rather than falsely presented as wire-compatible Databricks SQL.

The benefit is operational consistency. Administrators do not need embedded Ranger
credentials and JSON in every demo or migration. Ranger remains the audit and policy
system. SQE is the typed, validated SQL authoring interface. Spark remains an equal
consumer of the resulting policies.

## Views are names, not security boundaries

The demo also creates a view and grants its name:

```sql
CREATE OR REPLACE VIEW sales_wh.acparity.orders_eu AS
  SELECT id, region
  FROM sales_wh.acparity.orders
  WHERE region = 'EU';

GRANT SELECT ON VIEW sales_wh.acparity.orders_eu TO ROLE "analyst";
```

`SHOW GRANTS` must report `view-list` and `view-properties-read`, not table access
types. But the view is not a definer-rights privilege boundary. The engine expands its
plan and still authorizes the underlying table. A view grant does not smuggle access
past a missing base-table grant.

## Caches are part of the security model

This work found several bugs that only appear when identity, metadata, and policy
changes cross process boundaries.

Polaris's Ranger plugin polls policy, so GRANT and REVOKE are not instantaneous. The
demo polls for the documented state rather than sleeping a fixed number of seconds.
Each Spark assertion starts a new `spark-sql` process and clears its explicit development
policy-cache directory so it cannot inherit a stale bundle from the previous assertion.

SQE also has per-session catalog state. A row inserted by Alice and deleted by Carol
exposed a stale snapshot: Carol's session committed the delete, while Alice's session
continued reading the old table metadata. Successful INSERT, DELETE, UPDATE, MERGE,
TRUNCATE, and CTAS operations now invalidate the table across all session contexts.
The parity demo's write/delete sequence is the readable regression test for that fix.

Caching is not a performance footnote in an authorization system. A stale allow after
REVOKE and a stale table snapshot after DELETE are observable security behavior. Both
need bounded, tested invalidation.

## Run the contract yourself

Build and start the quickstart from the repository:

```bash
cd quickstart/polaris-ranger-keycloak
docker compose build sqe
docker compose up -d --force-recreate sqe
docker compose ps sqe
```

Then run the single-engine transcript:

```bash
./scripts/access-control-demo.sh
```

Or run the full cross-engine contract:

```bash
./scripts/access-control-parity-demo.sh
```

To reuse a healthy stack without bootstrapping it again:

```bash
AC_PARITY_NO_BOOTSTRAP=1 ./scripts/access-control-parity-demo.sh
```

The parity script takes longer because Spark starts a JVM for every independent probe.
That isolation is useful: each query gets the caller's own Polaris token and a fresh
Ranger policy view. The output prints every GRANT, REVOKE, policy statement, tag change,
query, engine result, and assertion. A nonzero exit means the documented behavior did
not occur.

## What we can honestly claim

SQE and Spark can govern the same Iceberg table from one Ranger Admin without keeping
two copies of every policy.

Polaris owns the object gate for both engines. The `query` and linked `tag` services own
fine-grained intent. SQE and Kyuubi independently enforce that intent in their logical
plans. SQE supplies the SQL management surface; Spark consumes the resulting policy.
Portable CUSTOM masks, resource row filters, tag projection, role scoping, writes, and
revocation can be checked live against the same data.

The remaining differences are not hidden: named mask rendering, keyed hash bytes, and
resource-versus-tag precedence are explicit parts of the test output. That is the
standard we want from cross-engine governance. “One policy” is only valuable when the
behavior is executable, the differences are named, and a future change has nowhere to
hide.
