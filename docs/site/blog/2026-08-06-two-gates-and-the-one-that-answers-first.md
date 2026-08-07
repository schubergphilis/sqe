---
title: "Two gates, and the one that answers first"
description: "Spark was connecting to our catalog as root, which meant every access-control policy we had written applied to exactly one engine. Fixing it took no engine code: Polaris already authorizes against the OIDC identity, so a per-user token was the whole change. Then we ran both enforcement tiers together for the first time and found they compose in an order nobody documents, with Spark's plugin answering before the catalog is asked. Along the way two of our own new tests passed for the wrong reason, the two engines turned out to disagree about which mask wins, and renaming a column silently removes its protection. Here is the full evaluation order, what we measured, and why Databricks and Snowflake do not have this class of problem."
pubDate: "2026-08-06"
author: "Jacob Verhoeks"
tags:
  - "security"
  - "ranger"
  - "spark"
  - "iceberg"
  - "governance"
---

*August 6, 2026*

Our access control worked. Grants, denies, row filters, column masks, tag-based
masking, all of it enforced and all of it tested against a live Apache Ranger.

It applied to one engine.

Spark sat in the same quickstart, reading the same Iceberg tables from the same
Apache Polaris catalog, and it connected with this line:

```
spark.sql.catalog.sales_wh.credential  root:polaris-root-secret
```

Every policy we had written was invisible to it. Not bypassed by a bug. Bypassed by a
credential, in a config file, in a demo we shipped.

## The fix was a token, not a feature

Polaris runs its own Ranger authorizer, keyed on the federated OIDC identity. It
already asks Ranger whether the caller may load a table. The caller was just the wrong
person.

So we gave Spark's Iceberg REST catalog a per-user Keycloak token instead of the
service account:

```
spark.sql.catalog.<c>.token=<the user's JWT>
spark.sql.catalog.<c>.token-refresh-enabled=false
```

That is the entire object-level change. No engine code. Measured immediately: bob
reads a table his role is granted, and on one nobody is granted he gets

```
org.apache.iceberg.exceptions.ForbiddenException: Forbidden:
  Principal 'bob' is not authorized for op 'LOAD_TABLE'
```

The second config line is load-bearing and easy to miss. Left at its default, Iceberg
takes your external JWT and exchanges it against Polaris's own token endpoint. The
identity reverts to the service account, every denial test passes, and you have
proved nothing. We found that one before it cost us anything, which was luck as much
as care.

There is a second trap in the same area, and we found it later, by accident, while
measuring something else. **A per-user token governs only the catalog it is attached
to.** If the session still has a service-account catalog pointed at the same
warehouse, that is a second identity sitting right there, and the user picks which one
by naming it:

```
bob's session, per-user token on catalog `p`:
  SELECT count(*) FROM p.ac.orders          -> denied, LOAD_TABLE
  SELECT count(*) FROM sales_wh.ac.orders   -> 3
```

Same table, same session, one alias away. Our own quickstart ships
`spark.sql.catalog.sales_wh.credential = root:...`, so anyone following the per-user
recipe while leaving that in place has added an identity rather than replaced one.

The session cannot defend itself either. Overriding the alias's `token` with the
user's JWT changes nothing, because Iceberg prefers `credential` when both are set.
Remove the service-account catalog; shadowing it does not work.

Writes behave the same way, with one wrinkle. An unauthorized `INSERT` is refused at
`ADD_TABLE_SNAPSHOT`, which is the snapshot commit, not the load. The table is
untouched and the row count does not move. But the data files were already staged, so
a denied writer can leave orphan files in object storage at will. Authorization holds.
Storage hygiene does not.

## Then we ran both tiers at once

Here is the part we did not expect, and the reason this post exists.

There are two enforcement tiers. Polaris decides whether you may load the object, from
the `polaris` Ranger service. The engine decides which rows and columns you see, from
a shared service we call `query`. We had probed each of them. We had not probed them
together.

With both live, a read that Polaris permits fails:

```
org.apache.kyuubi.plugin.spark.authz.AccessControlException:
  Permission denied: user [bob] does not have [select] privilege on [ac/orders/id]
```

Kyuubi checks its own privilege **first**, and default-denies without a matching
`policyType-0` access policy. SQE ignores `policyType-0` entirely, because its object
gate is Polaris. So the same grant works in one engine and fails in the other, and the
Spark failure looks exactly like a Polaris bug.

Two probes, each with one tier switched off, is how that gap stayed hidden. That is
worth saying plainly, because isolating a variable is normally the right instinct. Here
it isolated away the only interaction that mattered.

The fix is a decision, not a patch. Object level belongs to Polaris, so the `query`
service carries one deliberate blanket allow that makes Kyuubi defer:

```
policyType-0   database=*  table=*  column=*   group=public
               select, update, create, drop, alter, index, lock, read, write
```

Every one of those access types has to be listed. Kyuubi checks `update` for `INSERT`
and `create` for DDL, and a missing one short-circuits exactly as above.

Read out of context that policy says "everyone may select everything", which is why
there is a test whose only job is to prove it grants no data access: with the item in
place and no Polaris grant, the read is still refused, by Polaris. Delete the item to
tighten security and you break Spark while gaining nothing.

We could not even give it a self-documenting name. Creating a hive-type service makes
Ranger auto-generate `all - database, table, column` over exactly that resource
signature, and it owns the signature:

```
Validation failure: error code[3010], reason[Another policy already exists for
matching resource: policy-name=[all - database, table, column]]
```

Every other wildcard shape is taken by a sibling auto policy. So the item goes in
through Ranger's grant API, which merges into the existing match, and the intent lives
in a comment and in the docs rather than in a name. Not satisfying. Correct.

## Our own tests lied to us twice

We write mutation checks because passing tests are not evidence. The checks earned
their keep twice this round, on tests we had just written.

**The guard test asserted the wrong service.** It checked that the defer item existed,
but on the service SQE reads, while Kyuubi reads whatever its container config names.
A precondition asserted against a component that never sees it is worse than no
precondition, because it reads as proof. The plugin's own cache filename gave it away.

Then the corrected check was still wrong. It asked whether *any* `policyType-0` policy
granted group `public` a select, and Ranger seeds an `Information_schema` policy that
does exactly that. Revoke the defer item and the check still returned true. Matching
the resource, not just the item, fixed it.

**The tag-parity test passed with the feature disabled.** Ranger's tag store is global
and persists across runs. Our fixture cleaned policies and never touched it. So a run
that projected a tag left the association behind, and the next run read it happily
with projection turned off. The test proved nothing for as long as it existed.

The first attempt to fix that cleanup also did nothing, because
`DELETE /service/tags/resource/{id}` answers 500 while the resource still carries an
association. Silent 500, swallowed by a `let _ =`. It now goes through the bulk delete,
and the mutation fails properly: with projection off, Spark returns `111-11-1111` while
SQE masks it. That failure message is the whole feature in one line.

## The tag projector, and the bug it shipped with

Tag associations live in the Iceberg table property `sqe.column-tags`. We chose that
deliberately, so tags travel with the table. Kyuubi reads Ranger's tag store and has
no reader for Iceberg properties.

So a tag-masked column was protected in SQE and returned raw by Spark. The fix
projects the association into Ranger's tag store on `SET TAG`, keeping the Iceberg
property as the source of truth. One `PUT /service/tags/importservicetags` does it, and
`op: add_or_update` merges, so one table's projection does not disturb another's.

The two writes cannot be atomic, and that forced a real choice. We roll back the
Iceberg property when the projection fails. Keeping it would mask the column in SQE
while Spark returned it raw, and the statement would have reported success. A silent
one-sided protection is worse than a failed statement.

The projector shipped with a bug that our own test caught. `UNSET TAG` did not stop
Spark masking. The delete document was built from the new tag map, and a column whose
last tag was just removed is **absent** from that map, so the delete named nothing and
the stale association survived. The code comment claimed to handle this. It did not.
The projector now takes the previous map as a required argument.

That direction fails closed, so it was over-masking rather than a leak. It was still
two engines disagreeing about the same column, which is the thing this entire exercise
exists to prevent.

## The two engines disagree about which mask wins

Put a resource mask and a tag mask on one column. SQE applies the resource mask.
Kyuubi applies the tag mask.

Stock `RangerBasePlugin` evaluates tag policies before resource policies. Kyuubi is
following Ranger's ordering. **We are the ones who differ**, and we had it pinned as
intended behaviour on our side without knowing it disagreed with every other
Ranger-plugin engine.

Neither leaks. Both mask. But whichever mask is weaker becomes the effective one for
whoever picks that engine, so the same column is governed differently depending on how
it is read. My view is that we should move to tag-first and match Ranger. Being the odd
one out on a security-relevant evaluation order is the kind of difference that
surprises people at the worst possible moment. It is a behaviour change to a shipped,
tested rule, so it is a decision to take deliberately rather than a bug to fix quietly.

## Renaming a column removes its protection

One more, found by asking a question we had not asked: what does DDL do to a policy?

Nothing in the schema-change path rewrites `sqe.column-tags`, and that property is
keyed by column name. So `ALTER TABLE ... RENAME COLUMN ssn TO tax_id` leaves the tag
naming a column that no longer exists.

What happens next is worse than I predicted when I wrote the test. I expected the mask
to stop applying in both engines. Measured, the two engines break DIFFERENTLY:

```
SELECT id, tax_id FROM ...
  SQE:   [["1"], ["2"], ["3"]]                        one column, no error
  Spark: [["1","111-11-1111"], ["2","222-22-2222"]]   the raw value
```

SQE drops the column from the result entirely, silently, without an error. Spark
returns the data raw. So the stricter engine hides a column you asked for and the other
hands over the unmasked value, and neither tells you anything is wrong. I do not yet
know the mechanism on the SQE side. I know what it does.

A query that returns fewer columns than it projected, with no error, is a defect
independent of governance.

The rename gap is not a Spark problem. It affects SQE on its own, it has been there as
long as tag-based masking has, and a rename is exactly the sort of routine schema change
someone makes without thinking about governance. It is now an executable test that
asserts the gap as current behaviour, phrased so that closing the gap makes the test
fail loudly.

Adding a column was supposed to be the benign sibling. The new column is readable under
the existing grant, because the object tier has no column level, and unmasked, because
no policy names it. Both intended.

It also does not work. With a mask on `ssn`, adding a column and then selecting it
fails outright:

```
PhysicalExpr Column references column 'nickname' at index 2 (zero-based)
but input schema only has 2 columns: ["id", "ssn"]
```

The rewritten plan and the scan schema disagree. No leak, so this one fails closed, but
a governed table becomes unqueryable after a routine `ADD COLUMN`. Also unfixed.

Two questions about DDL, two defects. We had never asked what a schema change does to a
policy.

## Why Databricks and Snowflake do not have this class of problem

In Unity Catalog and in Snowflake, the engine is the policy authority. One system of
record holds the grants, evaluates them, and enforces them. There is one answer to who
can see a column, and two engines cannot disagree, because there is one engine.

We split the roles three ways. Polaris is the object authority. Ranger is the policy
store. Each engine enforces the fine-grained tier itself. That is what lets one policy
set govern SQE and Spark at the same time, which neither Databricks nor Snowflake
offers for a foreign engine.

Every divergence in this post is the price of that property.

Some of the comparison flatters us. Ranger deny items override allow items, so
"everyone in analytics except the contractors" is one policy item here and a
role-modelling exercise in both of the others, neither of which has a negative grant.
Snowflake's requirement that you hold `USAGE` along the whole path is the same
traversal problem our three-level grant expansion solves, except Snowflake makes the
operator write it and we write it automatically.

Some of it does not. Snowflake secure views evaluate with the owner's privileges, so a
view is a genuine privilege boundary and the normal way to hand out narrowed access.
SQE views are not a privilege boundary at all: we expand the view and plan against the
base tables, so the reader needs a grant there too. Masks still apply through a view
and cannot be dodged, which is the part we would defend. But do not reach for an SQE
view to grant indirect access.

And Snowflake's tag-based masking is the exact analogue of ours, with one difference
that explains this whole post. Snowflake stores the tag association in its own
metadata, so there is one place to look. We store it in the Iceberg property so it
travels with the table, then project it into Ranger so Spark can see it. The
sovereignty property and the consistency risk are the same decision viewed from two
sides.

If you run one engine, take the single authority. It is genuinely simpler and this post
is largely a catalogue of what it costs to give it up. If you need Spark and SQE on one
policy set, or you need the policy store to be something you operate yourself, then the
split is worth it, and the gap table in
[the evaluation-order reference](/book/features/access-control-evaluation-order) is
what you are signing up for. Read it first.

Nineteen tests now cover the Spark path, nine on object level and ten on the
fine-grained tier. Three of them assert a divergence or a gap rather than parity: the
named mask type that renders differently per engine, the precedence disagreement, and
the rename that removes protection. Writing down what is actually true beat pretending
the two engines agree.
