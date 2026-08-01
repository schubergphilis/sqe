# Access control: what is supported, and what is proven

Two independent gates run on every query.

**Polaris gates the catalog.** `GRANT` and `REVOKE` in SQE become Apache Ranger
policies on the `polaris` service, and Polaris 1.5's embedded Ranger authorizer
enforces them. This answers "may this user load this object at all". SQE does no
filtering on this axis.

**SQE gates the data.** Row filters, column masks, and column restriction are
applied by rewriting the logical plan before DataFusion optimizes it, from
policies read out of a Ranger `hive` service. This answers "which rows and
columns may this user see".

A query must pass both. Revoking the coarse `SELECT` denies it at Polaris before
any mask is computed.

Everything marked **Proven** below has an executable assertion in
`crates/sqe-coordinator/tests/it/access_control_e2e.rs`, running against a live
Polaris, Ranger 2.8 and Keycloak, asserting decoded Arrow values. Run it with
`make test-access-control`. `scripts/access-control-demo.sh` walks the same
ground as a readable SQL transcript.

## Catalog gate (Polaris)

The `polaris` service-def resource hierarchy is
`root -> catalog -> {namespace -> table, principal, policy}`.

| Level | SQL | Supported | Proven |
|---|---|---|---|
| Table | `GRANT SELECT ON cat.ns.tbl TO USER u` | Yes | Yes |
| Table, via role | `GRANT SELECT ON cat.ns.tbl TO ROLE r` | Yes | Yes |
| Table write | `GRANT INSERT ON cat.ns.tbl TO ROLE r` | Yes | Yes |
| Table drop | `GRANT DROP ON cat.ns.tbl TO ROLE r` | Yes | Yes |
| Namespace | `GRANT USAGE ON SCHEMA cat.ns TO ROLE r` | Yes | No |
| Namespace create | `GRANT CREATE TABLE ON SCHEMA cat.ns` | Yes | No |
| Catalog | `GRANT CREATE SCHEMA ON cat` | Yes | No |
| All tables in schema | `GRANT SELECT ON ALL TABLES IN SCHEMA cat.ns` | Yes, as a table wildcard | Yes, including a table created after the grant |
| Future tables in schema | `GRANT SELECT ON FUTURE TABLES IN SCHEMA cat.ns` | Yes, same policy as ALL | Unit-tested (shape); the ALL case proves the behaviour |
| Deny precedence | Ranger deny item overrides an allow | Yes | Yes |
| Revoke | `REVOKE SELECT ON cat.ns.tbl FROM ROLE r` | Yes | Yes |
| Introspection | `SHOW GRANTS`, `CHECK ACCESS` | Yes | Yes |
| **View** | `GRANT SELECT ON VIEW cat.ns.v` | **No** | n/a |

### Views are not covered by the SQL grant surface

This is the one gap worth stating on its own, because the syntax parses and the
failure is not self-explanatory.

The `polaris` service-def declares no `view` resource level. It does declare six
view access types (`view-create`, `view-drop`, `view-list`,
`view-metadata-full`, `view-properties-read`, `view-properties-write`), and the
quickstart's admin bootstrap grants all of them, so an admin principal can work
with views. But SQE's privilege mapping never emits any of them: `SELECT`
expands to `table-data-read`, `table-properties-read`, `table-list`, and there
is no privilege that maps to a view access type.

`GRANT SELECT ON VIEW cat.ns.v TO alice` is worse than unsupported, it is
misleading. The parser produces a `Views` object that SQE's
`extract_grant_statement` has no arm for, so catalog, namespace and table all
resolve to `None` and the statement fails with

```
Ranger GRANT requires a catalog (use catalog.namespace.table)
```

even though a fully-qualified name was supplied. The same holds for
`ON ALL VIEWS IN SCHEMA` and `ON DATABASE`. Fail-safe, since nothing is written,
but the message points at the wrong thing.

Dropping the `VIEW` keyword (`GRANT SELECT ON cat.ns.v`) does write a policy,
with the view name in the `table` resource slot and table access types. Do not
rely on it: it grants table operations on a name that Polaris handles as a view.

To gate views today, author the policy directly in Ranger against the access
types you need.

### What DOES work on views, verified

Views are supported objects: an admin can create and query them. Two questions
matter more than the grant surface, and both were tested live against the
quickstart stack.

**Column masks survive a view. There is no bypass.** A view that projects a
masked column returns the MASKED value, because the view expands to a
`TableScan` of the base table and the plan rewriter runs on that scan. Verified
with a user who is both an admin (so the view loads) and a member of the masked
role: `xxx-xx-1111` reading the base table, `xxx-xx-1111` reading the view.
Creating a view over a protected table is not a way around masking.

**A non-admin cannot read a view at all.** Because no SQL privilege emits a
`view-*` access type, a user without the admin grant gets "table not found"
(Polaris hides rather than 403s). So the practical position today is: views are
an admin-only surface, and masking still applies there.

**Row filters break on views when the filter references an unprojected column.**
This is a real defect, and it is view-specific. A row filter on `region` against
a view declared as `SELECT id, ssn FROM orders` fails the whole query:

```
Plan rewrite failed: Internal error: Failed to create policy filter:
Schema error: No field named region.
Valid fields are ...orders.id, ...orders.ssn
```

The same filter on a DIRECT query with the same narrow projection
(`SELECT id, ssn FROM orders`) works and returns the filtered rows, so this is
not the general case: SQE injects the filter below the user projection and it
resolves fine. Only the view path fails.

Behaviour is fail-closed (a hard error, no rows, nothing leaked) but the message
is a DataFusion internal error that names neither the policy nor the view. This
is the same class as Kyuubi's `#6889`, which the quickstart bootstrap already
cites as the reason no row-filter policy is seeded for the Spark cross-compare.
Until it is fixed, a row filter and a narrow view over the same table are
mutually exclusive.

## Data gate (SQE plan rewriting)

### Column masks

The full Ranger `hive` built-in vocabulary is implemented.

| Ranger `dataMaskType` | Result | Proven |
|---|---|---|
| `MASK_NULL` | typed NULL, row count unchanged | Yes |
| `MASK_SHOW_LAST_4` | `111-11-1111` becomes `xxx-xx-1111` | Yes |
| `MASK_SHOW_FIRST_4` | `111-11-1111` becomes `111-xx-xxxx` | Yes |
| `MASK` | `X` / `x` / `n` per char class, punctuation kept. `EU` becomes `XX` | Yes |
| `MASK_HASH` | HMAC-SHA256 hex, keyed by `policy.mask_key` | Yes, against an out-of-band digest |
| `MASK_DATE_SHOW_YEAR` | `2021-05-04` becomes `2021-01-01` | Yes |
| `CUSTOM` | arbitrary SQL with `{col}` | Yes |
| `MASK_NONE` | explicit exemption | Unit-tested. It depends on Ranger policy EVALUATION ORDER, which is a property of the policy set rather than one policy, so an e2e case needs explicit priorities |

The hash case is asserted against a digest computed outside the engine
(`openssl dgst -sha256 -hmac`), so the implementation is not checking itself. A
plain SHA-256 of the same input is a different value, which is what proves the
mask key reached the UDF.

### Row filters, restriction, tags

| Capability | Result | Proven |
|---|---|---|
| Resource row filter | only admitted rows returned; other users unaffected | Yes |
| Column restriction | column nullified in place, stays in the schema so `SELECT col` still plans | Yes |
| Tag column mask | mask applies to every column carrying the tag, association from the Iceberg `sqe.column-tags` property | Yes |
| Tag row filter | one rule filters every table holding a tagged column | Yes, with the Ranger property below |
| Precedence | restriction beats mask; resource mask beats tag mask; row filters AND together | Resource-beats-tag proven live; the rest unit-tested |
| Role-conditional masking | `current_user()`, `current_role()`, `is_role_in_session()` const-folded per session | Unit-tested |
| Masks block predicate pushdown | `WHERE ssn = '...'` evaluates the masked value, never the raw one | Unit-tested |

### Tag row filters need one Ranger Admin property

Tag masks work out of the box. Tag row filters need

```xml
<property>
  <name>ranger.servicedef.autopropagate.rowfilterdef.to.tag</name>
  <value>true</value>
</property>
```

in `ranger-admin-site.xml`. Ranger copies each component's `dataMaskDef` into
the `tag` service definition unconditionally but copies its `rowFilterDef` only
when that property is true, and it defaults to false. No Ranger upgrade changes
this. Without it the policy POST is rejected with "tag policy can specify values
for one of the following resource sets: does not have any resource hierarchies",
which names resource hierarchies rather than the missing capability.

Also author tag mask types component-qualified (`hive:MASK_SHOW_LAST_4`). The
tag service definition never defines bare names.

## Failure behaviour

| Condition | Result | Proven |
|---|---|---|
| Ranger unreachable | deny all rows; enforcement resumes after recovery | Yes, by stopping the container mid-test |
| Tag state unknown | deny all rows. Unknown is not "untagged" | Yes |
| Unmappable mask type, resource or tag | column restricted, never returned raw | Yes |
| Tag carrying NO rule | **inert**: the column is returned raw | Yes |

The last two rows are easy to conflate and they behave differently, so it is
worth being explicit. A tag with no policy anywhere is not a protection, so
there is nothing to fail closed about and the column reads normally. A tag whose
policy names a mask SQE cannot build (a `CUSTOM` with no expression, or another
component's prefix such as `trino:MASK_NULL`) IS a protection SQE cannot honour,
so the column is restricted. Tagging a column does not protect it by itself;
the rule in Ranger is what protects it.
| Unparseable row filter | becomes `lit(false)`, deny all | Unit-tested |
| Table not mappable to a policy key | deny all rows | Unit-tested |

One default is deliberately not fail-closed. The resolved-policy cache is
fail-**stale**: a mask tightened in the Ranger console is not honored until the
cached entry expires, up to `[policy.ranger] cache-ttl-secs`. Grants issued
through SQE do not have this window, because `GRANT`, `REVOKE` and `SET TAGS`
flush the cache on commit. The window is asserted at both edges by
`cache_ttl_bounds_policy_staleness`.

## Known gaps

| Gap | Detail |
|---|---|
| View-level grants | No `view` resource level and no privilege mapping, so views are admin-only. See above. |
| Row filters through views | A filter referencing a column the view does not project fails the query with a DataFusion schema error. Fail-closed, but the message names neither the policy nor the view. Direct queries with the same projection work. |
| Ranger glob patterns | Only exact match and bare `*` are matched. `orders*` is not, and a policy written that way silently never fires. Pinned by `ranger_glob_patterns_are_not_matched`. |
| Namespace flattening | `resolve_policy_key` passes only the LAST dotted component as the Ranger `database`, so `a.b.sales` and `sales` collide. Pinned by `resolve_policy_key_multilevel_takes_last_namespace_component`. |
| Tag parity with Spark | Spark reads tag associations from the Ranger or Atlas tag store, not from Iceberg properties. Masks are shared; associations are not. |
| ALL vs FUTURE tables | Ranger has no future-only resource, so both collapse to one wildcard policy. Snowflake distinguishes them. |
| `opa` and `cedar` engines | Legacy config values from an earlier design, superseded by `ranger`. Not wired, and selecting one errors rather than degrading to passthrough (pinned by `unwired_policy_engines_fail_loudly_rather_than_degrade`). They are not planned work; `ranger` is the backend. |
| Tag propagation | A column derived from a tagged column in a CTAS starts untagged. |

## Where to go next

- [Fine-grained access control](./fine-grained-access-control.md) for configuration and the mask vocabulary.
- [Fine-grained enforcement](../design-notes/ranger-fine-grained-enforcement.md) for the rewrite internals and the precedence contract.
- [Ranger access control](../design-notes/ranger-access-control.md) for the catalog path and the identity model.
- [Polaris + Ranger + Keycloak quickstart](../quickstart/polaris-ranger-keycloak.md) for a stack that runs all of it.
