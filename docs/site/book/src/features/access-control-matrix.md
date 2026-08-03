# Access control: what is supported, and what is proven

Two independent gates run on every query.

**Polaris gates the catalog.** `GRANT` and `REVOKE` in SQE become Apache Ranger
policies on the `polaris` service, and Polaris's embedded Ranger authorizer
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
| View | `GRANT SELECT ON VIEW cat.ns.v TO ROLE r` | Yes | Yes |
| All / future views in schema | `GRANT SELECT ON ALL VIEWS IN SCHEMA cat.ns` | Yes, as a wildcard | Yes |
| Deny | `DENY SELECT ON cat.ns.tbl TO USER u` | Yes | Yes |
| Group grantee | `GRANT SELECT ON cat.ns.tbl TO GROUP g` | Read path only, see gaps | Yes, for enforcement |

### Three grants, not one: the traversal is load-bearing

`GRANT SELECT ON cat.ns.tbl TO USER alice` on its own does **not** let alice read
the table through SQE. This surprises everyone, so it is worth the space.

Polaris will serve the table: a direct `LOAD_TABLE` with only the table-level
grant returns 200. But SQE resolves a table through its catalog provider, which
answers only for a namespace present in its cached namespace list, and building
that list takes two calls that must both succeed:

1. `LIST_NAMESPACES`, authorized at the **catalog** level. A namespace-scoped
   `namespace-list` does not satisfy it, because Polaris does not use Ranger's
   `SELF_OR_DESCENDANTS` matching. Listing is denied outright, never filtered.
2. A per-namespace visibility probe (`LOAD_NAMESPACE_METADATA`), needing
   **namespace**-level `namespace-properties-read`. On 403 the namespace is
   hidden, deliberately, so ungranted namespace names do not leak.

Either failure yields an empty schema list, and planning ends at
`table 'cat.ns.tbl' not found` with `LOAD_TABLE` never attempted. So the minimum
to read one table is three levels:

```sql
-- catalog: discovery
GRANT USAGE ON DATABASE cat TO ROLE analyst;
-- namespace: visibility
GRANT USAGE ON SCHEMA cat.ns TO ROLE analyst;
-- table: the data
GRANT SELECT ON cat.ns.tbl TO ROLE analyst;
```

In the quickstart the first two are already in place: the bootstrap seeds
wildcard discovery for the `analyst` and `engineer` roles, which is why a single
`GRANT SELECT` appears to be enough there. A user outside those roles gets
`table not found` until the traversal exists.

The cost of the catalog-level grant is real and worth stating: any grantee who
holds it can enumerate **every** namespace name in the catalog, so a name like
`pii_customer_health` is visible even though its rows are not. Verified on
Polaris 1.7 with a clean database; recorded in
`docs/internal/research/2026-08-02-catalog-traversal-gate.md`.

### A grant must be scoped at the privilege's own level

Each privilege binds to exactly one resource level, shown in the mapping table
in [Ranger access control](../design-notes/ranger-access-control.md). Naming an
object deeper than that level is refused rather than widened:

```sql
GRANT ALL PRIVILEGES ON wh.sales.orders TO USER alice;
```

`ALL` binds to the catalog, so this used to drop the namespace and table and
write `catalog-content-manage` on `wh`. One table was named, success was
reported, and alice got every table in the catalog. SQE now errors and names the
scope that would have been written. `USAGE` on a table and `CREATE SCHEMA` on a
namespace widen through the same path and are refused the same way.

### Views work, and are not a privilege boundary

A view has no resource level of its own. Its NAME goes in the `table` slot and
the access types are the `view-*` set, which is what `GRANT ... ON VIEW` writes.
Verified live: the resulting policy carries `view-properties-read` and
`view-list` on the view coordinate and no `table-data-read`.

**A view is not a security boundary.** SQE expands the view and plans against
its base tables, so the reader needs a grant there too. That is the opposite of
a Snowflake secure view, where the view's owner privileges stand in for the
reader's. Do not use a view to grant indirect access to a table.

What a view DOES give you is masking and filtering that cannot be dodged.

**Column masks survive a view. There is no bypass.** A view that projects a
masked column returns the MASKED value, because the view expands to a
`TableScan` of the base table and the plan rewriter runs on that scan. Verified
with a user who is both an admin (so the view loads) and a member of the masked
role: `xxx-xx-1111` reading the base table, `xxx-xx-1111` reading the view.
Creating a view over a protected table is not a way around masking.

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
| Scope must match the privilege | A privilege binds to one resource level. Naming an object deeper than that level is refused rather than widened: `GRANT ALL ON wh.sales.orders` errors instead of writing a catalog-wide policy. Re-issue it at the level the error names. Pinned by `all_privileges_on_a_table_is_refused_rather_than_widened_to_the_catalog`. |
| One grant is not enough to read a table | A grant writes ONE Ranger policy at the privilege's level. Reaching a table through SQE also needs catalog `namespace-list` and namespace `namespace-properties-read`. See "Three grants, not one" below. |
| Views are not a boundary | `GRANT ... ON VIEW` works, but SQE expands the view and plans against its base tables, so the reader needs a grant there too. Not a Snowflake secure view. |
| Group grantees, write path | `GRANT ... TO GROUP g` is rejected by the Ranger write path (`grantee_to_fields`) because Ranger only learns a user's groups under usersync. Group-bound policies authored in the Ranger console ARE enforced on the read path. |
| Row filters through views | A filter referencing a column the view does not project fails the query with a DataFusion schema error. Fail-closed, but the message names neither the policy nor the view. Direct queries with the same projection work. |
| Ranger glob patterns | Only exact match and bare `*` are matched. `orders*` is not, and a policy written that way silently never fires. Pinned by `ranger_glob_patterns_are_not_matched`. |
| Namespace flattening | `resolve_policy_key` passes only the LAST dotted component as the Ranger `database`, so `a.b.sales` and `sales` collide. Pinned by `resolve_policy_key_multilevel_takes_last_namespace_component`. |
| Tag parity with Spark | Spark reads tag associations from the Ranger or Atlas tag store, not from Iceberg properties. Masks are shared; associations are not. |
| ALL vs FUTURE tables | Ranger has no future-only resource, so both collapse to one wildcard policy. Snowflake distinguishes them. |
| Tag propagation | A column derived from a tagged column in a CTAS starts untagged. |

## Where to go next

- [Fine-grained access control](./fine-grained-access-control.md) for configuration and the mask vocabulary.
- [Fine-grained enforcement](../design-notes/ranger-fine-grained-enforcement.md) for the rewrite internals and the precedence contract.
- [Ranger access control](../design-notes/ranger-access-control.md) for the catalog path and the identity model.
- [Polaris + Ranger + Keycloak quickstart](../quickstart/polaris-ranger-keycloak.md) for a stack that runs all of it.
