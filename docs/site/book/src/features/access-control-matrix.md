# Access control: what is supported, and what is proven

Two independent gates run on every query.

**Polaris gates the catalog.** `GRANT` and `REVOKE` in SQE become Apache Ranger
policies on the `polaris` service, and Polaris's embedded Ranger authorizer
enforces them. This answers "may this user load this object at all". SQE does no
filtering on this axis.

**SQE gates the data.** Row filters, column masks, and column restriction are
applied by rewriting the logical plan before DataFusion optimizes it, from
policies read out of a Ranger `query` service. This answers "which rows and
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
| Group grantee | `GRANT SELECT ON cat.ns.tbl TO GROUP g` | Yes, as a Ranger role of the same name | Yes, for enforcement |

### One grant, three policies: the traversal is load-bearing

`GRANT SELECT ON cat.ns.tbl TO USER alice` writes THREE Ranger policies, not one,
and the reason is on SQE's side rather than Polaris's.

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
`table 'cat.ns.tbl' not found` with `LOAD_TABLE` never attempted.

So one statement produces a three-level plan, written outermost first:

| Level | Access type | Why |
|---|---|---|
| catalog | `namespace-list` | `LIST_NAMESPACES` is catalog-scoped and unfiltered |
| namespace | `namespace-properties-read` | the per-namespace visibility probe |
| table | the privilege's own set | the data |

The shape matches `grant-profile.json` v5, which the data-platform control plane
generates from. That is the point: both write to the same Ranger service, and a
SQL grant producing different policies from the equivalent API call makes "who
granted this" unanswerable. Pinned by `a_table_grant_writes_v4s_three_level_plan`
and, live, by `one_table_grant_writes_the_namespace_it_needs`.

`MANAGE` and `ALL` bind at the catalog level already and carry
`catalog-content-manage`, so their plan is a single policy.

**The catalog level is a real widening, accepted rather than hidden.** Any grantee
who holds it can enumerate **every** namespace name in the catalog, so a name like
`pii_customer_health` is visible even though its rows are not, and this now
happens on every table grant. Separate catalogs are the boundary if namespace
names are themselves sensitive. Verified on Polaris 1.7 with a clean database;
recorded in `docs/internal/research/2026-08-02-catalog-traversal-gate.md`.

`REVOKE` touches the deepest level only. The catalog and namespace policies are
shared with every other grant in that catalog, so walking the plan backwards would
strip discovery from unrelated grants. Traversal policies therefore accumulate and
are not cleaned up, which is the correct trade: an orphaned `namespace-list` is
discovery on a catalog the grantee could already reach, whereas over-revoking is an
outage. Clear it explicitly with `REVOKE USAGE ON DATABASE` **before**
`REVOKE USAGE ON SCHEMA` (see the hang in the gap table below).

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
| Precedence | restriction beats mask; tag mask beats resource mask by default (`policy.mask-precedence`, set `resource` to invert); row filters AND together | Both precedence modes proven live and unit-tested; the rest unit-tested |
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

## Cross-engine parity (SQE and Spark)

Tag associations need one extra thing beyond a shared policy. They are authored into
the Iceberg property `sqe.column-tags`, which only SQE reads, so with
`project-tags = true` SQE ALSO writes the association into Ranger's tag store where
Kyuubi looks. If that write fails the Iceberg property is rolled back and the
statement fails, because keeping it would mask the column in SQE while Spark returned
it raw, and the statement would have reported success.

Tag mask types on the tag service must be component-qualified (`hive:CUSTOM`, not
`CUSTOM`): Ranger's tag servicedef aggregates each component's mask vocabulary rather
than defining bare names.


One policy in the shared frontend service, two engines, output compared directly.
Per-engine checks are not enough: they pass while the engines disagree, which is the
failure that matters. Both engines are pointed at the same service, and the suite is
`make test-access-control-spark`.

| Property | Proven |
|---|---|
| A portable CUSTOM column mask renders byte-identically | Yes, `column_mask_is_byte_identical_across_engines` |
| A role outside the masked role sees the raw value in both | Yes, `an_unmasked_role_is_unmasked_in_both_engines` |
| A row filter selects the same rows in both | Yes, `row_filter_returns_identical_rows_across_engines` |
| A named mask type does NOT render identically | Yes, asserted as a divergence |
| Tag-based masks | Yes with `project-tags = true`, `tag_column_mask_is_byte_identical_across_engines` |
| A failed projection does not leave a half-applied tag | Yes, `a_failed_projection_rolls_back_the_tag` |

Object-level parity is covered separately by `spark_access_control_e2e`: grants written
through SQE's `GRANT` statement, asserted through Spark, for read and write.

## Known gaps

| Gap | Detail |
|---|---|
| Scope must match the privilege | A privilege binds to one resource level. Naming an object deeper than that level is refused rather than widened: `GRANT ALL ON wh.sales.orders` errors instead of writing a catalog-wide policy. Re-issue it at the level the error names. Pinned by `all_privileges_on_a_table_is_refused_rather_than_widened_to_the_catalog`. |
| Revoke narrows, it does not cascade | Ranger allows one policy per resource, so grants share an item and `WRITE_ACCESS` contains all of `READ_ACCESS`. `REVOKE INSERT` used to strip the grantee's independent `SELECT` too. SQE now labels each grant (`chm:<TYPE>:<name>:<PRIVILEGE>`) and holds back access types another labelled privilege still needs. The `chm` prefix is shared with the data-platform control plane deliberately: both write to the same Ranger service and both read these labels, so a private prefix would leave each blind to the other's grants and cascading over them. A label naming a privilege SQE does not map is dropped and logged rather than trusted, because an under-revoke is worse than the cascade. Grants written before labels existed fall back to the old behaviour, logged. Pinned by `revoking_write_leaves_an_independent_read_grant_intact`. |
| Catalog discovery with nothing visible stalls a current-thread runtime | A principal who can list a catalog's namespaces while every per-namespace probe 403s takes the slow path instead of getting "table not found": `contains_namespace` bridges to async through `runtime_bridge::block_on_compat`, and on a current-thread runtime that bridge blocks the calling runtime while it waits. **A deployed coordinator runs a multi-thread runtime and denies normally**; this affects tests (`#[tokio::test]` defaults to current-thread) and any single-threaded embedding. It no longer hangs: the bridge waits on an OS-level deadline (60s) and returns an error naming the cause, because a tokio timer cannot fire on a runtime whose thread is blocked. What remains unfixable there is the underlying stall, since a resource registered with the parked runtime's IO driver cannot make progress from anywhere else. Background in `docs/internal/research/2026-08-02-catalog-traversal-gate.md`. |
| Traversal policies accumulate | `REVOKE` releases the deepest level only, because the catalog and namespace policies a grant writes are shared with every other grant in that catalog. Orphaned `namespace-list` / `namespace-properties-read` are left behind and nothing cleans them up. Deliberate: over-revoking would strip discovery from unrelated grants. Clear them with `REVOKE USAGE ON DATABASE` then `REVOKE USAGE ON SCHEMA`, in that order. |
| Narrowing a privilege does not narrow past grants | Ranger's grant endpoint MERGES access types into the policy for a resource, and `REVOKE` removes only the types it names. So when SQE narrows what a privilege confers (as adopting `grant-profile` v4 narrowed `INSERT`), policies written by the earlier version keep the wider set, and a `REVOKE` from the new version cannot clear the residue. New grants get the narrower set; existing ones need a one-off cleanup. |
| Delegate admin does not cascade upward | A table grant writes catalog, namespace and table policies, and Ranger authorizes each against the grantor. Measured on 2.8: a grantor holding `delegateAdmin` on `cat.ns.tbl` gets 200 there and 403 on both `cat.ns` and `cat`, for grant and revoke alike, and 403 for an access type outside their delegate set. SQE skips a traversal level the grantee already holds at that exact resource (Ranger merges, so it is a no-op write), which is what makes `WITH GRANT OPTION` usable. A grantee with no discovery yet still needs an admin to seed it, and the error names the level and the statements. Pinned by `a_delegated_owner_grants_on_their_own_table_without_an_admin_role`. |
| `WITH GRANT OPTION` needs `grant_authority` | It maps to `delegateAdmin`, but the default `[access_control] grant_authority = "admin-role"` also requires an `[auth] admin_roles` role, so a table owner without one cannot use it. Set `grant_authority = "ranger-delegate"` to make Ranger's per-resource check the only one. Read the Ranger policies first: it widens grant authority to everyone holding `delegateAdmin`, and a wildcard `catalog = *` policy written with `delegateAdmin: true` covers its roles service-wide. `DENY` ignores the setting and stays admin-only. Pinned by `a_non_admin_cannot_grant_under_the_default_gate` and `deny_still_requires_an_admin_role_under_ranger_delegate`. |
| Views are not a boundary | `GRANT ... ON VIEW` works, but SQE expands the view and plans against its base tables, so the reader needs a grant there too. Not a Snowflake secure view. |
| Group grantees are Ranger roles | `GRANT ... TO GROUP g` writes to the Ranger ROLES field, not groups. The control plane materialises every Keycloak group as a Ranger role of the identical name, so a group grant and the same-named role grant are the same write. SQE does NOT auto-create the role: a typo would otherwise become an empty role and a grant conferring nothing, so an unknown grantee is refused by Ranger instead. |
| Row filters work through narrow views | A filter on a column the view does not project is enforced: the scan's projection is widened internally, the filter applied, then the original output columns restored, so the extra column never reaches the result. It previously failed the query with a DataFusion `No field named` error, making a row filter and a narrow view over one table mutually exclusive. Pinned by `row_filter_on_an_unprojected_column_is_enforced_not_an_error`. |
| Ranger wildcards | Supported: `*` matches any run, `?` exactly one, and comparison folds case, per the `query` servicedef's `matcherOptions: {wildCard: "true", ignoreCase: "true"}`. Previously only exact match and a bare `*` fired, so a policy written `orders*` or on `Orders` was silently inert. Pinned by `ranger_wildcards_and_case_folding_match_the_servicedef`. |
| Namespace keys are the full path | `resolve_policy_key` passes the whole dotted namespace, so `a.b.sales` and `sales` no longer collide on one Ranger `database`. A policy naming only the last component still matches, and logs that it did, so policies written against the old key keep working while operators rewrite them. Pinned by `namespaces_sharing_a_last_component_no_longer_collide`. |
| Tag parity with Spark | CLOSED by the tag projector. Spark reads associations from Ranger's tag store, not from Iceberg properties, so a tag-masked column used to be protected in SQE and returned RAW by Spark. With `[policy.ranger] project-tags = true`, `SET TAG` also writes the association into Ranger's tag store and both engines mask identically. Pinned by `tag_column_mask_is_byte_identical_across_engines`. Projection is OFF by default: a deployment with no second engine reading Ranger gains nothing and would acquire a hard dependency on the Ranger tag API in its DDL path. Left off, tag masks remain SQE-only. |
| A SQL grant authorizes the Spark path only with the defer policy | SQE writes only the `polaris` Ranger service. Kyuubi's `RangerSparkExtension` runs in `ACTIVE` mode against the `query` service and checks its own privilege FIRST, so without a matching `policyType-0` item it default-denies before Polaris is consulted, and a `GRANT` issued in SQE is not sufficient for Spark. Measured: `AccessControlException: Permission denied: user [bob] does not have [select] privilege on [acdemo/orders/id]` on a table Polaris permitted. The `query` service therefore carries a deliberate blanket allow so Kyuubi defers and Polaris decides object level: an item for group `public` on Ranger's auto-created `all - database, table, column` policy, written by the grant API because that auto policy owns the resource signature and a separately named policy is refused with error 3010. Pinned by `object_denial_survives_the_frontend_defer_policy`, which proves the blanket allow grants no data access of its own. A Spark path that connects as a service principal bypasses the `polaris` plane entirely and is subject to neither. |
| ALL vs FUTURE tables | Ranger has no future-only resource, so both collapse to one wildcard policy. Snowflake distinguishes them. |
| Tag propagation | A column derived from a tagged column in a CTAS starts untagged. |
| A leftover service-account catalog defeats per-user identity on Spark | Handing Spark a per-user token governs ONLY that catalog. Any other catalog configured for the same warehouse is a separate identity the caller can name instead. Measured: a user denied on a table through his own catalog read it through a `credential`-configured alias in the same session. Overriding that alias's `token` does not help, because Iceberg prefers `credential` when both are set. The fix is to remove the service-account catalog, not to shadow it. The quickstart no longer ships one, and two guards fail if it returns: `no_service_account_catalog_can_defeat_per_user_identity`, and the identity check in `parity-test.sh` that a tokenless `spark-sql` cannot load the table. Still open for any deployment that configures a service-account credential. |
| Identity assurance differs by tier on the Spark path | The object tier verifies a JWT signature: Spark presents a per-user Keycloak token to the Iceberg REST catalog and Polaris authorizes that user. The fine-grained tier trusts `HADOOP_USER_NAME`, an unauthenticated string the client picks. A mismatched pair gets one user's OBJECT rights and another's MASKS, which `mismatched_identity_reveals_the_two_tier_trust_split` demonstrates rather than fixes. In a deployment the platform controls `spark-submit`; closing the split means running Spark behind a Kyuubi server with real authentication. SQE has no equivalent gap, because it validates the token and derives both tiers from it. |
| Polaris denial messages name the principal and the operation | `Principal 'dave' is not authorized for op 'LIST_TABLES'`, where SQE hides a denied object as "not found". A Spark user therefore learns that an object exists and which operation was refused. |
| A refused write is refused at COMMIT | Polaris denies `ADD_TABLE_SNAPSHOT` rather than `LOAD_TABLE`, so an unauthorized `INSERT` can leave staged data files in object storage even though the table is untouched. Authorization holds and the row count does not move, which `spark_write_privileges_are_separate_from_read` asserts; storage hygiene does not. A denied writer can generate orphan files at will, and cleanup is the existing maintenance procedure's job. |
| Kyuubi's policy view lags its poll interval | The Spark plugin caches the policy bundle on disk and refreshes on a 10s poll, so a short-lived `spark-sql` JVM started seconds after a policy change can still enforce the previous bundle. Object-level tests are unaffected, because the only frontend policy in play is the static defer item. Anything that changes frontend policy mid-run needs settling time, and a passing assertion taken too soon proves nothing. |
| Spark row filters need the filter column projected | Kyuubi on Spark 3.5 throws `MISSING_ATTRIBUTES` (Kyuubi #6889) when a row filter references a column the query does not select. SQE has no such restriction, so a filter that is transparent in SQE breaks the query in Spark. |
| Named Ranger mask types render differently per engine | `MASK_SHOW_LAST_4` gives `xxx-xx-1111` in SQE and `nnnUnnU1111` in Kyuubi, because Kyuubi ignores the servicedef transformer and applies its own mask characters. The semantics agree (raw hidden, last four visible); only the rendering differs. Only a CUSTOM transformer written in portable standard SQL (`concat('xxx-xx-', substr({col},8,4))`) is byte-equal. Pinned in both directions by `a_named_mask_type_is_not_byte_portable` and `column_mask_is_byte_identical_across_engines`, which are each other's control: if the comparison ever reported equal regardless, the first would fail. |

## Where to go next

- [Fine-grained access control](./fine-grained-access-control.md) for configuration and the mask vocabulary.
- [Fine-grained enforcement](../design-notes/ranger-fine-grained-enforcement.md) for the rewrite internals and the precedence contract.
- [Ranger access control](../design-notes/ranger-access-control.md) for the catalog path and the identity model.
- [Polaris + Ranger + Keycloak quickstart](../quickstart/polaris-ranger-keycloak.md) for a stack that runs all of it.
