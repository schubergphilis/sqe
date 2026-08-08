# Access control: evaluation order, engine by engine

A reference for reviewers, auditors and data engineers who need to answer one
question precisely: **for this column, in this engine, who sees what, and which
component decided?**

Everything under "Measured" was observed on a live Apache Polaris 1.7, Apache Ranger
2.8, Keycloak 26.5 and Spark 3.5.9 with Kyuubi Authz 1.11.1, and has an executable
assertion behind it. The comparison to Databricks and Snowflake at the end is from
their product documentation, NOT measured here, and is marked as such. Treat the two
kinds of claim differently.

## Where the ACLs live

Five stores, and knowing which one answers a question is most of the work.

| Store | Contains | Written by | Read by |
|---|---|---|---|
| Ranger service `polaris` (custom servicedef, 69 access types) | object-level allow/deny per operation: catalog, namespace, table, view, principal, policy | SQE `GRANT`/`REVOKE`/`DENY`, or the platform control plane | **Polaris**, through its embedded Ranger authorizer |
| Ranger service `query` (servicedef type `hive`: `database`/`table`/`column`) | row filters (policyType 2), column masks (policyType 1), and one deliberate blanket allow (policyType 0) | Ranger Admin console, or the SQE test fixtures | SQE's plan rewriter AND Spark's Kyuubi plugin |
| Ranger service `tag` (attached to `query` via its `tagService` field) | mask and filter rules PER TAG | Ranger Admin console | both engines |
| Iceberg table property `sqe.column-tags` | which column carries which tag | `ALTER TABLE ... SET TAG` | SQE only |
| Ranger tag store (`/service/tags/...`) | the same associations, projected | SQE's tag projector when `project-tags = true` | Kyuubi, and any other Ranger-plugin engine |

The last two hold the same information in two places. That is deliberate: the Iceberg
property is the source of truth so tags travel with the table, and the Ranger tag
store is a projection so foreign engines can read them. It is also the single most
fragile part of the design, for reasons the gap table gives.

## Order of evaluation: SQE

Numbered in the order a failure surfaces, which is not the order the documentation
usually implies.

**1. Namespace resolution (SQE-side, before any authorization of the table).**
SQE resolves a table through its catalog provider, which answers only for a namespace
present in its cached namespace list. Building that list takes two calls, and BOTH
must succeed:

- `LIST_NAMESPACES`, authorized at the **catalog** level via `namespace-list`. A
  namespace-scoped grant does not satisfy it, because Polaris does not use Ranger's
  `SELF_OR_DESCENDANTS` matching. Listing is denied outright, never filtered.
- `LOAD_NAMESPACE_METADATA` per namespace, needing namespace-level
  `namespace-properties-read`. A 403 hides that namespace, deliberately, so ungranted
  names do not leak into `SHOW SCHEMAS`.

Either failure yields an empty schema list and the query ends at
`table 'cat.ns.tbl' not found`, with `LOAD_TABLE` never attempted. **Nothing in the
log shows a 403, because there was no denial to report.** For an auditor this is the
most misleading state in the system: a permission problem that presents as a missing
object.

**2. Polaris authorizes `LOAD_TABLE`** against the `polaris` service, using the
caller's bearer token. Needs `table-properties-read` and `table-data-read`. A denial
is a 403 that SQE surfaces as "table not found", matching the Polaris
information-hiding model.

**3. Polaris authorizes the write, at COMMIT.** An `INSERT` is refused at
`ADD_TABLE_SNAPSHOT`, not before the data is staged.

**4. SQE's plan rewriter applies the fine-grained tier**, reading the `query` and
`tag` services. Row filters inject as `Filter` nodes above the `TableScan`; column
masks replace column references with masking expressions, before DataFusion
optimizes, so the optimizer cannot push a user predicate through a mask onto raw
values.

Because a single `GRANT SELECT ON cat.ns.tbl` has to satisfy step 1 as well as step
2, it writes **three** Ranger policies, outermost first:

| Level | Access type | Why |
|---|---|---|
| catalog | `namespace-list` | `LIST_NAMESPACES` is catalog-scoped and unfiltered |
| namespace | `namespace-properties-read` | the per-namespace visibility probe |
| table | the privilege's own set | the data |

That catalog-level policy is a real widening: any grantee holding it can enumerate
**every namespace name in the catalog**. A name like `pii_customer_health` is visible
even though its rows are not. Separate catalogs are the boundary when namespace names
are themselves sensitive.

## Order of evaluation: Spark

Different order, and the first step surprises people.

**1. Kyuubi checks ITS OWN privilege first, before Polaris is consulted at all.**
Running in `ACTIVE` mode against the `query` service, it default-denies without a
matching `policyType-0` item:

```
org.apache.kyuubi.plugin.spark.authz.AccessControlException:
  Permission denied: user [bob] does not have [select] privilege on [ac/orders/id]
```

SQE ignores `policyType-0` policies entirely. So the same grant that works in SQE
fails in Spark, and the failure reads like a Polaris bug. The `query` service
therefore carries one deliberate blanket allow for group `public` that makes Kyuubi
defer, leaving object level to Polaris. It grants no data access, because Polaris
still decides.

**2. Polaris authorizes `LOAD_TABLE`**, exactly as for SQE, **provided Spark
presented a per-user token.** This is the step that is trivially skipped: with a
service-account `credential`, Polaris authorizes the service account and the entire
object tier is bypassed. Per-user identity requires

```
spark.sql.catalog.<c>.token=<the user's OIDC JWT>
spark.sql.catalog.<c>.token-refresh-enabled=false
```

The second line is load-bearing. Left at its default, Iceberg exchanges the external
JWT against Polaris's own token endpoint and the identity silently reverts to the
service account.

**A per-user token governs ONLY the catalog it is attached to.** Any other catalog
configured for the same warehouse in that session is a SEPARATE identity, and the
caller chooses which one by naming it. Measured: with
`spark.sql.catalog.sales_wh.credential` set to a service account, a user denied on a
table through his own catalog reads the same table through that alias in the same
session.

The session cannot defend itself. Overriding the alias's `token` with the user's JWT
does not help, because Iceberg prefers `credential` when both are set (measured). The
fix is a deployment one: **remove the service-account catalog, do not shadow it.**

The quickstart used to ship exactly that hazard and no longer does: no `credential`
and no `oauth2-server-uri`, `token-refresh-enabled` pinned off, and the caller passes
its own token per invocation. Two guards keep it that way, and both assert the
property rather than inspecting the config file, because a config file is easy to
regress:

- `no_service_account_catalog_can_defeat_per_user_identity` fails if naming another
  alias reads a table the caller was just denied.
- `parity-test.sh` fails if a `spark-sql` with no caller token can load the table at
  all.

A refusal without a credential is NOT the same refusal as a denied grant, and the
guard asserts which one it got. With a credential the alias was a second IDENTITY. With
none it has no identity, so the request never becomes an authorization question:
Polaris rejects it before it can answer and Iceberg reports `Unable to parse error
response` rather than a `ForbiddenException`. A test accepting either could not tell a
revoked grant from a catalog nobody gave a token.

**3. Kyuubi injects masks and row filters** from the `query` and `tag` services.

## Side by side

| Property | SQE | Spark (Kyuubi) |
|---|---|---|
| Object-level authority | Polaris, on the `polaris` service | Polaris, same service, IF a per-user token is presented |
| Identity for object level | OIDC JWT, signature verified | OIDC JWT, signature verified |
| Identity for fine-grained | derived from the same verified JWT | `HADOOP_USER_NAME`, an unauthenticated string the client picks |
| `policyType-0` access policies | ignored | REQUIRED, else default-deny |
| Row filters and masks | plan rewriter, before optimization | Kyuubi extension, at analysis |
| Tag associations read from | Iceberg property `sqe.column-tags` | Ranger tag store (needs the projector) |
| Resource mask vs tag mask | RESOURCE wins | TAG wins |
| Row filter on an unprojected column | works | `MISSING_ATTRIBUTES` (Kyuubi #6889) |
| Named mask types (`MASK_SHOW_LAST_4`) | honors the servicedef transformer, `xxx-xx-1111` | applies its own characters, `nnnUnnU1111` |
| Policy freshness | own cache TTL | on-disk bundle, 10s poll |

## Precedence rules

**Deny beats allow**, in both engines, on both tiers. A Ranger deny item on a policy
overrides an allow item on the same policy. Ranger keeps one policy per exact
resource, so deny precedence is expressed by editing that policy rather than adding a
second one.

**A deny hits every member of the named role, including admins.** Denying
`engineer` denies anyone in `engineer`, and that includes an operator who holds an
admin role as well. Verified the hard way: a deny on `engineer` locked the fixture
admin out of the table and cascaded into nine unrelated test failures.

**Resource masks and tag masks disagree between engines.** SQE applies the resource
mask; Kyuubi applies the tag mask, because stock `RangerBasePlugin` evaluates tag
policies before resource policies. Neither leaks, since both mask. But the WEAKER of
the two becomes effective for whoever picks that engine.

## What to read to answer an audit question

For "who can read `cat.ns.tbl`":

1. `GET /service/plugins/policies/service/name/polaris`, and look for policies whose
   resources match the table, its namespace, AND its catalog. Remember the three
   levels: a table-level allow with no catalog-level `namespace-list` is inert in SQE.
2. Check `denyPolicyItems` on those policies before `policyItems`. Deny wins.
3. Resolve Ranger ROLE membership, not OIDC claims. Polaris ignores the token's realm
   roles (they lack the `PRINCIPAL_ROLE:` prefix), so role-based access works through
   Ranger role membership.

For "what does this user SEE in that table":

4. `GET /service/plugins/policies/service/name/query` for policyType 1 and 2 items
   matching `database` (the last dotted namespace component), `table`, and `column`.
5. `GET /service/plugins/policies/service/name/tag` for tag rules, then find which
   columns carry those tags: `SHOW TAGS ON cat.ns.tbl` for SQE's view, and
   `GET /service/tags/download/query` for the projection Spark reads. **If those two
   disagree, the engines disagree.**
6. Decide which engine the question is about, and apply the precedence row above.

For "was this refused, and by what":

7. `AccessControlException: Permission denied: user [...]` means Kyuubi refused, before
   Polaris was consulted.
8. `ForbiddenException: ... not authorized for op '...'` means Polaris refused, and
   names the operation.
9. `table not found` from SQE may be a denial at step 1, with no 403 anywhere.

## Gaps, and which way each one fails

Direction matters more than severity. A fail-closed gap is an outage; a fail-open gap
is an incident.

| Gap | Direction | Detail |
|---|---|---|
| Spark's fine-grained tier trusts an asserted username | **open** | A mismatched pair gets one user's object rights and another's masks. The object tier follows the token, so it cannot be widened this way, but mask selection can be steered. Closing it means Spark behind a Kyuubi server with real authentication. |
| A service-principal Spark bypasses the object tier | **open by configuration**, guarded in the quickstart | Polaris authorizes the service account, and nothing in the `query` service compensates because object level is not its job. Unchanged as a hazard: any deployment that gives Spark a service-account `credential` gets this. What changed is that the quickstart no longer does, and two guards fail if it comes back (`no_service_account_catalog_can_defeat_per_user_identity` and the identity check in `parity-test.sh`). The mitigation is per-user tokens with `token-refresh-enabled=false`, not a policy. |
| A leftover service-account catalog defeats per-user identity | **open** | Adding a per-user catalog does not remove an existing one. Both identities are live and the caller picks by naming the catalog. Measured: denied through the per-user catalog, allowed through the service-account alias, same session, same table. A per-user `token` cannot shadow a `credential`; Iceberg prefers the credential. Remove the other catalog. |
| Renaming a tagged column silently UNMASKS it, in both engines | **open in both** | `sqe.column-tags` is keyed by column NAME and no schema-change path rewrites it, so after `RENAME COLUMN ssn TO tax_id` the association names a column that is gone, no tag matches, and the mask stops applying. Measured, both engines: `SELECT id, tax_id` returns the column RAW. A routine rename unmasks a governed column and nothing reports it. This row previously said the engines broke DIFFERENTLY, with SQE dropping the column entirely. That was real but it was a SCAN defect, not access control: the small-file read path resolved the projection against each data file's parquet names, so a renamed column matched nothing and was discarded. Fixed by resolving Iceberg field ids; the engines now agree and this one shared gap is what remains. |
| A column added after a grant is readable and unmasked | **open, by design** | The object tier has no column level, so `table-data-read` covers columns that did not exist when it was granted. No policy names the new column, so nothing masks it. |
| Adding a column to a MASKED table | closed (fixed) | Was: `ALTER TABLE ADD COLUMN nickname` then `SELECT id, ssn, nickname` failed with `PhysicalExpr Column references column 'nickname' at index 2 ... but input schema only has 2 columns`, making a governed table unqueryable after a routine schema change. Not a plan-rewriter defect: the identical query failed with NO policy at all. The small-file scan path dropped `nickname` (absent from files written before the ALTER) and the mask projection above the scan then indexed past the end of a narrower batch. Fixed by field-id resolution plus a NULL backfill. Regression-guarded: the mask still applies and the new column reads NULL, never another column's values. |
| Tag masks without the projector | **open** | Associations live only in the Iceberg property, which Kyuubi cannot read, so a column masked in SQE comes back raw in Spark. Closed by `project-tags = true`. |
| A CTAS-derived column starts untagged | **open** | Tags do not propagate through a projection. |
| Catalog-level widening on every table grant | **open, accepted** | Every grantee can enumerate all namespace names in the catalog. |
| Polaris denial messages name principal and operation | **open, minor** | A Spark user learns an object exists and which operation was refused. SQE hides denied objects instead. |
| Mask precedence differs between engines | **either** | Whichever mask is weaker becomes effective for that engine's users. |
| A refused write is refused at commit | closed, but messy | Authorization holds and the table is untouched; staged files can be left in object storage. A denied writer can generate orphan files at will. |
| Kyuubi's policy view lags 10s | closed | A revoke is not instant for a short-lived `spark-sql` JVM. Over-permissive for up to one poll interval. |
| Row filter on an unprojected column in Spark | closed | Query fails outright (Kyuubi #6889). Transparent in SQE, breaking in Spark. |
| `REVOKE` leaves traversal policies behind | closed, deliberate | An orphaned `namespace-list` is discovery on a catalog the grantee could already reach. Walking the plan backwards would strip discovery from unrelated grants. |
| Views are not a privilege boundary | **open, by design** | SQE expands a view and plans against its base tables, so the reader needs a grant there too. Masks still apply through the view and cannot be dodged. |
| Ranger 2.8 ships no tag row filters | closed | The tag servicedef has an empty `rowFilterDef` unless `ranger.servicedef.autopropagate.rowfilterdef.to.tag=true` is set on Ranger Admin. |

## How this differs from Databricks and Snowflake

**From product documentation, not measured here.** The architectural contrast is the
part worth internalizing; treat specific feature claims as a starting point for your
own check.

### The structural difference

In Unity Catalog and in Snowflake, **the engine is the policy authority.** One system
of record holds the grants, evaluates them, and enforces them. There is exactly one
answer to "who can see this column", and no possibility of two engines disagreeing,
because there is only one engine.

SQE splits the roles three ways. Polaris is the object authority, Ranger is the
policy store, and **each engine enforces the fine-grained tier itself**. That is what
makes the same policy set govern SQE and Spark at once, which neither Databricks nor
Snowflake offers for a foreign engine. Every divergence in the gap table above is the
price of that property. If you do not need multiple engines on one policy set, you are
paying for something you will not use.

### Object level

| | SQE + Polaris + Ranger | Unity Catalog | Snowflake |
|---|---|---|---|
| Grant model | Ranger policies per resource, allow and deny items | `GRANT`/`REVOKE` in the metastore, hierarchical inheritance | RBAC, role hierarchy, every privilege a role grant |
| Traversal | catalog `namespace-list` plus namespace `namespace-properties-read`, written automatically by one `GRANT` | inherited from catalog and schema | explicit `USAGE` on database and schema |
| Deny | first-class Ranger deny items, precedence over allow | no deny; absence of grant is the only negative | no deny; `REVOKE` only |
| Ownership | not a grant source; the `polaris` servicedef has an owner concept SQE does not lean on | owner has full rights, drives inheritance | owner role has full rights, `MANAGED ACCESS` schemas centralize it |
| Future objects | `ALL` and `FUTURE` collapse to one wildcard policy, because Ranger has no future-only resource | inheritance covers new objects | `FUTURE GRANTS`, a distinct first-class concept |

Snowflake's `USAGE`-on-the-path requirement is the closest analogue to SQE's
three-level expansion, and for the same underlying reason: reaching an object requires
traversing to it. Snowflake makes the operator write it; SQE writes it for you and
accepts the resulting namespace-name visibility.

Deny items are where SQE is genuinely ahead. Neither Databricks nor Snowflake offers a
negative grant that overrides a positive one, so "everyone in analytics except
contractors" is a role-modelling exercise there and a single policy item here.

### Fine-grained

| | SQE | Unity Catalog | Snowflake |
|---|---|---|---|
| Column masking | Ranger mask policy, applied by rewriting the plan | column mask, a SQL UDF attached to the column | masking policy object, attached with `ALTER TABLE ... SET MASKING POLICY` |
| Row filtering | Ranger row-filter policy, injected above the scan | row filter, a SQL UDF returning a boolean | row access policy attached to the table |
| Policy reuse | policy names a resource pattern, so one policy covers many columns by wildcard | function reused across tables | policy is a schema-level object referenced by name, reused explicitly |
| Tag-driven masking | mask rule on a tag in the `tag` service, association in the Iceberg property | governed tags with ABAC policies (newer capability; verify current status) | masking policy assigned to a tag, tag applied to the column |
| Enforcement point | before optimization, in the engine | in the engine | in the engine |
| Cross-engine | the same policy governs SQE and Spark | Unity Catalog only | Snowflake only |

Snowflake's tag-based masking is the direct analogue of SQE's tag masks, and the
comparison is instructive: Snowflake stores the tag association in its own metadata,
so there is one place to look. SQE stores it in the Iceberg property so it travels
with the table, then has to project it into Ranger for Spark. **The sovereignty
property and the consistency risk are the same design decision seen from two sides.**

### Views

Snowflake secure views hide the definition and evaluate with the view owner's
privileges, so a view is a genuine privilege boundary and the standard way to grant
narrowed access. **SQE views are not a privilege boundary.** SQE expands the view and
plans against the base tables, so the reader needs a grant there too. Do not use an
SQE view to grant indirect access to a table.

What SQE views do give you is masking that cannot be dodged: a view projecting a
masked column returns the masked value, because the rewriter runs on the base-table
scan.

### What to take from the comparison

If your governance model is single-engine, Unity Catalog and Snowflake give you one
authority, one answer, and no projection to keep in step. That is a real advantage
and this document is largely a catalogue of what it costs not to have it.

If you need Spark and SQE reading one policy set, or you need the policy store to be
something you run yourself, the split model is the reason to accept the gap table
above. Read it before deciding, not after.

## See also

- [Access control: support matrix](./access-control-matrix.md) for what is proven and
  by which test.
- [Ranger access control](../design-notes/ranger-access-control.md) for the write path
  and the two-tier design.
- [Fine-grained access control](./fine-grained-access-control.md) for the mask
  vocabulary.
