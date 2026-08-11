# Fine-grained enforcement with Apache Ranger (row filters, column masks, tags)

This is a reference for SQE's `ranger` **policy** backend: the fine-grained path
where SQE itself enforces row-level filters, column masks, and tag-based masking
by rewriting the query plan. It is separate from the catalog access-control path.

For the coarse, catalog-level path (where SQE translates `GRANT`/`REVOKE` into
Ranger policies on the `polaris` service and Polaris enforces them, and SQE does
no filtering of its own) see [ranger-access-control.md](./ranger-access-control.md). That document is the
companion to this one. This document does not repeat it.

## Overview

The two paths use two different Ranger services and two different config blocks.

- **Catalog path** ([ranger-access-control.md](./ranger-access-control.md)). `[access_control] backend
  = "ranger"`. SQE writes the `polaris` service; Polaris enforces. Coarse
  allow/deny per catalog operation. SQE does not filter rows or mask columns.
- **Fine-grained path** (this document). `[policy] engine = "ranger"`. SQE reads
  the `query` service-def, the same service Apache Spark's Kyuubi Ranger plugin
  reads, and enforces row filters and column masks in its own `LogicalPlan`
  rewriter, between planning and optimization.

The two are independent and both apply. A query must pass BOTH gates: the Polaris
catalog gate (may this user load this table?) AND SQE's fine-grained rewrite
(what rows and columns may this user see?). Revoking the coarse `SELECT` grant
denies the query at Polaris before any fine-grained check runs.

Why this lives in SQE and not Polaris: the `polaris` service-def declares no
`rowFilterDef` and no `dataMaskDef`, and the Polaris authorizer reads only a
boolean allow/deny. It cannot enforce row filters or column masks even though the
Ranger engine can compute them. Fine-grained enforcement has to happen in the
query engine. The full service-type rationale is in
[ranger-fine-grained-service-type.md](./ranger-fine-grained-service-type.md); the design notes are in
[fine-grained-policy.md](./fine-grained-policy.md).

## How it works

The store is `RangerStore` in `crates/sqe-policy/src/ranger_store.rs`. The
rewriter is `PolicyPlanRewriter` in `crates/sqe-policy/src/plan_rewriter.rs`.

```
Ranger Admin  --download bundle-->  RangerStore (resolve)  -->  ResolvedPolicy
ResolvedPolicy  -->  PolicyPlanRewriter  -->  rewritten LogicalPlan  -->  optimizer
```

### Download bundle

`RangerStore::fetch_bundle` calls one endpoint:

```
GET /service/plugins/policies/download/{service_name}
```

The `{service_name}` is `hive` by default (config `policy.ranger.service-name`); the quickstarts set it to `query`.
The call uses HTTP basic auth with the configured admin user and password. The
response is the full `ServicePolicies` JSON bundle: the resource `policies[]`
(each carries a `policyType`: 0 = access, 1 = DATAMASK, 2 = ROWFILTER), and an
optional nested `tagPolicies` block when a `tag` service is linked. This is the
same bundle the JVM Ranger plugin downloads, which is why the policy set is shared
with Spark/Kyuubi. The public-v2 `/api/policy` endpoint returns a flat
resource-only array and is insufficient.

### Resolve

`RangerStore::resolve(user, table, namespace)` returns a `ResolvedPolicy`:

```rust
ResolvedPolicy {
    row_filters: Vec<Expr>,
    column_masks: HashMap<String, MaskType>,
    restricted_columns: Vec<String>,
}
```

Resolution is keyed on the user plus the user's token roles. SQE matches policy
items directly against `SessionUser { username, roles }` (see `item_matches`):
a policy item applies if its `users` list contains the username OR its `roles`
list intersects the user's token roles. This differs from the catalog path:
SQE's session roles come from the token (`realm_access.roles`), so SQE matches
the token roles directly and does NOT depend on Ranger role membership the way
Polaris does.

Resource matching (`policy_matches_table`, `resource_matches`) compares the
policy's `database` and `table` resource values against the target. Only exact
match and bare `*` are supported; Ranger glob patterns like `orders*` are not
matched in this version. `isExcludes` inverts the match. The namespace is
flattened to a hive `database` name by `hive_database`; the rewriter passes the
LAST dotted component of the schema (so schema `sales_wh.sales` becomes database
`sales`), matching the write path's `namespace().last()` keying.

### Rewrite

`PolicyPlanRewriter::evaluate` walks the plan, collects every `TableScan`,
resolves a policy per scanned table, then rewrites top-down. For each scan with a
non-empty policy it builds wrappers with `LogicalPlanBuilder` so injected
expressions normalize against the real (qualified) scan schema:

1. **Row filters** inject as `Filter` nodes above the `TableScan`. User
   predicates can push through these (same semantics as a user `WHERE`).
   The filter sits *below* the masking projection, so it is evaluated against
   stored values: masking a column that a row filter reads does not change which
   rows survive. Kyuubi orders these two the other way around, which is why
   `scripts/access-control-parity-demo.sh` pins the difference.
2. **Column masks** replace the column reference in a projection with the masking
   expression, aliased back to the column's qualified name. User predicates
   cannot push through a mask expression (the expression boundary blocks
   pushdown on the raw value, matching PostgreSQL RLS).
3. **Restricted columns** are forced to NULL: the column stays in the output
   schema but every value becomes a typed NULL (restriction is a forced
   Nullify). `SELECT *` and any reference to the column resolve and return NULL;
   the raw value is never returned, and predicate pushdown on the real value is
   blocked. Restriction wins over a mask on the same column.

### Fail-closed throughout

Every uncertain path denies rather than leaks.

- A table reference that cannot be mapped to a policy key injects a `lit(false)`
  row filter (deny all rows). See `resolve_policy_key` returning `None`.
- A policy resolution error (transport, parse, breaker open) injects a
  `lit(false)` row filter for that table.
- An unparseable row-filter expression becomes `lit(false)` rather than being
  dropped.
- An unsupported mask type restricts the column rather than returning it raw.
- The download is guarded by a `PolicyCircuitBreaker`: repeated failures trip the
  breaker, and an open breaker returns an error, which the rewriter treats as
  deny-all.

Results are cached in a moka TTL cache keyed by username, namespace, table, and
the sorted role list. The cache invalidates on `invalidate_all` (called when
table properties change; see the tag section).

## Mask vocabulary

SQE realizes the complete Ranger hive built-in mask set. `map_mask` in
`ranger_store.rs` maps each `dataMaskType` string to an SQE `MaskType`. The
char-class transformer is the `sqe_mask_partial` DataFusion UDF in
`crates/sqe-policy/src/mask_udf.rs`.

| Ranger `dataMaskType` | SQE `MaskType` | Effect |
|---|---|---|
| `MASK_NULL` | `Nullify` | Replace the value with a typed NULL. |
| `MASK_HASH` | `Hash` | HMAC-SHA256 hex digest (plain SHA-256 when no mask key is set). |
| `MASK` | `PartialMask { 0, 0, 'X', 'x', 'n' }` | Full redact: uppercase to `X`, lowercase to `x`, digit to `n`; punctuation and non-ASCII kept. |
| `MASK_SHOW_LAST_4` | `PartialMask { 0, 4, 'x', 'x', 'x' }` | Show the last 4 characters; mask the rest with `x`. |
| `MASK_SHOW_FIRST_4` | `PartialMask { 4, 0, 'x', 'x', 'x' }` | Show the first 4 characters; mask the rest with `x`. |
| `MASK_DATE_SHOW_YEAR` | `DateShowYear` | Truncate a date to its year (`date_trunc('year', col)`); month and day zeroed. |
| `CUSTOM` | `Custom(Expr)` | Arbitrary SQL expression; see below. |
| `MASK_NONE` | (no mask) | Explicit exemption. The column is left visible and is not restricted. Place it first in Ranger to carve exceptions. |

The character conventions match the hive serviceDef transformer templates. Full
`MASK` uses `X`/`x`/`n`; the `MASK_SHOW_*` partial masks use `x` for every
replaced character type. Counting is by Unicode scalar (chars), matching Hive.
For `111-11-1111` with `MASK_SHOW_LAST_4` the output is `xxx-xx-1111`.

`CUSTOM` masks carry a `valueExpr` with `{col}` as the column placeholder.
`map_mask` substitutes the real column name into the template, then parses the
result into a DataFusion `Expr` via `parse_sql_predicate`. A parse failure
restricts the column (fail-closed). Any genuinely unknown `dataMaskType` also
restricts the column.

How a mask becomes an expression at rewrite time is in `apply_mask`
(`plan_rewriter.rs`): the masking expression is built to keep the column's Arrow
type (a `Nullify` on a BIGINT emits a typed Int64 NULL, not a Utf8 NULL), so
downstream Filter, Join, and GroupBy operators see the shape they expect and a
predicate cannot coerce both sides to Utf8 and leak masked rows.

### Masking on the value of another column

A CUSTOM mask is an arbitrary SQL expression, and it can reference other columns
of the same row, not only the column being masked. The Ranger `valueExpr` uses
`{col}` for the masked column; any other bare column name resolves against the
table's scan schema.

Example: mask `salary` only for rows outside the HR department.

```sql
-- Ranger CUSTOM mask valueExpr on column `salary`:
CASE WHEN department = 'HR' THEN {col} ELSE '0' END
```

Limitation: only bare column names resolve. A qualified reference such as
`t.department` fails to parse, and SQE fails closed by restricting the column
(it is forced to NULL, not returned raw). Reference siblings by their
bare name.

## Role-conditional policy (session-context functions)

SQE registers five session-context scalar UDFs, defined in
`crates/sqe-policy/src/session_udf.rs`. Each bakes in the session's
`SessionIdentity` at construction time and is `Volatility::Immutable`, so
DataFusion const-folds the call to a literal during logical optimization on the
coordinator. The folded literal is what ships to workers; the function call never
crosses the wire. This is what makes them distribution-safe.

| Function | Returns |
|---|---|
| `current_user()` | the session username |
| `is_role_in_session(role)` | true if `role` is in the session's token roles |
| `current_available_roles()` | the role set as a sorted JSON array string |
| `current_database()` | the session database, or NULL |
| `current_schema()` | the session schema, or NULL |

`is_role_in_session` matches the FLAT token role list directly (membership works
on unsorted input). These functions are usable in user SQL and inside
Ranger-authored policy expressions (row filters and `CUSTOM` mask `valueExpr`).

One current limitation in policy expressions: `RangerStore` builds the resolution
identity with `database: None` and `schema: None` (it does not hold the session
warehouse). So inside a Ranger policy expression, `current_user`,
`is_role_in_session`, and `current_available_roles` resolve correctly, but
`current_database()` and `current_schema()` fold to NULL. In ordinary user SQL
all five resolve fully. This is the documented MVP behavior.

## Tag-based masking

Tag-based masking splits into two independently-stored halves. The decision is
recorded in [Ranger tag storage](./ranger-tag-storage-decision.md).

1. **The mask-per-tag RULE** ("any column tagged `PII` is masked show-last-4")
   lives in Apache Ranger as a `tag`-service policy, returned in the download
   bundle's `tagPolicies` block. Shared with Spark/Kyuubi like resource policies.
2. **The tag-to-column ASSOCIATION** ("column `ssn` has tag `PII`") lives in the
   Iceberg/Polaris table property `sqe.column-tags`, a JSON object mapping column
   name to a list of tags. The mask RULE is shared with Spark/Kyuubi; the
   association is not yet, pending the Iceberg-to-Ranger tag sync.

### Authoring column tags

Attach tags to columns with `SET TAGS`. SQE stores the association in the
`sqe.column-tags` table property; the DDL writes that property for you.

```sql
ALTER TABLE sales.orders SET TAGS (email = ('PII', 'GDPR'), salary = ('PII'));

-- remove all tags on a column:
ALTER TABLE sales.orders UNSET TAGS (salary);
```

Snowflake's column-tag syntax works too. The tag name becomes the label; SQE has
no tag values, so the assigned value is ignored. `ALTER COLUMN` is accepted as a
synonym for `MODIFY COLUMN`.

### Authoring the tag policy: mask types carry a component prefix

A `tag`-service policy must name the mask type in component-qualified form:

```json
"dataMaskInfo": { "dataMaskType": "hive:MASK_SHOW_LAST_4" }
```

Ranger's `tag` service definition does not define bare mask names. It aggregates
the mask types of every component it can decorate, so its `dataMaskDef` lists
`hive:MASK_SHOW_LAST_4`, `hive:CUSTOM`, `trino:MASK_NULL` and so on. Ranger
rejects a policy naming a bare type with HTTP 400.

SQE reads a `hive`-type service, so `map_mask` normalizes the `hive:` prefix and
accepts either form. Another component's prefix is deliberately left unmatched,
which restricts the tagged column rather than applying a foreign engine's
policy.

This mattered in practice. Until 2026-07-31 the mapper matched bare names only,
so every tag mask fell through to the unsupported arm and the tagged column was
RESTRICTED instead of masked. Fail-closed, so no value ever leaked, but the
feature was inert from the day it shipped. Nothing caught it because the
harness of the day asserted the absence of raw digits, and a restricted column
has no digits either. The regression test is
`mask_type_component_prefix_is_normalized` plus the live-Ranger case
`tag_column_mask_applies_from_iceberg_property`.

### Tag row filters need a Ranger Admin property

A `tag`-service policy can carry a row filter as well as a mask (policyType 2 on
the tag service), so one rule filters every table holding a column with the tag.
SQE supports it, and Ranger ships the capability switched off:

```xml
<property>
  <name>ranger.servicedef.autopropagate.rowfilterdef.to.tag</name>
  <value>true</value>
</property>
```

in `ranger-admin-site.xml`. Ranger copies each component's `dataMaskDef` into the
tag service definition unconditionally, but copies its `rowFilterDef` only when
that property is true (`AbstractServiceStore`, default `false`). This is not a
version limitation and no upgrade changes it.

Without the property the tag service definition carries a populated
`dataMaskDef` and an empty `rowFilterDef`, and the policy POST is rejected with

```
tag policy can specify values for one of the following resource sets:
 does not have any resource hierarchies
```

The message names resource hierarchies rather than the missing capability, so it
reads like a malformed resource block. It is not: the resource block is fine and
the definition simply cannot express a row filter.

One caveat if you patch the definition over REST rather than setting the
property: Ranger's own aggregate `tag` definition does not round-trip through
Ranger's validator. It carries a duplicate `ozone:assume_role` access type and
elasticsearch implied grants naming access types the definition never declares,
both of which must be pruned before the PUT is accepted.

```sql
ALTER TABLE sales.orders MODIFY COLUMN email SET TAG PII = 'true';
ALTER TABLE sales.orders MODIFY COLUMN email UNSET TAG GDPR;
```

`SET TAGS` merges: it changes only the columns you name and leaves the rest of
the table's tags in place. Tags within a column are unioned and deduped.
`UNSET TAGS (col)` removes all tags on that column. The mask that a tag triggers
still lives in the Ranger tagPolicy; `SET TAGS` only authors which columns carry
which label.

Underneath, the association is one JSON value in the `sqe.column-tags` table
property, mapping each column to its list of tags:

```
sqe.column-tags = {"email": ["PII", "GDPR"], "salary": ["PII"]}
```

The write goes through a Polaris `updateProperties` commit. After the commit SQE
calls `invalidate_table` on the catalog and `invalidate_policy_cache()`, so the
new tags are visible on the next query without waiting for the cache TTL.

The association lives in the Iceberg property that SQE reads. Until the separate
Iceberg-to-Ranger tag sync lands, other engines (Spark/Kyuubi) do not see these
column tags. The mask-per-tag rule in the Ranger tagPolicy is shared with those
engines; the column-to-tag association is not yet.

Tags as table properties (rather than the Ranger tag store) win on four counts:
they cover federated catalogs that Polaris cannot gate, they need no Atlas/tagsync
deployment, they travel with the data through clone/replicate/rename, and SQE
already reads `table.metadata()` on every scan. The full rationale is in
[ranger-tag-storage-decision.md](./ranger-tag-storage-decision.md).

### Resolution and merge

At scan time the rewriter reads column-to-tags from the injected `TagSource`
(`crates/sqe-policy/src/tag_source.rs`; `NoopTagSource` by default,
`CacheTagSource` in production). It passes the FULL namespace path (split on `.`),
not the truncated last component, because the tag cache is keyed by the full table
identity. The `TagSource` fails safe: any miss or unparseable metadata returns an
empty map, since tags only ADD restrictions.

`RangerStore::resolve_tags` resolves the tag policies for the user's roles and
returns a three-tuple:

- mask specs keyed by TAG name, as `TagMaskSpec::Ready(MaskType)` for a
  fully-resolved mask or `TagMaskSpec::Custom(template)` for a `CUSTOM` mask whose
  `{col}` placeholder must be substituted per column at merge time;
- row filters that the matching tags triggered;
- the set of tags whose mask could not be mapped (genuinely unsupported type).

`merge_tag_masks` in `plan_rewriter.rs` joins tags to columns and enforces a
locked precedence contract:

1. **Restricted columns always win.** A tag cannot un-restrict a column.
2. **Tag masks win over resource masks, by default.** `policy.mask-precedence`
   selects this: `tag` (the default) matches the standard Ranger plugin order
   that Hive and Spark/Kyuubi implement, so one policy set renders one value in
   every engine. `resource` keeps the narrower most-specific-rule-wins reading
   SQE shipped earlier. Either way the column is masked, and either way an
   unmappable tag never strips or restricts a column that already has a working
   resource mask: replacing a readable masked value with NULL is not an
   improvement.
3. **Tag row filters are ANDed with resource row filters** (most restrictive).
4. **Within a column, the first tag in stored order with a matching mask wins**
   (deterministic, since `col_tags` preserves the parsed JSON order).
5. **Unmappable tags fail closed.** A column whose only protection is an
   unmappable tag, and which has no resource mask, is RESTRICTED (dropped),
   mirroring the resource path's behavior.
6. **`CUSTOM` tag masks are substituted and parsed.** The `{col}` placeholder is
   replaced with the column name and parsed; on parse failure the column is
   restricted (fail-closed).

If the bundle fetch fails during `resolve_tags`, SQE returns a single `lit(false)`
row filter (deny all rows), consistent with `resolve()`.

## How policies are changed

There are three authoring surfaces, one per layer.

- **Coarse catalog layer.** SQL `GRANT` / `REVOKE`. The access-control backend
  writes the `polaris` Ranger service; Polaris enforces. See
  [Ranger access control](./ranger-access-control.md).
- **Fine-grained row filters and column masks.** SQL `CREATE OR REPLACE POLICY`
  / `DROP POLICY` writes the `hive` Ranger service (row-filter policyType 2,
  data-mask policyType 1). SQE downloads and enforces them. The same policies
  enforce in Spark/Kyuubi. Ranger UI/REST remains an external authoring path.
- **Tag-to-column associations.** `ALTER TABLE ... SET TAGS / UNSET TAGS` (the
  Snowflake `MODIFY|ALTER COLUMN ... SET TAG` forms work too). The DDL writes the
  `sqe.column-tags` table property. The mask-per-tag rule itself is a `hive`/`tag`
  service policy in Ranger.

### Propagation delay, per surface

The SQL surfaces take effect immediately: `GRANT`, `REVOKE`, policy DDL and
`SET TAGS` / `UNSET TAGS` flush the resolved-policy cache after the mutation
commits, so the next query re-resolves (issue #207).

A policy authored in the Ranger UI or over REST does not, because SQE learns
about it only on the next download. The resolved-policy cache holds for
`[policy.ranger] cache-ttl-secs`, which bounds an over-permissive window: a user
who queried the table before the edit keeps the old decision until their cache
entry expires. Tightening a mask in the console is therefore eventually
consistent, up to the TTL.

The tag path is exempt on the association side, since `resolve_tags` re-reads the
column-to-tag map on every call. The tag RULE still comes from the cached bundle.

Lower `cache-ttl-secs` if prompt propagation of console-authored edits matters
more than download load against Ranger Admin. The window is pinned at both edges
by `cache_ttl_bounds_policy_staleness`.

## Catalog path vs fine-grained path

| | Catalog path | Fine-grained path |
|---|---|---|
| Config block | `[access_control] backend = "ranger"` | `[policy] engine = "ranger"` |
| Ranger service | `polaris` | `hive` (+ linked `tag`) |
| Granularity | catalog / namespace / table allow-deny | row filters, column masks, restricted columns, tag masks |
| Authored via | SQL `GRANT` / `REVOKE` | SQL `CREATE/DROP POLICY` + `ALTER TABLE SET TAGS` (Ranger UI/REST also supported) |
| Enforced by | Polaris embedded authorizer | SQE `PolicyPlanRewriter` (plan rewrite) |
| Does SQE filter? | No (write/read policies only) | Yes (rewrites the plan) |
| Shared with Spark? | No (Polaris-specific service) | Yes (the `query` service Kyuubi reads) |
| Identity matching | Ranger role membership (resolved by Polaris) | token roles, matched directly |
| Document | [ranger-access-control.md](./ranger-access-control.md) | this document |

Both gates apply to every query. The catalog gate runs first at Polaris; the
fine-grained rewrite runs in SQE on the loaded plan.

## Configuration

The fine-grained path is configured under `[policy]`, separate from
`[access_control]`. Setting `engine = "ranger"` activates `RangerStore`.

```toml
[policy]
engine = "ranger"

[policy.ranger]
url = "http://ranger-admin:6080"
service-name = "query"
admin-user = "admin"
# Set via SQE_POLICY__RANGER__ADMIN_PASSWORD rather than in the file.
admin-password = ""
timeout-secs = 5
cache-ttl-secs = 30
cache-max-entries = 10000
accept-invalid-certs = false
```

Field reference (`RangerPolicyConfig` in `crates/sqe-core/src/config.rs`):

| Key | Meaning | Default |
|---|---|---|
| `policy.engine` | policy backend selector; `ranger` activates this path | `passthrough` |
| `policy.ranger.url` | Ranger Admin base URL | (empty) |
| `service-name` | the frontend-query Ranger instance to read; shared with Spark/Kyuubi | `hive` |
| `admin-user` | Ranger Admin user for HTTP basic auth | `admin` |
| `admin-password` | Ranger Admin password (a secret) | (empty) |
| `timeout-secs` | HTTP timeout for one download call | `5` |
| `cache-ttl-secs` | resolved-policy cache TTL | `30` |
| `cache-max-entries` | max cached `ResolvedPolicy` entries | `10000` |
| `breaker-failure-threshold` | consecutive failures before the breaker opens | (OPA default) |
| `breaker-recovery-secs` | how long the breaker stays open before probing | (OPA default) |
| `accept-invalid-certs` | accept self-signed TLS on Ranger Admin | `false` |

The two Ranger config blocks are distinct. `[access_control.ranger]` points at
the `polaris` service for the write/enforce-at-Polaris catalog path.
`[policy.ranger]` points at the `query` service for the SQE-side fine-grained
path. They can target the same Ranger Admin host but read different services.

## Quickstart and related docs

The reference deployment is `quickstart/polaris-ranger-keycloak/`. Its
`OVERVIEW.md` has a "Fine-grained enforcement (SQE-side)" section that walks the
live setup, and `test.sh` section 5 proves a `MASK_NULL` on `orders.amount` and a
`MASK_SHOW_LAST_4` on `orders.ssn` for role `engineer`: bob (engineer) sees
`xxx-xx-1111` and an empty amount; alice (analyst-only) sees the raw values.

For cross-engine parity with Apache Spark on the same Ranger setup, see
[sqe-spark-ranger-parity.md](./sqe-spark-ranger-parity.md).

Related references:

- [ranger-access-control.md](./ranger-access-control.md) -- the catalog access-control path (companion).
- [ranger-fine-grained-service-type.md](./ranger-fine-grained-service-type.md) -- why the `query` service-def, the
  flattening sharp edge, cross-engine requirements.
- [ranger-tag-storage-decision.md](./ranger-tag-storage-decision.md) -- where tag associations are stored.
- [fine-grained-policy.md](./fine-grained-policy.md) -- design notes and Snowflake-parity mapping.

## Versions

- Apache Polaris 1.7.0 (embedded Ranger authorizer, Beta).
- Apache Ranger 2.8.0.
- Keycloak 26.5.
