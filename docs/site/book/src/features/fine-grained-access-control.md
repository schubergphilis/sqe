# Fine-grained access control

SQE enforces row filters and column masks by rewriting the query's logical plan before DataFusion optimizes it. Filters and masks are injected above the table scan, so the optimizer cannot push a user predicate through a mask to probe raw values. The model follows PostgreSQL row-level security: denied rows are invisible, masked columns return transformed values, and there is no information leakage.

The headline is where the policy comes from. SQE reads a Ranger service of servicedef type `hive`, named `query` in the quickstarts, the same instance Apache Spark reads through its Kyuubi authorization plugin. One policy, written once in the Ranger console, enforces byte-identically in SQE and in Spark: an SSN masked to `xxx-xx-1111` reads the same no matter which engine ran the query. See [Spark / Ranger Parity](../design-notes/sqe-spark-ranger-parity.md) for the validated cross-engine result and its edges.

## Enforcement is off by default

The default `[policy] engine = "passthrough"` returns plans unmodified. Turn enforcement on by selecting an engine:

- `ranger` reads row-filter and column-mask policies from that Ranger instance and feeds the plan rewriter. This is the production path, and the one shared with Spark/Kyuubi.
- `in-memory` keeps grants in a hash map, for development and tests.

## Configure the Ranger backend

```toml
[policy]
engine = "ranger"
mask-precedence = "tag"        # which mask wins on a column covered by both (default: "tag")

[policy.ranger]
url = "http://ranger-admin:6080"
service-name = "query"         # the Ranger instance to read; shared with Spark/Kyuubi (default: "hive")
admin-user = "admin"
# Set the password via SQE_POLICY__RANGER__ADMIN_PASSWORD, not in the file.
admin-password = ""
cache-ttl-secs = 30            # resolved-policy cache TTL
catalog-scoped = false         # true: policy `database` = "<catalog>.<namespace>" (default: false)
```

Keys in `[policy.ranger]` take either spelling, `service-name` or `service_name`, and every key can be set from the environment as `SQE_POLICY__RANGER__<KEY>` (for example `SQE_POLICY__RANGER__CATALOG_SCOPED=true`).

### Which `database` a policy names

A Ranger `hive` policy has a `database`, a `table` and, for masks, a `column` resource. SQE matches the `database` against a key derived from the table's name:

| Table | `catalog-scoped = false` (default) | `catalog-scoped = true` |
|---|---|---|
| Iceberg `ws_sales.finance.orders` | `finance` | `ws_sales.finance` |
| Iceberg `ws_sales.a.b.orders` (multi-level namespace) | `a.b` | `ws_sales.a.b` |
| Hive root `glue.bi.orders` | `glue.bi` | `glue.bi` |

The default drops the catalog, the convention Hive and Spark/Kyuubi use, so one policy covers the same `namespace.table` in every catalog. That is wrong for a deployment that runs several Iceberg catalogs with the same namespace and table names, such as one catalog per workspace: a mask written for one workspace applies to all of them. Turn `catalog-scoped` on there, and write each policy's `database` as `<catalog>.<namespace>`. With it on:

- A policy matches one catalog only. The compatibility fallback that lets a policy naming only the last namespace component (`finance` for `a.finance`) still match is off, so no policy can match across catalogs.
- A table reference that carries no catalog is denied rather than looked up without one. The planner fills the session's catalog in, so this only bites a reference that cannot be resolved.
- `CREATE POLICY ... ON TABLE` needs the three-part name and writes `<catalog>.<namespace>` as the `database`.
- Policies written for the default convention stop matching. Rewrite them before turning the setting on, or masks disappear.
- The catalog is the name the session uses for it, not the warehouse behind it. A config catalog `[catalogs.iceberg]` over warehouse `test_warehouse` is keyed `iceberg.<namespace>`, whether the table is read directly or through a view. Before #835 a view's tables were keyed by the warehouse instead, so a mask written for `iceberg.<namespace>` did not apply through the view. A policy written for `test_warehouse.<namespace>` to cover views no longer matches anything: move it to the session's catalog name. A table no policy matches is read without masks or row filters, so check for such policies before upgrading.

Tag-based policies are unaffected: a tag policy names a tag, not a table.

### Which item and which policy wins

SQE evaluates mask and row-filter policies the way Ranger does:

- **Within a policy the first matching item wins.** Ranger keeps `dataMaskPolicyItems` and `rowFilterPolicyItems` in the order you write them and stops at the first item whose users, groups or roles match. "Mask for everyone except group E" is therefore two items in this order: `{groups: [E], MASK_NONE}`, then `{groups: [public], MASK_NULL}`. Swap them and E is masked too. A row-filter item with an empty filter expression filters nothing, which is how an exemption is written there.
- **Across mask policies the first policy that decides a column wins**, an exemption included. Policies with override priority (`policyPriority` 1) go first; within one priority SQE takes the order of the downloaded bundle. Ranger breaks that tie with a resource-specificity heuristic SQE does not reproduce, so do not write two same-priority mask policies on the same column for the same user; give the one that must win override priority.
- **Across row-filter policies SQE applies every matching policy's filter**, ANDed. Ranger applies only the first. A user covered by two filter policies sees no more rows in SQE than in Ranger, and possibly fewer.

`mask-precedence` settles the one case where a column is covered twice, by a resource policy naming it and by a tag policy matching its classification. `tag` is the default and matches the plugin order Hive and Spark/Kyuubi implement, so a rule authored once in Ranger renders the same value whichever engine reads it. `resource` restores the narrower most-specific-rule-wins reading. The column is masked either way; only which mask applies changes.

`[policy.ranger]` is distinct from `[access_control.ranger]`. The `[policy]` block points at the frontend-query service for SQE-side fine-grained enforcement (row filters and masks that SQE applies). The `[access_control]` block points at the `polaris` service for the coarse GRANT-to-catalog path where Polaris enforces. They can target the same Ranger Admin host and read different services. See [GRANT and REVOKE](../sql-reference/grant-revoke.md) for the two-axis model.

## Column masks

SQE realizes the full Ranger hive-servicedef built-in mask set. Each Ranger `dataMaskType` maps to a mask SQE applies in the rewritten plan:

| Ranger `dataMaskType` | Effect |
|---|---|
| `MASK_NULL` | Replace the value with a typed NULL. |
| `MASK_HASH` | HMAC-SHA256 hex digest (plain SHA-256 when no mask key is set). |
| `MASK` | Full redact: uppercase to `X`, lowercase to `x`, digit to `n`; punctuation kept. |
| `MASK_SHOW_LAST_4` | Show the last 4 characters, mask the rest. `111-11-1111` becomes `xxx-xx-1111`. |
| `MASK_SHOW_FIRST_4` | Show the first 4 characters, mask the rest. |
| `MASK_DATE_SHOW_YEAR` | Truncate a date to its year. |
| `CUSTOM` | An arbitrary SQL expression with `{col}` as the column placeholder. |
| `MASK_NONE` | Explicit exemption. Place it first in Ranger to carve an exception. |

Character counting is by Unicode scalar, matching Hive, which is what makes the output byte-identical to Spark. Anything SQE cannot map, including a `CUSTOM` expression that fails to parse, restricts the column instead of leaking it. Masking is fail-closed.

## Table access for Hive roots: grant first, deny by default

For an Iceberg table the object-level decision (may this user read this table at all) is Polaris's, made against the caller's own token before SQE plans anything. SQE therefore ignores Ranger *access* policies (`policyType` 0) for Iceberg tables and reads only masks and row filters from the `hive` service.

A Hive root has no Polaris in front of it. A Glue catalog or a sidecar warehouse is read with the engine's credentials, so SQE is the only place a table-level decision can be made. For those tables the Ranger store evaluates the access policies too, with Ranger's own semantics:

- A table is readable when an enabled access policy on its `database` (the policy key `<root>.<database>`) and `table` grants `select` or `all` to the user, one of their roles, or one of their groups. Ranger's `public` group matches every authenticated user.
- A matching item in `denyPolicyItems` wins over every allow, across all policies, unless a `denyExceptions` item names the user. `allowExceptions` carve a grantee out of an allow the same way.
- The `column` resource must be `*` (or absent) for the grant to count as table access, exactly as Ranger requires for `SELECT *`. Grant the table, then mask the columns that need it; a grant on two columns alone does not make the table visible.
- Nothing matching means denied. The table reads as "table not found", the same wording an absent table has, and is left out of `SHOW TABLES`, `information_schema` and the MCP listing tools. Masks and row filters still apply on top of a grant.

This is on by default (`[policy.ranger] hive_access_policies = true`), because the alternative is that every table appearing in a Glue database is open to every authenticated user until someone writes a mask for it. Grant a broad group `select` on `<root>.*` to get the old behaviour deliberately; set the option to `false` to get it silently.

Groups arrive from the JWT (`groups_claim` on the auth provider). A group-bound grant on a deployment without that claim matches nobody, which under this gate means everybody is denied; the store logs the group-only items once per bundle download for that reason.

The same group binding applies to masks and row filters, on resource and tag policies alike. A policy item's `groups` are compared exactly, case included, with the strings in the token's groups claim; nothing is stripped or mapped. Keycloak's group-membership mapper sends `/SG-DataAnalysts` (a leading slash) when "Full group path" is on, which does not match a Ranger group named `SG-DataAnalysts`; turn the option off or name the group with the slash in Ranger.

## Row filters

A Ranger row-filter policy attaches a boolean SQL expression to a table for a user or role. SQE parses it and injects it as a filter above the scan, so a user sees only the rows the expression admits. Row-filter expressions can reference session context.

## Role-conditional masking

Row filters and `CUSTOM` masks can call session-context functions: `current_user()`, `current_role()`, and `is_role_in_session()`. SQE const-folds them per session before the plan is distributed, so a fragment running on a worker carries the resolved value rather than re-evaluating identity. That is how a single policy masks a column for an analyst but shows it to an auditor, the way Snowflake conditional masking does.

## Tag-based masking

A mask can apply to every column carrying a tag rather than to a named column. The mask-per-tag rule lives in Ranger as a `tag`-service policy (returned in the Ranger download bundle's `tagPolicies` block, shared with Spark). The tag-to-column association is authored in SQL and stored in the Iceberg `sqe.column-tags` table property:

```sql
ALTER TABLE sales.orders SET TAGS (ssn = ('PII'), amount = ('FINANCIAL'));
ALTER TABLE sales.orders UNSET TAGS (amount);
SHOW TAGS ON sales.orders;
```

The Snowflake form works too: `ALTER TABLE ... MODIFY COLUMN ssn SET TAG PII = 'true'`. `SET TAGS` merges, changing only the columns you name. Storing the association as a table property means it travels with the data through clone, rename, and replicate, and covers federated catalogs Polaris cannot gate. Tag parity with Spark stops at the association: Spark reads it from the Ranger or Atlas tag store, so full tag parity would need an Iceberg-to-Ranger tag sync, which is optional and not built.

Author the mask type under the `hive:` prefix (`hive:MASK_SHOW_LAST_4`). Ranger's `tag` service definition never defines bare mask names: it aggregates the mask types of every component it can decorate, so the entries are `hive:MASK_SHOW_LAST_4`, `trino:MASK_NULL`, and so on. SQE accepts the bare and `hive:` forms and deliberately leaves another component's prefix unmatched, which restricts the tagged column rather than applying another engine's policy.

## Tag row filters need one Ranger Admin property

A `tag`-service policy can carry a row filter as well as a mask, so every table with a column tagged `PII` is filtered by one rule. Ranger ships this switched off, and it is not a version limitation:

```xml
<property>
  <name>ranger.servicedef.autopropagate.rowfilterdef.to.tag</name>
  <value>true</value>
</property>
```

Ranger copies each component's `dataMaskDef` into the tag service definition unconditionally, but copies its `rowFilterDef` only when that property is set in `ranger-admin-site.xml`. Without it, tag masks work and tag row filters cannot be authored at all: the POST is rejected with

```
tag policy can specify values for one of the following resource sets:
 does not have any resource hierarchies
```

which names resources rather than the missing capability, so it reads like a malformed policy. See [Ranger tag storage](../design-notes/ranger-tag-storage-decision.md) for the source-level detail.

## What happens when policy lookup fails

Every failure mode denies rather than falling back to unfiltered data:

| Condition | Behaviour |
|---|---|
| Ranger unreachable | Deny all rows. The circuit breaker opens, and enforcement resumes on its own once Ranger returns. |
| Tag state unknown (the table's `sqe.column-tags` property cannot be parsed; for an Iceberg table SQE reads it from the metadata loaded to plan the query, so a cold or disabled metadata cache does not cause this) | Refuse the statement with an `AccessDenied` error naming the table. Unknown is not the same as untagged: a mask might exist that SQE cannot see. An empty result would look like real data, so SQE does not return one. |
| Mask type SQE cannot map, including a `CUSTOM` expression that fails to parse | Restrict the column. It is nullified in place, not dropped, so the query still plans. |

Each of these is pinned by a test in `crates/sqe-coordinator/tests/it/access_control_e2e.rs` against a live Ranger, including one that stops the `ranger-admin` container mid-suite.

## Admin-side edits are honored on a delay

`cache-ttl-secs` bounds an over-permissive window. SQE caches the resolved policy per user and table, so a mask or row filter authored directly in the Ranger console is not applied until that entry expires, up to `cache-ttl-secs` later. A query that ran before the edit keeps its old decision for the rest of the TTL.

Grants issued through SQE (`GRANT`, `REVOKE`) are not affected, since those flush the cache. Lower the TTL if prompt propagation of console-authored edits matters more than fetch load against Ranger Admin. The tag path re-reads the column-to-tag map on every call and has no such window.

## Walkthrough

For a worked example that sets up both gates in order, with the SQL and the
output at each step, see the
[access control tutorial](./access-control-tutorial.md). It covers the Polaris
catalog gate and this fine-grained path separately, then together.

## The in-engine SQL surface

Independent of Ranger, SQE parses a native grant surface (`GRANT ... ROWS WHERE`, `GRANT ... MASKED WITH`, `SHOW EFFECTIVE GRANTS`, `CHECK ACCESS`) that the `in-memory` engine enforces. See [Security & Policy](../architecture/security.md) and [GRANT and REVOKE](../sql-reference/grant-revoke.md).

## How it fits the trust model

Fine-grained enforcement is one layer. Catalog metadata and the write path are gated per user through the caller's bearer token; the read data path uses the engine's storage credentials. See [Security and trust model](../architecture/security-model.md) for the full boundary map, and [Fine-grained Enforcement](../design-notes/ranger-fine-grained-enforcement.md) for the rewrite internals and the precedence contract.
