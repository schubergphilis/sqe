# Fine-grained access control

SQE enforces row filters and column masks by rewriting the query's logical plan before DataFusion optimizes it. Filters and masks are injected above the table scan, so the optimizer cannot push a user predicate through a mask to probe raw values. The model follows PostgreSQL row-level security: denied rows are invisible, masked columns return transformed values, and there is no information leakage.

The headline is where the policy comes from. SQE reads a Ranger service of servicedef type `hive`, named `query` in the quickstarts, the same instance Apache Spark reads through its Kyuubi authorization plugin. One policy, written once in the Ranger console, enforces byte-identically in SQE and in Spark: an SSN masked to `xxx-xx-1111` reads the same no matter which engine ran the query. See [Spark / Ranger Parity](../design-notes/sqe-spark-ranger-parity.md) for the validated cross-engine result and its edges.

## Enforcement is off by default

The default `[policy] engine = "passthrough"` returns plans unmodified. Turn enforcement on by selecting an engine:

- `ranger` reads row-filter and column-mask policies from that Ranger instance and feeds the plan rewriter. This is the production path, and the one shared with Spark/Kyuubi.
- `in-memory` keeps grants in a hash map, for development and tests.
- `opa` and `cedar` are defined in config but not yet wired (selecting them errors today).

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
```

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
| Tag state unknown (the table's metadata is not in cache) | Deny all rows. Unknown is not the same as untagged: a mask might exist that SQE cannot see. |
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
