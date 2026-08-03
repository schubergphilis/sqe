# Access control tutorial

A working walkthrough of both halves of SQE's access control, in the order you
would actually build them: first decide who may open a table at all, then decide
which rows and columns they see inside it.

Every statement here runs against `quickstart/polaris-ranger-keycloak`. The same
ground is covered as an executable transcript by
`scripts/access-control-demo.sh` (32 steps, exits non-zero on any mismatch) and
asserted on decoded Arrow values by `make test-access-control` (23 cases). If
something in this page disagrees with those, they are right and this is stale.

## The two gates

```
        GRANT / REVOKE / DENY                 Ranger hive service
                 |                          (row filters, masks, tags)
                 v                                    |
  ranger "polaris" service                            v
                 |                            SQE plan rewriter
                 v                                    |
  Polaris embedded authorizer  --> table --> DataFusion --> rows
       "may you open it"                  "what do you see"
```

**Gate one is Polaris.** `GRANT` and `REVOKE` in SQE become policies on the
Ranger `polaris` service, and Polaris's embedded Ranger authorizer enforces them
when SQE asks to load a table. This answers *may this user open this object at
all*. SQE does no filtering here. A denial arrives as "table not found", because
Polaris hides rather than forbids.

**Gate two is SQE.** Row filters, column masks and column restriction are
applied by rewriting the logical plan before DataFusion optimizes it, from
policies on a Ranger `hive` service. This answers *which rows and columns may
this user see*.

They are independent, they use different Ranger services, and a query must pass
both. Revoking the coarse `SELECT` denies the query at Polaris before any mask is
ever computed, which is why the order in this tutorial matters: a mask you cannot
observe is a mask you cannot debug.

| | Gate one | Gate two |
|---|---|---|
| Enforced by | Polaris | SQE |
| Ranger service | `polaris` | `hive` |
| Authored with | `GRANT` / `REVOKE` / `DENY` in SQL | Ranger policies (console or REST) |
| Granularity | catalog, namespace, table, view | row, column, tag |
| Denial looks like | table not found | fewer rows, or masked values |

---

# Part 1: the Polaris gate

## 1.1 Two grants, not one

Start with the thing that trips up every first attempt. A table grant on its own
is not enough to read the table. Reaching `sales_wh.acdemo.orders` needs two
things to succeed, and only one of them is about the table:

- `LIST_NAMESPACES`, authorized at the **catalog** level. Polaris does not use
  Ranger's `SELF_OR_DESCENDANTS` matching, so a namespace-scoped `namespace-list`
  will not do: listing is denied outright, not filtered.
- a per-namespace visibility probe (`LOAD_NAMESPACE_METADATA`) needing
  **namespace**-level `namespace-properties-read`. A 403 hides the namespace,
  deliberately, so ungranted namespace names do not leak.

Either failure gives an empty schema list, and planning stops at "table not
found" without ever attempting `LOAD_TABLE`. The table exists and Polaris would
serve it; SQE never asks.

SQE writes the namespace half for you. `GRANT SELECT ON sales_wh.acdemo.orders`
also grants `namespace-properties-read` on `sales_wh.acdemo`, so the minimum to
read one table is two statements:

```sql
GRANT USAGE  ON DATABASE sales_wh         TO ROLE "analyst";  -- discovery
GRANT SELECT ON sales_wh.acdemo.orders    TO ROLE "analyst";  -- the data
```

The namespace grant is an ancestor **on the path** to the table you named. It is
required to reach that table and confers nothing about anything else, so adding
it is completing the statement rather than widening it. It grants visibility, not
data: a second table in the same namespace stays unreadable without its own
grant.

**The catalog grant stays yours to make, deliberately.** Catalog-level
`namespace-list` lets its holder enumerate every namespace name in the catalog,
including ones with no relation to the table being granted. A namespace called
`pii_customer_health` becomes visible even though not one of its rows is. Adding
that as a side effect of a table grant would widen the blast radius of every
`GRANT SELECT`, so SQE refuses to guess. Grant discovery per role, once, and
expect `SHOW SCHEMAS` to be the leak surface.

In the quickstart both halves already exist for `analyst` and `engineer`: the
bootstrap seeds wildcard discovery, which is why a lone `GRANT SELECT` looks
sufficient there. Any user outside those two roles needs the catalog grant spelled
out.

**Revoke is asymmetric here, on purpose.** `REVOKE SELECT` on the table does not
take back namespace visibility. One namespace policy serves every table granted
under it, so releasing it on the first revoke would break the grantee's access to
the others. A namespace whose tables have all been revoked leaves the grantee able
to see that the namespace exists and nothing more. Policies SQE added this way
carry the label `sqe:traversal:<GRANTEE_TYPE>:<name>` in the Ranger console.

To remove the residue, revoke catalog discovery **first**:

```sql
REVOKE USAGE ON DATABASE sales_wh       FROM ROLE "analyst";
REVOKE USAGE ON SCHEMA   sales_wh.acdemo FROM ROLE "analyst";
```

That order matters right now, and not for tidiness. A principal left holding
catalog discovery while every namespace under it is invisible currently **hangs**
instead of being denied: the per-namespace probe returns 403 for everything, and
SQE's catalog provider blocks in its sync-to-async bridge rather than reporting
"table not found". Revoking discovery first never passes through that state. The
hang is a known defect in the read path, recorded in
`docs/internal/research/2026-08-02-catalog-traversal-gate.md`, and it is not
specific to revoking: any principal configured with catalog discovery and no
visible namespace can reach it.

## 1.2 A grant is what enables a read

With the traversal in place, the grant is observable. Before:

```sql
-- as alice
SELECT id FROM sales_wh.acdemo.orders;
-- table 'sales_wh.acdemo.orders' not found
```

Grant it, wait for the Polaris plugin to poll (5 to 30 seconds; it is not
instant), and the same statement returns rows:

```sql
-- as carol, an admin
GRANT SELECT ON sales_wh.acdemo.orders TO ROLE "analyst";

-- as alice, a member of analyst
SELECT id, region FROM sales_wh.acdemo.orders ORDER BY id;
--  id | region
-- ----+--------
--   1 | EU
--   2 | US
--   3 | EU
```

`REVOKE` puts it back:

```sql
REVOKE SELECT ON sales_wh.acdemo.orders FROM ROLE "analyst";
```

**Revoking one privilege does not disturb another.** Ranger permits a single
policy per resource, so every grant on a table shares one item and their access
types union. `INSERT` requires everything `SELECT` does, which means a literal
`REVOKE INSERT` would strip the read access too. SQE labels each grant
(`sqe:<GRANTEE_TYPE>:<name>:<PRIVILEGE>`) and holds back the access types
another labelled privilege still needs, so narrowing a user from read-write to
read-only does what it says.

Two properties worth internalising:

**A role grant reaches only role members.** dave is in no role, so the grant
above does nothing for him. Role membership lives in **Ranger**, not in the
token: Polaris ignores the token's realm roles because they lack the
`PRINCIPAL_ROLE:` prefix it expects.

**Read does not imply write.** `SELECT` and `INSERT` are separate privileges and
map to disjoint access-type sets. An analyst holding `SELECT` gets a denial on
`INSERT` until `GRANT INSERT` is issued too.

## 1.3 Privileges and the level each binds to

| SQL privilege | Ranger access types | Level |
|---|---|---|
| `SELECT` | `table-data-read`, `table-properties-read`, `table-list` | table |
| `INSERT` / `UPDATE` / `DELETE` / `MODIFY` | `table-data-write` plus the full snapshot, schema, sort-order, partition-spec and properties commit set | table |
| `DROP` | `table-drop` | table |
| `CREATE TABLE` | `table-create` | namespace |
| `USAGE` | `namespace-list`, `namespace-properties-read` | namespace |
| `DROP SCHEMA` | `namespace-drop` | namespace |
| `CREATE SCHEMA` | `namespace-create` | catalog |
| `ALL PRIVILEGES` | `catalog-content-manage` | catalog |

Two things follow from that right-hand column.

**One privilege expands to many access types.** The Polaris embedded authorizer
does not honour service-def implied-grants, so SQE lists every access type the
operation will check. `INSERT` is 22 of them, because committing an Iceberg
snapshot fans out into many fine-grained Polaris operations.

**The level is not advisory.** Naming an object deeper than a privilege's level
used to silently widen the grant. `GRANT ALL ON sales_wh.acdemo.orders` dropped
the namespace and table and wrote `catalog-content-manage` on `sales_wh`: one
table named, success reported, the whole catalog conferred. SQE now refuses it
and names the scope that would have been written:

```
Privilege 'ALL PRIVILEGES' binds to the catalog level, but the statement names
a namespace or table. The policy would apply to 'sales_wh' and everything under
it, which is wider than the object named. Re-issue the statement against
'sales_wh', or name a privilege that binds to the object you meant.
```

`USAGE` on a table and `CREATE SCHEMA` on a namespace widen through the same
path and are refused the same way.

## 1.4 Wildcards: all and future

```sql
GRANT SELECT ON ALL TABLES IN SCHEMA sales_wh.acdemo TO ROLE "analyst";
GRANT SELECT ON FUTURE TABLES IN SCHEMA sales_wh.acdemo TO ROLE "analyst";
```

Both write the same policy, with `table = "*"`, so both cover existing and
future tables. Ranger has no future-only resource. Snowflake distinguishes the
two; SQE cannot, and treats `ON FUTURE` as a superset rather than rejecting it.
Use a table-specific grant when you mean one existing table.

Do not confuse either with `GRANT ... ON SCHEMA`, which stays a namespace
resource and does not reach the tables inside it. Namespace `USAGE` is
`namespace-list` plus `namespace-properties-read` and deliberately carries no
`table-data-read`.

## 1.5 Views

A view has no resource level of its own. Its NAME goes in the `table` slot and
the access types are the `view-*` set:

```sql
CREATE OR REPLACE VIEW sales_wh.acdemo.orders_eu AS
  SELECT id, region FROM sales_wh.acdemo.orders WHERE region = 'EU';

GRANT SELECT ON VIEW sales_wh.acdemo.orders_eu TO ROLE "analyst";

SHOW GRANTS ON sales_wh.acdemo.orders_eu;
-- view-properties-read | sales_wh.acdemo.orders_eu | ROLE | analyst | ALLOW
-- view-list            | sales_wh.acdemo.orders_eu | ROLE | analyst | ALLOW
```

Note what is absent: no `table-data-read`.

**A view is not a privilege boundary.** SQE expands the view and plans against
its base tables, so the reader needs a grant on `orders` as well. This is the
opposite of a Snowflake secure view, where the view owner's privileges stand in
for the reader's. Never use a view to hand out indirect access to a table.

What a view does give you is masking and filtering that cannot be dodged, which
is Part 2.

## 1.6 DENY

```sql
DENY SELECT ON sales_wh.acdemo.orders TO USER dave;
```

Deny beats allow in Ranger, so this overrides any grant dave holds directly or
through a role. It is idempotent (re-issuing updates the same policy rather than
stacking), reversible with `REVOKE`, and audited as a privilege change.

One caveat, deliberate: DENY goes through Ranger's policy API, which authorizes
the authenticated REST user rather than a named `grantor`. Unlike `GRANT` it is
therefore not resource-scoped to the caller, and the `admin_roles` config gate is
the only check. Ranger offers no grantor-scoped deny.

## 1.7 Introspection

```sql
SHOW GRANTS ON sales_wh.acdemo.orders;
```

Reads the policies back out of Ranger, one row per (access type, grantee).

```sql
CHECK ACCESS SELECT ON sales_wh.acdemo.orders FOR USER "alice";
--  allowed | reason
-- ---------+---------------------------
--  true    | Allowed via ROLE 'analyst'
```

`CHECK ACCESS` resolves the target user's Ranger roles, including nested roles,
and applies deny-overrides-allow. It is best-effort introspection, not the
enforcement path: it does not account for tag policies, conditions, or wildcard
resource matching beyond exact match and bare `*`. Polaris remains
authoritative.

It does not resolve **groups**, because Ranger only learns a user's groups when
usersync runs. A grant reachable only through a group will not show up here.

---

# Part 2: the SQE data gate

Everything in Part 2 is enforced by SQE, from a Ranger **`hive`** service, and is
invisible to gate one. A user must already hold `SELECT` for any of it to be
observable.

Policies here are authored in Ranger (console or REST), not in SQL. SQE reads
them; `GRANT ... MASKED WITH` and `ROWS WHERE` parse but are only honoured by the
in-memory engine, not the Ranger backend.

Resolved policies are cached. A mask tightened in the console is not honoured
until the cached entry expires, up to `[policy.ranger] cache-ttl-secs`. Grants
issued through SQE flush the cache on commit, so only console-authored changes
have that window.

## 2.1 Column masks

A `policyType: 1` (datamask) policy on the `hive` service, scoped to database,
table and column:

```json
{
  "service": "hive",
  "name": "acdemo-mask-ssn",
  "policyType": 1,
  "isEnabled": true,
  "resources": {
    "database": {"values": ["acdemo"]},
    "table":    {"values": ["orders"]},
    "column":   {"values": ["ssn"]}
  },
  "dataMaskPolicyItems": [{
    "roles":    ["engineer"],
    "accesses": [{"type": "select", "isAllowed": true}],
    "dataMaskInfo": {"dataMaskType": "MASK_SHOW_LAST_4"}
  }]
}
```

The `database` value is the **namespace**, not the catalog. bob (an engineer)
then sees the masked value while alice (analyst only) sees the raw one, from the
same statement against the same table. That contrast is the point: a mask is
per-principal, not per-column.

The full Ranger built-in vocabulary is implemented:

| `dataMaskType` | `111-11-1111` becomes | Notes |
|---|---|---|
| `MASK_NULL` | `NULL` | typed NULL, row count unchanged |
| `MASK_SHOW_LAST_4` | `xxx-xx-1111` | |
| `MASK_SHOW_FIRST_4` | `111-xx-xxxx` | |
| `MASK` | `nnn-nn-nnnn` | `X` / `x` / `n` per character class, punctuation kept. `EU` becomes `XX` |
| `MASK_HASH` | 64 hex chars | HMAC-SHA256 keyed by `policy.mask_key`. **Set the key**: without it SQE warns and hashes unkeyed, which is brute-forceable on low-entropy columns like SSN |
| `MASK_DATE_SHOW_YEAR` | `2021-05-04` becomes `2021-01-01` | dates only |
| `CUSTOM` | whatever you write | arbitrary SQL with `{col}` as the placeholder |
| `MASK_NONE` | unchanged | explicit exemption, depends on policy evaluation order |

Masks also block predicate pushdown on the raw value. `WHERE ssn = '111-11-1111'`
evaluates against the masked value, never the underlying one, so a mask cannot be
peeled off with a filter.

## 2.2 Row filters

`policyType: 2`, with a `filterExpr` that is SQL:

```json
{
  "service": "hive",
  "name": "acdemo-rowfilter-eu",
  "policyType": 2,
  "isEnabled": true,
  "resources": {
    "database": {"values": ["acdemo"]},
    "table":    {"values": ["orders"]}
  },
  "rowFilterPolicyItems": [{
    "roles":    ["engineer"],
    "accesses": [{"type": "select", "isAllowed": true}],
    "rowFilterInfo": {"filterExpr": "region = 'EU'"}
  }]
}
```

The filter is injected above the `TableScan`, before optimization, so the user's
own predicates can be pushed through it but not around it. Multiple applicable
filters AND together.

Session functions are const-folded per session, which is how one policy serves
many principals:

```sql
region = current_user() OR is_role_in_session('auditor')
```

`current_user()`, `current_role()` and `is_role_in_session()` are available.

**One caveat.** A row filter referencing a column the view does not project fails
the query when read through that view:

```
Plan rewrite failed: Internal error: Failed to create policy filter:
Schema error: No field named region.
```

Fail-closed, so nothing leaks, but the message names neither the policy nor the
view. The same filter with the same narrow projection in a direct query works.
Until this is fixed, a row filter and a narrow view over the same table are
mutually exclusive.

## 2.3 Column restriction

A mask SQE cannot build is not returned raw. The column is nullified in place and
stays in the schema, so `SELECT that_column` still plans rather than erroring on
an unknown field. This is the fail-closed path, and it is what you get from a
`CUSTOM` mask with no expression, or a mask type carrying another component's
prefix such as `trino:MASK_NULL`.

## 2.4 Tags

Tags let one rule protect a column wherever it appears, instead of one policy per
table. There are two halves, and they live in different places.

**Association: which columns carry which tag.** In SQE, on the Iceberg table
property `sqe.column-tags`, written with SQL:

```sql
ALTER TABLE sales_wh.acdemo.orders SET TAGS (ssn = ('PII'), region = ('GEO'));
SHOW TAGS ON sales_wh.acdemo.orders;
ALTER TABLE sales_wh.acdemo.orders UNSET TAGS (region);
```

`SET TAGS` merges rather than replaces, so a previous tag on another column
survives. Flushing the policy cache is part of the statement.

**The rule: what the tag means.** A policy on the Ranger **tag** service:

```json
{
  "service": "acdemo_tag",
  "name": "acdemo-tag-pii",
  "policyType": 1,
  "isEnabled": true,
  "resources": {"tag": {"values": ["PII"]}},
  "dataMaskPolicyItems": [{
    "roles":    ["engineer"],
    "accesses": [{"type": "hive:select", "isAllowed": true}],
    "dataMaskInfo": {"dataMaskType": "hive:MASK_SHOW_LAST_4"}
  }]
}
```

Two details that will cost you an afternoon each:

**Mask types must be component-qualified.** `hive:MASK_SHOW_LAST_4`, never the
bare name. The tag service definition does not define bare names.

**Tag row filters need a Ranger Admin property.** Tag masks work out of the box.
Tag row filters need this in `ranger-admin-site.xml`:

```xml
<property>
  <name>ranger.servicedef.autopropagate.rowfilterdef.to.tag</name>
  <value>true</value>
</property>
```

Ranger copies each component's `dataMaskDef` into the tag service definition
unconditionally, but copies its `rowFilterDef` only when that property is true,
and it defaults to false. No Ranger upgrade changes this. Without it the POST is
rejected with "tag policy can specify values for one of the following resource
sets: does not have any resource hierarchies", which names resource hierarchies
rather than the missing capability.

### A tag is not a protection

This is the one thing people get backwards.

| Situation | Result |
|---|---|
| Column tagged, **no rule anywhere** | column returned **raw** |
| Column tagged, rule SQE **cannot map** | column **restricted** |
| Tag state **unknown** (Ranger unreachable) | **all rows denied** |

A tag with no policy is not a protection, so there is nothing to fail closed
about. A tag whose policy names a mask SQE cannot build IS a protection SQE
cannot honour, so the column is restricted. **Tagging a column does not protect
it. The rule in Ranger is what protects it.**

### Spark parity

Masks are shared with Spark through the same `hive` service. Associations are
not: Spark reads tag associations from the Ranger or Atlas tag store, while SQE
reads them from Iceberg table properties. One mask rule, two association
sources.

## 2.5 Precedence

1. **Restriction beats mask.** A column SQE cannot safely return is nullified,
   whatever the mask says.
2. **A resource mask beats a tag mask.** The specific rule wins over the
   general one.
3. **Row filters AND together.**
4. **Deny beats allow**, on gate one.

## 2.6 What happens when things break

| Condition | Result |
|---|---|
| Ranger unreachable | all rows denied; enforcement resumes on recovery |
| Tag state unknown | all rows denied. Unknown is not "untagged" |
| Unmappable mask type | column restricted, never returned raw |
| Unparseable row filter | becomes `lit(false)`, all rows denied |
| Table not mappable to a policy key | all rows denied |
| Tag carrying no rule | column returned raw (see above) |
| Policy cache not yet expired | **fail-stale**, up to `cache-ttl-secs` |

Everything fails closed except the cache, which is deliberately fail-stale and
bounded by its TTL.

---

## Putting both gates together

An analyst who may read European orders, without ever seeing an SSN:

```sql
-- Gate one: may they open it
GRANT USAGE  ON DATABASE sales_wh        TO ROLE "analyst";
GRANT USAGE  ON SCHEMA   sales_wh.acdemo TO ROLE "analyst";
GRANT SELECT ON sales_wh.acdemo.orders   TO ROLE "analyst";

-- Gate two: what they see (author on the hive service)
--   policyType 2, filterExpr "region = 'EU'",       roles ["analyst"]
--   policyType 1, column ssn, MASK_SHOW_LAST_4,     roles ["analyst"]

-- Verify gate one
CHECK ACCESS SELECT ON sales_wh.acdemo.orders FOR USER "alice";
-- true | Allowed via ROLE 'analyst'

-- Verify gate two by reading as alice
SELECT id, region, ssn FROM sales_wh.acdemo.orders ORDER BY id;
--  id | region | ssn
-- ----+--------+-------------
--   1 | EU     | xxx-xx-1111
--   3 | EU     | xxx-xx-3333
```

Two rows, not three, and no raw SSN. If you see three rows the filter has not
landed yet; if you see the raw SSN the mask has not. Check the TTL before
changing anything.

## Where to go next

- [Access control: support matrix](./access-control-matrix.md) for what is
  supported and what is proven, with the test names.
- [Fine-grained access control](./fine-grained-access-control.md) for
  configuration keys and the mask vocabulary in reference form.
- [Ranger access control](../design-notes/ranger-access-control.md) for the
  catalog path, the identity model, and why role membership lives in Ranger.
- [Fine-grained enforcement](../design-notes/ranger-fine-grained-enforcement.md)
  for the plan-rewrite internals and the precedence contract.
- [Polaris + Ranger + Keycloak quickstart](../quickstart/polaris-ranger-keycloak.md)
  for a stack that runs all of it.
