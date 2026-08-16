# GRANT and REVOKE

> **Chameleon / SBP-specific.** The access-control SQL surface on this page
> (column masks, row filters, effective-grant inspection, `CHECK ACCESS`) is an
> SQE security extension built for the Chameleon platform. It is not part of
> the core open-source Iceberg SQL surface, and the grant backend is pluggable:
> SQE ships a Polaris backend and a Chameleon backend. A default OSS deployment
> can run without it. It is documented here for completeness; treat it as an
> optional, platform-specific layer.

SQE-specific security extensions on top of the SQL standard `GRANT` / `REVOKE`. The base shapes are parsed by `sqlparser-rs`; SQE adds:

- **Column masks**: `GRANT SELECT ON ... TO ... MASKED WITH expr`.
- **Row filters**: `GRANT SELECT ON ... TO ... ROWS WHERE expr`.
- **Effective-grant inspection**: `SHOW EFFECTIVE GRANTS FOR USER "x"` returns the resolved policy for a user across roles and inheritance.
- **Resource-scoped listing**: `SHOW GRANTS ON ns.table`.
- **Pre-flight check**: `CHECK ACCESS SELECT ON ns.table FOR USER "x"` returns boolean without executing.

These extensions are parsed in `crates/sqe-sql/src/classifier.rs` (pre-parser scan) and enforced by the policy engine in `crates/sqe-policy/`. The plan rewriter injects row filters above `TableScan` and substitutes column masks before DataFusion's optimizer runs, so the optimizer cannot push user predicates through a mask.

This page is the SQL surface reference.

## Privileges

| Privilege | Applies to | Effect |
|---|---|---|
| `SELECT` | table, view, schema, catalog | Read rows. Combines with row filters and column masks. |
| `INSERT` | table | Append new rows. |
| `UPDATE` | table | Modify existing rows. |
| `DELETE` | table | Remove rows. |
| `MODIFY` | table | Shorthand for INSERT + UPDATE + DELETE + MERGE. Required by maintenance procedures. |
| `DROP` | table, schema | Required by `DROP TABLE`, `DROP SCHEMA`, `system.expire_snapshots`. |
| `CREATE` | schema, catalog | Required to create new tables / schemas. |
| `ALL PRIVILEGES` | catalog to GRANT, any to REVOKE | Every privilege on the resource. See [Closing a gate](#closing-a-gate) for why the two directions differ. |

## Grantee types

| Type | Syntax | Source |
|---|---|---|
| User | `TO USER "alice"` | OIDC subject claim. |
| Role | `TO ROLE "analyst"` | Group claim from the OIDC provider, or a manually mapped role. |
| Public | `TO PUBLIC` | Every authenticated user. Avoid in production. |

## Statements

### Standard GRANT / REVOKE

```sql
GRANT SELECT ON analytics.events TO ROLE "analyst";
GRANT INSERT, UPDATE ON staging.tmp TO USER "etl";
GRANT ALL PRIVILEGES ON SCHEMA analytics TO ROLE "data_engineer";
REVOKE INSERT ON staging.tmp FROM USER "etl";
```

The standard form is parsed by `sqlparser-rs` and routed via `StatementKind::Grant` / `StatementKind::Revoke`.

### Closing a gate

`REVOKE SELECT` does not stop a principal reading while any remaining privilege
still confers `table-properties-read`. The statement reports success and the
rows still come back. That is the release limitation, not a failed revoke
(issue #396).

Privileges expand to Polaris access types. A writer must load the table before
it can write, so the INSERT profile includes `table-properties-read`. After
`LOAD_TABLE` succeeds, SQE reads parquet with FileIO (vended STS, or the
engine `[storage]` key when vending is off). Polaris `table-data-read` is
not the SQE read gate.

Measured on the live Ranger + Polaris stack:

| Remaining access types | `SELECT` after `REVOKE SELECT` |
|---|---|
| Full INSERT expansion (19 types) | reads succeed |
| Same expansion minus `table-data-read` (18) | reads still succeed |
| `table-data-write` alone | 403 `LOAD_TABLE` |
| `table-data-write` + `table-properties-read` | reads succeed |

Unity Catalog keeps SELECT and MODIFY independent. SQE cannot match that with
a `grant-profile.json` edit: dropping `table-properties-read` from INSERT
breaks `LOAD_TABLE` for writers.

Use `REVOKE ALL PRIVILEGES` to close a gate. It means "this grantee holds nothing
on this object afterwards", at any level, and needs no knowledge of which
privileges were granted. Follow it with `CHECK ACCESS` for that principal.

```sql
REVOKE ALL PRIVILEGES ON sales.orders FROM ROLE "analyst";
```

It reads the access types the grantee actually holds from Ranger rather than
planning them from a privilege name, so grants written before SQE tracked
provenance, or written straight through the Ranger console, are removed too. It
also clears that grantee's `DENY` items at the object, and is idempotent: running
it against an object the grantee never held is a successful no-op.

**`GRANT ALL PRIVILEGES` still binds no deeper than the catalog**, and the
asymmetry is deliberate. Granting "everything" at a coordinate requires a
definition of everything, and getting that wrong once wrote a catalog-wide policy
from a single-table grant. Revoking everything needs no such definition: it only
removes, so it cannot widen access.

### Column masks (SQE extension)

```sql
GRANT SELECT (id, name, email)
    ON users
    TO ROLE "support"
    MASKED WITH (
        email = sha256(email)
    );
```

The `MASKED WITH` clause is post-parse: SQE walks the AST after sqlparser succeeds and lifts the trailing extension into a `PolicyStatement` node. Anyone with the `support` role sees the masked email; the unmasked column never reaches the user's session. Plan optimization happens after the substitution so a `WHERE email = 'x@y.com'` predicate cannot bypass the mask.

### Row filters (SQE extension)

```sql
GRANT SELECT ON orders TO ROLE "regional_eu"
    ROWS WHERE region = 'EU';
```

The filter expression is injected as a `Filter` node directly above `TableScan` for the `orders` reference. DataFusion's predicate pushdown can move user `WHERE` clauses through the row filter (because filters compose), but cannot eliminate it.

### SHOW GRANTS

| Form | What it returns |
|---|---|
| `SHOW GRANTS ON ns.table` | All grants on the resource. |
| `SHOW GRANTS ON SCHEMA ns` | All grants on the schema. |
| `SHOW GRANTS TO USER "alice"` | Direct grants to the user (does not include role-inherited). |
| `SHOW GRANTS TO ROLE "analyst"` | Direct grants to the role. |
| `SHOW EFFECTIVE GRANTS FOR USER "alice"` | Resolved policy: direct grants + role-inherited + masks + row filters. The view a query planner uses. |

```text
sqe> SHOW EFFECTIVE GRANTS FOR USER "alice";
+------------------+--------+-----------+--------------+----------------------+
| resource         | privilege | grantee  | row_filter   | column_masks         |
+------------------+--------+-----------+--------------+----------------------+
| analytics.events | SELECT | role "an" | region='EU'  | none                 |
| users            | SELECT | role "su" | none         | email -> sha256(...) |
+------------------+--------+-----------+--------------+----------------------+
```

### CHECK ACCESS

A pre-flight test. Returns boolean without executing the query.

```sql
CHECK ACCESS SELECT ON analytics.events FOR USER "alice";
-- true

CHECK ACCESS DELETE ON analytics.events FOR USER "alice";
-- false
```

Useful in scripts that want to bail out before a long-running query if the user lacks permission, and in the test suite to verify policy logic.

## Comparison

| Feature | SQE | Trino + Iceberg | Spark + Iceberg | DuckDB |
|---|---|---|---|---|
| `GRANT` / `REVOKE` (SQL standard) | yes | yes (with Ranger) | yes (Ranger / Lake Formation) | no |
| Column masks | `GRANT ... MASKED WITH` | external (Ranger) | external (Ranger) | no |
| Row filters | `GRANT ... ROWS WHERE` | external (Ranger) | external (Ranger) | no |
| `SHOW EFFECTIVE GRANTS` | yes | no | no | no |
| `CHECK ACCESS` (pre-flight) | yes | no | no | no |
| Per-user OIDC bearer to storage | yes (catalog + writes; reads use the configured storage key) | no (service account) | no (service account) | no |
| Plan-level enforcement | yes (rewriter) | external middleware | external middleware | no |

The structural difference: SQE keeps policy in-engine, plan-rewritten before optimization, and tied to the per-query bearer token. Trino and Spark push the responsibility to Apache Ranger, which lives outside the engine and intercepts at the connector boundary.

## Backends

Two orthogonal settings control access control, and both default to off:

`[access_control] backend` decides where GRANT/REVOKE are stored and resolved:

| `backend` | Use case | Where it lives |
|---|---|---|
| `none` (default) | OSS default. GRANT/REVOKE parse but are not enforced. | n/a |
| `polaris` | Grants resolved from Apache Polaris (`PRINCIPAL` / `PRINCIPAL_ROLE` / `CATALOG_ROLE`). | `crates/sqe-policy/src/grants/polaris.rs` |
| `ranger` | Grants written to Ranger Admin via the Polaris embedded authorizer. | `crates/sqe-policy/src/grants/ranger.rs` |
| `chameleon` | Schuberg Philis platform API (`GROUP` / `USER` grantees). | platform API client |

`[policy] engine` decides how row filters and column masks are evaluated:

| `engine` | Use case | Where it lives |
|---|---|---|
| `passthrough` (default) | No enforcement. Plans returned unmodified. | `PassthroughEnforcer` |
| `in-memory` | Single-node dev and tests. Policies in a hash map. | `crates/sqe-policy/src/policy_store.rs` |
| `ranger` | Apache Ranger fine-grained policies (row-filter + data-mask), requires `[policy.ranger].url`. | `crates/sqe-policy/src/ranger_store.rs` |
| `opa` / `cedar` | Defined in config but not yet wired (selecting them errors today). | `crates/sqe-policy/src/opa.rs` (OPA) |

Configure them in the engine TOML:

```toml
[access_control]
backend = "ranger"          # none (default) | polaris | ranger | chameleon

[policy]
engine = "ranger"           # passthrough (default) | in-memory | ranger
```

A default OSS deployment leaves both at their defaults and runs the open Iceberg SQL surface with grants parsed but unenforced.

## Why plan rewriting, not connector hooks

A short rationale:

1. **Optimization safety**: filters added above `TableScan` survive predicate pushdown but cannot be eliminated. Connector-level hooks run after planning and can be bypassed by a clever WHERE clause.
2. **Information leakage**: a user querying `column_that_is_masked = 'secret'` gets zero rows, exactly as if the column did not exist. PostgreSQL RLS uses the same model.
3. **Auditability**: the rewritten plan is logged. Reviewers see exactly what filter was applied per query, per user.
4. **Composability**: row filters from multiple grants AND together; column masks from multiple grants are applied innermost-out. The semantics are explicit instead of implementation-defined.

## Known gaps

- No `WITH GRANT OPTION`. Grants are non-delegating; only an admin can grant.
- No column-level INSERT (`GRANT INSERT (col1, col2) ON ...`). The granularity is table-level for INSERT today.
- Mask expressions are scalar only; aggregate / table-valued mask expressions are not allowed.
- `REVOKE SELECT` is not a read gate while `table-properties-read` remains (INSERT is the common leftover). Use `REVOKE ALL PRIVILEGES` and `CHECK ACCESS`. See [Closing a gate](#closing-a-gate). This is a documented release limitation (issue #396), not something a profile rewrite can fix.

File an issue if any of these block your use case.
