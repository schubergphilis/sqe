================================================================================
FILE: openspec/changes/phase-5-policy-sql/proposal.md
================================================================================

# Proposal: Phase 5 — Policy SQL Extensions & Enforcement

## Summary

Add custom SQL statements (GRANT, REVOKE, SHOW GRANTS, SHOW EFFECTIVE POLICY)
that call an external policy engine to manage and enforce column-level and
row-level security. The policy engine is pluggable — OPA, Cedar, or a custom
backend — and enforcement happens via LogicalPlan rewriting before optimization.

## Motivation

The sovereign data platform needs fine-grained access control that the catalog
(Polaris) doesn't provide. Polaris handles table-level access via OIDC scopes,
but column masking and row filtering require a dedicated policy layer. Users
expect SQL-native commands to manage these policies — not a separate admin UI.

The Trino DCAF branch had no equivalent; this is net-new capability that makes
SQE the single interface for both querying and governing data.

## What Changes

- New crate `sqe-policy`: PolicyEnforcer trait implementation, policy engine client,
  plan rewriter, policy cache
- Extended crate `sqe-sql`: Custom AST nodes for GRANT/REVOKE/SHOW GRANTS/SHOW
  EFFECTIVE POLICY, parser extensions via sqlparser-rs
- Extended crate `sqe-coordinator`: Statement routing — policy DDL goes to
  sqe-policy, queries go through plan rewriting before execution
- New: Rego/Cedar policy bundles with base rules
- New: docker-compose addition for OPA (or Cedar) service

## Success Criteria

- `GRANT SELECT (col1, col2) ON schema.table TO role_x` persists in the policy engine
- `SELECT *` from a user without full column access returns only granted columns
- `SELECT *` with row filter returns only matching rows
- Column masks (REDACT, HASH, NULL) apply transparently
- `SHOW GRANTS ON schema.table` shows effective grants for current user
- `SHOW EFFECTIVE POLICY FOR user ON schema.table` shows resolved policy (admin only)
- Policy decisions are cached with configurable TTL (< 5ms overhead on hot path)
- All policy decisions are audit logged

## Impact

- New crate: sqe-policy
- Modified crates: sqe-sql (parser), sqe-coordinator (statement routing, plan rewrite)
- New docker-compose service: OPA or Cedar
- New policy bundle directory in repo

## Trino DCAF Branch Reference

No equivalent in DCAF branch. This is new capability.

## Rollback Strategy

The `PolicyEnforcer` trait already has a `PassthroughEnforcer` (shipped in Phase 1).
Rollback = set config back to passthrough. All policy SQL statements return
"policy engine not configured" error. No data path changes.

## Timeline

4-6 weeks


================================================================================
FILE: openspec/changes/phase-5-policy-sql/design.md
================================================================================

# Design: Phase 5 — Policy SQL Extensions & Enforcement

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                        SQL Input                                 │
│  GRANT SELECT (amount) ON finance.txns TO analyst                │
│  SELECT * FROM finance.txns                                      │
└──────────────┬───────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────────┐
│  sqe-sql: Extended Parser                                        │
│                                                                  │
│  sqlparser-rs AST                                                │
│    ├── Standard SELECT/INSERT/... → DataFusion planner           │
│    └── PolicyStatement variants  → PolicyStatementHandler        │
│         ├── GrantColumnAccess                                    │
│         ├── GrantRowFilter                                       │
│         ├── RevokeAccess                                         │
│         ├── ShowGrants                                           │
│         └── ShowEffectivePolicy                                  │
└──────────────┬───────────────────────────────────────────────────┘
               │
       ┌───────┴────────┐
       │                 │
       ▼                 ▼
  Query path         Policy DDL path
       │                 │
       ▼                 ▼
┌─────────────┐   ┌──────────────────────────────────────────────┐
│ sqe-coord:  │   │  sqe-policy: PolicyManager                   │
│ Plan &      │   │                                              │
│ Optimize    │   │  GRANT → PolicyManager::grant()              │
│             │   │    → serialize to policy engine format        │
│             │   │    → PUT to OPA data API / Cedar policy store │
│             │   │    → confirm + audit log                     │
│             │   │                                              │
│             │   │  SHOW GRANTS → PolicyManager::list_grants()  │
│             │   │    → query policy engine                     │
│             │   │    → return as Arrow RecordBatch              │
│             │   │                                              │
└──────┬──────┘   └──────────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────────────────────┐
│  sqe-policy: PlanRewriter (implements PolicyEnforcer trait)       │
│                                                                  │
│  BEFORE DataFusion optimization:                                 │
│                                                                  │
│  1. Extract user identity + roles from session                   │
│  2. Extract all table references from LogicalPlan                │
│  3. Batch-query policy engine for all tables at once             │
│  4. For each table:                                              │
│     a. Inject row filter as Filter node above TableScan          │
│     b. Replace masked columns with mask expressions              │
│     c. Strip disallowed columns from Projection                  │
│  5. Return rewritten LogicalPlan                                 │
│                                                                  │
│  THEN DataFusion optimizer runs (can push predicates through     │
│  security filters where safe)                                    │
└──────────────────────────────────────────────────────────────────┘
```

## Custom SQL Grammar (sqe-sql)

### GRANT Variants

```sql
-- Column-level access grant
GRANT SELECT (col1, col2, col3) ON [catalog.]schema.table TO role_name;

-- Full table access
GRANT SELECT ON [catalog.]schema.table TO role_name;

-- Row-level filter grant
GRANT ROWS WHERE <predicate> ON [catalog.]schema.table TO role_name;

-- Column mask
GRANT SELECT (ssn MASKED WITH 'REDACT') ON schema.table TO role_name;
GRANT SELECT (email MASKED WITH 'HASH') ON schema.table TO role_name;
GRANT SELECT (salary MASKED WITH 'NULL') ON schema.table TO role_name;
GRANT SELECT (salary MASKED WITH 'RANGE(10000)') ON schema.table TO role_name;

-- Combined: columns + row filter + masks in one statement
GRANT SELECT (id, amount, ssn MASKED WITH 'REDACT')
  ROWS WHERE region = 'EU'
  ON finance.transactions
  TO role_eu_analyst;
```

### REVOKE Variants

```sql
REVOKE SELECT ON schema.table FROM role_name;
REVOKE SELECT (col1) ON schema.table FROM role_name;
REVOKE ROWS ON schema.table FROM role_name;
```

### Inspection

```sql
-- What grants apply to this table?
SHOW GRANTS ON [catalog.]schema.table;

-- What does the current user actually see? (resolved: role expansion, mask
-- application, row filter combination)
SHOW EFFECTIVE POLICY ON [catalog.]schema.table;

-- Admin: what does a specific user see?
SHOW EFFECTIVE POLICY FOR 'username' ON [catalog.]schema.table;
```

### AST Representation (sqe-sql)

```rust
/// Custom policy statements parsed from SQL
pub enum PolicyStatement {
    Grant(GrantStatement),
    Revoke(RevokeStatement),
    ShowGrants(ShowGrantsStatement),
    ShowEffectivePolicy(ShowEffectivePolicyStatement),
}

pub struct GrantStatement {
    pub object: ObjectReference,        // catalog.schema.table
    pub grantee: String,                // role name
    pub columns: Option<Vec<ColumnGrant>>,
    pub row_filter: Option<Expr>,       // WHERE predicate as parsed expression
}

pub struct ColumnGrant {
    pub column: String,
    pub mask: Option<MaskType>,
}

pub enum MaskType {
    Redact,                             // → '***'
    Hash,                               // → SHA256(value)
    Null,                               // → NULL
    Range(i64),                         // → FLOOR(value / range) * range
    Custom(String),                     // → arbitrary SQL expression
}
```

### Parser Extension Strategy

sqlparser-rs already parses `GRANT` and `REVOKE` with a standard AST. Our
extension approach:

1. Attempt standard sqlparser-rs parse
2. If it parses as `Statement::Grant` — inspect for our extensions (ROWS WHERE,
   MASKED WITH) via post-parse transform
3. If MASKED WITH or ROWS WHERE are present, convert to `PolicyStatement::Grant`
4. `SHOW GRANTS` and `SHOW EFFECTIVE POLICY` are fully custom — add to parser
   as custom statement variants

This avoids forking sqlparser-rs. We wrap and extend.

## Policy Engine Abstraction (sqe-policy)

```rust
/// Trait for policy storage and retrieval backends.
/// Implementations exist for OPA, Cedar, and in-memory (testing).
#[async_trait]
pub trait PolicyStore: Send + Sync {
    /// Persist a grant
    async fn put_grant(&self, grant: &PolicyGrant) -> Result<()>;

    /// Remove a grant
    async fn delete_grant(&self, grant_id: &GrantId) -> Result<()>;

    /// List grants for a table (optionally filtered by role)
    async fn list_grants(
        &self,
        table: &ObjectReference,
        role: Option<&str>,
    ) -> Result<Vec<PolicyGrant>>;

    /// Evaluate: given a user+roles and a set of tables, return the
    /// resolved policy (allowed columns, masks, row filters) per table.
    async fn evaluate(
        &self,
        user: &SessionUser,
        tables: &[ObjectReference],
    ) -> Result<HashMap<ObjectReference, ResolvedPolicy>>;
}

/// The resolved policy for a single table after role expansion
pub struct ResolvedPolicy {
    pub allowed_columns: Option<HashSet<String>>,  // None = all allowed
    pub column_masks: HashMap<String, MaskType>,
    pub row_filters: Vec<Expr>,                    // combined with AND
    pub deny: bool,                                // hard deny
}
```

### OPA Implementation

```
PolicyGrant → serialized as JSON → PUT /v1/data/sqe/grants/{table}/{role}

evaluate() → POST /v1/data/sqe/authz
  Input:  { user, roles, tables: [{catalog, schema, table, columns}] }
  Output: { per_table: { allowed_columns, masks, row_filters } }
```

Rego policy structure:
```
sqe/
├── grants/          # Data: persisted grants (written by GRANT SQL)
│   └── {table}/
│       └── {role}.json
├── authz.rego       # Rules: resolve grants → effective policy
└── authz_test.rego  # Rego unit tests
```

### Cedar Implementation (alternative)

```
PolicyGrant → Cedar policy statement → PUT to Cedar policy store

evaluate() → Cedar authorization request
  Principal: User::"jacob"
  Action: Action::"select"
  Resource: Table::"finance.transactions"
  Context: { columns: [...] }
```

### Key Design Decision: Policy Cache

Policy evaluation is on the hot path (every query). To keep overhead < 5ms:

- Cache key: `(user_id, table_reference)` → `ResolvedPolicy`
- TTL: configurable, default 60s
- Invalidation: GRANT/REVOKE statements invalidate affected cache entries
- Cache implementation: `moka` (Rust async cache with TTL eviction)
- Metric: `sqe_policy_cache_hit_ratio`, `sqe_policy_eval_duration_ms`

## Plan Rewriting (sqe-policy → PolicyEnforcer trait)

The `PlanRewriter` implements the `PolicyEnforcer` trait from Phase 1's stub:

```rust
pub struct PolicyPlanRewriter {
    store: Arc<dyn PolicyStore>,
    cache: Arc<PolicyCache>,
}

#[async_trait]
impl PolicyEnforcer for PolicyPlanRewriter {
    async fn evaluate(
        &self,
        user: &SessionUser,
        plan: LogicalPlan,
    ) -> Result<LogicalPlan> {
        // 1. Walk the plan, collect all TableScan references
        let tables = extract_table_refs(&plan);

        // 2. Batch evaluate policy (cache-first)
        let policies = self.resolve_policies(user, &tables).await?;

        // 3. Rewrite plan
        let rewritten = plan.transform_down(|node| {
            match &node {
                LogicalPlan::TableScan(scan) => {
                    let policy = policies.get(&scan.table_name);
                    if let Some(p) = policy {
                        if p.deny {
                            return Err(access_denied_error(scan));
                        }
                        let mut node = node;
                        // a. Inject row filters
                        for filter in &p.row_filters {
                            node = LogicalPlan::Filter(Filter::try_new(
                                filter.clone(), Arc::new(node)
                            )?);
                        }
                        // b. Apply column masks (wrap col refs in expressions)
                        node = apply_column_masks(node, &p.column_masks)?;
                        // c. Strip disallowed columns
                        if let Some(allowed) = &p.allowed_columns {
                            node = restrict_projection(node, allowed)?;
                        }
                        Ok(Transformed::yes(node))
                    } else {
                        Ok(Transformed::no(node))
                    }
                }
                _ => Ok(Transformed::no(node))
            }
        })?;

        Ok(rewritten)
    }
}
```

### Key Design Decision: Rewrite Before Optimization

Plan rewriting inserts Filter and Projection nodes ABOVE the TableScan.
DataFusion's optimizer then runs and may push user predicates below the security
filter IF the predicate doesn't reference masked columns. This is safe because:

- Row filters are conjunctive (AND): user predicates narrow further, never widen
- Masked columns: the optimizer sees the mask expression, not the raw column,
  so it can't push predicates that would operate on unmasked data
- Column restriction: stripped columns aren't in the schema, so the optimizer
  can't reference them

### Key Design Decision: No Information Leakage via Errors

If a user queries a column they can't access:
- If the column is masked → return masked value (not an error)
- If the column is fully denied → the column doesn't appear in the schema at all
  (the user gets "column not found", same as if it didn't exist)
- Row filters are invisible — the user just sees fewer rows

This is the same model as PostgreSQL's Row-Level Security.

## Column Mask Expressions

```rust
fn mask_expression(col: &Column, mask: &MaskType) -> Expr {
    match mask {
        MaskType::Redact => lit("***"),
        MaskType::Hash => {
            // SHA256(CAST(col AS VARCHAR))
            Expr::ScalarFunction(ScalarFunction::new(
                "sha256", vec![cast(col.clone(), DataType::Utf8)]
            ))
        }
        MaskType::Null => lit(ScalarValue::Null),
        MaskType::Range(bucket) => {
            // FLOOR(col / bucket) * bucket
            floor(col.clone() / lit(*bucket)) * lit(*bucket)
        }
        MaskType::Custom(expr_str) => {
            // Parse the custom expression string as SQL
            parse_sql_expr(expr_str)
        }
    }
}
```

## Distributed Execution Interaction

In distributed mode (Phase 3+), the plan is rewritten on the coordinator BEFORE
distribution. Workers receive already-secured plan fragments. Workers never see
raw column references for masked/denied columns.

Token propagation (from Phase 3) is orthogonal — workers still authenticate to
Polaris/S3 as the user, and the plan they execute is already policy-enforced.

## Statement Routing in Coordinator

```rust
// In sqe-coordinator, after parsing:
match parsed {
    ParsedStatement::Query(plan) => {
        // Apply policy enforcement
        let secured_plan = policy_enforcer.evaluate(&session.user, plan).await?;
        // Optimize
        let optimized = optimizer.optimize(secured_plan)?;
        // Execute
        execute(optimized).await
    }
    ParsedStatement::Policy(stmt) => {
        // Route to policy manager
        match stmt {
            PolicyStatement::Grant(g) => {
                // Check: does the user have ADMIN role?
                authorize_admin(&session.user)?;
                policy_manager.grant(g).await?;
                // Invalidate cache
                policy_cache.invalidate(&g.object);
                Ok(result_ok("GRANT applied"))
            }
            PolicyStatement::Revoke(r) => {
                authorize_admin(&session.user)?;
                policy_manager.revoke(r).await?;
                policy_cache.invalidate(&r.object);
                Ok(result_ok("REVOKE applied"))
            }
            PolicyStatement::ShowGrants(sg) => {
                let grants = policy_manager.list_grants(&sg.object, None).await?;
                Ok(grants_to_record_batch(grants))
            }
            PolicyStatement::ShowEffectivePolicy(sep) => {
                let target_user = sep.for_user.unwrap_or(session.user.clone());
                if target_user != session.user {
                    authorize_admin(&session.user)?;
                }
                let policy = policy_manager.resolve(&target_user, &sep.object).await?;
                Ok(policy_to_record_batch(policy))
            }
        }
    }
}
```

## Configuration

```toml
# sqe.toml additions for Phase 5
[policy]
enabled = true
engine = "opa"                          # or "cedar" or "passthrough"
cache_ttl_secs = 60
cache_max_entries = 10000

[policy.opa]
url = "http://opa:8181"
data_prefix = "sqe/grants"
authz_path = "sqe/authz"

# [policy.cedar]
# url = "http://cedar:8080"
# policy_store_id = "sqe"
```


================================================================================
FILE: openspec/changes/phase-5-policy-sql/tasks.md
================================================================================

# Tasks: Phase 5 — Policy SQL Extensions & Enforcement

## Phase 5.1 — SQL Parser Extensions (sqe-sql)

- [ ] 5.1.1 Define PolicyStatement enum and child AST types (GrantStatement, RevokeStatement, ShowGrantsStatement, ShowEffectivePolicyStatement)
- [ ] 5.1.2 Define MaskType enum (Redact, Hash, Null, Range, Custom)
- [ ] 5.1.3 Implement GRANT parser: column-level, MASKED WITH clause, ROWS WHERE clause
- [ ] 5.1.4 Implement REVOKE parser: column-level, row filter, full table
- [ ] 5.1.5 Implement SHOW GRANTS parser
- [ ] 5.1.6 Implement SHOW EFFECTIVE POLICY [FOR user] parser
- [ ] 5.1.7 Unit tests: parse round-trip for all statement variants
- [ ] 5.1.8 Unit tests: error cases (malformed GRANT, missing ON, unknown mask type)

## Phase 5.2 — Policy Store Abstraction (sqe-policy)

- [ ] 5.2.1 Define PolicyStore trait and ResolvedPolicy struct
- [ ] 5.2.2 Implement InMemoryPolicyStore for unit/integration testing
- [ ] 5.2.3 Implement OPA PolicyStore (put_grant → OPA data API, evaluate → OPA query)
- [ ] 5.2.4 Write base Rego rules: role grant resolution, column mask resolution, row filter combination
- [ ] 5.2.5 Rego unit tests (via `opa test`)
- [ ] 5.2.6 Implement PolicyCache using moka with TTL and explicit invalidation
- [ ] 5.2.7 Unit tests: cache hit/miss/invalidation/TTL expiry
- [ ] 5.2.8 Add Prometheus metrics: cache hit ratio, eval duration, engine latency

## Phase 5.3 — Plan Rewriter (sqe-policy)

- [ ] 5.3.1 Implement extract_table_refs: walk LogicalPlan to collect all TableScan references
- [ ] 5.3.2 Implement mask_expression for each MaskType variant
- [ ] 5.3.3 Implement PolicyPlanRewriter (PolicyEnforcer trait): row filter injection
- [ ] 5.3.4 Implement PolicyPlanRewriter: column mask application
- [ ] 5.3.5 Implement PolicyPlanRewriter: column restriction (strip denied columns)
- [ ] 5.3.6 Implement PolicyPlanRewriter: hard deny (access denied error)
- [ ] 5.3.7 Unit tests: plan rewrite with row filter only
- [ ] 5.3.8 Unit tests: plan rewrite with column masks only
- [ ] 5.3.9 Unit tests: plan rewrite with combined row filter + column mask + column restriction
- [ ] 5.3.10 Unit tests: denied table returns access denied error
- [ ] 5.3.11 Unit tests: verify optimizer can still push predicates through row filters
- [ ] 5.3.12 Unit tests: verify masked columns block predicate pushdown on raw values

## Phase 5.4 — Coordinator Integration (sqe-coordinator)

- [ ] 5.4.1 Add statement routing: PolicyStatement → PolicyManager
- [ ] 5.4.2 Implement authorize_admin check (Keycloak role-based)
- [ ] 5.4.3 Replace PassthroughEnforcer with PolicyPlanRewriter (config-driven)
- [ ] 5.4.4 Implement cache invalidation on GRANT/REVOKE
- [ ] 5.4.5 Implement SHOW GRANTS → RecordBatch response
- [ ] 5.4.6 Implement SHOW EFFECTIVE POLICY → RecordBatch response
- [ ] 5.4.7 Add policy decision to audit log entries

## Phase 5.5 — Integration Testing

- [ ] 5.5.1 Add OPA service to docker-compose
- [ ] 5.5.2 Bootstrap: load base Rego policies on startup
- [ ] 5.5.3 Test: GRANT column access → SELECT only returns granted columns
- [ ] 5.5.4 Test: GRANT with MASKED WITH → SELECT returns masked values
- [ ] 5.5.5 Test: GRANT ROWS WHERE → SELECT returns only filtered rows
- [ ] 5.5.6 Test: combined grant (columns + mask + row filter) end-to-end
- [ ] 5.5.7 Test: REVOKE removes access, next SELECT reflects change
- [ ] 5.5.8 Test: SHOW GRANTS returns correct grants via JDBC
- [ ] 5.5.9 Test: SHOW EFFECTIVE POLICY resolves multiple roles correctly
- [ ] 5.5.10 Test: user queries denied column → column not found (no info leak)
- [ ] 5.5.11 Test: user queries denied table → access denied
- [ ] 5.5.12 Test: policy cache invalidation on GRANT (verify < 1s propagation)
- [ ] 5.5.13 Test: distributed mode — worker receives pre-secured plan fragments
- [ ] 5.5.14 Benchmark: policy overhead < 5ms on cached path (TPC-H Q1 with policy)


================================================================================
FILE: openspec/changes/phase-5-policy-sql/specs/sql-extensions/spec.md
================================================================================

# Delta for sql-extensions

## ADDED Requirements

### Requirement: GRANT column access

The system SHALL support `GRANT SELECT (columns) ON table TO role` to persist
column-level access grants in the policy engine.

#### Scenario: Grant specific columns

- **GIVEN** a policy engine is configured
- **AND** user has ADMIN role
- **WHEN** user submits `GRANT SELECT (id, amount) ON finance.txns TO analyst`
- **THEN** the grant is persisted in the policy engine
- **AND** subsequent queries by `analyst` role return only `id` and `amount`

### Requirement: GRANT with column masks

The system SHALL support `MASKED WITH` clause on column grants to apply
data masking (REDACT, HASH, NULL, RANGE, Custom).

#### Scenario: Grant with REDACT mask

- **GIVEN** a policy engine is configured
- **WHEN** admin submits `GRANT SELECT (ssn MASKED WITH 'REDACT') ON hr.employees TO viewer`
- **THEN** users with `viewer` role see `***` for the `ssn` column

#### Scenario: Grant with HASH mask

- **GIVEN** a policy engine is configured
- **WHEN** admin submits `GRANT SELECT (email MASKED WITH 'HASH') ON hr.employees TO analyst`
- **THEN** users with `analyst` role see SHA256 hash of `email` values

#### Scenario: Grant with RANGE mask

- **GIVEN** a policy engine is configured
- **WHEN** admin submits `GRANT SELECT (salary MASKED WITH 'RANGE(10000)') ON hr.employees TO viewer`
- **THEN** users with `viewer` role see salary bucketed to nearest 10000

### Requirement: GRANT row-level filter

The system SHALL support `GRANT ROWS WHERE <predicate>` to enforce row-level
filtering for a role.

#### Scenario: Row filter by region

- **GIVEN** a policy engine is configured
- **WHEN** admin submits `GRANT ROWS WHERE region = 'EU' ON finance.txns TO eu_analyst`
- **THEN** users with `eu_analyst` role only see rows where `region = 'EU'`

#### Scenario: Multiple row filters combine with AND

- **GIVEN** a user has roles `eu_analyst` (filter: region='EU') and `recent_only` (filter: year>=2025)
- **WHEN** the user queries `finance.txns`
- **THEN** only rows where `region = 'EU' AND year >= 2025` are returned

### Requirement: Combined GRANT statement

The system SHALL support combining column access, column masks, and row filters
in a single GRANT statement.

#### Scenario: Combined grant

- **GIVEN** a policy engine is configured
- **WHEN** admin submits:
  ```
  GRANT SELECT (id, amount, ssn MASKED WITH 'REDACT')
    ROWS WHERE region = 'EU'
    ON finance.transactions
    TO role_eu_analyst
  ```
- **THEN** `role_eu_analyst` sees only `id`, `amount` (clear), `ssn` (redacted)
- **AND** only rows where `region = 'EU'`

### Requirement: REVOKE access

The system SHALL support `REVOKE` to remove previously granted access.

#### Scenario: Revoke full table access

- **GIVEN** `analyst` has a grant on `finance.txns`
- **WHEN** admin submits `REVOKE SELECT ON finance.txns FROM analyst`
- **THEN** the grant is removed
- **AND** subsequent queries by `analyst` return access denied

#### Scenario: Revoke column access

- **GIVEN** `analyst` has a column grant on `(id, amount, ssn)` on `finance.txns`
- **WHEN** admin submits `REVOKE SELECT (ssn) ON finance.txns FROM analyst`
- **THEN** `analyst` can still access `id` and `amount` but not `ssn`

### Requirement: SHOW GRANTS

The system SHALL support `SHOW GRANTS ON table` to display all grants
applicable to a table.

#### Scenario: Show grants

- **GIVEN** multiple grants exist on `finance.txns`
- **WHEN** user submits `SHOW GRANTS ON finance.txns`
- **THEN** a result set is returned with columns: role, columns, masks, row_filter

### Requirement: SHOW EFFECTIVE POLICY

The system SHALL support `SHOW EFFECTIVE POLICY ON table` to display the
resolved policy for the current user (after role expansion and grant merging).

#### Scenario: Effective policy for current user

- **GIVEN** current user has roles `analyst` and `eu_team`
- **AND** grants exist for both roles on `finance.txns`
- **WHEN** user submits `SHOW EFFECTIVE POLICY ON finance.txns`
- **THEN** the resolved policy is shown: visible columns, active masks, combined row filter

#### Scenario: Effective policy for another user (admin only)

- **GIVEN** current user has ADMIN role
- **WHEN** admin submits `SHOW EFFECTIVE POLICY FOR 'jacob' ON finance.txns`
- **THEN** the resolved policy for `jacob` is shown
- **AND** non-admin users receive an authorization error for this form


================================================================================
FILE: openspec/changes/phase-5-policy-sql/specs/security-policy/spec.md
================================================================================

# Delta for security-policy

## MODIFIED Requirements

### Requirement: PolicyEnforcer trait

The system SHALL define a `PolicyEnforcer` trait that rewrites a LogicalPlan
based on the authenticated user's identity and roles.

#### Scenario: No-op passthrough (config: engine = "passthrough")

- **GIVEN** the `PassthroughEnforcer` is configured
- **WHEN** any query is planned
- **THEN** the LogicalPlan is returned unmodified

#### Scenario: Policy-enforced query (config: engine = "opa" or "cedar")

- **GIVEN** a `PolicyPlanRewriter` is configured with an active policy engine
- **WHEN** a query accesses table `finance.transactions`
- **THEN** the policy engine is consulted (cache-first) for column access, masks, row filters
- **AND** the LogicalPlan is rewritten with injected Filter and Projection nodes
- **AND** rewriting happens BEFORE DataFusion optimization

## ADDED Requirements

### Requirement: Column masking via plan rewrite

The system SHALL replace column references with mask expressions in the
LogicalPlan for columns with active masks.

#### Scenario: REDACT mask applied

- **GIVEN** `ssn` has a REDACT mask for role `viewer`
- **WHEN** `viewer` queries `SELECT ssn FROM hr.employees`
- **THEN** the result contains `***` for every row

#### Scenario: Masked column blocks predicate pushdown

- **GIVEN** `ssn` has a REDACT mask for role `viewer`
- **WHEN** `viewer` queries `SELECT * FROM hr.employees WHERE ssn = '123-45-6789'`
- **THEN** the predicate operates on the masked value, NOT the raw value
- **AND** no rows match (correct behavior — prevents unmasking via filter)

### Requirement: Row filtering via plan rewrite

The system SHALL inject Filter nodes into the LogicalPlan for tables with
active row-level policies.

#### Scenario: Row filter injection

- **GIVEN** role `eu_analyst` has row filter `region = 'EU'` on `finance.txns`
- **WHEN** `eu_analyst` queries `SELECT * FROM finance.txns`
- **THEN** only rows where `region = 'EU'` are returned

#### Scenario: User predicate combined with row filter

- **GIVEN** role `eu_analyst` has row filter `region = 'EU'`
- **WHEN** `eu_analyst` queries `SELECT * FROM finance.txns WHERE amount > 1000`
- **THEN** results satisfy BOTH `region = 'EU'` AND `amount > 1000`

### Requirement: Column restriction via plan rewrite

The system SHALL remove columns from the schema that the user has no grant for,
making them invisible (not an error, just absent).

#### Scenario: Ungrantable column is invisible

- **GIVEN** `viewer` has access to `(id, amount)` but not `ssn`
- **WHEN** `viewer` queries `SELECT * FROM finance.txns`
- **THEN** results contain only `id` and `amount`
- **AND** `ssn` does not appear in the schema

#### Scenario: Querying invisible column returns column not found

- **GIVEN** `viewer` has no grant for `ssn`
- **WHEN** `viewer` queries `SELECT ssn FROM finance.txns`
- **THEN** an error is returned: column `ssn` not found
- **AND** no indication that the column exists but is denied

### Requirement: Policy caching

The system SHALL cache policy evaluation results to keep query-path overhead
below 5ms for cached decisions.

#### Scenario: Cache hit on repeated query

- **GIVEN** user `jacob` queries `finance.txns` twice within the cache TTL
- **WHEN** the second query is planned
- **THEN** the policy engine is NOT called (cache hit)

#### Scenario: Cache invalidation on GRANT/REVOKE

- **GIVEN** a cached policy for `finance.txns`
- **WHEN** an admin issues a GRANT or REVOKE on `finance.txns`
- **THEN** the cache entry is invalidated
- **AND** the next query triggers a fresh policy evaluation

### Requirement: Policy audit logging

The system SHALL include policy evaluation results in the structured query
audit log.

#### Scenario: Audit log includes policy

- **GIVEN** a query is executed with policy enforcement
- **WHEN** the query completes
- **THEN** the audit log entry includes: applied_row_filters, masked_columns,
  restricted_columns, policy_cache_hit (boolean), policy_eval_duration_ms


================================================================================
FILE: openspec/changes/phase-5-policy-sql/specs/query-engine/spec.md
================================================================================

# Delta for query-engine

## MODIFIED Requirements

### Requirement: Custom SQL statement extensibility

The system SHALL provide a `CustomStatementHandler` trait that intercepts
non-query statements and routes them to appropriate backends.

#### Scenario: Unrecognized statement routing

- **GIVEN** a custom statement handler is registered for `GRANT` statements
- **WHEN** the user submits `GRANT SELECT ON table TO role_x`
- **THEN** the statement is routed to the registered handler
- **AND** it is NOT passed to the DataFusion query planner

#### Scenario: Policy DDL requires admin role

- **GIVEN** a non-admin user
- **WHEN** the user submits `GRANT SELECT ON table TO role_x`
- **THEN** an authorization error is returned
- **AND** the grant is NOT persisted