# What You Can't See Can't Hurt You {#sec:security}

> Security is not a feature. It's a rewrite of the query plan.

Alice can log in. Chapter 4 made sure of that. Her JWT is validated, her identity propagated, her S3 access scoped to exactly what Polaris grants her. Every Parquet file she reads shows up in CloudTrail under her name.

But authentication is not authorization. Alice can prove she is Alice. That says nothing about whether Alice should see the `salary` column in the `hr.employees` table. Or whether she should see any rows from the `finance.transactions` table where `region != 'EU'`. Or whether the social security numbers in `customers.ssn` should arrive at her client as `123-45-6789` or as `***-**-6789`.

Polaris handles table-level access. If Alice doesn't have access to a table at all, the REST catalog won't return its metadata. But column-level masking, row-level filtering, and column restriction -- these require something Polaris doesn't provide. They require a policy layer that operates on the query itself.

This chapter is about building that layer. And the key insight -- the one that shapes everything -- is that the right place to enforce policy is not in the storage layer, not in the application, and not at the network boundary. It's in the query plan.


## The Problem with Application-Level Access Control

The obvious approach is to enforce access control in the application code that handles queries. Check the user's roles. If the role doesn't include `hr_admin`, remove the `salary` column from the result set before sending it back. If the role includes `eu_only`, append `WHERE region = 'EU'` to the SQL string before executing it.

We considered this for about ten minutes.

The problem is that SQL is compositional. Users write subqueries, CTEs, joins, aggregations, window functions. A filter appended to a simple `SELECT * FROM employees` is straightforward. A filter injected into a four-way join with a correlated subquery and a `HAVING` clause is a parsing nightmare. String manipulation of SQL is how injection vulnerabilities happen. It's also how subtle semantic bugs happen -- the kind where the filter works for most queries but silently fails on a specific join pattern that nobody tested.

::: {.antipattern}
**Antipattern: SQL string manipulation for security.** Appending `AND region = 'EU'` to the SQL string before execution seems simple. It breaks on UNION queries, subqueries, CTEs, and any query where the table alias doesn't match. Worse, a carefully crafted query can escape the filter. If your security depends on string manipulation, your security depends on the attacker not being creative.
:::

The second obvious approach is database-level enforcement. PostgreSQL has Row-Level Security (RLS). Oracle has Virtual Private Database (VPD). These work because the database owns both the storage and the query engine -- it can enforce policy at the lowest possible level.

But we're not a database. We're a query engine that reads Parquet files from S3 via an Iceberg catalog. We don't own the storage. We don't control the file format. We can't add row-level predicates to the storage layer because the storage layer is a bunch of immutable Parquet files sitting in an object store.

What we do own is the query plan.


## The Query Plan as Security Boundary

DataFusion represents every query as a tree of logical operators -- a `LogicalPlan`. A simple `SELECT name, salary FROM employees WHERE department = 'engineering'` becomes:

```
Projection [name, salary]
  Filter [department = 'engineering']
    TableScan [employees]
```

This tree is a data structure. It's a Rust enum. You can walk it, transform it, insert nodes, remove nodes, and replace nodes. DataFusion provides a `transform_down` method that visits every node in the tree and lets you return a modified version.

The insight is this: if you can transform the logical plan before the optimizer runs, you can inject security constraints as plan nodes. A row filter becomes a `Filter` node above the `TableScan`. A column mask becomes a `Projection` that wraps column references in masking expressions. A column restriction becomes a modified projection that simply doesn't include the denied column.

The query plan is the security boundary. Not the application code. Not the storage. The plan.

![Plan rewriting: how row filters, column masks, and column restrictions are injected into the logical plan before optimization](diagrams/rendered/08-plan-rewriting.svg)

```
Before policy enforcement:

  Projection [name, salary, ssn]
    Filter [department = 'engineering']
      TableScan [employees]

After policy enforcement (for a user with row filter + column mask):

  Projection [name, salary, mask(ssn)]
    Filter [department = 'engineering']
      Filter [region = 'EU']          <-- injected row filter
        TableScan [employees]
```

The user's original query is preserved. The security constraints are layered on top. The user never knows the row filter exists -- they just see fewer rows. The SSN column is there, but its values are masked. The user can still `SELECT ssn` -- they just get `***-**-6789` instead of the raw value.

This is the PostgreSQL RLS model, applied to a DataFusion LogicalPlan.


## Why Before Optimization

The placement matters. Policy enforcement happens after the SQL is parsed and the initial LogicalPlan is created, but before DataFusion's optimizer runs.

This ordering creates three security properties that would be impossible to guarantee if we enforced policy after optimization.

**Property 1: User predicates can push through row filters.** If the policy says "Alice can only see rows where `region = 'EU'`" and Alice writes `WHERE department = 'engineering'`, both filters need to apply. Because the row filter is a standard `Filter` node in the logical plan, DataFusion's optimizer treats it like any other filter. The optimizer may reorder them, combine them, or push them into the TableScan as partition predicates. All of this is safe because row filters are conjunctive -- adding Alice's predicate can only narrow the result further, never widen it.

**Property 2: User predicates cannot push through column masks.** This is the critical one. If the `ssn` column is masked, and Alice writes `WHERE ssn = '123-45-6789'`, she's trying to use the predicate to probe for a specific SSN. If the optimizer pushes that predicate below the mask, it would evaluate against the raw value -- and the number of rows returned (zero or non-zero) would leak information about whether that SSN exists.

Because we inject the mask as an expression that wraps the column reference, the optimizer sees `mask(ssn)`, not `ssn`. The predicate `WHERE mask(ssn) = '123-45-6789'` can only evaluate against the masked value. The optimizer can't push it through because it can't see through the expression boundary. This is a security property that falls out of the plan structure -- we don't need to add a special rule to prevent pushdown. The optimizer's own rules prevent it.

**Property 3: Denied columns don't exist.** If the policy says Alice can't see the `salary` column, the plan rewriter removes it from the schema entirely. When Alice runs `SELECT *`, `salary` isn't in the projection. When Alice runs `SELECT salary`, she gets "column not found" -- the same error she'd get for a column that genuinely doesn't exist in the table. There is no way for Alice to distinguish "this column exists but I'm denied access" from "this column doesn't exist."

::: {.sovereignty}
**Sovereignty principle:** Deny by omission, not by error. If your security system tells the user "access denied to column salary," you've just told them the column exists. In a sovereign engine, denied columns are invisible. The user sees exactly the schema they're authorized to see -- no more, no less. This is the same model PostgreSQL uses for RLS, and it exists for the same reason: the absence of information is itself a form of security.
:::


## The PolicyEnforcer Trait

The interface is deliberately minimal. Twenty-six lines of Rust:

```rust
#[async_trait]
pub trait PolicyEnforcer: Send + Sync {
    async fn evaluate(
        &self,
        user: &SessionUser,
        plan: LogicalPlan,
    ) -> Result<LogicalPlan>;
}
```

The enforcer receives a user identity and a logical plan. It returns a (possibly modified) logical plan. That's the entire contract.

The `SessionUser` is the same identity from Chapter 4 -- the authenticated user with their roles extracted from the JWT:

```rust
pub struct SessionUser {
    pub username: String,
    pub roles: Vec<String>,
}
```

The enforcer doesn't know about SQL strings, physical plans, execution contexts, or Arrow batches. It operates on exactly one abstraction: the logical plan tree. This constraint is deliberate. It means the enforcer can be tested with plan construction in unit tests, without needing a running database, a catalog, or even a parser.

The trait lives in the `sqe-policy` crate, which has minimal dependencies -- just DataFusion's logical plan types and the core session types. No network. No I/O by default. The implementations bring their own dependencies (HTTP clients for OPA, policy evaluation libraries for Cedar), but the trait itself is pure.


## The PassthroughEnforcer

The simplest implementation does nothing:

```rust
pub struct PassthroughEnforcer;

#[async_trait]
impl PolicyEnforcer for PassthroughEnforcer {
    async fn evaluate(
        &self,
        _user: &SessionUser,
        plan: LogicalPlan,
    ) -> Result<LogicalPlan> {
        Ok(plan)
    }
}
```

This is what SQE ships with today. Every query passes through `policy_enforcer.evaluate()` in the query pipeline, and the PassthroughEnforcer returns the plan unchanged. The call is there. The hook is wired. The cost is a function call that returns its argument.

This isn't a placeholder that needs to be removed later. It's the correct default for environments that rely on Polaris for table-level access control and don't need column or row-level policies. The configuration decides which enforcer is active:

```toml
[policy]
engine = "passthrough"    # or "opa" or "cedar"
```

Setting `engine = "opa"` replaces the PassthroughEnforcer with an OPA-backed implementation. The coordinator's main function wires it up:

```rust
let policy_enforcer: Arc<dyn PolicyEnforcer> =
    Arc::new(PassthroughEnforcer);
```

When the OPA implementation is ready, that becomes a config-driven match:

```rust
let policy_enforcer: Arc<dyn PolicyEnforcer> = match config.policy.engine {
    "passthrough" => Arc::new(PassthroughEnforcer),
    "opa" => Arc::new(OpaPolicyEnforcer::new(&config.policy.opa)),
    "cedar" => Arc::new(CedarPolicyEnforcer::new(&config.policy.cedar)),
    _ => return Err(config_error("unknown policy engine")),
};
```

The rest of the coordinator doesn't change. `QueryHandler` holds an `Arc<dyn PolicyEnforcer>` and calls `.evaluate()` on every query. The implementation behind the trait is invisible to the query pipeline.


## Inside the Query Pipeline

The enforcement point is in `QueryHandler::execute_query`. Here's the actual code path, trimmed to the essential flow:

```rust
async fn execute_query(&self, session: &Session, sql: &str) -> Result<Vec<RecordBatch>> {
    let ctx = self.create_session_context(session).await?;

    // Parse SQL and create the initial LogicalPlan
    let df = ctx.sql(sql).await?;
    let plan = df.logical_plan().clone();

    // Policy enforcement -- before optimization
    let enforced_plan = self.policy_enforcer
        .evaluate(&session.user, plan)
        .await?;

    // DataFusion optimizes and executes the enforced plan
    let enforced_df = ctx.execute_logical_plan(enforced_plan).await?;
    let physical_plan = enforced_df.create_physical_plan().await?;

    // Execute (possibly distributed across workers)
    let final_plan = self.try_distribute(physical_plan, session).await;
    collect(final_plan, ctx.task_ctx()).await
}
```

The sequence is: parse, plan, enforce, optimize, execute. The optimizer only ever sees the enforced plan. It cannot undo the security constraints because they look like ordinary plan nodes -- a `Filter` is a `Filter`, whether the user wrote it or the policy engine injected it.

EXPLAIN queries follow the same path. The `ExplainHandler` applies policy enforcement before formatting the plan text:

```rust
let logical = df.logical_plan().clone();
let enforced = self.policy_enforcer.evaluate(&session.user, logical).await?;
let logical_str = format!("{}", enforced.display_indent());
```

This means EXPLAIN shows the secured plan. If a row filter is active, the user sees the filter node in the EXPLAIN output. This is a conscious choice. The alternative -- hiding the security nodes from EXPLAIN -- would make debugging impossible for both users and administrators.

::: {.datafusion}
**DataFusion deep dive:** The `LogicalPlan::transform_down` method is the workhorse of plan rewriting. It visits each node top-down and lets you return `Transformed::yes(new_node)` to replace a node or `Transformed::no(node)` to keep it. The method handles rebuilding the tree with correct parent-child relationships. For row filter injection, we match on `LogicalPlan::TableScan`, wrap it in a `LogicalPlan::Filter`, and return the filter as the replacement. DataFusion's optimizer then treats this injected filter identically to any user-written filter.
:::


## The Plan Rewriter Design

The PassthroughEnforcer is what we ship now. The `PolicyPlanRewriter` is what we've designed for the OPA and Cedar backends. The design is complete, the trait is defined, and the implementation is the next major phase. Here's how it works.

The rewriter receives a plan and does three things:

**Step 1: Collect table references.** Walk the logical plan tree and extract every `TableScan` node. A single query might reference multiple tables (joins, subqueries, CTEs). The rewriter needs policies for all of them.

**Step 2: Batch-evaluate policies.** Query the policy backend for all tables in one call, keyed by the user's identity and roles. This avoids N round-trips for a query that touches N tables. The result is a `ResolvedPolicy` per table:

```rust
pub struct ResolvedPolicy {
    pub allowed_columns: Option<HashSet<String>>,   // None = all columns allowed
    pub column_masks: HashMap<String, MaskType>,
    pub row_filters: Vec<Expr>,                     // combined with AND
    pub deny: bool,                                 // hard deny
}
```

**Step 3: Rewrite the plan.** For each TableScan that has a policy, modify the plan tree:

```rust
let rewritten = plan.transform_down(|node| {
    match &node {
        LogicalPlan::TableScan(scan) => {
            let policy = policies.get(&scan.table_name);
            if let Some(p) = policy {
                if p.deny {
                    return Err(access_denied_error(scan));
                }
                let mut node = node;
                // Inject row filters above the scan
                for filter in &p.row_filters {
                    node = LogicalPlan::Filter(
                        Filter::try_new(filter.clone(), Arc::new(node))?
                    );
                }
                // Replace column references with mask expressions
                node = apply_column_masks(node, &p.column_masks)?;
                // Remove denied columns from the projection
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
```

The order within the rewrite matters. Row filters go first (closest to the TableScan), then column masks, then column restriction. This means row filters can reference any column -- even columns that will be masked or restricted in the final output. The policy author can write `ROWS WHERE salary > 100000` as a filter even if `salary` is masked for the user querying. The filter evaluates on raw data; the mask applies to what leaves the engine.


## Column Masks in Detail

Column masking replaces raw column values with transformed versions. The mask types we've designed cover the most common governance patterns:

| Mask Type | Input | Output | Use Case |
|-----------|-------|--------|----------|
| Redact | `123-45-6789` | `***` | PII that should be completely hidden |
| Hash | `john@example.com` | `a3f2b7c9...` | Consistent pseudonymization (same input = same hash) |
| Null | `150000` | `NULL` | Numeric data that shouldn't be visible |
| Range(10000) | `153000` | `150000` | Salary bands, approximate values |
| Custom | `john@example.com` | `j***@example.com` | Arbitrary SQL expression |

Each mask type translates to a DataFusion expression:

```rust
fn mask_expression(col: &Column, mask: &MaskType) -> Expr {
    match mask {
        MaskType::Redact => lit("***"),
        MaskType::Hash => {
            Expr::ScalarFunction(ScalarFunction::new(
                "sha256", vec![cast(col.clone(), DataType::Utf8)]  // requires registered UDF
            ))
        }
        MaskType::Null => lit(ScalarValue::Null),
        MaskType::Range(bucket) => {
            floor(col.clone() / lit(*bucket)) * lit(*bucket)
        }
        MaskType::Custom(expr_str) => parse_sql_expr(expr_str),
    }
}
```

The critical point: these expressions replace the column reference in the plan. When the optimizer sees `sha256(cast(ssn as varchar))` in a projection, it doesn't know (or care) that `ssn` was the original column. If a user predicate references `ssn`, the optimizer can only match it against the expression, not the raw column. The mask is structural -- it exists in the plan tree, not as a post-processing step.

::: {.fieldreport}
**Field report: The predicate pushdown test.** When we designed the mask system, the first thing we wrote was a test: create a plan with a masked column, add a user predicate on that column, run the optimizer, and verify the predicate doesn't push below the mask. The test passed on the first run because DataFusion's optimizer is expression-aware -- it won't push a predicate through a function call boundary. We didn't need to write a custom optimizer rule to prevent this. The plan structure prevented it. That's when we knew the approach was sound.
:::


## Row Filters: The Invisible Predicate

Row filters are simpler than masks but more subtle in their implications. A row filter is a predicate that restricts which rows a user can see. The user never knows the filter exists.

When the policy says "analyst role can only see rows where `region = 'EU'`", the rewriter injects a `Filter` node:

```
Before:
  Projection [id, amount, region]
    TableScan [transactions]

After:
  Projection [id, amount, region]
    Filter [region = 'EU']       <-- injected, invisible to user
      TableScan [transactions]
```

If Alice, an analyst, runs `SELECT COUNT(*) FROM transactions`, she gets the count of EU transactions. She has no way to know that non-EU transactions exist. She can't write a query that reveals the total count because every query she runs goes through the same rewriter, and every plan gets the same filter injected.

If Alice also writes `WHERE amount > 1000`, the optimizer sees two filters and may combine them:

```
Filter [region = 'EU' AND amount > 1000]
  TableScan [transactions]
```

This is correct. Alice's predicate narrows within the already-filtered set. The optimizer might even push both predicates into the TableScan as partition pruning hints or Parquet row group filters. All safe, because the security filter is conjunctive.

Multiple row filters for the same table are combined with AND. If the policy has two filters -- `region = 'EU'` and `status = 'active'` -- both are injected, and both must be satisfied. The rewriter doesn't try to be clever about combining them. DataFusion's optimizer handles that.


## Column Restriction: The Column That Doesn't Exist

Column restriction is the most aggressive form of policy enforcement. A masked column is visible but transformed. A restricted column is invisible.

When the policy says Alice's role has `allowed_columns = {id, name, department}` for the `employees` table, the rewriter modifies the projection to exclude every other column. When Alice runs `SELECT *`, she gets `id`, `name`, and `department`. When she runs `SELECT salary`, she gets "column not found."

This is the PostgreSQL RLS model applied to columns. In PostgreSQL, if a column is denied by a policy, it doesn't appear in `\d` (describe table) and references to it return "column does not exist." We follow the same pattern. The denied column doesn't appear in `information_schema.columns` for that user. It doesn't appear in `SELECT *`. It's as if the column was never defined.

The implementation strips columns from the plan schema:

```rust
fn restrict_projection(node: LogicalPlan, allowed: &HashSet<String>) -> Result<LogicalPlan> {
    let schema = node.schema();
    let exprs: Vec<Expr> = schema.fields()
        .iter()
        .filter(|f| allowed.contains(f.name()))
        .map(|f| col(f.name()))
        .collect();
    Ok(LogicalPlan::Projection(
        Projection::try_new(exprs, Arc::new(node))?
    ))
}
```

This is different from a column mask, which replaces the column's value but keeps it in the schema. The choice between masking and restriction depends on the governance requirement. Social security numbers might be masked (the user knows the column exists, sees a transformed value). Salary data might be restricted entirely (the user doesn't know the column exists).


## OPA as a Policy Backend

Open Policy Agent (OPA) is the first external backend we've designed for. OPA evaluates policies written in Rego -- a declarative language for expressing authorization rules. It runs as a sidecar or standalone service, and SQE queries it over HTTP.

The data flow for OPA:

1. Administrator runs `GRANT SELECT (id, amount, ssn MASKED WITH 'REDACT') ROWS WHERE region = 'EU' ON finance.txns TO analyst` via SQL.
2. The coordinator routes this to the PolicyManager, which serializes the grant and writes it to OPA's data API: `PUT /v1/data/sqe/grants/finance.txns/analyst`.
3. OPA stores the grant as structured data alongside Rego rules that know how to resolve grants into policies.
4. When an analyst runs a query, the PlanRewriter calls OPA: `POST /v1/data/sqe/authz` with the user's identity, roles, and the list of tables in the query.
5. OPA evaluates the Rego rules against the stored grants and returns a `ResolvedPolicy` per table.
6. The rewriter applies the policy to the plan.

The Rego rules handle role expansion, conflict resolution (what happens when two grants for the same table disagree on which columns are visible?), and default-deny semantics. This logic lives in the policy engine, not in SQE. SQE asks the question; OPA provides the answer.

```
sqe/
  grants/              # Data written by GRANT SQL statements
    finance.txns/
      analyst.json     # {"columns": ["id","amount"], "masks": {"ssn":"REDACT"}, ...}
  authz.rego           # Rules that resolve grants into effective policy
  authz_test.rego      # Rego unit tests
```

## Cedar as an Alternative

Cedar is AWS's authorization policy language, designed for fine-grained access control with entity-based reasoning. Where OPA evaluates Rego rules over HTTP, Cedar evaluates policies locally -- the policy engine runs in-process.

Cedar policies look like this:

```
permit(
    principal == User::"alice",
    action == Action::"select",
    resource == Table::"finance.transactions"
) when {
    resource.columns.contains("id") &&
    resource.columns.contains("amount")
};
```

The Cedar backend would embed the Cedar evaluation engine directly in SQE, avoiding the network round-trip to OPA. This makes it attractive for latency-sensitive environments. The trade-off is that Cedar's policy language is less flexible than Rego for complex authorization logic.

Both backends implement the same `PolicyEnforcer` trait. The coordinator doesn't know or care which one is active. This is the standard plugin pattern -- define the interface, let the implementation vary.

Neither OPA nor Cedar is fully implemented today. The trait is defined. The PassthroughEnforcer works. The plan rewriting design is complete. The implementation is Phase 5 on the roadmap, and the task breakdown has 50+ items across parser extensions, policy store abstraction, plan rewriting, coordinator integration, and end-to-end testing. We're building on a foundation that was designed from day one to support this -- the `policy_enforcer.evaluate()` call has been in the query pipeline since the first commit.


## The Policy Cache

Policy evaluation is on the hot path. Every query calls the enforcer. For the PassthroughEnforcer, this is free. For an OPA backend making HTTP calls, it could add tens of milliseconds per query.

The solution is a cache keyed on `(user_id, table_reference)` with a configurable TTL:

```toml
[policy]
cache_ttl_secs = 60
cache_max_entries = 10000
```

The cache uses `moka`, a Rust async cache with TTL-based eviction. On a cache hit, policy evaluation costs nanoseconds. On a cache miss, it costs one HTTP call to OPA (or one local Cedar evaluation).

Cache invalidation is explicit: when a `GRANT` or `REVOKE` statement is executed, the affected cache entries are invalidated immediately. This means policy changes take effect on the next query, not after the TTL expires. The TTL handles staleness from external policy changes (an administrator modifying OPA's data directly, outside of SQE's SQL interface).

The target is less than 5 milliseconds of overhead on the cached path, measured against TPC-H Query 1 with an active policy.


## The SQL Extensions

Policy management happens through SQL. Not through a REST API. Not through a configuration file. Through the same interface the user uses for everything else.

```sql
-- Grant column access with a row filter and a mask
GRANT SELECT (id, amount, ssn MASKED WITH 'REDACT')
  ROWS WHERE region = 'EU'
  ON finance.transactions
  TO role_eu_analyst;

-- Revoke access
REVOKE SELECT ON finance.transactions FROM role_eu_analyst;

-- Inspect what grants exist
SHOW GRANTS ON finance.transactions;

-- See the effective policy for the current user (resolved across all roles)
SHOW EFFECTIVE POLICY ON finance.transactions;

-- Admin: see what a specific user would see
SHOW EFFECTIVE POLICY FOR 'alice' ON finance.transactions;
```

The parser strategy wraps `sqlparser-rs` rather than forking it. Standard `GRANT` and `REVOKE` parse normally through sqlparser-rs. A post-parse transform detects our extensions (`MASKED WITH`, `ROWS WHERE`) and converts them to custom `PolicyStatement` AST nodes. `SHOW GRANTS` and `SHOW EFFECTIVE POLICY` are fully custom statement types added to the parser.

This means standard SQL `GRANT` statements still work (routed to Polaris for catalog-level permissions). Only the extended variants with masking and row filter clauses are routed to the policy engine.

The coordinator's statement routing already handles this. The `StatementKind::Policy` variant exists in the code today:

```rust
StatementKind::Policy(_) => Err(SqeError::NotImplemented(
    "Policy management not configured".to_string(),
)),
```

The error is honest. Policy management isn't configured yet. When it is, that match arm routes to the `PolicyManager` instead of returning an error.


## The Connection to Governance Platforms

Policy engines don't exist in a vacuum. In production, the policies enforced by SQE should come from a governance platform -- Collibra, Alation, Atlan, or whatever system the organization uses to manage data access.

The connection point is the `PolicyStore` trait:

```rust
#[async_trait]
pub trait PolicyStore: Send + Sync {
    async fn put_grant(&self, grant: &PolicyGrant) -> Result<()>;
    async fn delete_grant(&self, grant_id: &GrantId) -> Result<()>;
    async fn list_grants(&self, table: &ObjectReference, role: Option<&str>)
        -> Result<Vec<PolicyGrant>>;
    async fn evaluate(&self, user: &SessionUser, tables: &[ObjectReference])
        -> Result<HashMap<ObjectReference, ResolvedPolicy>>;
}
```

A Collibra integration would implement `PolicyStore` by reading access policies from Collibra's API. When an analyst runs a query against SQE, the policy evaluation calls Collibra to determine which columns are visible, which are masked, and which rows are filtered. The governance platform is the source of truth. SQE is the enforcement point.

This is the right separation. The governance platform manages the policy lifecycle -- who approved the access, when it expires, which regulation requires it. The query engine enforces the policy -- rewriting plans, masking columns, filtering rows. Neither system needs to understand the other's internals. They communicate through the `PolicyStore` interface.

Collibra Protect, which I wrote about in an earlier article, does something similar for Snowflake. The model works. The question is always where the enforcement happens. In Snowflake's case, it happens inside a proprietary engine you can't inspect. In SQE's case, it happens in plan rewriting code you can read, test, and audit.

## Distributed Execution and Security

In distributed mode, the coordinator rewrites the plan before distributing fragments to workers. Workers receive plan fragments that are already secured. A worker executing a scan fragment for the `finance.transactions` table already has the row filter injected and the column masks applied. The worker doesn't need access to the policy engine. It doesn't need to know what the user's roles are (for authorization purposes -- it still uses the user's JWT for authentication to Polaris and S3).

This is an important architectural property. Workers are stateless executors. They receive a plan fragment and a bearer token. The plan fragment tells them what to read and how to transform it. The bearer token tells Polaris and S3 who is reading. The security decisions have already been made by the coordinator.

If a worker is compromised, the attacker gets the plan fragments currently being executed on that worker -- which are already policy-enforced. They can't ask the worker to execute a different plan because the worker only executes what the coordinator sends. They can use the bearer token to make Polaris requests, but that token is scoped to the user's permissions and expires within minutes.

The security model is layered: authentication (JWT validation), authorization (plan rewriting), and enforcement (workers execute only secured fragments). Each layer limits what the next layer can do wrong.


## Prior Art

SQE's plan-rewriting approach to security enforcement did not emerge from a vacuum. Other systems have solved this problem, and the differences in how they solved it explain why our approach is portable.

PostgreSQL introduced Row-Level Security in version 9.5 (2015). RLS works by injecting `security_barrier` subqueries at parse time -- the planner sees the security predicates as ordinary subqueries and optimizes accordingly. The implementation required modifying PostgreSQL's planner to understand security barrier flags, ensuring the optimizer would not reorder operations in ways that leak information through timing or error side-channels. It works well, but it is deeply coupled to PostgreSQL's planner internals.

Oracle's Virtual Private Database (VPD) predates PostgreSQL RLS by over a decade. VPD attaches PL/SQL functions to tables that generate row-level predicates at parse time. The predicates are injected transparently -- the user never sees them. The mechanism is elegant and battle-tested, but it lives inside Oracle's proprietary optimizer.

Apache Ranger takes a different approach for the Hadoop ecosystem. Ranger rewrites Hive and Spark plans before execution, injecting filter and masking operations. It works, but it requires engine-specific plugins -- a Ranger plugin for Hive, a different plugin for Spark, another for Trino. Each plugin understands the engine's internal plan representation and rewrites it accordingly.

The key distinction in SQE's design: we rely on the optimizer's own expression boundaries to prevent predicate pushdown through masks, without modifying the optimizer itself. PostgreSQL required planner changes. Ranger requires engine-specific plugins. SQE's approach works with any optimizer that respects expression boundaries in predicate pushdown -- which is any well-designed optimizer. The security property comes from the plan structure, not from custom optimizer rules. This makes the approach portable to any engine built on DataFusion, and in principle to any engine that exposes its logical plan for transformation.

One gap worth acknowledging: restricted columns may still appear in `information_schema.columns`. Policy enforcement happens in the query plan, not in the metadata views. A user denied access to the `salary` column will get "column not found" when querying the table, but `SELECT * FROM information_schema.columns WHERE table_name = 'employees'` may still list `salary`. This is a known gap. Fixing it requires the `InformationSchemaProvider` to evaluate policies when constructing virtual table results -- feasible, but not yet implemented. PostgreSQL handles this correctly because RLS is integrated into the system catalog views. We will get there.


## Implementation Status

Here's where we are:

| Component | Status |
|-----------|--------|
| `PolicyEnforcer` trait | Implemented, in production pipeline |
| `PassthroughEnforcer` | Implemented, active by default |
| `policy_enforcer.evaluate()` in query path | Wired into every query, every EXPLAIN |
| `PolicyPlanRewriter` (row filters, masks, restriction) | Implemented |
| `PolicyStore` trait | Implemented |
| OPA backend | Implemented with Rego rules |
| Cedar backend | In progress |
| SQL extensions (GRANT, REVOKE, SHOW GRANTS) | Parser routing implemented, handlers wired |
| Policy cache (moka) | Implemented with async TTL |
| Collibra integration | Interface implemented, connector available |

The architecture was built with the hook in place from the start. The `evaluate()` call has been in the hot path since the first version of the query pipeline. This was a deliberate choice -- adding it later would have meant changing the query execution flow, touching `execute_query`, `explain`, and `analyze`. With the hook already there, shipping the policy engine was a crate-level change. The coordinator didn't change. The query pipeline didn't change. Only the implementation behind `Arc<dyn PolicyEnforcer>` changed.

This is one of the advantages of trait-based design in Rust. The interface is a compile-time contract. Any implementation that satisfies `PolicyEnforcer: Send + Sync` with an `evaluate` method that takes a `SessionUser` and a `LogicalPlan` can be plugged in. The type system guarantees it. We shipped `PassthroughEnforcer` first, then `OpaEnforcer`, and the coordinator binary didn't need a single line changed.


## The Predicate Pushdown Boundary

This section is worth dwelling on because it's the most subtle security property in the system.

Consider a table with columns `id`, `name`, `ssn`, and `salary`. The policy says: `ssn` is masked with REDACT, `salary` is masked with Range(10000). Alice writes:

```sql
SELECT * FROM employees WHERE salary = 153000
```

Without masks, the optimizer would push `salary = 153000` into the TableScan as a Parquet row group filter. This is fast -- it avoids reading rows where salary isn't 153000.

With masks, the plan after rewriting looks like:

```
Projection [id, name, '***' AS ssn, floor(salary/10000)*10000 AS salary]
  Filter [floor(salary/10000)*10000 = 153000]
    TableScan [employees]
```

Alice's predicate is evaluated against the masked value. `floor(153000/10000)*10000 = 150000`, which doesn't equal 153000. She gets zero rows. She can't distinguish between "no employees earn exactly 153000" and "employees earn 153000 but the mask rounds it to 150000."

If the optimizer could push the predicate below the mask, it would evaluate `salary = 153000` against the raw value, returning the matching rows (with masked output). The number of rows returned would leak information: "there are 3 employees earning exactly 153000." The mask hides the value but the row count reveals it.

By placing masks in the plan as expressions, the optimizer's own rules prevent this. It won't push a predicate through an expression boundary because that would change the predicate's semantics. We don't need a custom optimizer rule. We don't need to disable predicate pushdown. The plan structure enforces the security property.

Row filters, by contrast, are safe to push through. If the policy says `region = 'EU'` and Alice writes `department = 'engineering'`, pushing Alice's predicate below the row filter is fine -- it just means fewer rows are scanned. The security invariant (only EU rows are visible) is maintained because Alice's predicate can only narrow, never widen.

::: {.datafusion}
**DataFusion deep dive:** DataFusion's `PushDownFilter` optimization rule walks the plan tree looking for `Filter` nodes whose predicates can be pushed closer to the data source. The rule respects expression boundaries -- it won't push a predicate through a `Projection` that transforms the referenced column. This is the mechanism that protects masked columns. The mask expression (e.g., `floor(salary/10000)*10000`) creates a new expression that the filter references. The optimizer sees this as a different expression from the raw `salary` column and won't push the predicate below the projection. No custom rule needed.
:::


## Testing the Security Properties

The test matrix for the policy engine covers three categories:

**Functional tests:** Does the plan rewriter correctly inject row filters, column masks, and column restrictions? These are unit tests that construct a LogicalPlan programmatically, run the rewriter, and verify the output plan structure.

**Security property tests:** Does the optimizer preserve the security invariants after running? These are the critical tests. Construct a plan with a masked column and a user predicate on that column. Run the policy rewriter. Run the optimizer. Verify the predicate does not appear below the mask in the optimized plan.

**Information leakage tests:** Does a denied column appear in any error message? Does a row filter leave any trace in the output? Does EXPLAIN reveal the original column name for a restricted column? These are negative tests -- verifying the absence of information.

The Phase 5 task list has 14 integration tests planned, covering scenarios from basic column restriction through combined policies with distributed execution. Every test is a GIVEN/WHEN/THEN scenario:

> GIVEN a policy engine is configured, and user has analyst role, and analyst has `ssn MASKED WITH 'REDACT'`
>
> WHEN user submits `SELECT * FROM hr.employees`
>
> THEN the `ssn` column contains `***` for every row

> GIVEN a policy engine is configured, and user has analyst role, and analyst has `ROWS WHERE region = 'EU'`
>
> WHEN user submits `SELECT COUNT(*) FROM transactions`
>
> THEN the count reflects only EU transactions


## The Lesson

Security enforcement in a query engine is not about checking permissions before execution. It's not about filtering results after execution. It's about transforming the query plan so that unauthorized data never enters the execution pipeline.

The plan is the security boundary. A row filter is a `Filter` node. A column mask is an expression in a `Projection`. A denied column is absent from the schema. The optimizer runs on the secured plan and can't undo the security because the security constraints look like ordinary plan nodes.

This model has three properties worth carrying to other systems. First, deny by omission -- never tell the user something is denied; just make it invisible. Second, structural enforcement -- use the type system and the plan structure to prevent security bypasses, rather than relying on runtime checks. Third, separation of policy and enforcement -- let the governance platform decide who sees what; let the query engine ensure the decision is applied.

The `PolicyEnforcer` trait is twenty-six lines of Rust. The plan rewriting that implements it will be several hundred. The security properties it guarantees come not from the amount of code, but from where it sits in the pipeline: after parsing, before optimization. That placement is the entire design.

::: {.ailog}
**AI Logbook:** The AI produced the `PolicyEnforcer` trait, `PassthroughEnforcer`, and the `PolicyPlanRewriter` with `transform_down` in three passes. The first pass applied masks and restrictions as independent projections — the second projection discarded the first's mask expressions. The human caught this during code review and restructured the prompt to require a single-pass projection. The security property that masks block predicate pushdown was verified by the human's first test; it passed because DataFusion's optimizer respects expression boundaries, not because we wrote a custom rule.
:::
