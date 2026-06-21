# The Lineage Trail {#sec:lineage-trail}

> Lineage is the question every data team asks at 3 AM:
> "Where did this number come from?"

The line had been on the matrix-parity plan for weeks. "Lineage. Add OpenLineage emitter once a user asks for it." It sat in the Deferred section. It had two upvotes. Nobody was paging us about it.

A user asked. The ask was small. The work was not.

This is the chapter about scope creep that we walked into willingly. Each "yes" to a follow-up question expanded the bag. Each expansion still felt right. By the time we shipped, we had a new crate, a column-level lineage extractor, two sinks, a disk spool, and a configuration block that operators could understand in five minutes. None of it was on the original whiteboard.

## Why OpenLineage and not our own schema

The first decision was the cheap one. The audit log already had a query history table. It had timestamps. It had user names. It had statement text. Adding two more columns called `inputs` and `outputs` would have been one afternoon of work.

We did not do that. The reason: tools.

Marquez exists. DataHub exists. The OL Java client exists. The OL Spark integration exists. The OL Airflow integration exists. The Marquez UI knows how to render an OL run graph. DataHub's metadata model speaks OL natively. Building a homegrown schema means building all of that ourselves.

OpenLineage is the lakehouse equivalent of OpenTelemetry. The wins are the same. Pick the open standard, get the ecosystem for free.

The current stable spec is 2-0-2. We pinned to it. The `RunEvent` type, the `RunFacets` map, the `ColumnLineage` facet, the URI scheme for namespaces. All directly out of the spec. Nothing custom in the wire format.

```rust
pub const SCHEMA_URL: &str =
    "https://openlineage.io/spec/2-0-2/OpenLineage.json";
```

That constant is in `crates/sqe-lineage/src/event.rs` and it never changes between events. Every event we emit advertises which version of the spec it conforms to. If we move to 2-1-0 next year, that line is the only place we update.

## Three scope decisions we made up front

Before the first line of code, we wrote down three things.

**Column-level lineage by default.** OL supports two granularities: dataset-level (this query touched these tables) and column-level (this output column comes from these input columns). Dataset-level is cheap. Column-level is real work. It needs a recursive walk over the `LogicalPlan` and a per-column `ColumnDep` set that tracks where each output column came from and how it was transformed.

We picked column-level. The reasoning was simple. Dataset-level lineage tells you "your dashboard uses orders". Column-level lineage tells you "the `total_revenue` column on your dashboard is a SUM of `orders.amount` filtered by `region = 'EU'`". The first is a footnote. The second is the answer to the 3 AM question.

**SELECT events optional.** The audit log already records every query. SELECT events would double the volume without adding new information. We made `emit_selects` a config flag. Off by default. Operators who want SELECT lineage in their catalog can flip it on. Most will not.

**Multi-catalog dataset URIs.** SQE talks to Polaris. It also talks to Nessie, Unity, Glue, S3 Tables, HMS, JDBC, and Hadoop. The OL `Dataset` type wants a `namespace` and a `name`. A naive scheme would set the namespace to `iceberg://my-warehouse` and lose the catalog identity. We made the namespace include the catalog: `iceberg://catalog-name/warehouse`. The dataset URI now uniquely identifies a table in any of the eight backends. Marquez and DataHub handle this fine. Both treat the namespace as an opaque string.

These three decisions cost no code at the time. They cost us the right to claim "we knew what we were building" later.

## The plan walk that mostly worked

DataFusion's `LogicalPlan` is a tree of nodes. `Projection`, `Filter`, `Aggregate`, `Join`, `Window`, `Sort`, `Union`, `Limit`, `Distinct`, `SubqueryAlias`, `TableScan`. Each node knows its input schema and its output schema. Each `Expr` knows which columns it references. The walk is the obvious thing: recurse on the inputs, get a per-column trace from each child, combine them according to the node type.

`Expr::column_refs()` does most of the work. For a node like `Projection`, every output column comes from some expression. The expression has a column ref set. Map each ref to its position in the child's schema. Copy the child's trace at that position. Done.

```rust
let bare = unwrap_alias(e);
if let Expr::Column(c) = bare {
    if let Some(idx) = column_index(p.input.as_ref(), c) {
        if let Some(deps) = child_trace.get(idx) {
            return deps.clone();
        }
    }
}
```

Eleven node types fell out in two weeks. Each rule is twenty to forty lines. The pattern is identical: pull the children's traces, classify the expression's column refs by which child owns them, attach the right `Transformation` enum (IDENTITY, COMPUTED, AGGREGATION, GROUP_BY, JOIN, WINDOW, FILTER), assemble the output trace.

Two surprises. The first was Window functions. `Expr::Column` is what we matched against to detect a passthrough. Window expressions are wrapped in `Expr::Alias` even when the underlying expression looks like a plain column. The first version of the trace rule treated every windowed column as a brand-new computation. The lineage facet showed window outputs as orphans with no upstream. The fix was three lines and an hour of debugging:

```rust
fn unwrap_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(a) => unwrap_alias(&a.expr),
        other => other,
    }
}
```

Strip the outer alias before pattern-matching. Then the same passthrough logic that handles `SELECT col` handles `SELECT row_number() OVER (PARTITION BY col)` correctly. The bug surfaced only when we wrote a test against a real window query. Without that test, the first user to run an OL extractor over a `LAG()` query would have found it.

The second surprise was MERGE. DataFusion 53.1 has no `LogicalPlan::Merge` node. MERGE statements get rewritten into a sequence of joins and CASE expressions before they reach the plan tree. The plan we see at lineage time is the post-rewrite version. The dataset-level lineage works. We extract the target table from the SQL classifier and the source tables from the rewritten joins. The column-level lineage works for the SQL columns the user wrote. What does not work is the per-branch annotation: which output columns came from a MERGE_INSERT branch versus a MERGE_UPDATE branch. The OL spec wants that. DataFusion does not give us the structure to derive it.

We deferred. The chapter on what we did not ship has the rest.

## The disk spool we did not want to write

The first design had no disk spool. The plan was: a bounded `tokio::sync::mpsc` channel between `query_handler` and the emitter task, drop-newest on overflow, fire-and-forget HTTP POST to the collector. Total: about six hundred lines of code.

The drop-newest channel is the right shape for normal back-pressure. An emitter task that is briefly slow because the collector is rate-limited gets a few dropped events. A counter increments. Nobody cares.

The case that broke the design was the unreachable collector. Marquez runs in a sidecar Pod. The sidecar restarts. For the next ninety seconds while Marquez comes back up, every HTTP POST fails. Ninety seconds of writes at production load fills the channel. Drop-newest kicks in. The events that get dropped are the ones the collector cares about most. The newest ones.

We could have switched to drop-oldest. We did not. The reason was order. OL run reconstruction is more forgiving when COMPLETE arrives without START than the inverse. If we drop oldest, we lose START events first. The collector ends up with COMPLETE-only events and cannot reconstruct the run. If we drop newest, the START events stay. The COMPLETE events that get dropped become orphan starts. Marquez handles those by leaving the run open. A human can clean up the orphans by querying the audit log.

But neither drop policy is acceptable when the collector is down for minutes. We added a disk spool.

The spool is a JSONL file at `spool_path`. Bounded by `spool_max_bytes`. When the HTTP sink fails, the event goes to the spool instead. A background replay loop wakes every `replay_interval_secs`, reads the spool, and re-emits. On success, the line is deleted. On failure, it stays. When the spool reaches its size cap, drop-newest kicks in there too.

```toml
spool_path           = "/var/spool/sqe-ol"
spool_max_bytes      = 104857600
replay_interval_secs = 30
```

The spool is opt-in. An operator who only wants the file sink does not configure a spool. An operator who runs HTTP-only with a flaky collector does configure one. The default is "no spool, drop on HTTP failure". Most production deployments will set it.

The disk spool was four hundred lines of code we did not want to write. It also turned out to be the feature that made the difference between "lineage is best-effort" and "lineage is reliable". A best-effort signal is a signal nobody trusts. A reliable signal becomes load-bearing in someone else's runbook.

## What we still do not ship

The deferred list is short and specific. None of it blocks a useful v1.

**Heartbeat events for long-running queries.** The OL spec defines a `RUNNING` event type for queries that take longer than a heartbeat interval. We emit START and COMPLETE only. A query that runs for an hour shows up in Marquez as a single line that flips from running to complete. No mid-execution metrics. Adding heartbeats means a periodic timer in the emitter task and a way to attach progress facets. A week of work. We have not done it.

**mTLS to the collector.** The HTTP sink supports `auth_mode = "bearer"` and `auth_mode = "user_token"`. Both ride over plain TLS via `reqwest`. Mutual TLS where the collector verifies SQE's client certificate is wired through the existing `RustlsConfig` infrastructure but not exposed in the OL config block. Operators who want it can build their own collector that reads the bearer token. Operators who want real mTLS have to wait.

**Per-event user token forwarding.** `auth_mode = "user_token"` is the feature where each OL event carries the bearer token of the user who ran the query, not a service-account token shared by SQE. This matches the rest of the engine: every catalog call uses the user's bearer. The OL emitter currently falls back to the static `api_key` even when configured for `user_token`. The plumbing for token capture is in place. The HTTP sink switch is a follow-up.

**Deeper-than-one-level correlated subqueries with full column granularity.** A `SELECT ... WHERE x IN (SELECT y FROM t2 WHERE t2.z = outer.col)` has a subquery whose plan we walk. Columns from the subquery's `t2` show up in the lineage as INDIRECT/FILTER dependencies on the outer query's output. Two levels of nesting work. Three levels work but with a coarser dependency classification. Four levels and we hit a wall in our column-classification code where outer references blur. This is rare in practice. Production analytics rarely nests subqueries that deep. We left it as a documented limitation.

**Embedded CLI emit.** `sqe-cli --embedded` runs DataFusion directly without the coordinator's `QueryHandler`. The OL emitter lives in `QueryHandler::execute_statement`. Embedded mode bypasses both. We could plumb a `LineageObserver` through `EmbeddedClient`, but the use cases for embedded-mode lineage are thin. Most embedded users are doing laptop analytics on local files. They are not feeding a metadata catalog. The decision is to leave embedded mode without lineage. If a user asks, we wire it through.

**MERGE per-branch column lineage.** Already covered above. Blocked on DataFusion exposing a `LogicalPlan::Merge` node. There is no upstream issue tracking it. We re-check at every DataFusion rebase.

**Maintenance procedures.** OPTIMIZE, VACUUM, REWRITE_MANIFESTS rewrite data files. They do not change the lineage of any column. They are filtered out at the `should_emit` decision in the coordinator. Not a gap. A deliberate exclusion.

## What it looks like running

A query goes in:

```sql
INSERT INTO sales.daily_summary
SELECT region, sum(amount), count(*)
FROM sales.orders
WHERE order_date = current_date
GROUP BY region;
```

Two events go out. The START arrives at submit time:

```json
{
  "eventType": "START",
  "eventTime": "2026-05-09T08:31:42.103Z",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
  "run": {"runId": "f4a8...d11c"},
  "job": {
    "namespace": "sqe-prod",
    "name": "query:sales-summary-load"
  },
  "inputs":  [{"namespace": "iceberg://polaris/warehouse",
               "name": "sales.orders"}],
  "outputs": [{"namespace": "iceberg://polaris/warehouse",
               "name": "sales.daily_summary"}]
}
```

The COMPLETE arrives on success with a `columnLineage` facet on the output dataset:

```json
{
  "eventType": "COMPLETE",
  "outputs": [{
    "namespace": "iceberg://polaris/warehouse",
    "name": "sales.daily_summary",
    "facets": {
      "columnLineage": {
        "fields": {
          "region": {"inputFields": [{
            "namespace": "iceberg://polaris/warehouse",
            "name": "sales.orders",
            "field": "region",
            "transformations": [{"type": "DIRECT", "subtype": "GROUP_BY"}]
          }]},
          "total":  {"inputFields": [{"field": "amount",
                                       "transformations": [{
                                         "type": "DIRECT",
                                         "subtype": "AGGREGATION"}]}]},
          "rows":   {"inputFields": [{"field": "amount",
                                       "transformations": [{
                                         "type": "INDIRECT",
                                         "subtype": "AGGREGATION"}]}]}
        }
      }
    }
  }]
}
```

The Marquez UI consumes both events and renders the run as a node in a graph. The columnLineage facet renders as fan-in arrows from `orders.region`, `orders.amount`, and `orders.order_date` (the last one classified as INDIRECT/FILTER) to the three output columns of `daily_summary`. A user clicks `daily_summary.total` and sees the full upstream chain. That is the 3 AM answer.

## Closing

Lineage is plumbing. Operators discover it the moment something goes wrong. The dashboard says the order count dropped by 30 per cent. Someone needs to know which upstream changed. Was it the ETL job that re-bucketed the regions? Was it the dbt model that filtered out null statuses? Was it the catalog migration that renamed a column?

Without lineage, the answer is "let me read the code and figure it out". With lineage, the answer is "click the column in Marquez and walk the graph". The first approach takes hours. The second takes seconds.

We did not need to ship this in v0.16. The matrix score does not move. The benchmarks do not move. No customer is paying for it. We shipped it because lineage is the kind of feature that matters most when you cannot retrofit it. The events have to be emitted at query time. You cannot reconstruct lineage from logs after the fact. Either the engine emits OL or it does not.

SQE emits OL.
