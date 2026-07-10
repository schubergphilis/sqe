---
title: "Shipping OpenLineage: column-level lineage for an Iceberg engine"
description: "SQE now emits OL 2-0-2 events with column-level lineage on every write. Here is what they look like, why we walked the LogicalPlan to build them, and the disk spool we did not want to write."
pubDate: "2026-05-09"
author: "Jacob Verhoeks"
tags:
  - "openlineage"
  - "lineage"
  - "datafusion"
  - "iceberg"
  - "observability"
---



*May 9, 2026*

A dashboard alerts at 3 AM. Order count dropped 30 per cent overnight. The on-call engineer opens the dashboard, finds the column, and asks one question.

Where did this number come from?

Lineage is the answer. Without it the engineer reads code, greps SQL files, opens the dbt project, traces back through views, and eventually finds the upstream model that filtered out null statuses last Tuesday. With it the engineer clicks the column in Marquez, walks the graph two hops upstream, and finds the same model in eight seconds.

We just shipped the lineage.

## What we built

SQE now emits OpenLineage 2-0-2 events for every write. INSERT, CTAS, MERGE, UPDATE, DELETE, plus DDL on tables. SELECT events are off by default and can be enabled per environment. The events carry dataset-level lineage (which tables this query touched) and column-level lineage on the output (which input columns produced each output column, and what transformation was applied).

Two transports run side by side. A JSONL file sink that appends events to disk, useful for SIEM ingestion or local dev. An HTTP sink that POSTs to any OL collector: Marquez, DataHub, an OL-compliant catalog. The HTTP sink falls back to a bounded disk spool when the collector is unreachable, then replays on recovery.

Off by default. Zero overhead in the query path when disabled. The whole thing lives in a new crate, `sqe-lineage`, behind a `LineageObserver` trait that the coordinator calls at three points in the query lifecycle.

Roughly six thousand lines of Rust including tests. Net-new dependencies: zero.

## A walk through one query

The most concrete way to explain column-level lineage is to show what comes out the other end. Run this CTAS:

```sql
CREATE TABLE polaris.sales.archive AS
SELECT
    order_id,
    customer_id,
    amount * 1.1 AS amount_with_tax
FROM polaris.sales.orders
WHERE region = 'EU';
```

Two events go to the collector. The START fires at submit time and is short:

```json
{
  "eventType": "START",
  "eventTime": "2026-05-09T08:31:42.103Z",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
  "run": { "runId": "f4a8...d11c" },
  "job": {
    "namespace": "sqe-prod",
    "name": "ctas:9b8e2f"
  },
  "inputs": [],
  "outputs": []
}
```

START events do not carry the plan. The plan is captured after policy enforcement runs, just before execution. The COMPLETE event arrives when the write succeeds and carries the lineage:

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2026-05-09T08:31:42.987Z",
  "producer": "https://github.com/sbp/sqe/v0.16.0",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
  "run": {
    "runId": "f4a8...d11c",
    "facets": {
      "nominalTime": { "nominalStartTime": "2026-05-09T08:31:42.103Z" }
    }
  },
  "job": {
    "namespace": "sqe-prod",
    "name": "ctas:9b8e2f",
    "facets": {
      "sql": {
        "query": "CREATE TABLE polaris.sales.archive AS SELECT ...",
        "dialect": "sqe"
      }
    }
  },
  "inputs": [{
    "namespace": "https://polaris.example/api/catalog",
    "name": "sales.orders"
  }],
  "outputs": [{
    "namespace": "https://polaris.example/api/catalog",
    "name": "sales.archive",
    "outputFacets": {
      "columnLineage": {
        "fields": {
          "order_id":    { "inputFields": [{
            "namespace": "https://polaris.example/api/catalog",
            "name": "sales.orders",
            "field": "order_id",
            "transformations": [{
              "type": "DIRECT", "subtype": "IDENTITY"
            }]
          }]},
          "customer_id": { "inputFields": [{
            "field": "customer_id",
            "transformations": [{
              "type": "DIRECT", "subtype": "IDENTITY"
            }]
          }]},
          "amount_with_tax": { "inputFields": [
            {
              "field": "amount",
              "transformations": [{
                "type": "DIRECT",
                "subtype": "TRANSFORMATION",
                "description": "amount * 1.1",
                "masking": false
              }]
            },
            {
              "field": "region",
              "transformations": [{
                "type": "INDIRECT", "subtype": "FILTER"
              }]
            }
          ]}
        }
      }
    }
  }]
}
```

A few things are doing real work in that JSON. The input namespace is the Polaris REST URL, not a friendly catalog name. Renames and S3 path moves do not break the lineage continuity in the OL UI because the namespace is the catalog identity itself. A query that joins `polaris.a.x` and `nessie.b.y` shows up with two input datasets in two namespaces.

The `columnLineage.fields.amount_with_tax.inputFields` array has two entries. One for `amount` (the column the projection actually multiplied), classified as DIRECT/TRANSFORMATION with the expression as the description. One for `region`, the WHERE-clause column, classified as INDIRECT/FILTER. That second entry is what tells you why this row is in the output: it passed the EU filter on `region`. Row count drops in the downstream dataset trace back to filter changes upstream. That is the lineage equivalent of the 3 AM dashboard alert.

The Marquez UI renders this as a node graph. Click `archive.amount_with_tax`, see two upstream arrows: a thick one to `orders.amount`, a dotted one to `orders.region`. Click again on `orders.region`, walk one more hop to whatever upstream produced the orders table. The chain is navigable.

## Why column-level and not just dataset

Dataset-level lineage answers "table A depends on table B". It is a dependency graph between tables. It is genuinely useful for impact analysis: "if I drop column X from orders, which downstream tables care?" gets you a shortlist of tables.

Column-level lineage answers a different question. It says "the `revenue` column on the executive dashboard is computed from `orders.amount * 1.1` filtered on `orders.region = 'EU'`". When the executive dashboard breaks, you do not need to know which tables changed. You need to know which input column drove the broken output.

Dataset-level is cheaper. We could have shipped it in two days. Column-level needed a recursive walk over the `LogicalPlan` and a per-column `ColumnTrace` that propagates dependencies up the tree. That was a week of work plus another week of edge cases.

We thought it was worth it. The 3 AM question is always at column granularity. Engines that emit dataset-level lineage are still useful, but the user has to read the SQL to translate "table A changed" into "this is why my column moved". With column-level, the OL collector does the translation.

## What surprised us

DataFusion's `Expr::column_refs()` does most of the work for free. Every expression in the plan tree exposes its column references as a `HashSet`. For a `Projection`, the rule is: each output column comes from one expression, the expression has a column-ref set, map each ref to a position in the child schema, copy the child's trace at that position. Eleven node types fell out in two weeks. Each rule is twenty to forty lines.

MERGE is a problem. DataFusion 53.1 has no `LogicalPlan::Merge` variant. MERGE statements get rewritten into a sequence of joins and CASE expressions before they reach the plan tree. We can extract the dataset-level lineage (target table from the SQL classifier, source tables from the rewritten joins). We cannot annotate per-branch column lineage: which output columns came from `WHEN MATCHED UPDATE` versus `WHEN NOT MATCHED INSERT`. The OL spec wants that. DataFusion does not give us the structure to derive it. Documented as a v1 limitation.

Window functions wrapped in `Expr::Alias` cost an hour. The first version of the trace rule treated every window-aliased column as a brand-new computation. Window outputs showed up as orphans with no upstream. The fix is three lines:

```rust
fn unwrap_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(a) => unwrap_alias(&a.expr),
        other => other,
    }
}
```

Strip the outer alias before pattern-matching. Then the same passthrough that handles `SELECT col` handles `SELECT row_number() OVER (PARTITION BY col)`. The bug surfaced only when we wrote a test against a real window query. Without that test, the first user with a `LAG()` query would have found it for us.

The disk spool, finally, is the part we did not want to write. The first design had no spool. A bounded `tokio::sync::mpsc` channel between `query_handler` and the emitter task, drop-newest on overflow, fire-and-forget HTTP POST. Six hundred lines total. The drop-newest channel is the right shape for normal back-pressure: brief slowness in the collector triggers a few dropped events, a counter increments, nobody cares.

The case that broke the design was the unreachable collector. Marquez running in a sidecar Pod that restarts. Ninety seconds of failed POSTs at production load fills the channel. Drop-newest kicks in. The events that get dropped are the ones the collector cares about most: the most recent ones.

We considered drop-oldest. We rejected it. The reason is order: OL run reconstruction is more forgiving when COMPLETE arrives without START than the inverse. Drop-oldest loses START events first, the collector gets COMPLETE-only events and cannot reconstruct runs. Drop-newest leaves the START events alive and turns dropped COMPLETE events into orphan starts. Marquez handles those by leaving the run open. A human can clean up by querying the audit log.

But neither drop policy is acceptable when the collector is down for minutes. So we wrote the spool. JSONL file at `spool_path`, bounded by `spool_max_bytes`. When HTTP fails, the event goes to the spool. A background replay loop wakes every `replay_interval_secs`, reads rotated spool segments, and re-emits. On success, the segment is deleted.

The spool was four hundred lines of code we did not want to write. It also turned out to be the feature that distinguishes "lineage is best-effort" from "lineage is reliable". Best-effort signals are signals nobody trusts. Reliable signals become load-bearing in someone else's runbook. We picked load-bearing.

## Where the observer lives

The architectural decision we kept revisiting was the location of the plan-walking code. Three options were on the table.

Option one: inline in `query_handler`. Simplest. Add a hundred lines after the plan is captured, build the OL event, fire-and-forget. The query handler is already a four-thousand-line file that does SQL parsing, policy enforcement, statement dispatch, audit logging, and metrics. Adding the lineage extractor would have made it longer and harder to test in isolation. Rejected.

Option two: standalone `sqe-lineage` crate with a trait. A `LineageObserver` interface the coordinator calls at three lifecycle points. A `ChannelObserver` implementation that pushes to an mpsc, an emitter task that owns the receiver and fans out to sinks. The plan-walking logic lives behind the trait, has its own tests, and is the only place column-trace edge cases land. Picked.

Option three: an OpenTelemetry side-car. Treat lineage as just another OTel signal, ride the existing OTel exporter pipeline. Conceptually clean. Practically wrong: OL is not OTel. OL events are run-shaped (one event per query, large, with structured facets). OTel signals are stream-shaped (many events per second, small, unstructured). Forcing one through the other costs more than it saves. Rejected.

The standalone crate also makes adding new sinks cheap. A Kafka sink, a Pulsar sink, an OTel-OL bridge would each be a single file behind the existing `Sink` trait. The plumbing is in place. We just have not had a user ask for any of them.

## What we did not ship

The deferred list is short and explicit.

**mTLS to the collector.** The HTTP sink supports `auth_mode = "bearer"` and `auth_mode = "user_token"` over plain TLS. Mutual TLS where the collector verifies SQE's client cert is wired through the existing rustls config but not exposed in the OL block. Operators who want it have to wait.

**Per-event user-token forwarding.** `auth_mode = "user_token"` is the feature where each OL event carries the bearer token of the user who ran the query. This matches the rest of SQE: every catalog call uses the user's bearer. The OL emitter currently falls back to the static `api_key` even when configured for `user_token`. The session-level token capture is in place. The HTTP sink switch is a follow-up.

**MERGE per-branch column lineage.** Blocked on DataFusion exposing a `LogicalPlan::Merge` node. There is no upstream issue tracking it. We re-check at every DataFusion rebase.

**Embedded CLI emit.** `sqe-cli --embedded` runs DataFusion directly without the coordinator's `QueryHandler`. The OL emitter lives in `QueryHandler::execute_statement`. Embedded mode bypasses both. We could plumb a `LineageObserver` through `EmbeddedClient`, but the use cases for embedded-mode lineage are thin. Most embedded users do laptop analytics on local files. They are not feeding a metadata catalog.

**DDL hint extraction.** CREATE TABLE, ALTER TABLE, DROP emit events with the dataset target captured but an empty plan facet. The schema is available from the catalog at completion time but we did not wire that into the column-lineage builder for v1. The events are still useful for "this table existed at this time" tracking. The column granularity on DDL is missing.

**Heartbeat events for long-running queries.** The OL spec defines a `RUNNING` event type. We emit START and COMPLETE only. A query that runs for an hour shows up in Marquez as one line that flips from running to complete. No mid-execution progress.

None of these block a useful v1. Each is scoped, documented, and tracked. v2 picks them up if a user asks.

## How to turn it on

The minimum config to point SQE at a local Marquez:

```toml
[metrics.openlineage]
enabled        = true
http_endpoint  = "http://localhost:5000/api/v1/lineage"
auth_mode      = "none"
spool_path     = "/var/spool/sqe-ol"
```

Run Marquez:

```bash
docker run -p 5000:5000 -p 5001:5001 marquezproject/marquez
```

Restart SQE. Run a query. Browse Marquez at `http://localhost:3000`. The full configuration reference lives in `docs/book/src/operations/openlineage.md`, including the DataHub setup and the troubleshooting matrix.

Every TOML key has an `SQE_METRICS__OPENLINEAGE__*` env override. The config validator runs in `bin/sqe_server.rs` between config load and `QueryHandler::new`. Misconfigured blocks (enabled but no sink, bearer without api_key, spool without http_endpoint) refuse to start the server. We wanted lineage misconfigurations to fail loud, not silent.

## Closing

Lineage is plumbing. Operators discover it the moment something breaks. The dashboard says the order count dropped by 30 per cent. Someone needs to know which upstream changed. Was it the ETL job that re-bucketed the regions? The dbt model that filtered out null statuses? The catalog migration that renamed a column?

Without lineage, the answer is "let me read the code". With lineage, the answer is "click the column and walk the graph". The first takes hours. The second takes seconds. That is the gap OL events close.

We did not need to ship this in v0.16. The matrix score does not move. The benchmarks do not move. No customer was paying for it. We shipped it because lineage is the kind of feature that matters most when you cannot retrofit it. The events have to be emitted at query time. You cannot reconstruct lineage from logs after the fact. Either the engine emits OL or it does not.

SQE emits OL.
