---
title: "Metabase connected, then showed zero tables"
description: "A BI tool connecting to a query engine is not one query. It is a scripted handshake of a dozen metadata calls, and every one has to look exactly like Trino or the tool gives up without an error. We pointed a real Metabase at SQE's Trino endpoint and watched it fail six different ways: a PREPARE the parser rejected, a SHOW TABLES column that collapsed every table into one, catalogs it never enumerated, quoted identifiers that matched nothing, and a timestamp type signature the JDBC driver refused to parse. None of them threw. They just showed nothing."
pubDate: "2026-07-02"
author: "Jacob Verhoeks"
tags:
  - "trino-compatibility"
  - "metabase"
  - "superset"
  - "jdbc"
---

*July 2, 2026*

Metabase said the connection succeeded. Then it synced the database and showed zero tables.

No error. No stack trace. No log line pointing at a bad response. A green checkmark on the connection test, and an empty schema browser. That is the worst kind of bug, because the tool has already decided everything is fine and moved on.

The reason is that connecting a BI tool to a query engine is not a query. It is a handshake. Before Metabase or Superset ever runs a chart, the JDBC driver walks through a fixed script: prepare a statement, list the catalogs, list the schemas, list the tables, describe each table's columns, then run a typed probe query. Every step in that script expects a response shaped exactly the way Trino shapes it. Off by one column name, one quote character, one type signature, and the tool does not error. It reads the malformed answer, concludes there is nothing there, and stops.

We pointed a real Metabase at SQE's Trino endpoint and watched it fail at six different points in that handshake. Here they are in the order the driver hits them.

## The connection that never connected

The very first thing the Trino JDBC driver does is issue a `PREPARE`. Not because the user asked for one, but because that is how the driver validates the connection.

SQE took `PREPARE stmt FROM SELECT 1` and handed the whole string to the SQL parser. The parser reads `PREPARE stmt`, expects `AS`, finds `FROM`, and rejects it. The connection test failed, and the database could never be added at all.

`PREPARE` and `DEALLOCATE PREPARE` are not queries. They carry no result set. Trino tracks the prepared SQL through response headers, `X-Trino-Added-Prepare` and `X-Trino-Deallocated-Prepare`, and never runs anything. We do the same now: register the statement in the header, skip the executor entirely. Combined with the `EXECUTE ... USING` rewrite that was already there, the full round-trip works. The driver prepares, gets its header, executes by name.

That fix moved the failure from "cannot connect" to "connects, shows zero tables." Progress, of a kind.

## One table named `gold.`

With the connection alive, Metabase ran `SHOW TABLES` to sync the schema. SQE returned two columns: `namespace` and `table_name`.

Trino returns one. A single column named `Table`, holding bare table names, one row each. The JDBC driver does not read by column name here. It reads column zero as the table name. So our leading `namespace` column meant the driver read the namespace, `gold`, for every single row. Metabase collapsed the entire schema into one malformed table called `gold.`, tried to activate it, and aborted the sync with "Error creating or reactivating tables." Zero tables again, this time for a shape mismatch of one column.

The fix is to match Trino exactly. `SHOW TABLES` returns one `Table` column, sorted, one row per table. While we were there we fixed the siblings: `SHOW SCHEMAS` returns `Schema`, `SHOW CATALOGS` returns `Catalog`, the exact column names Trino uses. Our compat test harness compares these against a real Trino instance, so aligning the names made the test converge instead of drift.

## The catalogs it never looked at

The tables still did not appear for the catalog that mattered.

SQE's `SHOW CATALOGS`, and the `system.jdbc.*` and `system.metadata.*` tables the driver reads for schema sync, only ever saw the default warehouse. A deployment with per-workspace catalogs, `ws_energy_co` and the like, exposed none of them to JDBC introspection. The driver asked what catalogs exist, got back only the default, and never thought to look at the workspace where the data lived. Worse, enumeration hit a hard stop the first time it reached a catalog the principal could not list, 403-aborting the whole sync.

Two changes. First, enumerate every reachable catalog: the configured ones plus the session's own catalog from `X-Trino-Catalog`, deduplicated, and iterate all of them in the JDBC and metadata system tables instead of a single primary. Second, skip the catalogs and namespaces the caller is not authorized to list, rather than letting one 403 abort the rest.

There was a matching bug in the `SHOW` path. `SHOW TABLES` with no explicit catalog fell straight to the default warehouse and ignored the session catalog from `X-Trino-Catalog`. The `SELECT` path worked, because a fully-qualified three-part name triggered catalog discovery on its own, but `SHOW TABLES` did not carry one. So the query editor could read a table the schema browser swore did not exist. We made `SHOW` fall back to the session catalog before resolving, so both paths discover the same thing.

## Tables sync, columns do not

Now the tables showed up. Click one to build a question, and Metabase reported zero columns.

Tables synced, fields did not. That split is a specific signature. Metabase discovers a table, then issues `DESCRIBE` or `SHOW COLUMNS` to learn its fields, and the JDBC driver quotes the identifier the standard way: `"catalog"."schema"."table"`. SQE rendered those quote characters into the lookup, split the name on the dots, and filtered `information_schema` for a table literally named `"table"`, quotes included. Nothing matched. No error, an empty column list, and a table you cannot ask any question about.

The `SELECT` planner already resolved quoted identifiers correctly. `DESCRIBE` and `SHOW COLUMNS` did not, even though both funnel through one query builder. We added a quote-aware splitter: separate qualifiers on dots only outside quotes, collapse doubled quotes to a literal one, strip the surrounding pair. The quoted form now resolves identically to the bare form, and the single chokepoint fixes both statements at once.

## The response the driver refused to read

Columns visible, Metabase built a question with a date grouping. Month over month. The query came back as "the server returned an invalid response."

The entire 200 response failed, not the query logic. The cause was one field in the type metadata. When SQE reports a `timestamp(6)` column, it was putting the parameterized string `timestamp(6)` directly into the `rawType` of the type signature. The Trino JDBC driver's type-signature parser does not accept a parameter inside `rawType`. It rejects the whole payload, and the driver reports a broken response for a query that ran perfectly on the server.

Trino carries the precision differently. The parameter lives in the `arguments` array with the base name in `rawType`, the same shape `decimal(p,s)` already used and that we already got right. We taught the timestamp path to do the same, for `timestamp(p)`, `timestamp(p) with time zone`, and the `time` variants. Bare `timestamp` and `time` still take the simple path.

Date bucketing is exactly what triggers this, because `date_trunc('month', ...)` produces a `timestamp(6)`. So the fix is what makes month, quarter, and year grouping work in a BI tool over JDBC, the most ordinary thing a dashboard does.

One related type bug rode along. SQE mapped Arrow `UInt64` to Trino `decimal(20,0)`. The only unsigned-64 columns SQE actually produces are computed aggregates, `count(*)`, `approx_distinct()`, `row_number()`, whose values fit comfortably in a signed 64-bit integer, and Trino types those as `bigint`. A dashboard showing a row count as `1234.` with decimal formatting is a small thing that looks wrong to everyone who sees it. We map them to `bigint` and render them as JSON numbers, and we normalized rendered timestamps to exactly six fractional digits so `date_trunc` and `CAST` agree with the advertised type.

## What we took from it

Every one of these was invisible from the SQE side. The server ran the queries. The logs were clean. The responses were valid JSON. And Metabase showed nothing, because a BI tool trusts the wire shape and fails closed when the shape is wrong.

The lesson is that wire-protocol parity is not "we support the SQL." The SQL was never the problem. Parity is matching the exact shape of a dozen metadata calls the tool makes before it runs a single line the user wrote: the column names of `SHOW TABLES`, the header protocol for `PREPARE`, the quoting rules for `DESCRIBE`, the argument layout of a type signature. Each one is small. Each one, wrong, is a silent zero.

You cannot find these with curl. curl reads whatever comes back and prints it, and to a human the response looks fine. You find them by running the actual driver, the actual tool, and watching where it goes quiet. We built the compat test to compare SQE's responses against a real Trino instance for exactly that reason, so "looks fine to a human" stops being the bar. The bar is byte-shape agreement with the thing the tool already trusts.

Metabase and Superset connect now. They sync catalogs, browse columns, and bucket dates. The handshake is invisible again, which is the only state in which a wire protocol is doing its job.
