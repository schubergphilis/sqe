---
title: "Speaking Quack: SQE as a DuckDB server, a DuckDB client, and a federation engine"
description: "DuckDB 1.5 ships a wire protocol called Quack. We re-implemented it in pure Rust, turned SQE into both server and client, and proved you can JOIN an Iceberg table with a remote DuckDB table in a single SELECT. Two waves of bugs, one federated query."
pubDate: "2026-05-26"
author: "Jacob Verhoeks"
tags:
  - "duckdb"
  - "quack"
  - "datafusion"
  - "iceberg"
  - "federation"
---



*May 26, 2026*

DuckDB 1.5 shipped a wire protocol called Quack. It is a tiny HTTP-based RPC: POST to `/quack` with `application/vnd.duckdb`, get bytes back. Two messages do most of the work, `PrepareRequest` and `FetchResponse`, both serialised with DuckDB's `BinarySerializer`. Any DuckDB instance can connect to a Quack endpoint by typing `ATTACH 'quack:host'` and then querying it like a local database.

The protocol is interesting because it cuts the dependency on a SQL dialect. Quack speaks Arrow at the data layer and DuckDB SQL at the request layer. A non-DuckDB engine can implement it and look exactly like a DuckDB to any tool that already knows how to attach.

We wanted to know if SQE could pull that off, in both directions.

## Server mode: SQE looks like a DuckDB

The first wave was the server. `sqe-server` accepts incoming Quack connections, runs the SQL through DataFusion, and streams Arrow batches back over the wire.

```
duckdb CLI --quack:9494--> sqe-server --DataFusion--> Iceberg via Polaris
```

The pure-Rust port lives in two crates. `sqe-quack-wire` is the codec: LEB128 varints, `field_id_t` little-endian u16, `0xFFFF` object terminator, the lot. `sqe-quack-server` wraps `axum` around it and dispatches messages to a `QueryExecutor` trait. We pinned everything to `SerializationCompatibility::FromIndex(7)`, which is DuckDB 1.5.x.

The bit that still surprises me is how small the codec is. About 1600 lines of Rust, including the Arrow bridge in both directions and every parameterised type DuckDB uses: `DECIMAL(p, s)`, `LIST<T>`, `STRUCT(...)`, `MAP<K, V>`, `ARRAY<T, N>`, `ENUM`, `UNION`. Each got a separate MR because each one taught us something specific about the format. The DECIMAL one uncovered that DuckDB packs values at four different physical widths depending on precision, narrowing i128 to i16/i32/i64/i128 based on whether precision fits in 4, 9, 18, or 38 digits. UNION turned out to reuse `StructTypeInfo` with a hidden tag field prepended, so the wire bytes are identical to STRUCT with a different `LogicalTypeId`.

From the outside, the result is a one-liner:

```sql
duckdb> INSTALL quack FROM core_nightly;
duckdb> LOAD quack;
duckdb> CREATE SECRET (TYPE quack, TOKEN 'bearer-...');
duckdb> SELECT * FROM quack_query(
          'quack:sqe.example.com:9494',
          'SELECT * FROM iceberg_catalog.default.events LIMIT 5');
```

The token there is a Polaris bearer. The DuckDB CLI does not know that. The DuckDB CLI thinks it is talking to another DuckDB instance and asks the same question it would ask any DuckDB. SQE translates it: parse SQL with sqlparser, plan with DataFusion, scan Iceberg through iceberg-rust, return Arrow batches encoded as DuckDB DataChunks. The catalog enforcement, OPA policies, OIDC token validation all happen inside SQE while the DuckDB CLI just sees rows.

That is the entire pitch for server mode. Every DuckDB-aware tool, every dbt-duckdb model, every `marimo` notebook, every `Evidence` dashboard, every SQL client that already knows how to attach to a DuckDB instance can talk to SQE without changing a single line of configuration. We did not write a new wire protocol. We adopted one that thousands of tools already understand.

## Client mode: SQE pulls from a remote DuckDB

The second wave was the inverse. The same codec, used as a client, lets SQE pull data from a remote DuckDB and treat it as a table.

```
DataFusion plan --sqe-quack-client--> remote DuckDB on :9495
```

`sqe-quack-client` is a thin wrapper around `reqwest::blocking::Client` plus our codec. `QuackClient::connect(uri, token)` does the handshake. `QuackClient::execute(sql)` runs `PrepareRequest`, drains `FetchRequest` until `needs_more_fetch` flips, and returns `Vec<RecordBatch>` plus the schema. The reverse arrow bridge (`data_chunk_to_record_batch`) does the same job as the forward one in the server, but for incoming DuckDB-encoded bytes.

The interesting question was how to expose this to SQL users. DuckDB has a built-in called `quack_query()` that takes a URI and a SQL string and returns rows. We made `sqe-server` register the same function name, with the same signature, as a DataFusion table-valued function. The implementation is small:

```rust
impl TableFunctionImpl for QuackQueryTvf {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let (uri, token, sql) = parse_args(exprs)?;
        let provider = QuackTableProvider::new(&uri, token.as_deref(), &sql)?;
        Ok(Arc::new(provider))
    }
}
```

`QuackTableProvider` eagerly runs the query at plan time, caches the batches, and exposes them through `MemTable` at scan time. Eager-fetch is the obvious limitation; large remote results pull into memory before DataFusion sees them. A streaming variant is a follow-up.

What the symmetry buys is composition. The DuckDB CLI calling SQE with `quack_query()` is just SQL. SQE can run that SQL, and inside that SQL there can be another `quack_query()` aimed at yet another DuckDB. The wire shows three hops.

```
outer DuckDB CLI --quack:9494--> sqe-server --quack:9495--> remote DuckDB
```

Live-verified with 1M rows. The full FetchRequest loop runs hundreds of DataChunk batches per query, then sees a final FetchResponse with the results list omitted entirely (DuckDB elides empty lists per `WriteListWithDefault`) and stops.

## The single-query federation

The actual reason we built client mode is on the next slide, so to speak. DataFusion does not distinguish between "Iceberg table" and "Quack TVF" at the planning layer. Both are `TableProvider`s. A single SELECT can mix them:

```sql
SELECT p.id, p.name AS person, r.color
FROM   "default".quack_demo p                       -- Iceberg / Polaris
JOIN   quack_query(
         'quack:remote-duckdb:9495',
         'remote-secret',
         'SELECT id, name AS color FROM colors'
       ) r                                          -- remote DuckDB
  ON   p.id = r.id;
```

DataFusion plans the join. The Iceberg scan reads Parquet from S3 through iceberg-rust. The Quack TVF round-trips Arrow batches over HTTP. Both feed into the same hash-join operator. The user does not know any of this; they wrote a SELECT.

We tested COUNT aggregation across the join, UNION ALL of an Iceberg side with a Quack side, CROSS JOIN with DECIMAL preserved end-to-end, filters that project from either side. All five shapes ran first try once the codec was sane.

That last clause is doing a lot of work, because the codec was not sane on the first run.

## What live testing surfaced that unit tests did not

Three bugs landed in `sqe-quack-wire` while we were testing the TVF against a real DuckDB sidecar. Two were the same class. One was specifically nasty.

The first: when a remote DuckDB returns zero rows, `PrepareResponse.results` (the list of DataChunks) gets omitted entirely on the wire. DuckDB writes the field with `WriteListWithDefault`. Our decoder called `expect_field(4)` unconditionally and tripped a `0x0004` vs `0x0005` mismatch the moment the field was absent. Same pattern hit `FetchResponse.results` at 100K-row queries: the terminal fetch sends an empty list which DuckDB elides, and we expected it. Both are one-line decode fixes once you know to look. Without live testing we would never have found them; every fixture we captured had non-empty data.

The second, which I will keep thinking about: NULL VARCHAR rows. DuckDB writes the underlying `string_t` 's inline payload at NULL positions rather than an empty string. Whatever happens to be sitting in the inline buffer goes on the wire. We saw a `0x80` byte on the wire for a NULL VARCHAR cell, which is not a valid UTF-8 leading byte, which made our `from_utf8` validation fail before we ever got to the validity-mask check. The fix is to skip the length-prefixed slot by position without trying to interpret the bytes when validity says the row is null.

The codec source code we ported from is correct C++. Empty strings are written for NULL positions in the DuckDB sources we read. But the actual bytes the server emits are not always empty strings. Live testing made the bytes visible. Captured fixtures would have surfaced this if we had captured the right query, but we did not. The 100,000 character `0x80` in a hex dump is more persuasive than a code review.

## What it cost

Eight MRs over a few days. The codec port was the biggest single chunk, but most MRs were small: scaffold the wire crate, add the message types, add the data chunk wrapper, add support for each parameterised type, build the server, build the client, register the TVF, fix the empty-results bug, fix the NULL-VARCHAR bug. Each one landed independently with a tight diff and a story to tell.

The protocol is now feature-complete on the codec side. DECIMAL, LIST, STRUCT, MAP, ARRAY, ENUM, UNION all round-trip end-to-end against real `duckdb 1.5.3`. Nested-type queries through the TVF work. Federated joins across Iceberg and Quack work. The remaining gaps are upstream (DataFusion's planner rejects some SQL syntax like `ENUM(...)` literals) or stylistic (streaming versus eager fetch).

There is a longer write-up about the type matrix in `docs/quack-datatype-matrix.md`. Every row marked ✅ has been verified against a real DuckDB CLI session.

## What it unlocks

The pitch is sharper than it sounds.

You can run SQE as the central query layer over Iceberg, with the policy enforcement and OIDC auth and lineage tracking and everything else that Iceberg-on-Rust brings. You can also expose a DuckDB-compatible endpoint on top of that, which means every DuckDB tool joins your platform for free. And in the same query, you can federate to a remote DuckDB instance that holds data Iceberg does not, which means your existing DuckDB workloads do not have to migrate before you can query across them.

That is a lot of work that we did not have to do, by adopting a protocol someone else designed. Most of the value lives in DuckDB's own ecosystem. We just made the protocol portable.
