---
title: "Half the bugs were in the benchmark"
description: "We pointed DuckDB, Polars and embedded SQE at the same Polaris tables and reran TPC-DS and TPCx-BB against Trino. SQE had six problems with memory and scans. Our data generator had more, and those were worse: they made wrong results look right. With the fixes, TPC-DS matches Trino on 99 of 99 queries."
pubDate: "2026-11-04"
author: "Jacob Verhoeks"
tags:
  - "benchmarks"
  - "performance"
  - "correctness"
  - "tpc-ds"
  - "iceberg"
  - "datafusion"
---

Two days ago we published [150M rows under a 4.5 GiB cap](./2026-11-02-150m-rows-under-a-4-5-gib-cap.md). SQE is our SQL engine for Apache Iceberg, built on Apache DataFusion. Since that post we did two more things. We pointed DuckDB, Polars and embedded SQE at the Iceberg tables in Polaris, so every engine reads the same snapshot through a catalog. And we reran TPC-DS and TPCx-BB at scale factor 1 against Trino, query by query.

Our goal for SQE is plain: fast and reliable. A benchmark helps with neither unless it is right itself.

The runs found problems on both sides. Six were in SQE, all about memory or scans. More were in our own data generator, and those were the worse kind. They did not make anything fail. They made wrong results look right.

## Part 1: what SQE got wrong

### Embedded SQE spilled where the server did not

With the new Polaris readers, `sqe-cli --embedded` attaches the Polaris catalog directly and runs the query in the CLI process. The window query on 150M rows at a 3 GB limit took 20.3 s and spilled 96 times, 3.4 GB in total. The server, with the same engine and the same data, did not spill at all.

We first suspected the Iceberg scan's memory reservations and measured them. They were not the cause. The cause was the memory pool. The coordinator has used a greedy pool with 20% headroom for unspillable memory for a while. Embedded mode still built DataFusion's `FairSpillPool`, which divides the pool, minus the unspillable bytes, equally over every consumer that can spill. The scan's fetch and decode buffers cannot spill, so every buffer the scan held shrank the share of every aggregate. Switching the server to the fair pool reproduced it: 88 spills where greedy had none.

| Run, q3 at 3 GB | Before | After |
|---|---|---|
| embedded, 150M rows on Polaris | 20.3 s, 96 spills | 14.2 s, 0 spills |
| embedded, local `read_parquet` | 5.8 s, 24 spills | 5.2 s, 0 spills |

Embedded mode now uses the server's pool. Two smaller gaps came with it. The server caps each sort partition at an equal share of the spillable memory, so the first partitions to start cannot take everything and fail a sibling that has not started yet. Embedded mode had no per-query pool to hold that share, so the share now lives in the pool itself. A full `ORDER BY` of 150M rows at 3 GB completes in 13.8 s. And embedded writes ran under the server's default 256 MB query cap instead of the CLI's `--memory-limit`.

### The TopK filter that never changed

For `ORDER BY x DESC LIMIT 10`, DataFusion pushes a dynamic filter from the TopK operator into the scan. Once the TopK holds ten rows, the scan can skip every row below the tenth. In SQE the filter reached the scan and then never changed.

Our parallel-scan rules re-run DataFusion's `EnsureRequirements` pass, and that pass rebuilds the TopK sort. A rebuilt sort creates a new filter. The scan kept the old one, which no operator updated, so it stayed at its first generation forever. It pruned nothing, and the scan waited its full 100 ms per partition for the filter to become ready. Our star-schema join reordering orphaned join filters the same way.

SQE now settles the filters after planning. It drops every scan filter that no operator produces and runs filter pushdown again, so the live filter arrives. The scan does not wait for TopK filters. It reads their current bound on every batch.

There was a second cause. For row-group pruning, SQE turns the dynamic filters into an Iceberg predicate. The bound for a descending sort is `x IS NULL OR x > t`, because nulls sort first. `IS NULL` did not convert, so the whole `OR` was dropped.

Measured on the 150M-row table on Polaris, `SELECT order_id, <key> FROM sales ORDER BY <key> ... LIMIT 10`:

| | Before | After |
|---|---|---|
| scan time spent waiting for the filter | 837-851 ms | 8 ns |
| rows the scan hands to the TopK (`price`) | 150.0M | 90.1K |
| rows decoded, `customer_id DESC` | 150.0M | 59-60M |
| wall time, `customer_id DESC` | 3.67-3.85 s | 2.69-2.76 s |

One early build of the fix ran `ORDER BY price ASC` in 1.2 s. That number was wrong, and we threw it away. Parquet's min and max statistics leave out NaN, and DataFusion sorts NaN above every number. Pruning `price > t` on a float column can therefore drop the NaN rows that belong in the answer. Range comparisons on floats no longer reach row-group pruning. Float sort keys keep the shorter wait and the smaller TopK input, and lose the decode savings. Correct beats fast.

### Memory caps that did not cap

`EXPLAIN ANALYZE` ran the query on the plain session context. Every normal query runs inside a per-query memory pool with the `max_query_memory` cap, the sort share and the join fallback, which retries a hash join as a sort-merge join when the hash table does not fit. `EXPLAIN ANALYZE` had none of them. A full sort of 150M rows finished as a query in 18.2 s and failed as `EXPLAIN ANALYZE`. Anyone could also go past `max_query_memory` by putting `EXPLAIN ANALYZE` in front of a query. It now runs under the same pool and the same fallback, and reports the plan that actually produced the rows.

`spill_compression` was dead configuration. The server logged it and never passed it to DataFusion, so every spill was written uncompressed. With lz4 applied, the peak spill directory of that 150M-row sort went from 2.9 GB to 1.3 GB, with no measurable change in wall time. A new `spill_max_bytes` setting caps the spill disk, and its error names the setting.

### Two small ones

An embedded `ATTACH ... TYPE iceberg_rest` ignored the CLI's `SQE_STORAGE__S3_*` settings and failed with "region is missing". And `sqe-cli -f json` printed SQL NULL as an empty string, so a script could not tell NULL from `''`.

## Part 2: what the benchmark got wrong

We generate TPC-DS and TPCx-BB data with our own Rust generator in `sqe-bench`, so a benchmark run does not need the dsdgen toolchain. The Trino comparison runs every query on both engines over the same tables. When both agree, we call it a match. If the data is wrong, both engines agree on the wrong question.

### Keys with nothing to join to

TPCx-BB's click and review tables drew item keys from `1..SF x 18,000`. The TPC-DS item table grows linearly only up to SF1. Above it, dsdgen has 102,000 items at SF10, not 180,000. So 43% of the click keys at SF10, and 89% at SF100, had no item row. The inner joins dropped those rows without a word, and every query ran on less data than it should. With the fix, the q01 join at SF10 is about 1.8 times larger. Any SF10 TPCx-BB number from before the fix measured a smaller problem.

The same tables drew web page keys from `SF x 2,040` while `web_page` has 60 rows at SF1, and click dates that ran past the end of `date_dim` from SF2. Both now come from the TPC-DS generator's own row counts.

### Money as floats

The generator wrote all 80 decimal columns of TPC-DS as `Float64`. The spec says `DECIMAL(7,2)` for money and `DECIMAL(5,2)` for tax rates. The type matters for both goals. Decimal aggregation costs different work than float aggregation, and a decimal sum is exact where a float sum is not. The columns now carry the spec types, and a test parses the spec DDL and checks every generated table against it. Prices are rounded to cents before the extended amounts are derived, so `ext_sales_price = sales_price * quantity` holds exactly. `ext_tax` was the tax on one unit. It is now the tax on the line.

### Dimensions that grew too fast

Above SF1, dsdgen grows the dimension tables much slower than the facts. We grew them linearly. At SF10 we wrote 1M customers where the spec has 500,000, and 600 web pages where it has 200. At SF100 we wrote 10M customers where it has 2M. The generator now interpolates between the official counts, which we read from Trino's `tpcds` connector with `count(*)` at each scale.

Some key draws used exclusive ranges and never reached their last value. Birth months came from `1..12`, so no customer was born in December, and q18's `c_birth_month IN (1, 6, 8, 9, 12, 2)` never matched one.

### A query that returned nothing, twice

TPC-DS q64 returned no rows on SQE and no rows on Trino. Both engines agreed. The official SF1 answer has 2 rows. Our comparison checks every empty-on-both result against a manifest of canonical row counts for exactly this case, and it flagged q64 as a vacuous bug on every run.

No filter in the query was wrong. q64 needs a rare coincidence: an item priced 65 to 74 in one of six colours, sold and returned at the same store in both 1999 and 2000, also sold through the catalog for more than twice its refunds, to a buyer whose marital status has changed since. Random data gives about five such pairs at SF1. Our seed gave none. The generator now plants one, the same way it already planted the coincidences for q17, q24, q25 and q29.

### A comparator that said 100 equals 1

The result comparator trimmed trailing zeros, so that `12.50` would equal `12.5`. It trimmed them from integers too, so `100` matched `1`. It now trims only after a decimal point.

### Row counts are not answers

The Trino comparison classified each query by its row count. A wrong sum over the right number of rows was a match. The next change compares the values. It sorts the rows of both engines and compares them cell by cell. Exact numbers are compared exactly after rounding both sides to the smaller scale. DataFusion returns `avg` over a `DECIMAL(7,2)` as `DECIMAL(11,6)` where Trino keeps `DECIMAL(7,2)`, so 12.345000 and 12.35 are the same answer, and 12.34 and 12.35 are not. Floats are compared within a relative tolerance of one in a million, because the order of a parallel sum changes the last bits.

## Where it stands

On the fixed data, at SF1 on one laptop:

| Suite | Matches Trino | Vacuous bugs | SQE total | Trino total | Median speedup |
|---|---:|---:|---:|---:|---:|
| TPC-DS, 99 queries | 99/99 | 0 | 20.3 s | 73.8 s | 3.2x |
| TPCx-BB, 10 queries | 10/10 | 0 | 16.0 s | 66.0 s | 3.9x |

A single run on an Apple M3 Pro laptop, with Trino 481 in Docker on the same machine. Treat the times as a sanity check, not a result.

Every result recorded on data generated before these changes measures a different dataset, and we do not compare against it. The next SF10 TPCx-BB run will be slower on q01. That is the data catching up with the spec, not the engine slowing down.

Speed claims rest on correct answers, and correct answers rest on correct data. We now check the generated data against the spec DDL and the official row counts, every empty result against a canonical answer, and, with the next change, every value. The SSB and TPC-E generators still write money as `Float64`. They are next, followed by the SF10 rerun.

## Update: the SF10 rerun

Later the same day we ran four suites at SF10 against Trino, on data from the fixed generator. Same laptop, one run per suite. SQE runs as a native process. Trino 481 runs in Docker with a 10 GB container cap, because without a cap the Docker VM killed it earlier that day. Both read the same Iceberg tables in RustFS.

| Suite | Matches Trino | SQE total | Trino total | Ratio |
|---|---:|---:|---:|---:|
| TPC-H, 22 queries | 22/22 | 53.1 s | 69.0 s | 1.3x |
| SSB, 13 queries | 13/13 | 27.9 s | 11.8 s | 0.4x |
| TPC-DS, 99 queries | 91/99 | 74.9 s | 131.6 s | 1.8x |
| TPCx-BB, 10 queries | 9/10 | 147.5 s | 627.9 s | 4.3x |

The ratio is Trino's total over SQE's. SQE is ahead on three suites and behind on SSB.

SSB lost ground, and that is SQE's. In July, on a dedicated 8-core host, SSB at SF10 took 20.4 s on SQE and 21.4 s on Trino. On the laptop TPC-H got faster, from 66.5 s to 53.1 s, but SSB went from 20.4 s to 27.9 s. Nine of the ten queries in flights 2 to 4 take 1.7 to 4.8 s each, where Trino needs 0.6 to 2.2 s. The cause is not traced yet. SSB is the next trace.

The value comparison found eight TPC-DS queries that do not match. Six are about decimal digits, not wrong rows:

- q06, q35 and q44 use `avg` over a decimal. Trino keeps the input's scale and rounds. SQE returns more digits. In q06 and q44 that decides which rows sit on the boundary of a comparison or a rank, so one count differs by one and q44 returns 10 rows where Trino returns 14.
- In q58 and q78 Trino's quotient carries fewer digits than SQE's: 10.512600 against 10.5126333.
- In q61 it is the other way round. SQE stops at six digits, Trino carries more.

Whether SQE should follow Trino's scale for `avg` is still open. The digits SQE returns are not wrong. They are not the digits Trino returns.

The other two, q27 and q75, are new at SF10 and not explained yet. Both end in `ORDER BY ... LIMIT 100`. Before we call them ties at the limit, we check that each `ORDER BY` is a total order. TPCx-BB q02 returns no rows on both engines. At SF10 no item class has a return rate above the query's 10% threshold, the same kind of data problem as q64, and it goes to the generator next.

Every number in this table comes from data that matches the spec, and every answer is compared value by value. The published SF10 numbers from July were measured on the old generator, and we do not compare against them except where the data did not change.
