---
title: "Six groups where the spec allows four"
description: "TPC-H q01 returned six (returnflag, linestatus) combinations on our generated data. The spec defines exactly four. Both SQE and Trino agreed on the wrong answer, because both read the same broken tables. The bug class behind it: fields the spec derives from other fields, drawn instead as independent uniform random. Five defects in TPC-H, one in SSB, all the same shape."
pubDate: "2026-06-13"
author: "Jacob Verhoeks"
tags:
  - "benchmarks"
  - "testing"
  - "duckdb"
  - "correctness"
  - "tpc-h"
---

*June 13, 2026*

q01 is the first query in TPC-H. The pricing summary report. It groups lineitem by two flag columns, `l_returnflag` and `l_linestatus`, and sums up quantities and revenue. Every TPC-H result ever published returns exactly four group rows. The spec defines four valid combinations and no more.

Our generated data returned six.

The compare harness never flagged it. SQE returned six groups, Trino returned six groups, the rows matched value for value, the harness printed Match. Two engines agreeing perfectly on a result the spec says cannot exist.

## A different failure mode on the same data path

Yesterday's post was about vocabulary. County names that did not exist, item colors the queries never asked for, a warehouse count that truncated to zero. DuckDB's official generators caught all of it by producing spec-conformant data we could diff against, with no query engine in the loop.

The oracle is the same today. DuckDB ships the `tpch` extension, the official dbgen, and `CALL dbgen(sf=1)` produces the canonical data. We diff our parquet against DuckDB's, engine-free. The setup is identical to yesterday.

The bug is not. Yesterday's defects were wrong *values*. Today's are wrong *correlations*, and they hide better.

## The bug class

Here is the shape, stated once. TPC-H does not draw every column independently. Several columns are *derived* from other columns by fixed rules in the spec. Our generator drew them as independent uniform random values instead.

That single mistake, repeated across five columns, produced five defects. They look unrelated until you see the pattern.

`l_returnflag` and `l_linestatus` are the clearest case. The spec does not roll a die for them. It derives them from the row's dates against a fixed cutoff, 1995-06-17:

- `l_linestatus` is `'O'` (open) if `l_shipdate` is after the cutoff, else `'F'` (fulfilled).
- `l_returnflag` is `'N'` (not returned) if `l_receiptdate` is after the cutoff, else `'R'` or `'A'`.

The receipt date always follows the ship date. An order ships before it arrives. So if a line is open, its ship date is past the cutoff, which means its receipt date is past the cutoff too, which forces `l_returnflag` to `'N'`. An open line can never be a returned line. The combinations `(A, O)` and `(R, O)` are impossible by construction.

Four combinations remain: `(N, O)`, `(R, F)`, `(A, F)`, `(N, F)`. Four group rows. The number every published q01 returns.

Our generator drew both flags by uniform random choice, untethered from the dates. So it happily emitted `(A, O)` and `(R, O)` rows. Six groups. The two impossible ones carried real quantities and real revenue, and both engines summed them up and agreed.

## Four more of the same

Once we knew what to look for, the same draw-it-random pattern turned up four more times.

| Defect | What we drew | What the spec derives it from | What it broke |
|---|---|---|---|
| Returnflag / linestatus | Both flags uniform random | The 1995-06-17 cutoff applied to ship and receipt dates | q01: six groups instead of four |
| Date chain | Ship, commit, receipt each independent random | Receipt follows ship follows commit, all offset from the order date | Receipts landing before their own ship date |
| Extended price | Price with the discount already baked in | Quantity times part price, discount applied separately in revenue math | Double-applied discount in every revenue sum |
| Retail price | Roughly $90,000 | A formula yielding the spec's ~$900 to $2,100 | Every price-bounded predicate, off by 100x |
| Lineitem to supplier | Suppkey off by one against partsupp | The partsupp association rule | 25% of (partkey, suppkey) pairs missing from partsupp |

The last one is the quiet killer. q09 and q11 join lineitem to partsupp on the `(partkey, suppkey)` pair. When a quarter of the lineitem pairs have no match in partsupp, the join silently drops a quarter of the rows. No error. No warning. A smaller, plausible-looking result that both engines compute identically and the harness calls Match.

The retail price defect is the funny one. Parts that should cost a few hundred dollars cost ninety thousand. Any query with a price band returns the wrong set, and the wrong set is consistent across engines, so it passes. A hundred-times error in plain sight, invisible to a differential test.

## The fix

The fix is to stop rolling dice and follow the spec. We derive a bounded order date first. The commit, ship, and receipt dates chain off it with the spec's offsets, so receipt always follows ship. The two flags derive from the 1995-06-17 cutoff. Extended price drops the discount. Retail price uses the spec formula. The supplier association matches partsupp.

The SSB generator had the same inert pattern hiding in it: commit date drawn independent of order date, the date-chain defect under a different name. We fixed it in the same pass.

After the fix, DuckDB and our data agree where it counts. q01 returns four groups, with a distribution within about 1% of official dbgen. Zero lineitem pairs miss partsupp. Receipt is always after ship. The impossible combinations are gone because the generator can no longer construct them.

## What independent random draws cost you

The lesson from yesterday holds: two engines reading the same broken data agree perfectly, so a passing differential benchmark is not a correctness proof. Match meant "both engines agree", never "the answer is right".

The new edge today is narrower and sharper. A column profile would have passed every one of these defects. The returnflags were valid characters. The dates were valid dates. The prices were positive decimals. Every value, in isolation, looked fine. What was broken was the *relationship* between columns, and no per-column check sees a relationship.

The spec encodes those relationships on purpose. q01 leans on the date-to-flag derivation. q09 and q11 lean on the lineitem-to-partsupp association. The queries are designed to exercise structure, and a generator that draws independently destroys exactly the structure the queries depend on.

When you write a benchmark generator, the dangerous fields are not the random ones. They are the ones that look random but are not.
