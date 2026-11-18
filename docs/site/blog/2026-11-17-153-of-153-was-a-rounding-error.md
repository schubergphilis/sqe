---
title: "153 of 153 was a rounding error"
description: "Our Trino parity harness said SQE matched Trino on every query. It rounded numbers to two places and ignored column types. Read strictly, over both protocols and against Trino's own function list, SQE matches 137 of 153 queries over HTTP, 129 over Flight SQL, and 121 of 296 Trino functions. Here is what the lenient check hid, how we measure now, and the first fixes."
pubDate: "2026-11-17"
author: "Jacob Verhoeks"
tags:
  - "trino"
  - "compatibility"
  - "testing"
  - "flight-sql"
  - "iceberg"
---

SQE has a parity harness. It runs the same SQL on SQE and on a real Trino, both pointed at one Polaris catalog, so both engines read the same Iceberg files. Any difference in the answer is an engine difference. It reported 153 of 153.

The number was true and meant very little. The comparator rounded every number to two places, read numeric strings as floats, and ignored column types. `mod(5, 2)` returning `1.0` matched Trino's `1`. `length('abc')` returning an `integer` matched Trino's `bigint`. A BI tool that reads the type from the result header does not round anything.

We made the check strict. The score dropped to 137.

## What strict means

Two results now MATCH only when three things hold. The column types are the same. The values are the same after canonicalising both sides by Trino's type: decimals as decimals, doubles to nine significant digits, timestamps without trailing zeros. The row order is the same when the query has a top-level `ORDER BY`.

Anything else gets a verdict that says what broke. `TYPE` is the same values under a different type. `DIFF` is a different value or row count. `SQE-ERR` and `TRINO-ERR` mean one side failed. A statement both engines reject counts as parity, because error text is not part of the contract and we will not chase message wording.

The harness also sends `X-Trino-Client-Capabilities: PARAMETRIC_DATETIME`, as the Trino JDBC driver does. Without that header Trino reports `timestamp with time zone` with no precision. The precision gap was there all along. The old harness never asked the question that showed it.

## The second protocol

SQE speaks two protocols. BI tools use the Trino HTTP protocol through the Trino JDBC driver. Notebooks, dbt and ADBC clients use Flight SQL. The harness only spoke HTTP, so half the surface was never compared.

We added a Flight client over ADBC. Trino is still read over its own HTTP protocol, since that is the reference, and SQE is read over either protocol or both. A Flight result is Arrow, so the client maps the Arrow type to a Trino type name and encodes the values the way Trino's JSON does.

Flight scored 129, eight below HTTP. All eight were the same row: `row_number`, `rank`, `dense_rank`, `ntile` and `cardinality` came back as `UInt64`. DataFusion returns unsigned integers for counts and positions. Trino has no unsigned types. The HTTP encoder had always labelled a `UInt64` as `bigint`, so the HTTP harness never saw it. A Flight client got an unsigned column that no Trino-trained tool expects.

## A denominator we did not pick

Our compatibility matrix said about 96% of Trino SQL was covered. We wrote that matrix by hand, one row per function, and a row was green when SQE had a function of that name doing the same job. It counted names, not results.

The function suite now takes its list from Trino. It runs `SHOW FUNCTIONS` on Trino 484, keeps every scalar, aggregate and window signature whose types Iceberg can store, and calls each one on both engines with sample arguments of the declared types. Signatures over HyperLogLog, Geometry or IPADDRESS are listed but not counted, because no Iceberg table can hold those values. A signature the generator cannot build a call for counts as `UNTESTED`, against SQE, until the generator learns it. Nothing leaves the denominator because it is inconvenient.

A function is covered when every counted signature matches with real values. One that only matched on typed NULLs is reported apart, since `NULL = NULL` proves very little.

| Suite | Trino HTTP | Flight SQL |
|---|---|---|
| Queries | 137 of 153 | 129 of 153 |
| Iceberg DDL, DML and interop scenarios | 20 of 20 | 20 of 20 |
| Trino functions | 121 of 296 | 115 of 296 |

121 of 296 is 41%. Of the 175 functions missing over HTTP, 92 do not resolve on SQE at all. 29 fail on some signature, often because a cast uses Trino's type syntax (`array(double)`, `time(3)`) and the parser rejects it. 31 return a different value, 18 a different type. 5 the generator cannot call yet.

The Iceberg suite is the one that held. Each scenario runs the same DDL and DML on each engine's own table and diffs a read, or has one engine write a shared table and both read it. SQE-written merge-on-read deletes read back correctly in Trino, and Trino's files read back in SQE. The only fix there was in the scenario itself: it tried to downgrade a v3 table to v2, which Trino refuses as well. SQE creates v3 tables by default and Trino 484 creates v2. Which default SQE should have is a product decision, not a parity fix.

## Most of the gap is types

The query failures have few causes, and they are mostly about types. DataFusion types the literal `1` as `bigint`; Trino types it as `integer`. Eight query rows fail on that alone (`abs(-5)`, `greatest(size, 10)`, `CASE ... THEN 1`). `length`, `strpos` and `extract` return `integer` where Trino returns `bigint`. `mod`, `sign`, `ceil` and `floor` return `double` for an integer argument.

None of these is a wrong answer in the arithmetic sense. They are a different contract. A dashboard that formats `1.0` where it expected `1`, or a JDBC client that calls `getInt` on a column it was told is a `bigint`, breaks on the contract, not on the arithmetic.

## The first fixes

The first fix branch, in review as we write, takes three gaps at the result boundary, where a value leaves the engine.

Unsigned results become signed on Flight SQL: `UInt64` and `UInt32` go out as `Int64`, `UInt16` as `Int32`, `UInt8` as `Int16`. We convert at the Flight boundary and not in an analyzer rule, for one reason. DELETE and UPDATE read row positions back as `UInt64` inside the engine, and a plan-wide rewrite would have broken them. A value above `i64::MAX` now fails the query. The default Arrow cast would have turned it into a silent NULL, and a silent NULL is worse than an error.

NaN and infinity travel over HTTP as Trino sends them, the strings `"NaN"`, `"Infinity"` and `"-Infinity"`. SQE sent `null`. `asin(2.5)` was a DIFF for that reason only.

`CAST(json AS varchar)` returns Trino's text. A JSON string comes back without its quotes, a number or boolean as its text, and JSON `null` as SQL NULL. An array or object is an error, and NULL under `TRY_CAST`. SQE returned the JSON text, quotes included, under a `json` label. The rewrite runs before type coercion and only on casts the SQL planner wrote. A cast the engine builds for itself, such as a partial column mask over a JSON column, keeps the JSON text. Column masks must never fail on the data they hide.

## What comes next

Each remaining gap is a task in one OpenSpec change, worked in themed branches. Next come timestamp precision and zones, then function return types, Trino's type syntax, the value DIFFs one function family at a time, and the missing functions. Each missing function gets a DataFusion check first: we add a function ourselves only when upstream does not have it.

Integer literals go last, on their own. Typing `1` as `integer` changes every plan that mentions a number. It brings Trino's overflow at 2^31, and it can put a cast on the column side of a predicate. A cast there defeats Iceberg file pruning. It can also make a predicate fallible, and SQE then reads row-filtered tables through a slower guarded scan so that an error cannot leak a hidden row. That change ships with an access-control run and a TPC-H comparison before and after.

The target is 100% on every suite, over both protocols. The suite passes when `scripts/parity-test.sh --all --protocol both` exits 0. Until then, the [testing page](https://docs.getsqe.com/development/testing.html#trino-parity) carries the current numbers, and they are the strict ones.
