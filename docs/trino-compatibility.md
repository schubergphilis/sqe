# Trino SQL Compatibility Matrix

> Living document. Last updated: 2026-04-08.
> Rating: ✅ equivalent | ⚠️ partial/different semantics | ❌ missing | 🔧 SQE-specific

SQE aims to be a drop-in replacement for Trino in Iceberg-only environments.
This document maps every Trino SQL function and feature to its SQE equivalent,
noting semantic differences and gaps.

## Summary

| Category | Total | ✅ | ⚠️ | ❌ | Coverage |
|---|---|---|---|---|---|
| Scalar: String | — | — | — | — | —% |
| Scalar: Math | — | — | — | — | —% |
| Scalar: Date/Time | — | — | — | — | —% |
| Scalar: JSON | — | — | — | — | —% |
| Scalar: URL | — | — | — | — | —% |
| Scalar: Regex | — | — | — | — | —% |
| Scalar: Conditional | — | — | — | — | —% |
| Scalar: Conversion | — | — | — | — | —% |
| Aggregate | — | — | — | — | —% |
| Window | — | — | — | — | —% |
| DDL/DML | — | — | — | — | —% |
| Type System | — | — | — | — | —% |
| Iceberg-Specific | — | — | — | — | —% |

## How to Read This Document

Each section lists Trino functions with their SQE status:

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `concat(s1, s2, ...)` | `concat(s1, s2, ...)` | ✅ | Native DataFusion |
| `json_extract(json, path)` | — | ❌ | Use `json_object()` for construction |
| `year(date)` | `year(date)` | ✅ | Trino compat UDF in sqe-coordinator |

---

## Scalar Functions: String

_To be filled in Task 2_

## Scalar Functions: Math

_To be filled in Task 2_

## Scalar Functions: Date/Time

_To be filled in Task 3_

## Scalar Functions: JSON

_To be filled in Task 3_

## Scalar Functions: URL

_To be filled in Task 2_

## Scalar Functions: Regex

_To be filled in Task 2_

## Scalar Functions: Conditional

_To be filled in Task 2_

## Scalar Functions: Conversion / Type Cast

_To be filled in Task 2_

## Aggregate Functions

_To be filled in Task 3_

## Window Functions

_To be filled in Task 3_

## DDL / DML Statements

_To be filled in Task 4_

## Type System

_To be filled in Task 4_

## Iceberg-Specific SQL

_To be filled in Task 4_

## Operational Comparison

_To be filled in Task 12_
