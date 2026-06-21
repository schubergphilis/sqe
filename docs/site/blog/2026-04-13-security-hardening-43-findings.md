---
title: "43 Findings, Zero Deferred: A Production Security Audit of a Rust SQL Engine"
description: "We ran a full production sign-off audit against SQE and found 43 issues across security, runtime safety, logic bugs, and code quality. Then we fixed all of them in one session."
pubDate: "2026-04-13"
author: "Jacob Verhoeks"
tags: ["security", "rust", "audit", "production-readiness"]
---

We asked a hard question: would you approve this codebase for production at a bank?

The answer was no. Not because the engine was broken. It ran 221 out of 222 benchmark queries correctly, beat Trino by 2.5x to 8.8x, and had 1,334 passing unit tests. The answer was no because there were 43 issues that a regulated financial services deployment cannot tolerate.

So we fixed all 43.

## What the audit found

The audit covered six categories, ordered by severity.

**Security (13 findings).** The session context cache was keyed by username. Two users with the same username from different identity providers would share a Polaris catalog session. Cross-user data access. Critical.

Eight Flight SQL metadata endpoints had no authentication check. Any network client could enumerate catalog names without credentials. The Trino cancel-query endpoint let any client cancel any other user's query. OIDC error bodies were forwarded verbatim to clients, enabling user enumeration.

**Runtime correctness (12 findings).** Sixteen call sites in the date extraction functions called `.unwrap()` on `date32_to_datetime()`, which returns `Option`. A query like `SELECT year(extreme_date_column)` would panic and kill the coordinator. Three more `.expect()` calls in production paths. Two unguarded `[0]` array indexes in MERGE and DELETE handlers.

**Logic bugs (7 findings).** The OPA policy cache key did not include user roles. A user whose analyst role was revoked would continue getting analyst-level row filters until the cache expired. The manifest cache had no TTL backstop. A corrupted manifest overwritten at the same S3 path would be cached forever.

**Dead code (2 findings).** A `prom_metrics` field threaded through the entire scan path but never read. A `should_distribute()` function with zero callers, hidden behind `#[allow(dead_code)]`.

**Code quality (6 findings).** Two parallel authentication architectures running simultaneously with no shared token cache. S3 credential assembly duplicated in three places. `eprintln!` instead of `tracing` on audit failure paths.

**Rust-specific (3 findings).** Blocking `std::fs` I/O on Tokio worker threads. The `checksum()` UDF used `DefaultHasher`, which is non-deterministic across processes. The `adaptive_sort` module used `std::sync::Mutex` inside a single-threaded closure, risking poison masking.

## What we fixed

Every finding. Two Critical, 13 High, 21 Medium, 7 Low. 33 files changed, +1,272 / -372 lines. Zero deferred.

The critical fixes took the most thought. The session cache key changed from `username` to `username:sha256(token)[..16]`. This required switching from moka's simple `get`/`insert` to `try_get_with` for atomic cache population, which eliminated the TOCTOU race condition as a bonus. The 16 date `.unwrap()` calls each needed individual attention because the containing functions had different error handling patterns.

The adaptive sort default was the last piece. We initially set `sort_mode = partition_only` (safest: never sort non-partition columns). But that broke every integration test that uses ORDER BY for result verification. The right answer is `adaptive`: sort normally when memory is available, strip non-partition sorts under pressure. Correct for small data. Safe for large data. Never crashes.

## The numbers

Before the hardening:

| Metric | Value |
|---|---|
| Unit tests | 1,334 pass |
| Integration tests | 52/60 pass (8 failed from partition_only default) |
| Security audit | 43 open findings |
| Panic paths in production code | 22+ |
| Unauthenticated endpoints | 9 |

After:

| Metric | Value |
|---|---|
| Unit tests | 1,334 pass |
| Integration tests | 60/60 pass |
| Security audit | 0 open findings |
| Panic paths in production code | 0 (all converted to Result) |
| Unauthenticated endpoints | 0 |

## Three lessons

**Audits find what tests miss.** The session cache cross-user bug is invisible in unit tests because tests use a single user. The OPA role cache gap only matters when roles change mid-session. The 16 date unwraps only panic on extreme values that no generated test data contains. You need a human reviewer thinking adversarially.

**Fix everything or fix nothing.** Deferring 7 Low findings sounds reasonable. But Low findings compound. The `DefaultHasher` in `token_fingerprint()` is Low until you add a distributed cache that depends on fingerprint stability. The audit logger mutex poison is Low until it drops a record during an incident investigation. We fixed all 43 because the cost of fixing was hours. The cost of not fixing is uncertainty.

**Adaptive defaults beat conservative defaults.** `partition_only` is the safest sort mode. It is also wrong for every developer running integration tests and every dbt model that uses ORDER BY. The right default is the one that works correctly in the common case and degrades gracefully in the extreme case. Adaptive sort does both.

The full audit is in `docs/issues.md`. The code is in MR !61.
