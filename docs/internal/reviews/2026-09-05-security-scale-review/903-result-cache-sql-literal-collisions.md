# 903: Result-cache normalization changes SQL literal semantics

- Area: Result-cache authorization and correctness
- Priority: P1
- Order within area: 3
- Scope: security and large-data execution
- Severity: high
- Category: reliability
- Effort: S
- Status: fixed (branch `fix/security-scale-review-2026-09-05`, 2026-09-05)
- Issue: #648
- Confidence: high in source-level defect; deployment impact depends on stated prerequisites

## Problem

`ResultCache::cache_key` at `crates/sqe-coordinator/src/query_cache.rs:101` splits all whitespace and uppercases the entire SQL, including string literals and quoted identifiers:

```rust
let normalized: String = sql
    .split_whitespace()
    .collect::<Vec<_>>()
    .join(" ")
    .to_uppercase();
```

Consequently `SELECT 'secret'` and `SELECT 'SECRET'` share a key; so do `SELECT 'a  b'` and `SELECT 'a b'`. The key is used both on lookup and insertion.

## Why it matters

A warmed cache returns the result of a different query. Predicates on case-sensitive identifiers/values can return the wrong subset of business data. These are deterministic semantic collisions, not SHA-256 collisions. Table-access revalidation cannot detect them.

## Proposed change

Hash exact SQL bytes as the safe initial fix. If normalization is required later, use a parser-aware representation that preserves literal values and quoted identifier semantics.

## Validation and acceptance criteria

The offline reproduction extracts and runs the current Rust normalization expression and confirms both collisions. Add ResultCache tests that store one query and require a miss for each semantically different query; include quoted identifiers and comments.

## Related

Not covered by 406 (prepared statement substitution) or 701 (Trino pagination cache).

## Resolution (2026-09-05)

`ResultCache::cache_key` (`crates/sqe-coordinator/src/query_cache.rs`) hashes the length-prefixed user scope and the exact SQL bytes; whitespace folding and uppercasing are removed. Tests `cache_key_preserves_whitespace`, `cache_key_preserves_case` and `different_sql_literals_never_reuse_cached_results` cover string literals, whitespace inside literals and quoted identifiers.
