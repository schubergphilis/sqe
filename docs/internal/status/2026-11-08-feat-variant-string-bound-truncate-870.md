# 2026-11-08 feat/variant-string-bound-truncate-870

Part of #870: string variant manifest bounds are truncated, not dropped.

## What changed

- Vendor `variant_shred.rs` (`SQE PATCH (v3-shredded-variant)`): replaces
  `MAX_STRING_BOUND = 64` (a longer string dropped the path's bound) with
  Iceberg Java's `truncate(16)`:
  - The lower bound is the first 16 characters (`truncate_lower`).
  - The upper bound is cut to 16 characters with its last character
    incremented (`truncate_upper`, as `UnicodeUtil.truncateStringMax` does).
    U+10FFFF carries into the character before it, and the surrogates are
    skipped (U+D7FF -> U+E000).
  - A string of only U+10FFFF has no upper bound, so the path gets neither
    bound.
  - Truncation counts characters, not bytes. The in-memory min/max keep
    the full strings; the cut happens only at encode time.
- The reader (`sqe-catalog/src/variant_prune.rs`) only does range checks
  (`=`, `<`, `<=`, `>`, `>=`), so a prefix lower and a rounded-up upper
  bound stay safe there.
- Matrix: the "string over 64 bytes" caveat is dropped from
  `sqe:shredded-variant:v3`, and the new test is added as evidence.
  `cargo xtask matrix-report --print-only` -> 158/162.

## Tests

- Vendor `cargo test -p iceberg --lib variant_shred` -> 8 passed. The new
  test `long_strings_are_truncated_like_iceberg_java` covers multi-byte
  characters, the carry, the surrogate skip and the no-upper case.
  `doubles_long_strings_and_quoted_keys_get_no_bound` became
  `doubles_and_quoted_keys_get_no_bound`.
- Integration `sqe_prunes_on_truncated_string_variant_bounds` uses two
  files whose strings share 16-character prefixes:
  - an equality filter on either file -> 1/2 files read;
  - a value inside the truncated range that no row has -> 1/2 files read, 0 rows;
  - `> 'customer-0000001'` -> 1/2;
  - a value outside both ranges -> 0/2.
  Spark 4.1 + Iceberg 1.12 return the same rows with these bounds.
- Gate: `make test-integration FILTER=variant_spark_interop` with the Spark
  container up (`scripts/start-spark-interop.sh`) -> 8 passed, none skipped.
  Without the container every test there skips and still reports passed.

## Left open (#870)

- Typed read of shredded variants without unshredding.
- Variant manifest bounds for nested variants (`VariantPrune` resolves
  top-level columns only), and for double paths.

**NEXT:** #812 (b) `system.runtime.query_history` over the audit JSONL, or
the refactors #637 / #636 / #589.
