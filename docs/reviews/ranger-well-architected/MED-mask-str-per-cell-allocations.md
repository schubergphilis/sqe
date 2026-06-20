# mask_str does two heap allocations per masked cell

- **ID:** mask-str-per-cell-allocations
- **Pillar:** Performance
- **Severity:** Medium
- **Status:** Open
- **Files:** crates/sqe-policy/src/mask_udf.rs:43-62,147-153

## Problem
`mask_str` does two heap allocations per masked cell: a `Vec<char>` collect plus a `String` collect. On a masked Utf8 column at SF10 (~120M rows) that is roughly 240M short-lived allocations on the scan hot path. The `Vec<char>` is gratuitous; it is only needed for a character count and index.

## Proposed fix
Use `String::with_capacity` plus a single `chars().enumerate()` pass. Skip the count when `show_last == 0`, which covers MASK and MASK_SHOW_FIRST_4.

## Acceptance criteria
A criterion bench shows at least 40% fewer allocations. An SF10 query over a masked Utf8 column shows no scan-stage regression versus the `benchmarks/results` baselines.
