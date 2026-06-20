# properties_for linear-scans the metadata cache per table per query

- **ID:** properties-for-linear-scan
- **Pillar:** Performance
- **Severity:** Low
- **Status:** Open
- **Files:** crates/sqe-catalog/src/rest_catalog.rs:279-297

## Problem
`properties_for` iterates the entire metadata cache (bounded at 1000) with `ends_with` per key and clones the matched property map, once per table per query. This is real but on the order of microseconds at the 1000-entry bound, which is noise versus the HTTP round-trips.

## Proposed fix
Optional: a suffix-to-key index, if `max_capacity` is ever raised by orders of magnitude.

## Acceptance criteria
Revisit only if the cache bound grows.
