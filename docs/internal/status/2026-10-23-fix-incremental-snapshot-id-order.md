**STATUS 2026-10-23: `FOR INCREMENTAL BETWEEN SNAPSHOT` orders the pair by the parent chain, not by id (`fix/incremental-snapshot-id-order`).**

Found by the Ranger AC suite: `access_control_e2e::unknown_tag_state_denies` failed with `FOR INCREMENTAL BETWEEN SNAPSHOT 5121633547561628585 AND SNAPSHOT 2296073137140798942: start must be older than end`. The start snapshot was the older one; it just drew the larger random id.

Cause: `extract_incremental_spec` (`crates/sqe-sql/src/time_travel.rs`) and `resolve_range` (`crates/sqe-catalog/src/incremental_scan.rs`) both rejected `start > end` numerically. Iceberg snapshot ids are random 63-bit values, so every incremental read had about a one-in-two chance of being refused.

Fix: both numeric checks are gone. `resolve_range` already walks the end snapshot's parent chain and refuses a start that is not an ancestor ("is not an ancestor"), which covers a truly descending pair.

Gate:
- `cargo test -p sqe-sql -p sqe-catalog --lib incremental`: 17 + 7 passed, including the new `resolve_range_orders_by_parent_chain_not_by_id` (ids 900 -> 500 -> 100) and `incremental_accepts_a_numerically_descending_pair`.
- `make test-integration FILTER=incremental`: 3 passed.

The integration tests cannot pin a snapshot id, so the unit test is the deterministic proof; the AC test above was the intermittent one.

**NEXT:** unchanged
