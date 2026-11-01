**STATUS 2026-11-01: vendored iceberg-rust, backport round 3 (`fix/iceberg-backports-round3`).**

## What changed

Seven upstream fixes in `vendor/iceberg-rust`; the table is in its `README.md`
under "Round 3".

- Applied as is: apache #3539 (NaN in writer min/max), #3420 (partition name check for
  a dropped source), RW #269 (unpartitioned spec id on written files).
- Applied with a hunk ported by hand: apache #3437 (test import only), #3144 (`IN`
  pruning; keeps this tree's fallible `bytes_to_datum`), RW #264 (non-primitive bounds;
  keeps the shredded-variant branch first).
- Ported by intent: apache #3432. `Datum`'s `Display` rendered a dropped-source
  (`Unknown`) or promoted (`Long` type, `Int` literal) partition value through
  `unreachable!()`, so a commit that removed such a file panicked while writing the
  summary. `SnapshotProducer::collect_file` now picks the newest schema that still holds
  the spec's source columns for removed files. The upstream test targets a
  `transaction/snapshot/mod.rs` this tree does not have, so a `Datum` display test in
  `spec/values/tests.rs` replaces it.
- Two upstream tests needed this tree's API: the `variant_*_bounds` `DataFile` fields
  and the three-argument `add_partition_field`.

## Deferred

- apache #3277: data-file opens spawned on the IO runtime, with abort on drop. It fixes
  an HTTP/2 window stall; 5 of its 8 pipeline hunks collide with SQE's pipeline patches.
- apache #3516, #3454 and RW #263: perf. They need bench numbers, not just tests.
- RW #265: deleting data files referenced twice (duplicate appends by external writers).
  It rewrites the commit/summary path that carries SQE's own patches. Until then such a
  delete fails with "Cannot delete files referenced multiple times", which is safe.

## Verified

- `cargo test -p iceberg --lib` in `vendor/iceberg-rust`: 2169 passed.
- `cargo check-server` and `cargo test -p sqe-catalog --lib`.
- `make test-integration`: see the MR.

**NEXT:** the deferred perf MR (#3277 port, #3516, #3454, RW #263) measured with the
engine bench; then RW #265 on its own.
