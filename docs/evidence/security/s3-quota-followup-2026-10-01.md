# S3 spill quota follow-up — 2026-10-01

Issue #796, residual of #382. Base: main `19cdbfb37d9c8b6972a8f0b0ea34bbf5406a0579`.

## Reproduction

The prior #382 fix atomically reserved 64 KiB per open writer, but `QuotaHold::settle` increased the byte counter to the full serialized segment size only after PUT, without checking the cap. A 64 KiB total budget therefore admitted and published a roughly 256 KiB IPC segment. Multiple writers could also publish more than the aggregate cap even though their small initial reservations were admitted safely.

An in-memory ObjectStore test with 32,768 int64 values and a 64 KiB cap failed on unmodified production code: `finish` succeeded when `SpillQuotaExceeded` was expected. The baseline log is `/tmp/sqe-s3-quota-baseline.log` on the review workstation. No external S3 or unbounded workload was used.

## Fix

Before PUT, atomically reconcile each writer's reservation to the actual serialized size. Growth consumes only the delta against the same maximum byte counter; shrinking refunds the unused estimate. A rejected growth publishes no object, and dropping the writer refunds its original reservation and object slot. Failed/cancelled publication refunds the complete resized reservation. Successful publication retains exactly the already-reserved byte count.

Creation now uses checked arithmetic for bytes and object slots rather than accepting a saturated counter at u64::MAX. No object key, format, lifecycle tagging or successful small-segment behavior changes.

## Validation

- Baseline oversized-segment regression failed as expected before the fix.
- `cargo test -p sqe-spill --lib`: 47 passed.
- `cargo clippy -p sqe-spill --tests -- -D warnings`: passed.
- New tests cover oversize refusal before upload; multiple open writers finishing without exceeding published-byte quota; eight synchronized threads competing to enlarge their reservations; full refund of enlarged holds/publication failures; and byte/object arithmetic overflow.
- `git diff --check`: passed.

## Scope

This enforces the process-local published-object byte quota, including in-flight reservations. IPC still buffers in memory before the serialized-size check. Process counters still reset on restart, and an object-store write that remotely succeeds but locally returns an error/cancellation can leave an orphan for the configured lifecycle rule. Those pre-existing limitations remain tracked by #382; this MR closes only the demonstrated byte-cap bypass in #796.
