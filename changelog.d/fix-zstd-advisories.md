### Security

- zstd runs the 0.14.3 code on zstd-safe 8.2.0, which fixes RUSTSEC-2026-0391 and RUSTSEC-2026-0395 to 0406

    arrow/parquet 59, DataFusion 55 and apache-avro require zstd ^0.13. That
    line pulled in zstd-safe 7.3.0, which carries the unsoundness advisories
    (use-after-free, double free, uninitialized memory), and zstd 0.13.3
    carried the reused-context leak. `vendor/zstd` is zstd 0.14.3 as
    published, labeled 0.13.4 and patched in with `[patch.crates-io]`. 0.14
    only adds API over 0.13, so the callers build unchanged. cargo-audit
    still matches the 0.13.4 label against RUSTSEC-2026-0391; that one ignore
    is recorded with a review date. Drop the patch when DataFusion 56 moves
    to arrow 60.
