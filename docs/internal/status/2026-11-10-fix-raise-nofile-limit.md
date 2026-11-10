**STATUS 2026-11-10: `sqe-server` raises RLIMIT_NOFILE to the hard limit at startup (`fix/raise-nofile-limit`).**

Found while qualifying #788 at SF100: `row_number() OVER (ORDER BY l_comment)` over all of lineitem spilled 23 GB in 818 files and failed with EMFILE after 86 s, because the coordinator ran with a 1024 soft limit (hard 1048576). Only `scripts/benchmark-test.sh` raised the limit before; a systemd unit or a plain shell start did not. The rerun at 65536 held 1678 descriptors. Clippy (`--no-default-features --bin sqe-server -D warnings`) passes; `hakari generate --diff` is empty.

Limits: no automated test (the limit is process-wide); the warn threshold of 8192 is a judgement call, not a computed bound. Spill fan-in is not capped by this change: a sort with more runs than the hard limit still fails.

**NEXT:** consider capping merge fan-in from the descriptor budget (`with_max_spill_merge_fan_in`) so large sorts degrade to multi-pass merges instead of EMFILE.
