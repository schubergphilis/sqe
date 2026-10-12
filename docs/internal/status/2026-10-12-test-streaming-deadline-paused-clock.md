**STATUS 2026-10-12: `query_deadline_fires_while_batches_still_flow` runs on a paused clock (`test/streaming-deadline-paused-clock`).**

The test failed on !1407's CI job with 0 batches before the 500 ms deadline: a starved runner woke the 100 ms batch timer and the deadline together. It is the last wall-clock test in `streaming.rs`; a09a3ac7 already moved its two idle-extension siblings to `start_paused`. The stream only uses tokio timers, so the test now measures elapsed time with `tokio::time::Instant` and is deterministic. Test-only change.

**NEXT:** unchanged: native audit log rotation; #835; #803 when it recurs.
