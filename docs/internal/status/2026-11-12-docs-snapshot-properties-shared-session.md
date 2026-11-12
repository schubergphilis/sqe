**STATUS 2026-11-12: snapshot-properties spec corrected for shared bearer sessions (`docs/snapshot-properties-shared-session`).**

The merged `write-snapshot-properties` spec claimed the transaction form is race-free for parallel loaders on one bearer token. That holds only for Flight SQL `BeginTransaction`, whose id rides each request (`in_request_transaction`). A SQL `BEGIN` is stored on the session (`set_session_transaction`), so every client on the token joins it. The design, the spec scenario and docs task 6.1 now say so.

No code changed.

**NEXT:** unchanged: task 1.1 of `write-snapshot-properties`.
