**STATUS 2026-10-13: light reads are admitted while coordinator memory is red (`fix/light-query-admission-788`, #788 open item 3).**

Admission runs before the parse, so at red it refused every statement, including light queries (the SF10 concurrency probe counted 2 of 41 rejections on light queries). Now metadata statements (`SHOW`, `DESCRIBE`, `EXPLAIN` without `ANALYZE`) pass at red. Any other statement is admitted under `query.pressure_admission_memory` (default 64MB, clamped to `max_query_memory`), and `execute` refuses it after classification unless it is a read. The reduced cap is carried as `QueryQuota` from `admit` through `execute_query` / `open_stream`, so `ResourcesExhausted` and the buffered-result refusal name the pressure setting. A query admitted under pressure gets no join fallback. `"0"` restores the old refusal.

Tests: `a_light_read_runs_under_memory_pressure` (includes a GROUP BY + ORDER BY at the default partition count), `a_heavy_read_under_pressure_names_the_pressure_cap`, `writes_and_a_zero_cap_are_refused_under_pressure`. Unit tests only; the item is not closed until the SF10 concurrency probe shows zero light-query rejections.

**NEXT:** rerun the SF10 concurrency probe on the merged build; then #788 item 2 (sort merge headroom at tight caps), #755, #784 qualification.
