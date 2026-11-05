**STATUS 2026-11-05: grouping sets no longer inherit a constant group key (`fix/rollup-constant-group-key`, TPC-DS q27).**

TPC-DS q27 at SF10 differed from Trino on 91 of 100 rows. The cause is DataFusion 55.2: `WHERE s_state = 'TN' GROUP BY ROLLUP (i_item_id, s_state)` projected the filter's "s_state is constant" through the grouping-set aggregate, though ROLLUP emits s_state as NULL in its subtotal rows. Two effects followed. The final aggregate ran `PartiallySorted` on s_state and emitted groups each time the "constant" flipped between 'TN' and NULL, so the same group came out more than once (92,435 to 94,122 rows for 87,989 groups, varying per run). `ORDER BY i_item_id, s_state` became a sort on `i_item_id` alone. With `s_state IN ('TN','TX')` as a control, the plan and the counts were correct.

apache/datafusion#24422 (closes #24421, merged upstream 2026-08-19) fixes this, but no 55.x release has it. `vendor/datafusion-physical-plan` is 55.2.0 as published plus that change in `src/aggregates/mod.rs`, wired in through `[patch.crates-io]` like `vendor/object_store` and `vendor/zstd`. The cost: grouping-set aggregates lose sorted-input streaming, as they do upstream. The wasm playground has its own lockfile and is not patched.

Regression tests: `session_tuning::grouping_sets_constant_key` (plan keeps both sort keys and has no `ordering_mode`; one row per group; detail row before subtotal). Both fail on stock 55.2 (220 rows for 101 groups) and pass with the patch.

**NEXT:** drop `vendor/datafusion-physical-plan` and its patch line when the DataFusion 56 upgrade lands.
