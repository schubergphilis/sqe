**STATUS 2026-10-15: cartesian joins over `query.max_cartesian_rows` are refused at plan time (`fix/cartesian-admission-755`, #755).**

Every join runs on the coordinator, which all tenants share. Memory admission bounds a join's build side, but a `CROSS JOIN` or a join without an equal-key condition (`ON a.x < b.y`) holds a core for `left x right` comparisons until the query timeout. `CartesianAdmissionRule` (`sqe-planner/src/cartesian_admission.rs`) walks the physical plan after `JoinStrategyRule`, multiplies the row-count statistics of each `CrossJoinExec` / `NestedLoopJoinExec` input, and refuses the query with `ResourcesExhausted` when the product exceeds the cap. It runs as a fail-closed post-planning step. The error names the cap only; the estimates go to the coordinator log. They would add no oracle anyway, since `SHOW STATS` already exposes snapshot row counts to anyone who can load the table.

Default 10,000,000,000 row pairs; `0` turns the gate off; an input with absent statistics is admitted with a warning.

Calibration: SF1 TPC-H 22/22 and TPC-DS 99/99 pass with the rule active (`benchmarks/results/*-sf1-flight-2026-10-15T15:1*.json`). The rule saw 15 cross joins, all scalar subqueries against one row; the largest product was 1 x 9,869. Those grow linearly with scale factor, so the default leaves roughly four orders of magnitude at SF100.

Not gated: skewed equi-joins (a hot key is still `n x m`), aggregations (bounded by memory admission and the timeout), and the write path.

Verified: planner unit tests 6/6, `post_planning` tests 7/7 (CROSS JOIN and a non-equi join refused at cap 17 over 18 pairs, admitted at 18 and at 0, an equi-join admitted at cap 1, the error carries no row counts).

**NEXT:** if skewed equi-joins show up in practice, gate hash joins on the largest key's frequency from Puffin NDV/top-k sketches rather than on the row product.
