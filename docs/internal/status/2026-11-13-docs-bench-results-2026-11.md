**STATUS 2026-11-13: eight uncommitted benchmark reports from 2026-11-05 and 2026-11-10 committed (`docs/bench-results-2026-11`).**

The reports sat untracked in a checkout. They are committed as they were recorded; nothing was rerun.

| Report | Result |
|---|---|
| `tpcds-sf0.1-flight-2026-11-05T08:59:04` | 99/99 pass, 9.7 s |
| `tpcds-sf1-flight-2026-11-05T09:02:04` | 99/99 pass, 18.3 s |
| `tpcds-sf10-flight-2026-11-05T09:07:43` | 99/99 pass, 98.1 s |
| `compare-tpcds-sf0_1-2026-11-05T09:00:06` | 90 matched, 9 expected empty, 0 diffs vs Trino |
| `compare-tpcds-sf1-2026-11-05T09:03:43` | 99/99 matched vs Trino |
| `clickbench-sf100-flight-2026-11-10T10:25:04` | 43/43 pass, 26.7 s |
| `tpch-sf100-flight-2026-11-10T09:49:51` | 20/22: q18, q21 timed out at 60 s |
| `tpch-sf100-flight-2026-11-10T10:15:41` | 18/22: q18, q21 timed out; q07, q09 failed with "Too many open files" |

The two TPC-H SF100 runs with errors are the early runs of that session. The later runs of the same day (`11:10:24`, `11:41:54`, already committed) passed 22/22, so the errors are kept as history, not as an open regression.

**NEXT:** none for this branch. If "Too many open files" shows up again on an SF100 run, raise the file descriptor limit on the rig first and record it in the run notes before treating it as an engine bug.
