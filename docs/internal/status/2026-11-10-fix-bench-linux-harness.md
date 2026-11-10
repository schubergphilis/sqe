**STATUS 2026-11-10: SF100 bench harness fixed for small Linux hosts; `scripts/bench-profile.py` added (`fix/bench-linux-harness`, #784, #787).**

TPC-H SF100 on an 8-core, 31 GB Linux box failed for harness reasons, not engine ones: q07/q09 hit EMFILE at the 1024 open-file soft limit (one spilling sort wrote 834 run files), q18/q21 hit the 60s per-file timeout, the config's 64GB memory ceiling sat above physical RAM, and spill went to a tmpfs `/tmp`. Trino never started because its catalog file was mode 600 and the container user could not read it. `benchmark-test.sh` now fixes the Trino permission and sizes the four settings to the host when they are unset, printing them on a `Host defaults:` line. With those settings the box ran 22/22 both uncapped (570s) and under a 4GB query cap (912s).

`scripts/bench-profile.py` reads the coordinator's `query_profile` records and prints the hot operators per query. On the 4GB-cap run it shows the #784 sort-merge fallback cost: q09 spends 313s CPU in one `SortExec` on `l_partkey` over 600M lineitem rows and spills 48.8 GB.

**NEXT:** rerun ClickBench SF100 with `--compare-trino` on the Linux box for #787; follow up #784 on the fallback sorting whole inputs.
