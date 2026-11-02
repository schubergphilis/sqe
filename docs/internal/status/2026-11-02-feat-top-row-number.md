**STATUS 2026-11-02: "first row per partition" as a hash aggregate (`feat/top-row-number`).**

`TopRowNumberRule` (`crates/sqe-catalog/src/session_tuning/top_row_number.rs`)
rewrites `Filter(rn = 1, Window(ROW_NUMBER() OVER (PARTITION BY p ORDER BY o)))`
into `Projection(Aggregate(GROUP BY p, __sqe_top_row(o..., payload...)))`.
The aggregate keeps per group the row-encoded ordering key of the best row
and that row's other columns; a column that is itself an `ORDER BY` key is
decoded from the key rather than stored twice. It runs inside DataFusion's
hash aggregate, so it gets partial/final stages, memory accounting and
spilling. It is the last logical rule, after projection pruning.

Matched: `rn = 1`, `1 = rn`, `rn <= 1`, `rn < 2`, `rn` under a cast, with
other conjuncts kept in a filter above, the window directly under the filter
or under a column-only projection. Declined (window plan unchanged): any
other bound, OR, a second window expression, no `PARTITION BY`, volatile
keys, RANK/DENSE_RANK, and any key or payload column that is not fixed width
(strings, lists).

engine-bench q3 (latest row per customer, 150M rows, 15M customers,
`--memory-limit 3g`, 8 partitions, alternating runs in one binary): window
8.3-9.7 s, rewrite 5.5-6.5 s (medians ~8.9 s vs ~5.6 s, 1.6x), RSS
2.4 -> 2.1 GiB. Output identical to DuckDB's (14,999,347 rows, 0 diff). The
same query as `max()` per customer takes 3.6-3.9 s, so ~1.7 s is left: the
final stage spends 24.8 s of CPU over 8 partitions in `merge_batch` and
spills 1.4 GB (24 spills) at 3 GiB.

DataFusion 55.2 has no fix to take: its `WindowTopN` does not match `rn = 1`
and its `PartitionedTopK` keeps one heap per key without spilling; the
upstream rework is on arrow 60 and arrives with DataFusion 56. Drop this rule
once 56 handles q3 at least as fast.

**NEXT:** close the gap to `max()` in the final stage (tighter `size()`
accounting so 3 GiB spills less, a cheaper merge loop), then re-run
engine-bench q3 at 150M with `STORAGE=s3`.
