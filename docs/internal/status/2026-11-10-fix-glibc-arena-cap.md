**STATUS 2026-11-10: glibc malloc arenas capped at two in `sqe-server` (#788, `fix/glibc-arena-cap`).**

The SF100 concurrent qualification for #788 (2 x sort86m, 2 x agg, 2 x join
on the coordinator, 8 cores / 31 GB) was OOM-killed at 405 s with 25 GB
anonymous RSS while the DataFusion pool held 4.6-10 GB. The untracked
memory was glibc arena fragmentation: the same run with
`MALLOC_ARENA_MAX=2` completed all six queries, peak RSS 18.5 GB, peak
spill 36 GB, no spill files left, pool residue 0.

`crates/sqe-server/src/malloc_arena.rs` calls `mallopt(M_ARENA_MAX, 2)` as
the first statement of `main`, before the runtime spawns threads, on
`linux` + `gnu` only. An operator-set `MALLOC_ARENA_MAX` is left alone. The
outcome is logged after "Starting sqe-server".

Re-qualified with this binary on the same rig, `MALLOC_ARENA_MAX` unset
(checked in `/proc/<pid>/environ`; startup log: `arenas=2`), one process
for the whole sequence:

| Phase | Result | Peak RSS | Peak spill | RSS after |
|---|---|---|---|---|
| uncapped (before) | OOM-killed at 405 s | 25 GB | - | - |
| concurrent run 1 | 5/6 OK | 18.0 GB | 39.5 GB | 7.95 GB |
| concurrent run 2 | 6/6 OK | 16.5 GB | 38.1 GB | 11.3 GB |

Run 2 peaked lower than run 1, so the heap kept between runs is reused, not
stacked. The run-1 failure is one `sort86m` aborted by the 300 s result
idle timeout at 600 s: the query returns one row at the end, and under
six-way load it sat past the limit (the other sorts took 582-640 s). That is
the idle timeout on single-row long queries, not memory. Light queries
alongside: 0 failures in 2905, p95 under 1 s. Alone: agg 155 s (169 s
before), join 249 s (222 s before); `sort86m` 343 s cold (first query on a fresh
process), 190 s and 191 s warm (182 s before).

**NEXT:** the result idle timeout: a long query that emits one row at the
end trips it under load. After a concurrent run the idle process keeps about
11.8 GB RSS with zero bytes in the pool. That is heap glibc does not return;
try `malloc_trim(0)` when the pool drains, or a jemalloc build qualified on
the same rig.
