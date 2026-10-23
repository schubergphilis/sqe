**STATUS 2026-10-22: the sort share no longer refuses a merge (`fix/sort-share-merge-874`, #874).**

CI job 5652996 (MR !1509, unrelated) failed
`join_fallback::tests::a_partitioned_sort_spills_within_a_tight_quota`:
`sort share: each of 16 sort operators may grow to 3355443 bytes;
ExternalSorterMerge[1] requests 49120 additional bytes with 3340287
reserved`. About 1 in 30 CI runs; 6/6 locally.

`HeadroomBudget`'s sort share (`spillable / registered sort consumers`)
refused `ExternalSorterMerge[n]` like a buffer. A merge's pre-reservation is
exempt (held == 0), so the share only hit its later growth, and the output
merge's BatchBuilder growth cannot adapt. With every partition still
registered the share was below what partition 1's merge needed.

- `crates/sqe-coordinator/src/memory_headroom.rs`: `is_sorter` (the
  denominator) still counts buffers and merges; the new `is_sort_buffer`
  decides who is refused. Dropping merges from the denominator instead
  doubled every buffer's cap, and the window query in the same test then
  failed 2/20 on the total (`67100887 already reserved`).
- The fair share (sort-merge retry only) still caps merges: dropping them
  there breaks the #784 guarantee
  (`a_growing_merge_leaves_room_for_the_sorters_that_have_not_started`).
  Its output-merge refusal is the same kind of hard failure; it is a
  known trade-off, noted on #874.

Gate: `merges_count_towards_the_sort_share_but_are_not_refused_by_it`
(CI's numbers; fails on main), the tight-quota test 20/20, `cargo test -p
sqe-coordinator --lib` 872 passed (`RUST_MIN_STACK=32MiB`).

**NEXT:** if the fair-share retry ever fails on an output merge, give the
merge its own cap there instead of the share.
