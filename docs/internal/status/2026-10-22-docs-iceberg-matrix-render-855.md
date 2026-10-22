**STATUS 2026-10-22: Iceberg matrix page re-rendered from the state file (`docs/iceberg-matrix-render-855`, #855).**

#855 listed stale caveats in `docs/evidence/iceberg-matrix-state.json`. The
sweep landed with the matrix MRs since (#853, #854, #858, #859): the
position-deletes pinning caveat and the "needs the Spark 4.1 stack" caveats
are gone, and the read-support V3 caveat now says shredded variants read
unshredded. Every caveat left on main was re-checked against the code.

- `docs/internal/design-archive/iceberg-matrix.md`: re-rendered with
  `python3 -I scripts/render-iceberg-matrix.py`. Score 147/162 -> 150/162
  (92.6%); refreshed notes for MoR, equality deletes, type promotion,
  branching and bloom filters.

**NEXT:** after !1507 merges, re-render once more (type-promotion caveats)
and run the leak-gated site sync from a clean main.
