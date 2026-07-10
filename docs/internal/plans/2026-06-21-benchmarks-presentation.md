# Benchmarks Presentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Implement on branch `docs/benchmarks-presentation` in worktree `/Users/jjverhoeks/sqe-d-wt` (NOT the primary checkout). Steps use checkbox (`- [ ]`) tracking.

**Goal:** Make the published benchmark story coherent and honest across all surfaces, citing the committed numbers consistently and stating where SQE trails.

**Architecture:** Docs-only. Book `features/benchmarks.md` becomes the canonical story; the evidence narrative is tightened as its source; `performance.md` + `vs-other-engines.md` are aligned; a cross-surface number check prevents drift. No pipeline changes, no new numbers.

**Tech Stack:** mdBook, markdown. Numbers come from `docs/evidence/performance.json` (SF1 headline) and `docs/evidence/benchmark/index.md` (SF10 evidence).

**Spec:** `docs/internal/specs/2026-06-21-benchmarks-presentation-design.md`

**Worktree note:** all work in `/Users/jjverhoeks/sqe-d-wt`; confirm `git branch --show-current` = `docs/benchmarks-presentation` at the start of each task.

## The canonical numbers (cite these; do not invent)

From `docs/evidence/performance.json` (`asOf 2026-06-12`, vs Trino 465, SF1, 222/222 pass):
| Suite | SQE | Trino | Speedup | Pass |
|---|---|---|---|---|
| TPC-E (11) | 9.3s | 172.0s | 18.5x | 11/11 |
| TPC-BB (10) | 28.0s | 255.7s | 9.1x | 10/10 |
| TPC-C (8 read) | 0.41s | 2.65s | 6.5x | 8/8 |
| TPC-DS (99) | 13.4s | 45.6s | 3.4x | 93/99 |
| ClickBench (43) | 1.3s | 4.46s | 3.4x | 43/43 |
| TPC-H (22) | 16.8s | 26.7s | 1.6x | 22/22 |
| SSB (13) | 8.3s | 5.8s | 0.70x (TRAILS) | 13/13 |

SF10 (from `docs/evidence/benchmark/index.md`, "level rig", Trino 481, single-node / distributed-2w / Trino-range):
- TPC-H: 130.5s / 95.5s / 106.4s-138.6s (roughly par to ahead distributed)
- SSB: 42.0s / 53.6s / 28.0s-41.1s (TRAILS)
- TPC-DS: 543.9s / 338.3s / 328.4s-468.0s (close, distributed ahead of single)

Validation: differential vs Trino, with DuckDB `dsdgen` as an independent data oracle; caught a Trino q75 rounding bug + 16 vacuous TPC-DS queries (the "benchmark that lied" finding, blog `2026-06-12-the-benchmark-that-lied`).

---

## Task 1: Rewrite the book benchmark page into the canonical honest story

**Files:** Modify `docs/site/book/src/features/benchmarks.md`

- [ ] **Step 1: Add a "Results" section near the top (after the suite table + "Why these benchmarks")**

Insert a new `## Results (SF1, vs Trino 465)` section that presents the canonical SF1 table above (verbatim numbers), states `asOf 2026-06-12` and that it is SF1, and notes 222/222 queries pass. Keep it a clean table, not a dev-log.

- [ ] **Step 2: Add a "Where SQE trails" honesty subsection**

Immediately after Results, add `### Where SQE trails`:
- SSB at 0.70x: SQE is slower than Trino on the Star Schema Benchmark. Why: SSB is small-fact star joins where Trino's build-side bloom/key-set shipping wins; SQE's build-side key-set shipping to workers is in progress.
- SF10 scaling: the SF1 wins are decisive, but at SF10 the gap narrows and SSB still trails (cite the SF10 figures above, labelled SF10). TPC-H at SF10 is roughly par to ahead distributed; SSB trails; TPC-DS is close. State plainly that SF10+ is where the distributed path is still closing.

- [ ] **Step 3: Add a "How the numbers are validated" subsection**

Add `### How it is validated`: differential comparison against Trino row-for-row (`--compare-trino`), with DuckDB `dsdgen` as an independent data oracle. The differential pass caught a Trino q75 rounding bug and 16 vacuous TPC-DS queries that returned empty/trivial results. This is why the pass counts are trustworthy, not self-graded.

- [ ] **Step 4: Trim the stale internal dev-log content**

Remove or condense the dated, internal-only sections that read as a dev log, NOT a published story: the "Apr 2 baseline vs Apr 6" per-query table, the "Full Benchmark Matrix (Apr 7)" config table, "Spill behavior across configs", "Scheduler observations", "TPC-E: the outlier" (fold its one honest point into "Where SQE trails" if useful), and "Metrics gaps / wiring tasks" (delete: it is a stale internal TODO). The longitudinal per-run detail belongs in the evidence narrative + getsqe.com/performance, which the page links to. Keep a one-line pointer to the longitudinal view.

- [ ] **Step 5: KEEP the useful reference content**

Do NOT remove: the `sqe-bench` generate/load/test usage, scale-factor table, query-result statuses, JSON report format, query files layout, "Adding New Benchmarks". These are good reference and stay.

- [ ] **Step 6: Update the "Comparing against Trino" section**

Replace the stale "~3.1x speedup on TPC-H SF1" framing with the current Results framing (link to Results section) + keep the correctness-parity explanation (differential validation). Keep the link to the benchmark quickstart.

- [ ] **Step 7: Style + build + commit**

Style: no emdash/endash/unicode-arrows; no "comprehensive/leverage/utilize"; short sentences; honest. Then:
```bash
cd /Users/jjverhoeks/sqe-d-wt
( cd docs/site/book && mdbook build -d /tmp/d1 2>&1 | grep -iE 'error|incomplete|broken' || echo "book builds clean" )
grep -n '—\|–' docs/site/book/src/features/benchmarks.md || echo "no emdash"
git add docs/site/book/src/features/benchmarks.md
git commit -m "docs(benchmarks): canonical honest results story (headline + trailing cases + validation), trim stale dev-log"
```

---

## Task 2: Tighten the evidence narrative for consistency

**Files:** Modify `docs/evidence/benchmark/index.md` and the per-suite pages (`tpch.md`, `tpcds.md`, `ssb.md`, `clickbench.md`, `tpcc.md`, `tpce.md`, `tpcbb.md`) as needed.

- [ ] **Step 1: Make the evidence numbers consistent with performance.json**

Read `index.md` + each per-suite page. Where a headline-style claim appears (e.g. a speedup or pass count), ensure it matches `performance.json` or is clearly labelled as an SF10/specific-run figure. Fix contradictions (e.g. a per-suite page claiming a different SF1 speedup than performance.json). Do NOT rewrite the timeline narrative wholesale; tighten for coherence and number-consistency.

- [ ] **Step 2: Ensure the evidence narrative reads as the deep source**

Confirm `index.md` clearly frames itself as the longitudinal per-run record (it already does) and links to the book benchmark page + getsqe.com/performance. Fix any stale cross-references (e.g. links to moved files).

- [ ] **Step 3: Build (evidence is not in the book, but check links) + commit**
```bash
cd /Users/jjverhoeks/sqe-d-wt
grep -rn '—\|–' docs/evidence/benchmark/*.md || echo "no emdash"
git add docs/evidence/benchmark/
git commit -m "docs(benchmarks): tighten evidence narrative; reconcile numbers with performance.json"
```

---

## Task 3: Align the marketing prose + vs-other-engines + cross-surface check

**Files:** Modify `docs/site/web/performance.md`, `docs/site/book/src/reference/vs-other-engines.md`

- [ ] **Step 1: Align `docs/site/web/performance.md`**

Read it. Ensure its "where we stand" prose is consistent with the book Results: same headline framing (6/7 at SF1), SSB trails, SF10 closing. It must not state a number that contradicts `performance.json`. (It is prose, not the numbers table; the getsqe page renders performance.json for the table.)

- [ ] **Step 2: Align `reference/vs-other-engines.md`**

Read it. Where it cites benchmark figures or "within 2x of Trino"-type claims, make them consistent with the Results (SF1 6/7; SSB the known trailing case; SF10 closing). Keep it honest about where Trino/DuckDB/Spark are stronger.

- [ ] **Step 2b: Cross-surface number-consistency check**

Run:
```bash
cd /Users/jjverhoeks/sqe-d-wt
grep -rnoE '[0-9]+(\.[0-9]+)?x|[0-9]+/[0-9]+|6/7|0\.70x' \
  docs/site/book/src/features/benchmarks.md \
  docs/site/web/performance.md \
  docs/site/book/src/reference/vs-other-engines.md | sort | head -60
```
Eyeball that every speedup / pass-count / "6/7" matches `performance.json` (SF1) or is labelled SF10. Fix any contradiction.

- [ ] **Step 3: Build + emdash + commit**
```bash
( cd docs/site/book && mdbook build -d /tmp/d3 2>&1 | grep -iE 'error|incomplete|broken' || echo "book builds clean" )
grep -rn '—\|–' docs/site/web/performance.md docs/site/book/src/reference/vs-other-engines.md || echo "no emdash"
git add docs/site/web/performance.md docs/site/book/src/reference/vs-other-engines.md
git commit -m "docs(benchmarks): align performance.md + vs-other-engines with the canonical results"
```

---

## Task 4: Verify + finish

- [ ] **Step 1: Full build + leak-scan**
```bash
cd /Users/jjverhoeks/sqe-d-wt
make rustbook
make leak-scan
```
Expected: book builds; `leak-scan: docs/site clean`.

- [ ] **Step 2: Number-trace check** — confirm each speedup / pass-count / "6/7" in benchmarks.md, performance.md, vs-other-engines.md maps to `performance.json` (SF1) or a labelled SF10/evidence figure. No contradictions across surfaces.

- [ ] **Step 3: Honesty check** — benchmarks.md contains: the SSB-trails statement, the SF10 crossover statement, and the validation-methodology statement.

- [ ] **Step 4: getsqe sync unaffected** — the docs sync reads `docs/site/...`; confirm the touched pages still have valid frontmatter/lead where applicable (benchmarks.md is a book page, no frontmatter; performance.md keeps its lead).

- [ ] **Step 5: Push + MR**
```bash
git push -u origin docs/benchmarks-presentation
```
Open the MR (target main).

---

## Self-review (coverage check)

- Book benchmarks.md = canonical honest story (headline + trailing + validation, trim dev-log, keep reference) -> Task 1. ✓
- Evidence narrative tightened + number-consistent -> Task 2. ✓
- performance.md + vs-other-engines aligned + cross-surface check -> Task 3. ✓
- Every number traces to performance.json / labelled SF10 -> Tasks 3.2b, 4.2. ✓
- Honesty (SSB, SF10, validation) -> Task 1.2/1.3, 4.3. ✓
- Non-goals respected (no pipeline change, no static-chart fix, no new numbers) -> nothing in the plan touches performance.json pipeline or render-benchmark-charts.py. ✓
- Verify (build, leak-scan, getsqe sync) -> Task 4. ✓
