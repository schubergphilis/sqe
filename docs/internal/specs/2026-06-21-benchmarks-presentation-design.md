# Benchmarks Presentation: Narrative + Comparisons (Sub-project D)

Date: 2026-06-21
Status: Design approved, ready for implementation plan
Scope: SQE repo (`docs/site/book/src/features/benchmarks.md`, `docs/site/book/src/reference/vs-other-engines.md`, `docs/site/web/performance.md`, `docs/evidence/benchmark/`)

## Guiding principle

The benchmark story is visible and honest. Every published surface cites the
same committed numbers; trailing cases are stated plainly. Honesty about where
SQE loses is what makes a benchmark page trustworthy rather than marketing.

## Problem

The good benchmark narrative exists but readers do not see it. The rich timeline
+ per-suite deep-dives live in `docs/evidence/benchmark/` (the evidence zone,
unpublished). The published surfaces are thin or scattered:

- Book `features/benchmarks.md`: a suite table + a link to getsqe.com/performance.
- getsqe `/performance`: numbers from `docs/evidence/performance.json` + prose in
  `docs/site/web/performance.md`.
- Book `reference/vs-other-engines.md` (added in sub-project B): when-to-choose.

Nothing published tells the honest story: what each suite proves, the headline
results, where SQE trails (SSB 0.70x; the SF10 scaling crossover), and how the
numbers were validated. This is sub-project D of the docs overhaul (A, B, C done).

## Goals

1. Make the published benchmark story coherent and honest across all surfaces.
2. Every cited number traces to the committed `docs/evidence/performance.json`
   (with its `asOf` date); no drift between surfaces.
3. State trailing cases plainly (SSB, SF10 crossover) and disclose the validation
   methodology.

## Non-goals

- No pipeline refactor. `performance.json` stays hand-maintained; we cite it, we
  do not re-derive it.
- No static-chart outlier fix. `scripts/render-benchmark-charts.py` still includes
  incomplete/skip-heavy runs (the same class the interactive aggregator fix
  addressed). Noted as a known data-quality follow-up, not done here.
- No new big page; strengthen existing surfaces.
- No new numbers / re-running benchmarks. Use the committed results.

## Design (Approach A: strengthen published surfaces, tighten the evidence source)

### Surface 1 - Book `features/benchmarks.md` (canonical published story)

Rewrite/expand to tell the story, keeping the existing accurate suite table:

- **What each suite proves** - the SQL-coverage rationale (TPC-H/SSB analytical
  core; TPC-DS complex SQL/windows/subqueries; TPC-C/E OLTP read+write;
  ClickBench wide scans; TPC-BB SQL subset). Keep/refine the existing "why these"
  prose.
- **Headline results vs Trino** - from `performance.json`: 6/7 suites won at SF1,
  the per-suite speedups, 222/222 pass. State the `asOf` date and the scale.
- **Where SQE trails (honesty section)**: SSB at 0.70x and why (build-side key
  sets / bloom filters to workers, in progress); the SF10 scaling crossover
  (decisive wins through SF1, TPC-H and SSB trail at SF10). Drawn from
  `performance.json` `scale.progress` + the evidence narrative.
- **How it is validated**: differential vs Trino, with DuckDB `dsdgen` as an
  independent data oracle; the q75 Trino rounding bug + 16 vacuous TPC-DS queries
  caught (the "benchmark that lied" finding).
- **Links**: longitudinal getsqe.com/performance; `reference/vs-other-engines.md`.

### Surface 2 - `docs/evidence/benchmark/` narrative (deep source, tightened)

`index.md` (timeline) + per-suite `.md`. Tighten for coherence; ensure numbers
are consistent with `performance.json`. Stays in the evidence zone (it is the
per-run longitudinal record); the book draws insight from it. Do not delete the
timeline; make it consistent and readable.

### Surface 3 - getsqe `docs/site/web/performance.md` + book `reference/vs-other-engines.md`

Align both with the book benchmark story: same numbers, same honesty (SSB trails,
SF10 crossover, validation methodology). `performance.md` already has a "where we
stand" block - make it consistent. `vs-other-engines.md` already states SQE is
stronger/weaker per engine - make the benchmark figures it cites match.

### Cross-surface consistency (the data-integrity requirement)

Build a small mental/explicit cross-check: list every benchmark number that
appears in benchmarks.md, performance.md, vs-other-engines.md, and the evidence
narrative, and confirm each matches `performance.json` (or is explicitly an
SF10/evidence figure, labelled as such). No surface may state a speedup or pass
count that contradicts another.

## Honesty requirements (credibility core)

- SSB trails (0.70x) - stated with the reason.
- SF10 scaling crossover - stated (SF1 decisive, SF10 TPC-H/SSB trail).
- "6/7 won" scoped to its scale (SF1); do not imply all-scale dominance.
- Validation methodology disclosed (differential + DuckDB oracle).

## Style

Repo voice: no emdash/endash/unicode-arrows; no "delve/leverage/utilize/
comprehensive/robust"; no exclamation marks; short sentences; direct and
opinionated; honest about tradeoffs.

## Phases (for the plan)

- **P1** Rewrite book `features/benchmarks.md` as the canonical story (suite
  rationale + headline + honesty section + validation + links).
- **P2** Tighten `docs/evidence/benchmark/index.md` + per-suite narrative for
  coherence and number-consistency.
- **P3** Align `docs/site/web/performance.md` + `reference/vs-other-engines.md`;
  run the cross-surface number-consistency check.
- **P4** Verify: `make rustbook` builds; `make leak-scan` clean; emdash-clean;
  every cited number traces to `performance.json` (or is a labelled SF10/evidence
  figure); getsqe docs sync unaffected.

## Verification

- `make rustbook` builds; `make leak-scan` -> docs/site clean.
- `grep -rn` for em/endash in touched files -> none.
- Number-trace: each speedup / pass-count / "6/7" in the published surfaces maps
  to `performance.json` or a labelled evidence figure; no contradictions.
- The honesty sections (SSB, SF10, validation) are present in benchmarks.md.

## Rollback

Docs-only on a feature branch. Rollback = revert the branch.

## Open items for the plan

- Exact SF10 figures to cite (from the evidence narrative / clean-rig record) and
  how to label them as SF10 vs the SF1 headline.
- Whether any per-suite evidence page is too stale to keep and should be trimmed.
