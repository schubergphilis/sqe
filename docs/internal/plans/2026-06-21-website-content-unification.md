# Website Content Unification Implementation Plan

> **For agentic workers:** implement on branch `docs/website-content-unify` in the worktree `/Users/jjverhoeks/sqe-content-wt` (NOT the primary checkout). Steps use checkbox (`- [ ]`) tracking.

**Goal:** Make the SQE repo the single home for ALL website content (markdown + structured copy), so authors edit "from here"; the getsqe repo keeps only Astro code/templates that render it.

**Architecture:** Two phases. **B1** (this branch, SQE repo, collision-safe): extract the ~3,600 words of authored copy that currently live only in the getsqe repo into `docs/site/web/`. **B2** (getsqe repo, deferred until `feat/perf-chart` merges): extend the sync to copy `docs/site/web/` into the Astro app, and rewire the 6 inline-content pages to render it.

**Why split:** B2 rewrites `performance.astro` and `index.astro`, which the perf author is editing uncommitted on `feat/perf-chart`. Doing B2 now collides. B1 establishes the content home with zero getsqe contact; B2 follows on a clean getsqe branch after perf-chart lands.

## Source -> target mapping (B1)

Authored content currently ONLY in `/Users/jjverhoeks/git/getsqe/website`, to transcribe verbatim (preserving wording, fixing only emdash/endash/arrows per repo style) into `docs/site/web/`:

| getsqe source | authored content | -> SQE target | format |
|---|---|---|---|
| `src/pages/about.astro` | hero lede + Origin + Where-it's-going + credit | `docs/site/web/about.md` | markdown + frontmatter |
| `src/pages/performance.astro` | hero lede, "Where we stand", footer note (NOT the perf.json stats) | `docs/site/web/performance.md` | markdown |
| `src/pages/compare/duckdb.astro` | `aheadRows`, `inspired`, `gaps` arrays (curated) | `docs/site/web/compare-duckdb.yaml` | yaml |
| `src/pages/index.astro` | `features`, `compareRows`, `bench` arrays + hero lede | `docs/site/web/landing.yaml` | yaml |
| `src/pages/roadmap.astro` | `state`, `milestones`, `roadmap.{progress,planned,blocked}` | `docs/site/web/roadmap.yaml` | yaml |
| `src/data/quickstarts.ts` | groups + items + blurbs | `docs/site/web/quickstarts.yaml` | yaml |

Rules:
- Transcribe the ACTUAL current copy. Do not invent, summarize, or "improve" wording.
- Prose -> markdown; structured lists/tables -> YAML (human-editable, the point of "edit from here").
- Repo style: NEVER emdash/endash/unicode-arrows; no delve/leverage/utilize/comprehensive/robust.
- `docs/site/web/` is under `docs/site/`, so `make leak-scan` already guards it; keep placeholders.

## B1 tasks

- [x] Create `docs/site/web/` and a short `docs/site/web/README.md` explaining the zone (authored website copy; rendered by the getsqe Astro app via sync; edit here).
- [x] Transcribe each source per the mapping table above into its target file.
- [x] `make leak-scan` -> docs/site clean (web/ included).
- [ ] Commit; push; open GitLab MR (target main) titled "docs: website content home (docs/site/web)".

## B2 (deferred — separate getsqe PR after feat/perf-chart merges)

- Extend `getsqe/website/scripts/sync-from-sqe.sh`: copy `$SQE_DIR/docs/site/web/*.md` -> `src/content/web/`, and `*.yaml` -> `src/data/` (convert YAML->JSON in the sync, or add a YAML import to Astro).
- Rewrite `about/performance/roadmap/index/compare-duckdb` pages + `quickstarts.ts` to render the synced content instead of inline literals. Coordinate `performance.astro`/`index.astro` with the perf author.
- Verify `npm run sync` + `npm run build`; leak-scan green.
- Net result: getsqe holds only template/code; all copy edits happen in `docs/site/web/`.
