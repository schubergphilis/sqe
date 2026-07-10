# Documentation Three-Zone Restructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize the SQE repo's `docs/` into three zones (`site/` published, `internal/` working history, `evidence/` data) so every file has one home, the book is canonical, design history is published as design-notes, and the website sync simplifies to copy + leak-scan-guard.

**Architecture:** A file-move + path-repoint migration across two repos. Moves use `git mv` to preserve history and the in-flight account-ID redactions. Each phase ends with a verification command (the "test") and a commit. No code logic changes; the engine is untouched. The getsqe pipeline is re-pointed, not rebuilt.

**Tech Stack:** git, mdBook, Pandoc (ebook), Python+matplotlib (benchmark charts), bash (getsqe sync + leak-scan), Astro (getsqe website).

**Repos:**
- SQE source: `/Users/jjverhoeks/git/schuberg/vpf-data-ai/chameleon/Applications/sqlengine` (branch `docs/three-zone-restructure`, already created)
- getsqe: `/Users/jjverhoeks/git/getsqe` (sub-repos `website/`, `docs-website/`)

**Spec:** `docs/superpowers/specs/2026-06-20-docs-coherence-three-zone-design.md`

**REVIEW items resolved (final dispositions):**
- `runbook.md` -> BOOK (`book/src/operations/`), it is generic/namespace-parameterized.
- `hf-glob-research.md` -> NOTES (current research note).
- `iceberg-matrix-compare.md` + `iceberg-matrix.md` -> INTERNAL/design-archive together (mutual relative links; full matrix not publish-clean).
- whole `docs/benchmark/` dir -> EVIDENCE (narrative + charts coupled by relative image links move together; book `features/benchmarks.md` stays canonical).
- ranger-* -> moved as four separate design-notes pages under a Ranger heading (no risky prose merge; consolidation is a follow-on).

---

## Phase 0: Prep and baseline

### Task 0: Capture a green baseline

**Files:** none (read-only verification).

- [ ] **Step 1: Confirm branch and clean tree intent**

Run:
```bash
cd /Users/jjverhoeks/git/schuberg/vpf-data-ai/chameleon/Applications/sqlengine
git branch --show-current   # expect: docs/three-zone-restructure
git status --short          # in-flight redaction edits may be present; that is expected
```

- [ ] **Step 2: Build the book to confirm it is green before any move**

Run: `make rustbook`
Expected: mdBook builds to `target/book` with no error.

- [ ] **Step 3: Render benchmark charts to confirm the script runs**

Run: `make benchmark-charts`
Expected: charts (re)render under `docs/benchmark/charts/` with no error. (If the Python venv is missing, note it and skip; the path-repoint in Phase 7 is still verifiable by inspecting the script.)

- [ ] **Step 4: Confirm the ebook toolchain is present (Phases 7-8 depend on it)**

Run: `make ebook`
Expected: builds `docs/ebook/build/sovereign-by-design.{pdf,epub}`. If Pandoc/xelatex
is missing and the build fails, do NOT proceed to Phase 8 ebook re-sanitization blind:
either install the toolchain, or mark Task 10 Step 4 as "deferred — sanitize ebook
chapter sources by hand, rebuild on a machine with the toolchain." Record which.

- [ ] **Step 5: Record the loose-file inventory for later diffing**

Run:
```bash
find docs -maxdepth 1 -type f | sort > /tmp/docs-loose-before.txt
cat /tmp/docs-loose-before.txt
```
Expected: the 35 loose files (33 `.md` + `iceberg-matrix-state.json` + `performance.json`).

### Task 1: Create the three zone directories

**Files:**
- Create: `docs/site/`, `docs/internal/`, `docs/evidence/` (via moves below; create `.gitkeep`-free by moving real content into them).

- [ ] **Step 1: Create the internal subdirectories that need to exist before moves**

Run:
```bash
cd /Users/jjverhoeks/git/schuberg/vpf-data-ai/chameleon/Applications/sqlengine
mkdir -p docs/site/compare docs/internal/process docs/internal/audit docs/internal/design-archive docs/evidence
```
Expected: directories created (empty dirs are not tracked by git yet; content moves populate them).

---

## Phase 1: Move published directories into site/

### Task 2: Move book, ebook, blog under site/

**Files:**
- Move: `docs/book` -> `docs/site/book`
- Move: `docs/ebook` -> `docs/site/ebook`
- Move: `docs/blog` -> `docs/site/blog`

- [ ] **Step 1: git mv the three published dirs**

Run:
```bash
cd /Users/jjverhoeks/git/schuberg/vpf-data-ai/chameleon/Applications/sqlengine
git mv docs/book docs/site/book
git mv docs/ebook docs/site/ebook
git mv docs/blog docs/site/blog
```
Expected: three renames staged; `git status --short` shows `R` entries.

- [ ] **Step 2: Verify the book still builds from the new path**

Run: `cd docs/site/book && mdbook build && cd -`
Expected: builds clean (its `book.toml` build-dir is relative, `../../target/book`, which still resolves; if it 404s on build-dir, note it for Phase 7 Makefile step).

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs(restructure): move book/ebook/blog into docs/site/"
```

---

## Phase 2: Move compare sources into site/compare/

### Task 3: Move the three published comparison docs

**Files:**
- Move: `docs/trino-compatibility.md` -> `docs/site/compare/trino-compatibility.md`
- Move: `docs/duckdb-comparision.md` -> `docs/site/compare/duckdb-comparision.md`
- Move: `docs/features.md` -> `docs/site/compare/features.md`

- [ ] **Step 1: git mv the compare sources (keep filenames; getsqe sync renames on copy)**

Run:
```bash
git mv docs/trino-compatibility.md docs/site/compare/trino-compatibility.md
git mv docs/duckdb-comparision.md docs/site/compare/duckdb-comparision.md
git mv docs/features.md docs/site/compare/features.md
```
Expected: three renames staged.

- [ ] **Step 2: Commit**

```bash
git add -A
git commit -m "docs(restructure): move comparison sources into docs/site/compare/"
```

---

## Phase 3: Merge book-duplicate loose files into the canonical book

> **CONTENT-LOSS HOTSPOT.** Tasks 4-5 are prose merges, the one place this migration
> can silently drop material. These steps need judgment, not mechanical moves. For
> every loose file, before `git rm`, confirm that each unique paragraph/section either
> already exists in the book or has been folded in. Do not delete on faith.

For each file: read the loose file AND its book counterpart, fold any content present in the loose file but missing from the book page into that page under a clearly-titled subsection, then `git rm` the loose file. The book is canonical; nothing reference-y remains loose.

### Task 4: Merge architecture / deployment / operations / catalogs / testing

**Files:**
- Modify: `docs/site/book/src/architecture/overview.md` (or the closest architecture page)
- Modify: `docs/site/book/src/deployment/*` , `docs/site/book/src/operations/*`, `docs/site/book/src/getting-started/catalogs.md`, `docs/site/book/src/development/*`
- Delete: `docs/architecture.md`, `docs/deployment.md`, `docs/operations.md`, `docs/catalogs.md`, `docs/testing.md`

- [ ] **Step 1: For each pair, diff to find unique loose content**

Run (repeat per file, example for architecture):
```bash
echo "=== loose ===" ; sed -n '1,40p' docs/architecture.md
echo "=== book   ===" ; ls docs/site/book/src/architecture/ ; sed -n '1,40p' docs/site/book/src/architecture/overview.md
```
Decision rule: content already represented in the book page -> drop. Content unique and still accurate -> append to the most relevant book page under a `## <topic>` heading matching the loose file's intent.

- [ ] **Step 2: Fold unique content into the book pages**

Edit the relevant `docs/site/book/src/...` page(s) to add only the unique, current material. Do not duplicate what the book already says. Keep the book's voice and heading style.

- [ ] **Step 3: Delete the loose dupes**

Run:
```bash
git rm docs/architecture.md docs/deployment.md docs/operations.md docs/catalogs.md docs/testing.md
```

- [ ] **Step 4: Rebuild the book to confirm no broken internal links**

Run: `cd docs/site/book && mdbook build && cd -`
Expected: builds clean; no "incomplete link" warnings for the edited pages.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "docs(restructure): merge architecture/deployment/operations/catalogs/testing into book"
```

### Task 5: Merge cli-embedded and runbook; relocate roadmap

**Files:**
- Modify: `docs/site/book/src/getting-started/cli.md` and/or `docs/site/book/src/features/embedded.md` (cli-embedded target)
- Modify/Create: `docs/site/book/src/operations/runbook.md` (new page) and `docs/site/book/src/SUMMARY.md`
- Move: `docs/roadmap.md` -> `docs/site/book/src/development/roadmap.md`
- Delete: `docs/cli-embedded.md`

- [ ] **Step 1: Fold cli-embedded.md into the CLI/embedded book page(s)**

Read `docs/cli-embedded.md` ("Embedded CLI Reference"). Fold unique reference material into `docs/site/book/src/getting-started/cli.md` or `features/embedded.md`, then:
```bash
git rm docs/cli-embedded.md
```

- [ ] **Step 2: Add runbook as a published operations page**

Run:
```bash
git mv docs/runbook.md docs/site/book/src/operations/runbook.md
```
Then add to `docs/site/book/src/SUMMARY.md` under the Operations section:
```markdown
- [On-Call Runbook](./operations/runbook.md)
```

- [ ] **Step 3: Relocate the working roadmap into the book development section**

Run:
```bash
git mv docs/roadmap.md docs/site/book/src/development/roadmap.md
```
Then add to `docs/site/book/src/SUMMARY.md` under the Development section (if not already present):
```markdown
- [Roadmap](./development/roadmap.md)
```

- [ ] **Step 4: Rebuild and commit**

```bash
cd docs/site/book && mdbook build && cd -
git add -A
git commit -m "docs(restructure): merge cli-embedded, publish runbook + roadmap in book"
```

---

## Phase 4: Promote design history into a published design-notes section

### Task 6: Create the design-notes section and move promoted docs

**Files:**
- Create: `docs/site/book/src/design-notes/index.md`
- Move (into `docs/site/book/src/design-notes/`):
  `datafusion-architecture.md`, `ballista-evaluation-learnings.md`, `dbt-sqe.md`,
  `fine-grained-policy.md`, `s3vending.md`, `polaris-principal-provisioning.md`,
  `row-level-writes.md`, `quack-protocol.md`, `trino-client-compatibility.md`,
  `sqe-spark-ranger-parity.md`, `hf-glob-research.md`,
  `ranger-access-control.md`, `ranger-fine-grained-enforcement.md`,
  `ranger-fine-grained-service-type.md`, `ranger-tag-storage-decision.md`,
  `features/cdc.md`, `features/mor-vs-cow.md`, `features/runtime-filter-pushdown.md`,
  `features/ssb-sf1-trace.md`, `specs/iceberg-caching-strategy.md`
- Modify: `docs/site/book/src/SUMMARY.md`

- [ ] **Step 1: Write the design-notes index page**

Create `docs/site/book/src/design-notes/index.md`:
```markdown
# Design Notes

The engineering story behind SQE: the decisions, the dead ends, and the designs
that shipped. The ebook *Sovereign by Design* is the narrative version; these are
the technical companions, kept close to current state.
```

- [ ] **Step 2: git mv the promoted docs into design-notes/**

Run:
```bash
cd /Users/jjverhoeks/git/schuberg/vpf-data-ai/chameleon/Applications/sqlengine
for f in datafusion-architecture ballista-evaluation-learnings dbt-sqe \
         fine-grained-policy s3vending polaris-principal-provisioning \
         row-level-writes quack-protocol trino-client-compatibility \
         sqe-spark-ranger-parity hf-glob-research \
         ranger-access-control ranger-fine-grained-enforcement \
         ranger-fine-grained-service-type ranger-tag-storage-decision; do
  git mv "docs/$f.md" "docs/site/book/src/design-notes/$f.md"
done
git mv docs/features/cdc.md                   docs/site/book/src/design-notes/cdc.md
git mv docs/features/mor-vs-cow.md            docs/site/book/src/design-notes/mor-vs-cow.md
git mv docs/features/runtime-filter-pushdown.md docs/site/book/src/design-notes/runtime-filter-pushdown.md
git mv docs/features/ssb-sf1-trace.md         docs/site/book/src/design-notes/ssb-sf1-trace.md
git mv docs/specs/iceberg-caching-strategy.md docs/site/book/src/design-notes/iceberg-caching-strategy.md
```
Expected: 20 renames staged. `docs/features/` is now empty (remove if so: `rmdir docs/features 2>/dev/null || true`).

- [ ] **Step 3: Add the Design Notes section to SUMMARY.md**

Edit `docs/site/book/src/SUMMARY.md`, adding a new top-level section after Development:
```markdown
# Design Notes

- [Overview](./design-notes/index.md)
- [DataFusion Architecture](./design-notes/datafusion-architecture.md)
- [Ballista Evaluation](./design-notes/ballista-evaluation-learnings.md)
- [dbt Compatibility](./design-notes/dbt-sqe.md)
- [Fine-grained Policy](./design-notes/fine-grained-policy.md)
- [Ranger Access Control](./design-notes/ranger-access-control.md)
  - [Fine-grained Enforcement](./design-notes/ranger-fine-grained-enforcement.md)
  - [Service Type](./design-notes/ranger-fine-grained-service-type.md)
  - [Tag Storage Decision](./design-notes/ranger-tag-storage-decision.md)
- [Spark / Ranger Parity](./design-notes/sqe-spark-ranger-parity.md)
- [S3 Credential Vending](./design-notes/s3vending.md)
- [Polaris Principal Provisioning](./design-notes/polaris-principal-provisioning.md)
- [Row-level Writes](./design-notes/row-level-writes.md)
- [Iceberg Caching Strategy](./design-notes/iceberg-caching-strategy.md)
- [Change Data Capture](./design-notes/cdc.md)
- [Merge-on-Read vs Copy-on-Write](./design-notes/mor-vs-cow.md)
- [Runtime Filter Pushdown](./design-notes/runtime-filter-pushdown.md)
- [SSB SF1 Trace](./design-notes/ssb-sf1-trace.md)
- [Quack Protocol](./design-notes/quack-protocol.md)
- [Trino Client Compatibility](./design-notes/trino-client-compatibility.md)
- [HuggingFace Glob Research](./design-notes/hf-glob-research.md)
```

- [ ] **Step 4: Fix any cross-links broken by the move**

Run:
```bash
cd docs/site/book && mdbook build 2>&1 | grep -i 'incomplete\|broken\|error' || echo "no link warnings"
cd -
```
For each warning, fix the relative link in the offending page (paths shift from `docs/` to `docs/site/book/src/design-notes/`).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "docs(restructure): publish design history as book Design Notes section"
```

---

## Phase 5: Sweep internal working history

### Task 7: Consolidate internal dirs and process/audit docs

**Files:**
- Move: `docs/superpowers/specs` -> `docs/internal/specs`; `docs/superpowers/plans` -> `docs/internal/plans`
- Move: `docs/specs/performance-roadmap.md` -> `docs/internal/plans/performance-roadmap.md`
- Move: `docs/reviews` -> `docs/internal/reviews`; `docs/audit` -> `docs/internal/audit-runs`; `docs/prompts` -> `docs/internal/prompts`
- Move: `docs/openspec.md`, `docs/openspec-explain.md` -> `docs/internal/process/`
- Move: `docs/issues.md`, `docs/security_audit.md` -> `docs/internal/audit/`
- Move: `docs/archive/*`, `docs/iceberg-matrix.md`, `docs/iceberg-matrix-compare.md`, `docs/quack-datatype-matrix.md` -> `docs/internal/design-archive/`

- [ ] **Step 1: Move the superpowers + specs working dirs**

Run:
```bash
cd /Users/jjverhoeks/git/schuberg/vpf-data-ai/chameleon/Applications/sqlengine
git mv docs/superpowers/specs docs/internal/specs
git mv docs/superpowers/plans docs/internal/plans
git mv docs/specs/performance-roadmap.md docs/internal/plans/performance-roadmap.md
rmdir docs/superpowers docs/specs 2>/dev/null || true
```
Note: this plan file itself lives under `docs/superpowers/plans/` and moves with it; the executing agent should re-open it from `docs/internal/plans/` after this step.

- [ ] **Step 2: Move reviews / audit / prompts**

Run:
```bash
git mv docs/reviews docs/internal/reviews
git mv docs/audit docs/internal/audit-runs
git mv docs/prompts docs/internal/prompts
```

- [ ] **Step 3: Move process + audit loose docs**

Run:
```bash
git mv docs/openspec.md docs/internal/process/openspec.md
git mv docs/openspec-explain.md docs/internal/process/openspec-explain.md
git mv docs/issues.md docs/internal/audit/issues.md
git mv docs/security_audit.md docs/internal/audit/security_audit.md
```

- [ ] **Step 4: Move design-archive content (keep iceberg-matrix pair together)**

Run:
```bash
git mv docs/archive/ballista-evaluation docs/internal/design-archive/ballista-evaluation
git mv docs/archive/market-research-sql-engines-iceberg.md docs/internal/design-archive/market-research-sql-engines-iceberg.md
git mv docs/archive/matrix-parity-tracking-issue.md docs/internal/design-archive/matrix-parity-tracking-issue.md
git mv docs/archive/matrix-parity-workflow.md docs/internal/design-archive/matrix-parity-workflow.md
git mv docs/archive/README.md docs/internal/design-archive/README.md
git mv docs/iceberg-matrix.md docs/internal/design-archive/iceberg-matrix.md
git mv docs/iceberg-matrix-compare.md docs/internal/design-archive/iceberg-matrix-compare.md
git mv docs/quack-datatype-matrix.md docs/internal/design-archive/quack-datatype-matrix.md
rmdir docs/archive 2>/dev/null || true
```

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "docs(restructure): sweep working history into docs/internal/"
```

---

## Phase 6: Sweep evidence (data artifacts)

### Task 8: Move benchmark, perf, and data JSON into evidence/

**Files:**
- Move: `docs/benchmark` -> `docs/evidence/benchmark`
- Move: `docs/perf` -> `docs/evidence/perf`
- Move: `docs/iceberg-matrix-state.json` -> `docs/evidence/iceberg-matrix-state.json`
- Move: `docs/performance.json` -> `docs/evidence/performance.json`

- [ ] **Step 1: git mv the evidence content**

Run:
```bash
cd /Users/jjverhoeks/git/schuberg/vpf-data-ai/chameleon/Applications/sqlengine
git mv docs/benchmark docs/evidence/benchmark
git mv docs/perf docs/evidence/perf
git mv docs/iceberg-matrix-state.json docs/evidence/iceberg-matrix-state.json
git mv docs/performance.json docs/evidence/performance.json
```
Expected: benchmark narrative + charts move together (relative image links `charts/*.png` inside `benchmark/*.md` stay valid).

- [ ] **Step 2: Confirm docs/ top level now holds zero loose files**

Run:
```bash
find docs -maxdepth 1 -type f
```
Expected: empty output. Only `docs/site`, `docs/internal`, `docs/evidence` (and possibly leftover empty dirs to clean) remain.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs(restructure): move benchmark/perf/data into docs/evidence/"
```

---

## Phase 7: Re-point SQE build references and verify

### Task 9: Update Makefile, render script, and CLAUDE.md

**Files:**
- Modify: `Makefile:16-17`, benchmark-charts echo/clean targets (`Makefile:104,261,283-284`)
- Modify: `scripts/render-benchmark-charts.py:33` (and docstring lines 2,15-16)
- Modify: `CLAUDE.md` (Documentation Structure + Key docs sections)

- [ ] **Step 1: Update Makefile path variables**

Edit `Makefile`:
```make
BOOK_DIR     := docs/site/book
EBOOK_DIR    := docs/site/ebook
```
And replace every `docs/benchmark/charts` occurrence (lines 104, 261, 283-284) with `docs/evidence/benchmark/charts`.

- [ ] **Step 2: Update the chart render output path**

Edit `scripts/render-benchmark-charts.py` line 33:
```python
OUT_DIR = ROOT / "docs" / "evidence" / "benchmark" / "charts"
```
And update the docstring references (lines 2, 15-16) from `docs/benchmark/charts/` to `docs/evidence/benchmark/charts/`.

- [ ] **Step 3: Update CLAUDE.md doc references**

Edit `CLAUDE.md`. In "Documentation Structure" and "Key docs", re-point:
- `docs/datafusion-architecture.md` -> `docs/site/book/src/design-notes/datafusion-architecture.md`
- `docs/openspec.md` -> `docs/internal/process/openspec.md`
- `docs/dbt-sqe.md` -> `docs/site/book/src/design-notes/dbt-sqe.md`
- `docs/ballista-evaluation-learnings.md` -> `docs/internal/design-archive/ballista-evaluation/` or its design-notes home (`docs/site/book/src/design-notes/ballista-evaluation-learnings.md`)
- `docs/ebook/voice.md` -> `docs/site/ebook/voice.md`
Also add a short paragraph documenting the new three-zone layout (`docs/site`, `docs/internal`, `docs/evidence`) and the `docs/site` no-secrets invariant.

- [ ] **Step 4: Verify all three build paths**

Run:
```bash
make rustbook        # builds from docs/site/book
make ebook           # builds PDF/EPUB from docs/site/ebook
make benchmark-charts  # renders into docs/evidence/benchmark/charts (skip if venv missing)
```
Expected: each succeeds. If `make ebook` fails on an internal relative path, fix it in `docs/site/ebook/Makefile`.

- [ ] **Step 5: Grep for stragglers in the SQE repo**

Run:
```bash
grep -rnE 'docs/(book|ebook|blog|benchmark|perf)[/ ]|docs/(architecture|features|catalogs|deployment|operations|testing|roadmap|datafusion-architecture|openspec|dbt-sqe)\.md' \
  --include='*.md' --include='*.rs' --include='*.toml' --include='*.sh' --include='*.py' --include='Makefile' \
  . | grep -v 'docs/site/\|docs/internal/\|docs/evidence/' | grep -v '^./docs/internal/' || echo "no stragglers"
```
Expected: `no stragglers` (or only intentional historical mentions inside `docs/internal/`). Fix any live reference found.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "docs(restructure): re-point Makefile, chart script, and CLAUDE.md to new zones"
```

---

## Phase 8: Make the site zone publish-clean at source

### Task 10: Sanitize docs/site in place and add a repo leak-scan

**Files:**
- Modify: any file under `docs/site/` containing real account IDs, internal crate paths, internal URLs, regions, or personal IAM names
- Create: `scripts/leak-scan-site.sh`
- Modify: `Makefile` (add a `leak-scan` target)

- [ ] **Step 1: Create a SECURITY-ONLY repo-local scanner**

Per the spec's split invariant, this scanner enforces only secrets/PII at source. It
must NOT match cosmetic patterns (MR/branch refs, regions, `amazonaws`, crate paths) —
those stay in source and are rewritten presentation-only by the getsqe sync.
Create `scripts/leak-scan-site.sh` covering only: 12-digit account IDs, personal IAM
names (`jacobadmin`/`jacobbuilder`), internal GitLab host, and monorepo path.
```bash
#!/usr/bin/env bash
set -euo pipefail
ROOT="${1:-docs/site}"
# SECURITY-ONLY. Cosmetic patterns (MR !/feat/chore/region/amazonaws/crate paths) are
# intentionally NOT here — they are kept in source and rewritten by the getsqe sync.
PATTERN='([0-9]{12})|sbp\.gitlab\.schubergphilis\.com|vpf-data-ai/chameleon|jacobadmin|jacobbuilder'
if grep -rnaIE "$PATTERN" "$ROOT" \
     --include='*.md' --include='*.toml' --include='*.json' --include='*.svg' \
   | grep -vE '123456789012|000000000000'; then
  echo "LEAK-SCAN: docs/site contains secrets/PII (hits above)" >&2
  exit 1
fi
echo "leak-scan: docs/site clean"
```
Then: `chmod +x scripts/leak-scan-site.sh`.

- [ ] **Step 2: Run it and fix every SECURITY hit at source (leave cosmetics alone)**

Run: `bash scripts/leak-scan-site.sh docs/site`
For each hit, edit the source file to use the placeholder: `123456789012` for account
IDs, `quickstart-admin`/`quickstart-builder` for IAM names, `github.com/schubergphilis/sqe`
for the internal GitLab URL/monorepo path. Do NOT touch `MR !`, `feat/`, region, or
crate-path text — those are kept. Re-run until it prints `leak-scan: docs/site clean`.
Note: the ebook `build/` PDF/EPUB are binaries the text scan skips; rebuild them with
`make ebook` after sanitizing the chapter sources so the artifacts are clean too.

- [ ] **Step 3: Add a Makefile target**

Edit `Makefile`, add:
```make
leak-scan:
	@echo "==> Scanning docs/site for leaks"
	@bash scripts/leak-scan-site.sh docs/site
```

- [ ] **Step 4: Rebuild the ebook from sanitized sources and re-scan**

Run:
```bash
make ebook
make leak-scan
```
Expected: ebook builds; `leak-scan: docs/site clean`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "docs(restructure): make docs/site publish-clean + add leak-scan target"
```

---

## Phase 9: Re-point and simplify the getsqe pipeline

These tasks run in the getsqe repo. They only take effect when run/merged there; the SQE branch is independent.

### Task 11: Re-point website/scripts/sync-from-sqe.sh and retire its sanitizer

**Files:**
- Modify: `/Users/jjverhoeks/git/getsqe/website/scripts/sync-from-sqe.sh`
- Modify: `/Users/jjverhoeks/git/getsqe/CLAUDE.md`

- [ ] **Step 1: Re-point every source path**

Edit `website/scripts/sync-from-sqe.sh`:
- line 28: `$SQE_DIR/docs/trino-compatibility.md` -> `$SQE_DIR/docs/site/compare/trino-compatibility.md`
- line 29: `$SQE_DIR/docs/duckdb-comparision.md` -> `$SQE_DIR/docs/site/compare/duckdb-comparision.md`
- line 30: `$SQE_DIR/docs/features.md` -> `$SQE_DIR/docs/site/compare/features.md`
- lines 35,37,38: `$SQE_DIR/docs/ebook/...` -> `$SQE_DIR/docs/site/ebook/...`
- lines 39,40: `$SQE_DIR/docs/ebook/build/...` -> `$SQE_DIR/docs/site/ebook/build/...`
- lines 45,46: `$SQE_DIR/docs/blog/...` -> `$SQE_DIR/docs/site/blog/...`
- line 76: `$SQE_DIR/docs/iceberg-matrix-state.json` -> `$SQE_DIR/docs/evidence/iceberg-matrix-state.json`
- line 81: `$SQE_DIR/docs/performance.json` -> `$SQE_DIR/docs/evidence/performance.json`
- line 84: `$SQE_DIR/docs/benchmark/charts/$c-cross-scale.png` -> `$SQE_DIR/docs/evidence/benchmark/charts/$c-cross-scale.png`
- line 86: `$SQE_DIR/docs/ebook/diagrams/rendered/13-distributed-execution.svg` -> `$SQE_DIR/docs/site/ebook/diagrams/rendered/13-distributed-execution.svg`
- line 87: `$SQE_DIR/benchmarks/results` -> unchanged (repo root)
- line 129 sed: the `.md` -> GitHub-blob rewrite base `docs/\1` -> `docs/site/compare/\1` (compare docs are what carry these links)

- [ ] **Step 2: Retire only the SECURITY redaction rules; keep the cosmetic rewrites**

`docs/site` is now secret-clean at source, so delete the *security* sed rules from the
sanitizer block: the 12-digit account-id rule, the 13+-digit snapshot guard, the
`sbp.gitlab…`/monorepo-path rules, and the `jacobadmin`/`jacobbuilder` rules. KEEP the
cosmetic rewrites that carry traceability or normalization: `MR !` -> "an earlier
change", `feat/`/`chore/` -> branch phrasing, `eu-central`/`eu-west` -> `eu-example-*`,
`amazonaws` -> `aws-endpoint`/`aws`, `crates/sqe-*` -> bare crate name. Keep the final
leak-scan gate as a guard so the build still fails if a secret ever reappears.

- [ ] **Step 3: Update getsqe/CLAUDE.md source paths**

Run:
```bash
cd /Users/jjverhoeks/git/getsqe
grep -nE 'docs/(book|ebook|blog|trino|duckdb|features|benchmark|performance|iceberg)' CLAUDE.md
```
Edit each hit to the new `docs/site/...` or `docs/evidence/...` path.

- [ ] **Step 4: Dry-run the website sync and build**

Run:
```bash
cd /Users/jjverhoeks/git/getsqe/website
SQE_DIR=/Users/jjverhoeks/git/schuberg/vpf-data-ai/chameleon/Applications/sqlengine npm run sync
npm run build
```
Expected: every copy resolves (no "warn: not found"); leak-scan passes; Astro build succeeds.

- [ ] **Step 5: Commit (in website sub-repo)**

```bash
cd /Users/jjverhoeks/git/getsqe/website
git add -A
git commit -m "sync: re-point to docs/site + docs/evidence, retire sanitizer (source is clean)"
```

### Task 12: Re-point docs-website/scripts/sync-from-sqe.sh

**Files:**
- Modify: `/Users/jjverhoeks/git/getsqe/docs-website/scripts/sync-from-sqe.sh`

- [ ] **Step 1: Re-point the book source path**

Edit line 23: `SRC_BOOK="$SQE_DIR/docs/book"` -> `SRC_BOOK="$SQE_DIR/docs/site/book"`.
Leave the quickstart OVERVIEW overlay (line 42, reads `$SQE_DIR/quickstart/*/OVERVIEW.md`) unchanged.

- [ ] **Step 2: Retire only the SECURITY rules; keep cosmetics and the book.toml de-brand**

From the `sed -E -i ''` block (lines 80-98), delete the *security* rules only: the
12-digit account-id rule (85), the 13+-digit snapshot guard (84), the `sbp.gitlab…`
host + URL rules (93-94), the monorepo-path rule (95), and `jacobadmin`/`jacobbuilder`
(96-97). KEEP the cosmetic rules: crate-path strip with the `@@KEEP@@` sentinel (81-83),
region rewrites (86-87), `amazonaws` (88-89), `MR !` (90), `feat/`/`chore/` (91-92).
KEEP the `book.toml` de-brand `sed` (lines 52-56) — repo URL/authors/build-dir, not
secrets. Keep the leak-scan gate (lines 101-105) as a guard.

- [ ] **Step 3: Dry-run the docs-website sync and build**

Run:
```bash
cd /Users/jjverhoeks/git/getsqe/docs-website
SQE_DIR=/Users/jjverhoeks/git/schuberg/vpf-data-ai/chameleon/Applications/sqlengine bash scripts/sync-from-sqe.sh
mdbook build
```
Expected: `sync OK`; mdBook builds.

- [ ] **Step 4: Commit (in docs-website sub-repo)**

```bash
cd /Users/jjverhoeks/git/getsqe/docs-website
git add -A
git commit -m "sync: re-point book source to docs/site/book, retire sanitizer"
```

---

## Phase 10: Final verification

### Task 13: Cross-repo verification sweep

- [ ] **Step 1: SQE repo — zero loose files, all builds green**

Run:
```bash
cd /Users/jjverhoeks/git/schuberg/vpf-data-ai/chameleon/Applications/sqlengine
test -z "$(find docs -maxdepth 1 -type f)" && echo "docs/ top is clean"
make rustbook && make leak-scan
```
Expected: "docs/ top is clean", book builds, `docs/site` clean.

- [ ] **Step 2: getsqe — both sites sync + build from new paths**

Run:
```bash
cd /Users/jjverhoeks/git/getsqe/website && SQE_DIR=/Users/jjverhoeks/git/schuberg/vpf-data-ai/chameleon/Applications/sqlengine npm run sync && npm run build
cd /Users/jjverhoeks/git/getsqe/docs-website && SQE_DIR=/Users/jjverhoeks/git/schuberg/vpf-data-ai/chameleon/Applications/sqlengine bash scripts/sync-from-sqe.sh && mdbook build
```
Expected: both succeed, no missing-file warnings, leak-scans pass.

- [ ] **Step 3: Push the SQE branch and open a PR**

Run:
```bash
cd /Users/jjverhoeks/git/schuberg/vpf-data-ai/chameleon/Applications/sqlengine
git push -u origin docs/three-zone-restructure
```
Then open a PR (the getsqe sub-repo changes go as their own PRs in their respective repos).

---

## Deferred follow-on (not in this plan's scope)

**Backfill MR/branch references to permalinks.** A later pass converts `MR !358`-style
references in `docs/site/book/src/design-notes/` to stable links (linked commits or a
public changelog entry) so they read cleanly in published docs AND stay traceable. Once
done, the cosmetic `MR !`/`feat/`/`chore/` rewrites can be removed from both getsqe sync
scripts, moving the sync closer to a pure copy. Tracked here so the decision ("keep MR
refs in source now, backfill later") is not lost.

## Self-review notes (coverage check)

- Three zones created + the no-loose-file rule -> Tasks 1-8, verified Task 13 Step 1. ✓
- Book canonical, merge + delete dupes -> Tasks 4-5. ✓
- `features.md`/`trino`/`duckdb` kept as compare sources -> Task 3. ✓
- Design history published as design-notes -> Task 6. ✓
- Internal + evidence sweeps -> Tasks 7-8. ✓
- Split invariant: security fixed at source (leak-scan-site.sh), cosmetics kept + MR backfill deferred -> Tasks 10-12 + Deferred follow-on. ✓
- Full migration surface (Makefile, render script, both CLAUDE.md, both sync scripts) -> Tasks 9, 11, 12. ✓
- Verification + rollback (feature branch) -> Task 13; rollback = revert branch. ✓
- All four REVIEW items resolved in the header. ✓
