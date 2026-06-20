# Documentation Coherence: Three-Zone Restructure (Sub-project A)

Date: 2026-06-20
Status: Design approved, ready for implementation plan
Scope: SQE source repo (`Applications/sqlengine`) + getsqe website repo (`~/git/getsqe`)

## Problem

`docs/` has accumulated 33 loose `.md` files plus two loose data files directly in
the top level, alongside well-organized subdirectories. The loose files are a mix of:

- **Reference content** that duplicates the curated mdBook (`architecture.md`,
  `deployment.md`, `operations.md`, `catalogs.md`, `testing.md`, `cli-embedded.md`).
- **Design-and-build evolution history** (`datafusion-architecture.md`,
  `ballista-evaluation-learnings.md`, the four `ranger-*.md` files, `s3vending.md`,
  `polaris-principal-provisioning.md`, `hf-glob-research.md`, etc.).
- **Published comparison sources** the getsqe site reads
  (`trino-compatibility.md`, `duckdb-comparision.md`, `features.md`).
- **Internal working notes** (`issues.md`, `security_audit.md`, `openspec*.md`).
- **Data/evidence** (`iceberg-matrix-state.json`, `performance.json`).

There is no rule for what is published, what is internal history, and what is data.
The curated book is buried under the sprawl. The website's `sync-from-sqe.sh` carries
12 sed sanitization rules because the source is not publish-clean.

This is sub-project A of a larger effort (A: docs coherence, B: website "what can it
do / how to start", C: quickstarts-as-integration-tests, D: benchmarks presentation).
Each sub-project gets its own spec -> plan -> implementation cycle. This spec covers A.

## North star (the end-user outcome this serves)

Two audiences must understand SQE easily and get the right information, with
comparisons and benchmarks within reach:

- **The SQL user** — what SQL works, how to query, dialect/compat vs Trino/DuckDB.
- **The platform user** — how to deploy, secure, and operate it.

Sub-project A makes the *source content* coherent and correctly homed so that B (the
website "what can it do / how to start" story) can present a clear path for each
audience. Reference lives in the book (docs.getsqe.com), the comparison sources and
benchmark evidence are correctly placed and publish-clean, and the engineering story is
published as design-notes. A is the foundation; B is where the audience-facing clarity
is built.

## Goals

1. Every file in `docs/` has a defined home. Zero loose prose at the top level.
2. The curated mdBook is the single canonical source for reference content.
3. The valuable design history is curated and published (not buried, not deleted).
4. `docs/site/` is publish-clean at source, so the website sync simplifies to a
   copy + a leak-scan guard, and the sed sanitizer is retired.
5. The getsqe pipeline keeps working: every moved path is re-pointed and verified.

## Non-goals

- No rebuild of the getsqe Astro site, its content collections, or its CI. Paths are
  re-pointed; the architecture is unchanged.
- No website IA / landing-page work. That is sub-project B.
- No change to `benchmarks/` at the repo root (result JSONs, queries, schemas stay).
- No rewrite of every design doc's prose in this pass. Promotion to design-notes means
  *relocated and lightly reconciled to current state*; deep rewrites are follow-on.

## Design

### Three zones

```
docs/
  site/                      # ONLY published content (feeds both public sites)
    book/                    # mdBook -> docs.getsqe.com        (was docs/book/)
      src/design-notes/      # NEW: curated engineering story (promoted design docs)
    ebook/                   # Sovereign by Design -> getsqe.com reader + downloads
    blog/                    # -> getsqe.com/blog
    compare/                 # trino.md, duckdb.md, features.md  (was loose docs/*.md)
  internal/                  # working history, NEVER published
    specs/                   # superpowers/specs + docs/specs
    plans/                   # superpowers/plans (+ archive)
    reviews/                 # reviews/
    audit/                   # audit/ + issues.md + security_audit.md
    prompts/                 # prompts/
    process/                 # RELEASING.md, openspec.md, openspec-explain.md
    design-archive/          # docs/archive/ + superseded design docs not promoted
  evidence/                  # generated data artifacts, not prose
    benchmark/charts/        # rendered PNGs
    perf/                    # sf10-explains
    iceberg-matrix-state.json
    performance.json
```

`benchmarks/` at the repo root is unchanged.

### The rule

Top-level `docs/` holds zero loose files. Every file lands in exactly one zone:

- **`site/`** = published to a public site. Reference -> `book/`. Narrative/marketing
  -> `ebook/`, `blog/`, `compare/`.
- **`internal/`** = working history, methodology, audits, planning. Never synced.
- **`evidence/`** = generated data (charts, explain dumps, matrix/perf JSON).

### Publish-clean-at-source invariant (the key principle)

The current sync sanitizer does two unlike jobs. Split them:

- **Security redaction (fix at source).** Real account IDs -> `123456789012`, no
  personal IAM names (`jacobadmin`), no internal hostnames (`sbp.gitlab…`), no
  monorepo paths (`vpf-data-ai/chameleon`). These are secrets/PII and must be
  placeholders even internally. `docs/site/` must never contain them. A repo-local
  leak-scan (`scripts/leak-scan-site.sh`) enforces exactly this set as a guard, and
  the corresponding rules are retired from both getsqe sync scripts.
- **Cosmetic normalization (keep in sync for now).** `MR !358` -> "an earlier
  change", `feat/x` -> "a feature branch", `eu-central-1` -> `eu-example-1`,
  `amazonaws` -> `aws-endpoint`, `crates/sqe-foo` -> `sqe-foo`. These are not secrets;
  they carry internal traceability (MR numbers are load-bearing references). They stay
  in the source and remain presentation-only rewrites in the getsqe sync scripts.

Consequence: the website sync drops the fragile security rules (including the
ordering-sensitive 13-digit-before-12-digit account redaction) and keeps only the thin
cosmetic rewrites. The leak-scan still gates as a guard.

Deferred backfill: a later pass converts MR/branch references to stable permalinks
(linked commits) so they read cleanly AND stay traceable; at that point the cosmetic
rewrites can retire too and the sync becomes a pure copy.

Migration cost: a one-time security sanitize-in-place sweep of the site zone (the ebook
source still carries real account IDs today per `PUBLISH-CHECKLIST.md`).

### Design notes (the published engineering story)

Valuable design docs are promoted into `site/book/src/design-notes/` and added to the
book `SUMMARY.md` as a new top-level section. The ebook stays the narrative version;
design-notes is the technical companion. Docs not worth publishing go to
`internal/design-archive/`. Per-file dispositions are in the triage table below.

## Per-file triage table

Disposition key: **BOOK** = merge unique content into the matching book page, delete
loose file. **NOTES** = promote to `site/book/src/design-notes/`. **COMPARE** = move to
`site/compare/`. **EVIDENCE** = move to `docs/evidence/`. **INTERNAL/<sub>** = move to
that internal subdir. **ARCHIVE** = `internal/design-archive/`. **REVIEW** = decide
during implementation (default noted).

### Loose `.md` files

| File | Disposition | Notes |
|---|---|---|
| `architecture.md` | BOOK | -> `book/src/architecture/` |
| `deployment.md` | BOOK | -> `book/src/deployment/` |
| `operations.md` | BOOK | -> `book/src/operations/` |
| `catalogs.md` | BOOK | -> `book/src/getting-started/catalogs.md` |
| `testing.md` | BOOK | -> `book/src/development/` |
| `cli-embedded.md` | BOOK | "Embedded CLI Reference" -> book CLI/embedded page |
| `features.md` | COMPARE | getsqe sync reads as `compare/features.md` (not a dupe) |
| `trino-compatibility.md` | COMPARE | getsqe sync reads as `compare/trino.md` |
| `duckdb-comparision.md` | COMPARE | getsqe sync reads as `compare/duckdb.md` |
| `datafusion-architecture.md` | NOTES | core architecture deep-dive (CLAUDE.md ref) |
| `ballista-evaluation-learnings.md` | NOTES | strong engineering story (CLAUDE.md ref) |
| `dbt-sqe.md` | NOTES | dbt compatibility design (CLAUDE.md ref) |
| `ranger-access-control.md` | NOTES | consolidate the 4 ranger-* into one design-notes page |
| `ranger-fine-grained-enforcement.md` | NOTES | consolidate (see above) |
| `ranger-fine-grained-service-type.md` | NOTES | consolidate (see above) |
| `ranger-tag-storage-decision.md` | NOTES | consolidate (see above) |
| `fine-grained-policy.md` | NOTES | policy enforcement design |
| `s3vending.md` | NOTES | S3 credential vending design |
| `polaris-principal-provisioning.md` | NOTES | Polaris principal provisioning design |
| `row-level-writes.md` | NOTES | MoR / position-delete write design |
| `quack-protocol.md` | NOTES | DuckDB Quack RPC design |
| `quack-datatype-matrix.md` | ARCHIVE | data-type sub-table; fold into quack notes or archive |
| `trino-client-compatibility.md` | NOTES | Trino wire compat design |
| `sqe-spark-ranger-parity.md` | NOTES | Spark/Ranger parity design |
| `hf-glob-research.md` | REVIEW | research note; default NOTES, ARCHIVE if stale |
| `roadmap.md` | BOOK | -> `book/src/development/roadmap` (working roadmap) |
| `runbook.md` | REVIEW | "On-Call Runbook"; default BOOK `operations/`, INTERNAL if infra-specific |
| `openspec.md` | INTERNAL/process | openspec methodology (CLAUDE.md ref) |
| `openspec-explain.md` | INTERNAL/process | EXPLAIN feature spec |
| `issues.md` | INTERNAL/audit | "Production Sign-Off Audit" |
| `security_audit.md` | INTERNAL/audit | |
| `iceberg-matrix.md` | INTERNAL/design-archive | full matrix incl. unpublishable cells |
| `iceberg-matrix-compare.md` | REVIEW | "Public Iceberg Matrix Comparison"; default INTERNAL |

### Loose data files

| File | Disposition |
|---|---|
| `iceberg-matrix-state.json` | EVIDENCE (`docs/evidence/`) |
| `performance.json` | EVIDENCE (`docs/evidence/`) |

### Subdirectories

| Dir | Disposition | Notes |
|---|---|---|
| `book/` | SITE | -> `docs/site/book/` (canonical reference) |
| `ebook/` | SITE | -> `docs/site/ebook/` (incl. diagrams, build/, voice.md) |
| `blog/` | SITE | -> `docs/site/blog/` (incl. images/) |
| `benchmark/charts/` | EVIDENCE | -> `docs/evidence/benchmark/charts/` |
| `benchmark/*.md` | REVIEW | per-suite narrative; default SITE book benchmarks section, reconcile with `features/benchmarks.md` |
| `perf/` | EVIDENCE | sf10-explains -> `docs/evidence/perf/` |
| `features/` (cdc, mor-vs-cow, runtime-filter-pushdown, ssb-sf1-trace) | NOTES | technical deep-dives -> design-notes |
| `specs/` (iceberg-caching-strategy, performance-roadmap) | INTERNAL/specs | |
| `superpowers/specs/` | INTERNAL/specs | |
| `superpowers/plans/` | INTERNAL/plans | incl. `archive/` |
| `reviews/` | INTERNAL/reviews | |
| `audit/` | INTERNAL/audit | |
| `prompts/` | INTERNAL/prompts | |
| `archive/` | INTERNAL/design-archive | incl. `ballista-evaluation/`, market research |

## Migration surface (everything that breaks and gets fixed)

### SQE repo

- **`Makefile`**: `BOOK_DIR := docs/site/book`, `EBOOK_DIR := docs/site/ebook`;
  benchmark-charts target output -> `docs/evidence/benchmark/charts`; help text.
- **`scripts/render-benchmark-charts.py`**: output path -> `docs/evidence/benchmark/charts`.
- **`CLAUDE.md`** (project): "Documentation Structure" + "Key docs" sections re-pointed
  (`docs/datafusion-architecture.md`, `docs/openspec.md`, `docs/dbt-sqe.md`,
  `docs/ballista-evaluation-learnings.md`, `docs/ebook/voice.md`).
- **mdBook**: `book.toml` build-dir is relative (low risk); fix internal cross-links
  broken by promoting design-notes and merging loose dupes; update `SUMMARY.md` for the
  new design-notes (and possibly benchmarks) section.
- **New**: leak-scan over `docs/site/` wired into repo checks (`make` target or CI).

### getsqe repo (`website/scripts/sync-from-sqe.sh`)

- Compare paths (lines 28-30) -> `docs/site/compare/`.
- Ebook (35-40) -> `docs/site/ebook/`.
- Blog (45-46) -> `docs/site/blog/`.
- Iceberg matrix + performance.json (76, 81) -> `docs/evidence/`.
- Benchmark charts (84) -> `docs/evidence/benchmark/charts/`.
- Distributed svg (86) -> `docs/site/ebook/diagrams/rendered/`.
- `benchmarks/results` (87) -> unchanged (repo root).
- `.md` -> GitHub-blob link rewrite (line 129) -> new `docs/site/...` base.
- **docs-website** sync (mdBook -> docs.getsqe.com) -> `docs/site/book`.
- **`getsqe/CLAUDE.md`** source-path references.
- **Retire the 12 sed sanitizer rules** once `docs/site/` is provably clean; keep
  leak-scan as a guard.

## Verification

Migrate with `git mv` to preserve history and the in-flight parallel edits
(account-ID redactions in `16c-following-through.md`, `03-the-engine-you-already-have.md`,
`roadmap.md`, `iceberg.md`, `catalogs.md`, `iceberg-matrix-state.json`).

1. `make rustbook` builds clean from `docs/site/book`.
2. `make ebook` builds PDF/EPUB from `docs/site/ebook`.
3. `make benchmark-charts` renders to `docs/evidence/benchmark/charts`.
4. Leak-scan over `docs/site/` passes (source is clean).
5. Dry-run `SQE_DIR=… website/scripts/sync-from-sqe.sh` -> all copies resolve; website
   leak-scan passes; `npm run build` succeeds.
6. `grep` sweep across both repos for lingering `docs/book`, `docs/ebook`, `docs/blog`,
   and loose-doc references -> zero stragglers.

## Rollback

The change is file moves + path edits on a feature branch. Rollback = revert the
branch. No data is destroyed (deletions are merges into the book; merged content is
preserved in git history). The getsqe sync changes are on their own branch and only
take effect when merged + run.

## Open items for the implementation plan

- Resolve the REVIEW rows (`runbook.md`, `iceberg-matrix-compare.md`, `hf-glob-research.md`,
  `benchmark/*.md`) with a quick read of each.
- Decide the design-notes `SUMMARY.md` ordering and whether ranger-* consolidates into
  one page or stays as several.
- Sequence: (1) create zones + `git mv` published dirs, (2) merge book dupes, (3) promote
  design-notes, (4) sweep internal + evidence, (5) sanitize site zone, (6) re-point SQE
  build refs + verify, (7) re-point getsqe sync + retire sanitizer + verify.
