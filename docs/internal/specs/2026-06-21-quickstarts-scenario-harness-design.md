# Quickstarts as Scenario Tests + Documentation (Sub-project C)

Date: 2026-06-21
Status: Design approved, ready for implementation plan
Scope: SQE repo (`quickstart/`, `scripts/`, `.gitlab-ci.yml`, `docs/site/book/src/development/testing.md`, quickstart READMEs)

## Guiding principle: one artifact, two jobs

A quickstart is simultaneously a **documented starting point** and an **asserted
test scenario**. Neither is secondary. The same directory serves both:

- `README.md` documents the scenario (what, how, why, every config explained).
- `run.sh --check` runs it and asserts the scenario behaves correctly.
- `OUTPUT.md` is the bridge: the real captured output shown in the docs is the
  same output the asserts verify. Docs and tests cannot drift apart, because
  they are produced by the same run.

Every design decision below keeps both jobs first-class.

## Problem

Three overlapping test mechanisms exist, none reconciled, and the quickstart
docs are inconsistent:

- `scripts/integration-test.sh` -> `docker-compose.test.yml` (Polaris in-mem +
  RustFS) -> `cargo test -p sqe-coordinator` (engine-internals, Rust assertions).
- `scripts/distributed-test.sh` -> `docker-compose.distributed.yml` (+ Keycloak,
  coordinator, worker) -> bash CLI assertions (`assert_contains`) for system
  tables, CTAS, caching, Trino HTTP.
- 14 quickstart `run.sh` -> per-backend `docker-compose.yml` -> CLI queries ->
  capture `OUTPUT.md`. **No assertions** today: they prove "it ran", not "it ran
  correctly", and they are not in CI.

Quickstart READMEs vary from 237 lines (gold standard: `polaris-keycloak-client-id`)
to 72 (`attach-catalogs`). The thin ones do not explain configs well enough to
be a starting point.

This is sub-project C of the docs/website overhaul (A, B done). It gets its own
spec -> plan -> implementation cycle.

## Goals

1. One coherent **two-tier** scenario-test harness with a single entry point.
2. Quickstarts become **asserted** tests (catch real regressions, not just "ran").
3. Every quickstart README is a **good starting point**: what, how, why, and every
   config file explained, at the gold-standard bar.
4. Wired into CI for the self-contained scenarios; AWS scenarios gated on creds.
5. One canonical **testing guide** documenting the harness and the scenario catalog.

## Non-goals

- No new test framework / declarative DSL. Reuse bash + the existing assert pattern.
- No change to the engine or to what the cargo integration tests assert (Tier 1
  stays as-is in substance).
- No per-MR gating policy change beyond adding the scenario job; cadence tuning
  (which subset per-MR vs nightly) is a CI-config detail settled in the plan.

## Design

### Tier 1 - Engine integration (kept)

`scripts/integration-test.sh` unchanged in substance: brings up
`docker-compose.test.yml`, bootstraps, runs `cargo test -p sqe-coordinator`
(integration_test + sql_compat_test). Relabeled "Tier 1" and invoked through the
unified entry point. This tests engine internals with Rust-level assertions.

### Tier 2 - Scenario tests (the quickstarts)

- **Shared assert helpers.** Promote `assert_contains` / `assert_not_empty` (and
  add `assert_query_rows`, `assert_query_contains`) from `distributed-test.sh`
  into `quickstart/_shared/lib.sh`, next to the existing `step`/`ok`/`die`/`cap`.
- **`run.sh --check`.** Each quickstart's `run.sh` gains a `--check` mode. Today
  `run.sh` does up -> queries -> capture `OUTPUT.md`. `--check` adds a final
  assertions block that verifies the scenario's key invariants (examples:
  polaris-keycloak: admin sees N rows AND a restricted user's masked column is
  NULL/masked; distributed: `system.runtime.nodes` lists the worker, a CTAS
  round-trips, the result cache hits; embedded-files: `read_parquet` returns the
  expected row count). Invariants live inline in `run.sh --check` (small
  scenarios) or a sibling `checks.sh` sourced by `run.sh` (larger ones).
- **Fold in `distributed-test.sh`.** Its compose stack + CLI assertions become
  the "distributed" scenario under this model (either a quickstart-style dir or a
  scenario entry that reuses `docker-compose.distributed.yml`), using the shared
  helpers. `distributed-test.sh` is retired once its assertions move over.
- `OUTPUT.md` capture stays exactly as is (it is the docs evidence). `--check`
  asserts against the live query results, not by diffing `OUTPUT.md` (timestamps
  / durations make golden-diff brittle).

### Single entry point

`scripts/test.sh` dispatches and prints one consistent pass/fail summary:

- `scripts/test.sh engine` -> Tier 1 (delegates to `integration-test.sh`).
- `scripts/test.sh scenario <name>` -> one quickstart `run.sh --check`.
- `scripts/test.sh scenario all` -> all self-contained scenarios.
- `scripts/test.sh ci` -> the CI subset (Tier 1 + self-contained Tier 2).

### Quickstart documentation standard

Every quickstart README follows the gold-standard structure (from
`polaris-keycloak-client-id`):

1. Frontmatter (`slug`, `title`, `description`) - drives the docs.getsqe.com sync.
2. **What you get** - the scenario and its use-case (the "why").
3. **Prerequisites**.
4. **Run it** - exact commands (`./run.sh`, `--down`, `--check`).
5. **How it works** - the flow that matters for this scenario (auth / data path /
   catalog federation / embedded).
6. **Configuration explained** - every config file annotated: `sqe.toml` (or CLI
   flags), `.env(.example)`, `docker-compose.yml`, and the relevant `_shared`
   assets. This is the part that makes it a starting point.
7. **Output** - what a correct run produces (mirrors `OUTPUT.md`).
8. **How it is tested** - the `--check` invariants, so the reader sees what
   "correct" means.
9. **Gotchas**.

Bring the thin READMEs up to this bar (`attach-catalogs`, `polaris-ranger-keycloak`,
`observability`, `aws-s3-tables`, `embedded-sqlite-catalog`, and any other below
the bar). The `quickstart/README.md` index documents this structure as the
standard/checklist (it already sketches it; make it explicit).

### CI wiring (`.gitlab-ci.yml`)

- Existing `integration-test` job = Tier 1 (unchanged).
- New `scenario-test` job (docker-in-docker, same pattern as `integration-test`):
  runs `scripts/test.sh ci` over the **self-contained** scenarios:
  `polaris-keycloak-client-id`, `polaris-keycloak-user-token`,
  `polaris-ranger-keycloak`, `nessie`, `unity-oss`, `embedded-files`,
  `embedded-sqlite-catalog`, `attach-catalogs`, `quack`, `observability`,
  `benchmark`.
- **AWS** scenarios (`aws-glue`, `aws-s3-tables`, `glue-lake-formation`) need real
  cloud credentials. They are excluded from the default job and run via a
  separate manual/scheduled job gated on AWS secret variables; documented as
  local-run otherwise. The harness skips them unless `RUN_AWS_SCENARIOS=1` (or
  similar) is set.
- Cadence (full self-contained set per-MR vs a fast subset per-MR + full nightly)
  is a CI-config decision finalized in the plan; the default is: run the
  self-contained set on changes touching `quickstart/`, `crates/`, or the compose
  files, and nightly.

### Documentation home

`docs/site/book/src/development/testing.md` is rewritten as the canonical testing
guide: the two tiers, how to run each (`scripts/test.sh ...`), the scenario
catalog (each scenario, what it covers, self-contained vs cloud-gated), and how
`OUTPUT.md` + `--check` relate. The per-quickstart READMEs remain the
how-to-run-X source of truth (synced to docs.getsqe.com).

## Reconciliation summary (what changes)

| File | Change |
|---|---|
| `quickstart/_shared/lib.sh` | add `assert_contains`/`assert_not_empty`/`assert_query_*` |
| `quickstart/*/run.sh` (x14) | add `--check` mode + scenario invariants |
| `quickstart/*/README.md` | upgrade thin ones to the gold-standard structure |
| `quickstart/README.md` | document the README standard explicitly |
| `scripts/distributed-test.sh` | fold assertions into the scenario model; retire |
| `scripts/integration-test.sh` | keep as Tier 1; invoked via entry point |
| `scripts/test.sh` | NEW unified entry point (engine / scenario / ci) |
| `.gitlab-ci.yml` | add `scenario-test` job + gated AWS job |
| `docs/site/book/src/development/testing.md` | rewrite as the canonical testing guide |

## Decomposition (phases for the plan)

- **P1** Shared asserts in `_shared/lib.sh` + `run.sh --check` on 2-3 reference
  quickstarts (one Polaris, one embedded) + `scripts/test.sh` entry point.
- **P2** Roll `--check` across all self-contained quickstarts; fold
  `distributed-test.sh` into the scenario model and retire it.
- **P3** CI: `scenario-test` job (self-contained subset) + gated AWS job.
- **P4** Docs: upgrade all quickstart READMEs to the standard; rewrite
  `development/testing.md`.
- **P5** Verify: full `scripts/test.sh ci` green locally; CI green; docs build +
  leak-scan clean; docs.getsqe.com sync still resolves.

## Verification

- `scripts/test.sh scenario all` passes locally (self-contained scenarios).
- `scripts/test.sh engine` passes (Tier 1 unchanged).
- A deliberately broken invariant (e.g. disable a mask) makes the relevant
  scenario `--check` FAIL (proves the asserts bite).
- `make rustbook` builds; `make leak-scan` clean; quickstart READMEs render.
- The getsqe docs sync (`docs-website` reads `quickstart/*/README.md` + `OUTPUT.md`)
  still resolves with the upgraded READMEs.

## Rollback

File-additive on a feature branch (new `--check` paths, new entry point, new CI
job, doc edits). Rollback = revert the branch. `distributed-test.sh` is retired
only after its assertions are proven in the new model.

## Open items for the plan

- Exact `--check` invariants per quickstart (one careful read of each scenario).
- Whether `distributed-test.sh` becomes a `quickstart/distributed/` dir or a
  scenario entry reusing `docker-compose.distributed.yml`.
- CI cadence (per-MR subset vs full + nightly) and the AWS-secrets job shape.
- `scripts/test.sh` summary format + exit-code contract.
