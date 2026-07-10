# Use-Case Validation and Documentation for the Public Reveal

Date: 2026-06-03
Status: design (pending review)
Branch: `docs/use-cases-validation`

## Goal

SQE is going public (open source on `github.com/schubergphilis/sqe`). Before the
reveal, every headline use-case must be: documented with real usage + config,
validated by an actual run this round, and summarized in a single results table.
The engine already has deep integration-test coverage; this effort closes the
thin spots, surfaces what is hidden (Quack), and produces a reader-facing
"Use Cases" section plus a validation matrix backed by captured evidence.

This is not a feature build. It is documentation + targeted test gap-fills +
a validation pass.

## Scope (the 10 use-cases)

1. Trino-compat protocol, single server -> Apache Polaris
2. Trino-compat protocol, multi server (coordinator + workers) -> Polaris
3. Arrow Flight SQL, single server -> Polaris
4. Arrow Flight SQL, multi server -> Polaris
5. Alternative catalog backends: AWS Glue, AWS S3 Tables, Unity Catalog OSS
   (and HMS, Nessie covered alongside)
6. Single/embedded mode (sqe-cli) including s3tables/glue and the Hadoop
   filesystem catalog "without a catalog service"
7. Quack protocol: server (sqe-quack-server) and client (sqe-quack-client)
8. File-format TVFs: read_csv / read_parquet / read_json (read_delta if present)
9. Chameleon/SBP-specific SQL: GRANT / REVOKE (+ SHOW GRANTS / SHOW EFFECTIVE POLICY)
10. Benchmarks: TPC-H, TPC-DS, SSB (and the others sqe-bench supports)

## Decisions (locked with the user)

- Deliverable: improve the existing tests where thin, document usage + config
  per use-case, and produce a results table. (Not a from-scratch test suite.)
- Live backends: run live where credentials exist. Local `.env` provides
  `AWS_PROFILE=jacobbuilder` (never committed; `.env` is gitignored at
  `.gitignore:14`). All gated backends run live this round: Glue, S3 Tables,
  Unity OSS, HMS, Nessie.
- Structure: one combined branch + one MR.
- Chameleon split: GRANT/REVOKE stays in the public book but under a clearly
  marked "Chameleon / SBP-specific" callout, not removed.
- Book section name: "Use Cases" (grouped, ~7 pages).
- Validation matrix: curated markdown, evidence-backed (no new generator tooling).
- Live scope: all gated backends live.

## Current coverage baseline (from reconnaissance)

| # | Use-case | Existing coverage | Verdict |
|---|----------|-------------------|---------|
| 1 | Trino single | `sqe-coordinator/tests/integration_test.rs` (test_trino_http_query, type mapping); `scripts/trino-compat-test.sh`, `trino-parity-test.sh` | solid |
| 2 | Trino multi | `scripts/distributed-test.sh` (Trino :28080, worker dispatch via system.runtime.tasks) | solid |
| 3 | Flight single | `integration_test.rs` (test_authentication, test_simple_select, keycloak) | solid |
| 4 | Flight multi | `integration_test.rs::test_distributed_select` (#[ignore]); `distributed-test.sh` (Flight :60051) | solid |
| 5 | Glue/S3T/Unity/HMS/Nessie | `sqe-catalog/tests/backends_integration.rs` live tests (#[ignore] + env); compose: hms/nessie/unity | solid (needs live run) |
| 6 | Embedded | `sqe-cli/tests/cli_smoke.rs` memory only | THIN: no SQLite/Hadoop/cloud embedded tests |
| 7 | Quack | `quack_e2e.rs`, quack-server/client/wire tests; `docs/quack-protocol.md` exists | solid tests; NOT in book SUMMARY |
| 8 | TVFs | only `read_parquet` has an integ test (test_read_parquet_local_file) | PARTIAL: csv/json/delta untested |
| 9 | GRANT/REVOKE | policy rewriter + OPA tested; GRANT/REVOKE SQL not e2e | THIN |
| 10 | Benchmarks | `sqe-bench` + `benchmark-test.sh`/`benchmark-matrix.sh`; JSON results | solid (refresh numbers) |

Real "make it better" targets: #6 embedded, #8 TVFs, #9 GRANT/REVOKE, plus
surfacing #7 Quack in the book.

## Infrastructure facts

- Container runtime: Docker is installed (`/Applications/Docker.app/.../docker`)
  and a stack appears to be running locally (ports 8181, 9000, 8080, 50051).
- Compose files: `docker-compose.{test,distributed,hms,nessie,unity,spark,
  compare,observability,bench-2w,bench-4w}.yml`.
- Ports of record: Flight SQL internal 50051 (60051 external in compose),
  Trino HTTP 8080 (28080 in compose), workers 50052 (60061/60062 external),
  Quack in-process (ephemeral in tests; DuckDB default 9494).
- Env keys (`.env.example`): `AWS_PROFILE`, `AWS_REGION`,
  `SQE_TEST_GLUE_WAREHOUSE`, `SQE_TEST_S3TABLES_WAREHOUSE`, plus HMS/Nessie/
  Unity/PG keys with sensible localhost defaults.
- Bootstrap sequence: `docker compose -f docker-compose.test.yml [-f
  docker-compose.distributed.yml] up -d` then `scripts/bootstrap-test.sh`,
  then the relevant `scripts/*-test.sh`.

## Architecture of the deliverable

### A. mdBook "Use Cases" section (grouped, ~7 pages)

New top-level section in `docs/book/src/SUMMARY.md` after "Getting Started",
before or after "Deployment". Pages, each following one runbook template:

1. `use-cases/index.md` - what this section is + the validation matrix table.
2. `use-cases/flight-sql.md` - Flight SQL, single and distributed.
3. `use-cases/trino-http.md` - Trino HTTP compat, single and distributed.
4. `use-cases/quack.md` - Quack server + client (new book home; links to the
   existing `docs/quack-protocol.md` content, migrated/linked into the book).
5. `use-cases/catalog-backends.md` - Polaris, Glue, S3 Tables, Unity, HMS,
   Nessie, and the Hadoop/no-catalog filesystem warehouse. Short, links to
   `getting-started/catalogs.md` for the deep config reference.
6. `use-cases/embedded.md` - sqe-cli embedded/single-node, including the
   no-catalog filesystem mode and cloud-catalog embedded use.
7. `use-cases/file-format-tvfs.md` - read_csv/parquet/json/delta worked
   examples (links to `features/file-format-tvfs.md`).
8. `use-cases/benchmarks.md` - how to run TPC-H/DS/SSB + the latest numbers
   (links to `features/benchmarks.md`).

GRANT/REVOKE is documented in the existing `sql-reference/grant-revoke.md`
under a clearly marked "Chameleon / SBP-specific" callout, and linked from
the Use Cases index (not its own use-case page, to keep the split clean).

Runbook page template (every page):
- Purpose - one paragraph, what the scenario is and when to use it.
- Prerequisites - compose file(s), env vars, bootstrap command.
- Configuration - the minimal `sqe.toml` (or CLI flags) snippet.
- Run - exact copy-pasteable commands.
- Expected output - captured from the real run this round.
- How it is tested - link to the test file/fn or script that proves it.
- Notes / gotchas - auth requirements, port offsets, feature flags.

### B. Validation matrix (`use-cases/index.md`)

A single curated markdown table. Columns:

| Use-case | Protocol / Backend | Topology | Infra needed | How validated | Status | Latest result |

"How validated" links to the test or script. "Latest result" carries the
real datum from this round: benchmark timing, test pass-count, or a
round-trip confirmation, with the date. Every row backed by an actual run.

### C. Test gap-fills (the "better")

All new tests follow existing patterns in the same crates.

1. File-format TVFs (`sqe-coordinator/tests/`): mirror
   `test_read_parquet_local_file` to add `read_csv` and `read_json` local-file
   round-trips (generate fixture -> CTAS via TVF -> assert schema + rows).
   Add `read_delta` only if a fixture is feasible without new heavy deps; else
   document as covered-by-example. These are stack-gated like the parquet test.
2. Embedded mode (`sqe-cli/tests/cli_smoke.rs` or a new
   `embedded_catalog_test.rs`): SQLite-catalog round-trip (create -> reopen ->
   query) and a Hadoop/filesystem-catalog (`--catalog name=PATH`) smoke that
   reads a pre-seeded metadata.json with no catalog service. In-process where
   possible so CI can run them.
3. GRANT/REVOKE e2e (`sqe-coordinator/tests/`): a new test that issues
   `GRANT`/`REVOKE`, then `SHOW GRANTS` / `SHOW EFFECTIVE POLICY`, and asserts
   the parsed statement + resulting policy state. Uses the in-memory policy
   store so it runs without OPA/Cedar. Flagged in code as Chameleon-specific.

CI note: pure in-process additions (csv/json TVF where they can run without the
stack, embedded memory/SQLite, GRANT parse) join the `cargo test --workspace
--lib`/unit lanes. Live and docker-stack runs stay manual and documented, since
CI has no creds and dind is unavailable.

### D. Quack into the book

Add a `use-cases/quack.md` page and wire `docs/quack-protocol.md` +
`docs/quack-datatype-matrix.md` into `SUMMARY.md` (Features or Use Cases).
Include a runnable server + client example.

### E. Live validation runs (capture evidence)

Create local `.env` (gitignored) with `AWS_PROFILE=jacobbuilder`, `AWS_REGION`,
and the glue + s3tables warehouse vars. Then, capturing output for the matrix:
- Polaris single + distributed: compose up -> bootstrap -> Flight + Trino runs.
- Backends live: run the `#[ignore]` Glue + S3 Tables tests with creds; bring
  up Unity, HMS, Nessie compose stacks and run their live tests.
- Benchmarks: refresh at least TPC-H SF1 single-node (and distributed if quick)
  for current numbers.

## Build sequence

1. Branch + local `.env` + `.gitignore` confirm. Scaffold the Use Cases section
   and SUMMARY wiring with the page template (skeleton).
2. Gap-fill tests (TVF csv/json, embedded sqlite/hadoop, GRANT/REVOKE e2e);
   get the in-process ones green via `cargo test --workspace --lib`.
3. Live runs: Polaris single/multi (Flight + Trino), backends (Glue/S3T/Unity/
   HMS/Nessie), benchmarks. Capture outputs.
4. Fill each runbook page's "Expected output" + the validation matrix from the
   captured evidence.
5. Quack page + SUMMARY wiring. Chameleon callout on grant-revoke.
6. mdBook builds clean; voice-rule grep clean on the new prose; one MR.

## Out of scope

- New engine features. This is validation + docs only.
- A generated matrix harness (chosen: curated markdown).
- Trino-compat layer expansion beyond what exists.
- Refactoring unrelated code.

## Success criteria

- A "Use Cases" book section, ~7 runbook pages, each with captured real output.
- A validation matrix with every row backed by a run done this round.
- New tests for the thin spots (TVF csv/json, embedded sqlite/hadoop,
  GRANT/REVOKE e2e); in-process ones green in the workspace test lanes.
- Quack discoverable in the book.
- GRANT/REVOKE clearly marked Chameleon/SBP-specific.
- mdBook builds; new prose passes the voice-rule grep; one MR.

## Risks

- Live AWS (Glue/S3 Tables) depends on the `jacobbuilder` profile being valid
  in this environment; if a call fails, that row is documented as creds-gated
  with the prior evidence rather than blocking the MR.
- The local docker stack state is unknown; bootstrap may need a clean restart.
- `read_delta` may not be cheaply testable; treat as example-only if so.
- One combined MR risks growing large; keep test additions minimal and focused
  on the three thin use-cases, lean on links over duplicated prose.
