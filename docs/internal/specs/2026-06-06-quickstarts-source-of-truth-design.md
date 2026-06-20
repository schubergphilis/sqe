# Quickstarts as the User-Facing Source of Truth (and Validation Base)

Date: 2026-06-06
Status: design (pending review)
Branch: `feat/quickstarts`

## Goal

Today SQE's how-to material is scattered across three places: a single
top-level `QUICKSTART.md`, the doc-only `docs/book/src/use-cases/*` pages
(shipped from the 2026-06-03 spec), and the root `docker-compose.*.yml` +
`scripts/*-test.sh`. The user's complaint is exact: "it's all over the place."

Build a `quickstart/` tree where each use-case is a **self-contained,
runnable, validated directory**. Each quickstart is two things at once:

1. A **user-facing source of truth** ("I want to do X -> here is the exact
   path"), written for a new user, not a developer. The ebook and `docs/book`
   stay developer-oriented (the "why we built it this way" narrative); the
   quickstarts answer "how do I use it."
2. A **validation base**: running a quickstart's `run.sh` from a clean state
   proves the use-case works end to end and captures real output as committed
   evidence.

The quickstart READMEs are also the content source for getsqe.com.

## Relationship to the 2026-06-03 use-cases spec

This **supersedes the doc-only form** of `docs/superpowers/specs/2026-06-03-use-cases-validation-design.md`.
That effort produced narrative book pages for the same use-cases. This effort
makes the runnable directory the canonical artifact. The book's
`use-cases/*` pages and the top-level `QUICKSTART.md` are reduced to pointers
into `quickstart/`. The test gap-fills from that spec stay valid; in
particular the Keycloak auth tests it implied already exist at
`crates/sqe-coordinator/tests/integration_test.rs:1787+` (gated on
`SQE_TEST_KEYCLOAK_URL`) and become this effort's validation harness.

## Decisions (locked with the user)

- **Canonical source**: `quickstart/<name>/` dirs are the source of truth.
  Book `use-cases/*` pages and top-level `QUICKSTART.md` thin to pointers.
  getsqe.com syncs from the quickstart READMEs.
- **Compose style**: hybrid. Each dir has its own runnable `docker-compose.yml`
  but mounts bootstrap configs from `quickstart/_shared/` by relative path.
  `cd quickstart/<name> && ./run.sh` works standalone; bootstrap stays DRY.
- **AWS validation**: the user authorized `cdk deploy` + `cdk destroy` in the
  `jacobbuilder` account this session for the AWS batch. Teardown is confirmed
  after each run. (Not part of the reference build below; Batch B.)
- **Website**: produce repo-side READMEs with Astro frontmatter
  (`slug`, `title`, `description`) and sanitizable placeholder values. Do NOT
  touch the separate getsqe website repo or its sync script this session.
- **Filesystem**: rustfs everywhere (never MinIO) in new assets.
- **First deliverable this session**: the reference quickstart
  `polaris-keycloak-client-id/`, fully validated from clean, locking the
  template + `_shared/` layout before any fan-out.

## Directory layout

```
quickstart/
  README.md                        # index: table of all quickstarts + status + batch
  _shared/                         # DRY bootstrap assets, referenced by relative path
    keycloak/realm-sqe.json        # realm: client `sqe-client` + test users, placeholder secret
    polaris/bootstrap.sh           # derived from scripts/bootstrap-test.sh
    rustfs/                        # rustfs env/config if needed
    .env.example                   # shared defaults: offset ports, placeholder creds
    lib.sh                         # shared run.sh helpers (wait-for-health, capture, assert)
  polaris-keycloak-client-id/      # <- REFERENCE, built + validated this session
    README.md                      # frontmatter + why/how/config/run/output/tested/gotchas
    docker-compose.yml             # polaris + keycloak + rustfs; runnable standalone
    sqe.toml                       # the minimal annotated config
    run.sh                         # up -> wait -> bootstrap -> start sqe -> test -> capture -> (down)
    queries.sql                    # the useful test queries
    OUTPUT.md                      # captured real output (committed evidence)
  ... one dir per use-case (see catalog below) ...
```

Each quickstart dir is self-contained to *run* but reuses `_shared/` for the
fiddly bootstrap configs. Rejected alternatives: (a) fully duplicating
keycloak/polaris assets into every dir -- maximally portable but drifts out of
sync; (b) a shared base compose with `include:`/overlays -- most DRY but a new
user must understand the layering to read it. The hybrid matches the user's
phrasing: "a self-contained directory that can use shared assets for bootstrap
config."

## The runbook README template (repo doc + website page)

Every `quickstart/<name>/README.md` opens with Astro frontmatter, then follows
one fixed structure so the dirs feel like a series:

```
---
slug: polaris-keycloak-client-id
title: "Polaris + Keycloak (client credentials)"
description: "Run SQE against Apache Polaris with Keycloak as the IdP, SQE minting user tokens via the OIDC password grant."
---
```

Sections (each written for a new user, every option explained):

1. **Why / when** -- one paragraph: what this scenario is and when to reach for it.
2. **Prerequisites** -- what must be installed; nothing assumed.
3. **What you get** -- the compose services and what each is for.
4. **Configuration explained** -- the `sqe.toml` (or CLI flags) walked line by
   line: what each option does and what changes it.
5. **Run** -- exact copy-pasteable commands (`./run.sh`).
6. **Output** -- the captured real result (from `OUTPUT.md`).
7. **How it's tested** -- link to the test file/fn or the assertions in `run.sh`.
8. **Gotchas** -- ports, auth, feature flags, teardown.

All values are placeholders or sanitizable (no real account ids, ARNs, secrets)
so the getsqe.com leak gate stays green when the site later syncs.

## `run.sh` = the validation base

A per-quickstart script, sourcing `_shared/lib.sh`:

1. `docker compose up -d` (where applicable).
2. Wait for health (polaris `/q/health`, keycloak, rustfs) -- bounded retries.
3. Bootstrap: `_shared/polaris/bootstrap.sh` + keycloak realm import.
4. Start the engine (coordinator from `sqe.toml`, or embedded `sqe-cli`).
5. Run `queries.sql` via `sqe-cli`; assert expected row counts / values.
6. Where gated integration tests already cover the path (the Keycloak tests),
   export the needed env (`SQE_TEST_KEYCLOAK_URL`, `SQE_TEST_CLIENT_SECRET`)
   and run them `--ignored` as the real assertion.
7. Capture stdout into `OUTPUT.md` (committed evidence).
8. Tear down unless `--keep` is passed.

"Validate" therefore means: re-run `run.sh` from a clean state.

## Clean-state validation and ports

A live demo stack is already running locally (`sqe-*`, keycloak, polaris, opa,
openobserve) holding host ports 80, 443, 8000, 8080, 50051, 8181, 5080. The
reference quickstart must validate from clean, not against that stack. New
compose files use offset host ports (the repo convention: polaris 18181,
rustfs 19000, Flight 60051) and a keycloak port chosen to avoid collision; the
exact ports live in each dir's `.env` and are documented in the README. If a
collision is unavoidable, `run.sh` brings the conflicting project down first.

## The reference quickstart: `polaris-keycloak-client-id/`

The "client credentials" / ROPC path. SQE holds a confidential Keycloak client
(`sqe-client` + secret) and exchanges the user's username + password for a
bearer token via the OIDC Resource Owner Password Credentials grant against
Keycloak, then passes that token through to Polaris. This is the wiring the
existing tests use: `config.auth.keycloak_url` set, `config.auth.client_id =
"sqe-client"`, `config.auth.token_endpoint` cleared to force Keycloak ROPC mode
(`integration_test.rs:1792-1801`).

Shared assets this builds (reused by the rest of Batch A):
- `_shared/keycloak/realm-sqe.json`: realm with client `sqe-client`
  (placeholder secret `sqe-secret-change-me`), test user(s), and the
  password-grant flow enabled.
- `_shared/polaris/bootstrap.sh`: warehouse + namespace creation, derived from
  `scripts/bootstrap-test.sh` (parameterized, idempotent).
- Polaris configured to trust the Keycloak realm so a Keycloak-minted token is
  accepted for catalog access. The exact federation wiring is resolved during
  build and proven by the run; if Polaris cannot accept the Keycloak token
  directly, the README documents the actual token flow used.

Validation for this dir: `run.sh` brings up polaris+keycloak+rustfs, bootstraps,
starts the coordinator, runs `SHOW SCHEMAS` + a `SELECT`, and runs the gated
`test_keycloak_*` integration tests with `SQE_TEST_KEYCLOAK_URL` set. Captured
output lands in `OUTPUT.md`.

## The full quickstart catalog (grouped into themed batches)

| Batch | Quickstarts | Shared assets |
|---|---|---|
| **A. Catalog + Auth (local stack)** | polaris+keycloak (client_id) [REF], polaris+keycloak (user token), nessie, other REST catalogs (Unity OSS / generic REST) | keycloak realm, polaris bootstrap, rustfs |
| **B. AWS managed (CDK)** | s3tables, glue, glue + lake formation | one CDK app (deploy + destroy), jacobbuilder profile |
| **C. Embedded** | local + remote files, quack server, quack client, attach options, local sqlite catalog | file fixtures |
| **D. Ops** | monitor / logs / metrics | observability compose (VictoriaMetrics + Grafana) |
| **E. Benchmark** | TPC-H / TPC-DS / SSB | sqe-bench |

The two keycloak quickstarts differ only in auth flow:
- **client_id**: SQE's confidential client does the ROPC grant (above).
- **user token**: an upstream app mints the Keycloak user token; the client
  passes `--token <jwt>`; SQE validates it against the realm JWKS and passes it
  through. No client secret needed for minting.

## Build sequence

1. Branch `feat/quickstarts` (done). Scaffold `quickstart/` + `_shared/` and the
   index README.
2. Build `_shared/` assets: keycloak realm, parameterized polaris bootstrap,
   `.env.example`, `lib.sh`.
3. Build `polaris-keycloak-client-id/`: `docker-compose.yml`, annotated
   `sqe.toml`, `queries.sql`, `run.sh`, `README.md` skeleton.
4. Validate from clean: run `run.sh`, fix until green, capture `OUTPUT.md`.
5. Fill the README "Output" + "How it's tested" from captured evidence.
6. Thin `QUICKSTART.md` and `docs/book/src/use-cases/*` to pointers into
   `quickstart/`. Update the roadmap/nextsteps per CLAUDE.md "After Completing
   Work."
7. Voice-rule grep clean on new prose. One MR for the reference + scaffold.
8. (Subsequent plans/PRs) the remaining batches, each following the template.
   Batch B runs `cdk deploy`/`destroy` against jacobbuilder.

## Out of scope

- New engine features. This is docs + runnable scaffolding + validation.
- Touching the separate getsqe website repo or its sync script this session.
- Building all 13+ quickstarts this session -- only the reference is built and
  validated now; the rest are repeated application of the locked template.
- A generated docs/matrix harness -- the index README is curated markdown.

## Success criteria (this session)

- `quickstart/` + `_shared/` scaffold and the index README exist.
- `quickstart/polaris-keycloak-client-id/` runs clean via `./run.sh`, lights up
  the gated `test_keycloak_*` tests, and has captured real output in `OUTPUT.md`.
- The README is website-ready (frontmatter + leak-safe placeholders) and
  explains every config option for a new user.
- `QUICKSTART.md` and the book `use-cases/*` pages point into `quickstart/`
  instead of duplicating commands.
- New prose passes the voice-rule grep. One focused MR.

## Risks

- **Polaris <-> Keycloak token trust**: SQE minting a Keycloak token is proven;
  whether Polaris accepts that token directly for catalog access depends on
  Polaris federation config. Resolved during build and proven by the run; the
  README documents the actual flow. This is the main technical unknown.
- **Port collisions** with the running demo stack -- mitigated by offset ports
  and a documented clean-state run.
- **AWS batch** (later) creates billable resources; teardown is confirmed each
  run, and the CDK app is the single point that creates and destroys them.
- **Scope creep**: the catalog is large; discipline is to lock the template on
  one reference before fanning out.
