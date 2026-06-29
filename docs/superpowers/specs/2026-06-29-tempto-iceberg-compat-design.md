# Tempto-based Trino compatibility test for Iceberg

Status: approved (design), pending implementation plan
Date: 2026-06-29
Branch: `test/tempto-iceberg-compat`

## Goal

Exercise SQE's Trino wire-protocol mode (the `sqe-trino-compat` HTTP endpoint) against
the official `trinodb/tempto` test framework, using the upstream `trino-product-tests`
Iceberg suite, to measure how Trino-compatible SQE is when querying Iceberg tables.

Success = a repeatable local command that brings up SQE, runs a curated set of upstream
Iceberg product-tests against it, and reports pass/fail with documented exclusions.

## Decisions (settled during brainstorming)

| Decision | Choice |
|---|---|
| Test source | Upstream `trino-product-tests` Iceberg group (real tempto tests) |
| Run mechanism | Pinned `trinodb/trino` repo @ tag **465** (matches compare-stack image), build the product-tests jar, run via tempto's runner |
| Stack | Reuse `docker-compose.test.yml` + `docker-compose.compare.yml` (SQE on host port 28080) |
| Auth | **Basic `root:s3cr3t` over a TLS proxy** (overrides initial "Bearer" pick) |
| Failure handling | Curated allow-list suite + documented exclusion list (green = real pass) |
| Delivery | Local `scripts/tempto-test.sh` + docs page; no CI wiring yet |

### Auth rationale (override of "Bearer token" pick)

Direct evidence drove this:
- `crates/sqe-trino-compat/src/server.rs` decodes HTTP **Basic** `user:pass`.
- Test `[auth]` in `tests/distributed/coordinator.toml` only configures a Polaris
  `token_endpoint` (used to *exchange* Basic creds for a Polaris token), not inbound
  JWKS validation. A Polaris Bearer token would not be validated by SQE in this stack.
- The existing `scripts/trino-compat-test.sh` already authenticates with `-u root:s3cr3t`.

The Trino JDBC driver (which tempto uses) refuses to send **either** a password or an
`accessToken` over a non-TLS connection. TLS is therefore required regardless of which
credential we use. Given that, Basic-over-TLS reuses SQE's proven auth path. The user's
real intent (TLS + a real credential, not anonymous/user-only) is satisfied.

## Coverage reality

The upstream Iceberg group is dominated by Spark/Hive cross-engine tests that set up
tables via `onSpark()` / `onHive()` and cannot run against SQE alone. Verified against
tag-465 sources:

| Test class | onTrino | onSpark | onHive | Runnable vs SQE |
|---|---|---|---|---|
| `TestIcebergInsert` | 4 | 0 | 0 | yes |
| `TestCreateDropSchema` | 9 | 0 | 0 | yes |
| `TestIcebergPartitionEvolution` | 11 | 1 | 0 | mostly |
| `TestIcebergOptimize` | 12 | 1 | 0 | if `OPTIMIZE` supported |
| `TestIcebergProcedureCalls` | 73 | 10 | 2 | partial (procedures) |
| `TestIcebergSparkCompatibility` | 381 | 308 | 2 | no (needs Spark) |
| `TestIcebergFormatVersionCompatibility`, `TestIcebergRedirectionToHive`, `TestIcebergHiveViewsCompatibility`, `TestIcebergHiveMetadataListing` | mixed | mixed | mixed | excluded (Hive/Spark coupled) |

The runnable core is a curated pure-Trino set: INSERT, schema DDL, partition evolution,
and the runnable parts of optimize/procedures. It grows as SQE gains features. This is
why the "curated allow-list + exclusions" model fits: it makes the runnable surface and
the gaps both explicit.

## Update (reuse pivot, 2026-06-29)

Two findings simplified the build and are now load-bearing:

1. **Catalog is already `iceberg`.** The upstream tests hardcode catalog `iceberg`.
   SQE registers the single `[catalog]` block under
   `Config::LEGACY_CATALOG_NAME = "iceberg"` (`crates/sqe-core/src/config.rs:2941`)
   regardless of the Polaris `warehouse` value, and the compare-stack Trino catalog
   is named `iceberg`. So the existing compare stack already serves catalog `iceberg`
   on both engines. No new Polaris warehouse, bucket, or bootstrap is needed; reuse
   `test_warehouse` and `scripts/bootstrap-test.sh`.
2. **Layer on the existing parity stack.** Compose
   `docker-compose.test.yml + docker-compose.compare.yml + docker-compose.tempto.yml`.
   The overlay adds only `tls-proxy` (caddy) and `tempto-runner`, and remounts a
   single-node SQE config (the compare stack mounts the distributed `coordinator.toml`
   that expects workers). Pointing tempto at the compare stack's real Trino gives a
   harness-sanity baseline for free (`--baseline`).

## Architecture

```
docker-compose.test.yml + docker-compose.compare.yml   (existing: SQE Trino HTTP on 28080)
  + docker-compose.tempto.yml overlay:
      tls-proxy        caddy, self-signed cert: https://tls-proxy:8443 -> sqe:8080
      tempto-runner    JVM container:
                         - trino @ tag 465, prebuilt product-tests jar (cached)
                         - testing/tempto/tempto-configuration.yaml
                         - testing/tempto/suite-iceberg.xml  (curated includes)
                         - testing/tempto/exclusions.md       (documented skips)
```

Why a TLS proxy instead of native TLS: `sqe-trino-compat` has no native TLS listener
(grep confirms only test-only `https` references). A self-signed caddy/nginx terminator
in front of port 8080 is the smallest change that satisfies the JDBC driver's SSL gate.

## Components

1. `scripts/tempto-test.sh` — orchestrator. Brings up the stack, waits for SQE
   `/v1/info`, ensures the trino@465 product-tests jar is built/cached, runs the tempto
   runner against the curated suite, prints a pass/fail/skip summary. Flags for category
   filtering and verbose output.
2. `testing/tempto/tempto-configuration.yaml` — one `databases` entry: the Trino JDBC
   connection pointing at the TLS proxy with `user=root`, `password=s3cr3t`, `SSL=true`,
   `SSLVerification=NONE` (self-signed). Catalog/schema set to the SQE test warehouse.
3. `testing/tempto/suite-iceberg.xml` — TestNG suite (or `--groups iceberg` + `--tests`
   includes) enumerating the curated allow-list of runnable test classes/methods.
4. `testing/tempto/exclusions.md` — every excluded class/method with a reason
   (needs Spark, needs Hive, unsupported procedure, known SQE gap + tracking pointer).
5. `docker-compose.tempto.yml` — the `tls-proxy` and `tempto-runner` services + the
   self-signed cert material (generated on first run, gitignored).
6. `docs/internal/...` (working-history zone) page: how to run, what is covered, how to
   add a test to the allow-list, how to record an exclusion.

## Data flow

tempto reads config -> opens Trino JDBC to the TLS proxy -> proxy terminates TLS and
forwards Basic creds to SQE `:8080` -> SQE exchanges creds with Polaris and runs the
query against Iceberg (Polaris REST catalog + S3 storage) -> results return over the
Trino wire protocol -> tempto asserts expected vs actual.

## Failure handling

- The curated suite is the gate: any failure in an allow-listed test fails the run.
- Excluded tests are skipped explicitly and documented in `exclusions.md` with reasons.
- Output summary distinguishes pass / fail / skipped(excluded) so green means real pass.

## Open risk to validate first (implementation spike)

The exact standalone runner entrypoint at tag 465 is unconfirmed: TestNG suite XML vs
`io.trino.tempto.runner.TemptoRunner` main vs the product-tests launcher. The first
implementation task is a spike that runs a single trivial test against SQE to lock down
the runner invocation before building the full harness. If the standalone runner is not
viable at 465, fall back to vendoring the runnable convention/Java tests into
`testing/tempto/` and running them with a thin `tempto-runner` jar.

## Out of scope (YAGNI)

- CI pipeline wiring (later).
- Spark/Hive cross-engine tests (excluded by design).
- Full Keycloak + production TLS stack (test stack only).
- Generating a historical trend report (single run summary for now).
