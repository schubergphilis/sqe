# Build and test time program

Date: 2026-09-04
Status: proposal, measured baseline attached
Owners: SQL Engine team

## Objective

Cut the time between "I changed a line" and "I know it is green", locally and
in CI, without losing coverage. Every number below was measured on 2026-09-04
against `main` at `17c85f0f` unless a job id says otherwise. Raw data
(timings HTML, CI traces, feature fingerprints) lives in the session
scratchpad and is summarised here; re-measure with the protocol at the end.

## 1. Where the time goes today

### 1.1 Local (M3 Pro, 11 cores, 36 GB, `jobs = 8`, `ld64.lld`)

| Measurement | Result |
|---|---|
| Cold `cargo test --workspace --no-run` (default features) | 8m14s wall, 2357 CPU-seconds, 24 GB of artifacts |
| Average parallelism during that build | 4.8 of 8 allowed jobs; 189 of 494 seconds had 2 or fewer units running |
| No-op `cargo test --workspace --no-run` | 6 s |
| Touch `sqe-core/src/lib.rs`, rebuild all workspace tests | 1m46s |
| Touch `sqe-coordinator/src/lib.rs`, rebuild all workspace tests | 31 s |
| Same touch, `cargo test -p sqe-coordinator --no-run` | **5m50s** (797 CPU-seconds) |
| Same touch, `cargo test -p sqe-coordinator --lib --no-run` | 11 s |
| `target/` before cleanup | 147 GB on a full disk (5 GB free); 65 GB `debug`, 77 GB `dev-release` of which 49 GB `incremental` |
| Distinct compiled copies of `datafusion` / `iceberg` in `target/debug/deps` | **38** each; 320 `sqe_coordinator` artifacts |
| `cargo sweep --time 2` | freed 67 GB |

Where the 2357 CPU-seconds of the cold build go:

| Bucket | Units | CPU-seconds |
|---|---|---|
| Third-party crates (incl. vendored iceberg, DataFusion) | 688 | 1113 |
| First-party libs | 19 | 285 |
| First-party bins | 6 | 145 |
| First-party lib unit-test harnesses | 19 | 285 |
| First-party bin unit-test harnesses | 7 | 72 |
| **Integration test files (`tests/*.rs`), one binary each** | **77** | **454** |

First-party code is 53 percent of the cold build, and the 77 integration test
binaries alone cost more than every first-party library. By crate: coordinator
130 s (16 files), catalog 125 s (23 files), lineage 73 s (15 files for a 1756
line crate), policy 36 s, worker 34 s.

Critical path of the cold build (508 s of 493 s wall, so the build is
path-bound, not core-bound):

```
aws-lc-sys build script            95 s   (starts at 19 s, nothing TLS-dependent can start before 115 s)
rustls -> reqwest -> object_store -> parquet -> datafusion-* chain   ~150 s
datafusion                         40 s
sqe-policy                         40 s
sqe-catalog                        64 s
sqe-worker                         36 s
sqe-coordinator lib                67 s
sqe-coordinator bin                43 s
```

The last five first-party crates form a strictly serial chain because
`sqe-catalog` depends on `sqe-policy` (three symbols: `PolicyStore`,
`plan_rewriter::HiveCatalogs`, `grants::*`) and `sqe-coordinator` depends on
`sqe-worker` (six symbols: flight service header, `runtime::build_session_context`,
credential channel, compaction, bootstrap).

### 1.2 Feature unification is the reason for the 38 copies

Resolver 2 unifies features across the packages you select, so every distinct
selection resolves a different feature set for low-level crates and gets a
different artifact hash for the whole graph above them. Measured with
`cargo tree -e features`:

| Selection | Resolved dependency features |
|---|---|
| `--workspace` | 1977 |
| `--workspace --all-features` (CI gate, `make clippy`) | 2098 |
| `-p sqe-coordinator` | 1801 (differs from `--workspace` in `tracing/log`, `libc/extra_traits`, `hashbrown`, `tonic/tls-native-roots`, `clap`, `rand`) |
| `-p sqe-catalog` | 1645 |
| `-p sqe-bench -p sqe-coordinator` (every benchmark script) | 1362 (differs in `tokio`, `bytes`, `serde_json`, `arrow`, `datafusion`) |
| `-p sqe-worker` | 1660 |
| `-p sqe-cli` | 1844 |

`tracing` sits under everything, so a different `tracing` feature set means a
different DataFusion. That is why `cargo test -p sqe-coordinator` after a
one-line change took 5m50s while the same change through `--workspace` took
31 s, and why the bench scripts, `make test`, `make clippy` and the CI gate
each keep their own copy of the stack.

### 1.3 Test execution is not the problem

`cargo nextest run --workspace -j 6` on the same laptop, default features,
ignored tests excluded:

| Measurement | Result |
|---|---|
| Tests run | 5031 passed, 242 skipped (the `#[ignore]` docker suites) |
| Wall clock | 67.6 s |
| Sum of per-test durations | 395 s |
| Median / p90 / p99 / max | 0.017 s / 0.04 s / 1.24 s / 9.7 s |
| Tests over 1 s | 54 tests, 238 s (60 percent of the total) |
| Tests over 5 s | 24 tests, 160 s |

The heavy tail is concentrated: `sqe-worker` flight-service signature tests
(9 to 10 s each, three of them), the two worker memory-budget scans, the
TPC-DS generator tests in `sqe-bench`, and `sqe-cli` smoke tests that spawn
the binary (6 s each). `sqe-bench` runs its 224 generator unit tests twice,
once in the lib harness and once in the `bin` harness, because the binary
includes the same modules; that is 66 s of duplicated execution and a
duplicated compile. Everything else is noise. Speeding up test execution buys
at most a minute; the compile, link and CI topology buy an hour.

### 1.4 CI (GitLab shared runner 2432, docker executor, amd64, size unknown)

Successful pipelines, 2026-09-01 to 2026-09-04:

| Pipeline | Ref | Total | `aikido-test-rust` | `aikido-build` |
|---|---|---|---|---|
| 1600968 | main | 79m34s | 34m42s | 42m28s |
| 1600377 | main | 108m15s | 38m21s | 67m03s |
| 1601474 | MR 926 | 72m10s | 30m25s | 38m38s |
| 1600806 | MR 919 | 170m30s (failed) | 48m11s | 115m34s |

The pipeline is a straight line: `.pre` guardrails (1 to 2 min) -> `test`
stage (`aikido-test-rust`) -> `.post` (`aikido-build`, then five sub-minute
jobs that `need` it). Two jobs are 95 percent of wall clock and the image build
does not start until the tests finish. 22 of the last 25 main pipelines were
auto-cancelled by a newer push; 166 MRs merged in the last 30 days.

Inside `aikido-test-rust` (job 5284525, main, 34m42s):

| Phase | Time | Note |
|---|---|---|
| Cache restore | 7 s | Restores a stale cache; see below |
| `cargo audit`, `cargo deny` | 22 s | advisory |
| `cargo test --workspace --all-features --no-run` | **12m45s** | 21m51s for job 5283611 that ran during a burst of 6 concurrent Rust jobs |
| `cargo test --workspace --all-features` (execute, incl. doc-tests) | **2m00s** | the actual tests |
| `cargo clippy` | 6 s | fails on `sqe-core` (`obfuscated_if_else`, pre-existing on main); advisory, so zero lint signal |
| `ci/aikido-rust-extra.sh` (6 x `cargo check -p sqe-catalog` feature variants) | **7m49s** | inside the blocking job |
| `archive_cache` | **10m19s** | zips `target/`, upload fails `EntityTooLarge` (over 5 GiB) on every run |

Consequences of the cache design: the `target/` zip never uploads, so every
run pays 10 minutes to build an archive that is thrown away, and the restored
cache is whatever last fit under the limit. The key is
`aikido-test-rust-$CI_COMMIT_REF_SLUG`, so a new MR branch has no cache at all.
Net: roughly 35 percent of the gating job is compile, 6 percent is tests, and
the rest is overhead that adds no signal.

Inside `aikido-build` (job 5284529, 42m28s): kaniko v1.23.2 ignores
`RUN --mount=type=cache`, so the four BuildKit cache mounts in `Dockerfile`
are no-ops. `cargo chef cook` compiles the dependency graph in release for
9m20s with sccache reporting 0 hits and 923 misses, then the builder stage
compiles the workspace with thin LTO for 28m25s, plus about 4 minutes of
filesystem snapshots. This runs on every MR pipeline too, as a `--no-push`
validation build, after the tests. kaniko itself was archived by Google on
2025-06-03; GitLab documents rootless BuildKit as the replacement.

## 2. Root causes, ranked by wall-clock impact

1. **Serial CI topology.** Image build waits for tests; both are 20 to 60 minutes.
2. **Cache that cannot upload.** 10 minutes per job wasted, compile always near-cold.
3. **Feature-set churn.** Five different selections in daily use, each a full
   copy of the DataFusion stack, locally and in CI.
4. **102 link units.** 77 integration test files; consolidation into
   `tests/it/main.rs` was done once (3.8x measured) and has unwound.
5. **Compiling in the image build.** Twice (cook + build), in release with
   thin LTO, on a shared runner, per MR.
6. **`aws-lc-sys` on the critical path.** 95 s of C compilation before any
   TLS-dependent crate can start, while `ring` is already in the tree.
7. **Serial first-party chain.** policy -> catalog -> worker -> coordinator,
   about 250 s that cannot overlap.
8. **Broken advisory gates in the blocking job.** clippy dead on main, 8 min
   of feature-matrix checks serialised behind the tests.
9. **Local disk hygiene.** 147 GB target, 49 GB of `dev-release` incremental
   state, Defender scanning every artifact.

## 3. The plan, in pieces

Each piece is one focused MR. Gains are estimates from the measurements above
and are to be re-measured, not assumed.

### Phase A: days, no code-structure change

| # | Change | Expected effect | Risk |
|---|---|---|---|
| A1 | `aikido-build` runs in parallel with the test stage (`needs: []` or its own stage), and on MRs only when `Dockerfile*`, `Cargo.lock` or `deploy/**` change. | MR pipeline 72 min -> about 35; main 80 -> about 45. | Image validation on MR becomes conditional; main still builds every commit. |
| A2 | Replace the `target/` zip cache. Either (a) `sccache` with the GitLab S3 bucket already used for caches (`SCCACHE_BUCKET`, `SCCACHE_ENDPOINT`), `CARGO_INCREMENTAL=0`, no `target/` in `cache:`; or (b) keep `cache:` but key it on `files: [Cargo.lock, rust-toolchain.toml]` with `fallback_keys`, `policy: pull` on MRs, and shrink it under 5 GiB with `CARGO_PROFILE_TEST_DEBUG=0` and `cargo sweep` before archiving. Prefer (a). | Removes 10 min of failed archiving per job; compile phase 12 to 22 min -> 4 to 8 min on warm cache. | sccache hit rate depends on identical checkout paths; GitLab uses a fixed `/builds/...` path so this is fine. |
| A3 | Drop `aws-lc-rs`: `reqwest` feature `rustls-no-provider` instead of `rustls`; `rustls = { default-features = false, features = ["ring", "std", "tls12"] }` in the coordinator; `aws-smithy-runtime` / `aws-config` with `rustls-ring`. Install `ring` as the process default provider once at each binary's start (coordinator already does). | 95 s off the cold critical path, removes cmake/clang need for the crypto build, one crypto provider instead of two. | Provider must be installed before the first TLS handshake in every binary and in tests that hit HTTPS (wiremock is plain HTTP). |
| A4 | Fix the two clippy failures on main so the advisory lint reports something; move `ci/aikido-rust-extra.sh` into its own parallel job. | 8 min off the blocking job; lint coverage restored. | None. |
| A5 | Local hygiene: `make maintain` documented as weekly; `cargo sweep --time 7` in it; `docker system prune` note; Defender exclusion request for `target/` and `~/.cargo`. | Stops the full-disk failures; measured 67 GB reclaimed. | Needs IT for the Defender exclusion. |
| A6 | Stop `sqe-bench` compiling and running its unit tests twice: the binary should depend on the lib (`use sqe_bench::...`) instead of `mod`-including the sources. | One fewer 224-test harness to compile (about 25 CPU-seconds) and 66 s less test execution. | None. |
| A7 | Align `RUST_VERSION` in the three Dockerfiles with `rust-toolchain.toml` (1.98.0 vs 1.97.1 today) and make the CI test image follow the same pin. | No time gain; removes a correctness gap. | None. |

### Phase B: one to two weeks, structural

| # | Change | Expected effect | Risk |
|---|---|---|---|
| B1 | **cargo-hakari workspace-hack crate.** Every selection then resolves the same features. Add `cargo hakari verify` to CI. | Kills the 38-copy churn; `cargo test -p sqe-coordinator` after a touch 5m50s -> about 35 s; bench scripts, `make test`, `make clippy` and CI share one dependency build. | Publishing crates gets a small wrinkle (hakari has a documented disable-on-publish step). |
| B2 | **Re-consolidate integration tests** into one `tests/it/main.rs` per crate: coordinator 16 -> 3 (keep the two process-isolation standalones), catalog 23 -> about 3 (`#![cfg(feature)]` file gates become `#[cfg] mod` lines), lineage 15 -> 1 (one-time insta snapshot rename). | 77 -> about 15 link units, roughly 350 CPU-seconds off every cold build and every dependency-touching incremental; the 2021 Cargo precedent is 3x faster test compile. | Snapshot renames are noisy in one MR; do lineage separately. Shared harness means a panicking test can take siblings with it, which is why the two standalones stay. |
| B3 | **cargo-nextest** with `.config/nextest.toml`: test groups with `max-threads = 1` for the port-bound suites, `slow-timeout`, JUnit report into GitLab. In CI: `cargo nextest archive` in the compile job, `nextest run --archive-file` in one or more shard jobs. | Execution phase 2 min -> under 1 min; failures visible per test in the MR UI; sharding available if the suite grows. | Doc-tests are not run by nextest; keep `cargo test --doc` (about 40 s) in the compile job. |
| B4 | **Stop compiling inside the image build.** A `cargo build --profile <p> --bin ...` job (sccache-backed, parallel to tests) publishes the four binaries as artifacts; the image job is a `COPY`-only Dockerfile. Move off kaniko to rootless BuildKit or buildah while doing it. | `aikido-build` 42 to 67 min -> about 3 min, and it no longer waits for tests. | Dockerfile split (builder stage stays for local `docker build`); base-image digest pinning unchanged. |
| B5 | **Right-size the CI profile.** MR validation binaries use `dev-release` (no LTO, 16 codegen units); `release` with thin LTO only on main and tags. | The 28 min release link on MRs becomes about 8 min. | MR image is not byte-identical to the release image; it never was published anyway. |

### Phase C: weeks, architecture

| # | Change | Expected effect | Risk |
|---|---|---|---|
| C1 | Break the serial chain: move the three `sqe-policy` items that `sqe-catalog` needs (the `PolicyStore` trait, `HiveCatalogs`, grants types) into `sqe-core` or a tiny `sqe-policy-api` crate; extract the six `sqe-worker` items the coordinator uses into a `sqe-worker-api` crate. | policy and catalog compile in parallel, worker and coordinator compile in parallel: about 75 s off the cold critical path, more on incremental touches to policy. | Trait moves touch many files once. |
| C2 | Split `sqe-coordinator` (64K lines, 67 s lib + 43 s bin + 16 test binaries). The write path extraction plan already exists (`2026-07-13-sqe-write-crate-extraction.md`); hive and maintenance are the next candidates. | Coordinator lib compile roughly halves; touches to write code stop rebuilding the whole coordinator. | Large refactor; do after B1 and B2 so the gain is measurable. |
| C3 | Runner capacity: a dedicated runner with a persistent `target/` on local disk, or a larger shared class. Runner 2433 (`docker_socket`) already exists and would also let `make test-integration` return to CI. | The GitLab-based precedent for this shape went from 20 min to under 5. | Platform team ownership; not in this repo. |

### Progress log

- 2026-09-04, A1 (MR !928, `ci/parallel-image-build`): image build in
  parallel with the test gate on MRs, `target/` removed from the CI cache,
  feature-matrix checks in their own job. Platform side: aikido !155
  (merged) gives the gate `needs: []`, `CARGO_INCREMENTAL=0` and an
  `aikido-rust-extra` job; consumable once a v0.12.0 tag is cut through the
  manual `create-pipeline-release` job.
- 2026-09-04, B1 (MR !929, `build/workspace-hack`): cargo-hakari crate plus
  `sqe-catalog` default features aligned with the workspace union. After a
  one-line coordinator change: `cargo test -p sqe-coordinator --no-run`
  5m50s to 35 s; `cargo test -p sqe-catalog --no-run` 24 s; the bench
  scripts' `cargo build -p sqe-bench -p sqe-coordinator` 30 s; cold
  workspace build 8m25s (was 8m14s, noise). Two things learned: hakari
  unifies the none, default and all feature sets of every member, so the
  optional backends have to be kept out with `final-excludes` and three
  lines pinned by hand; and first-party crates are outside hakari's reach,
  so a member that another member enables extra features on (sqe-cli on
  sqe-catalog) has to carry those features in its default set.
- 2026-09-07, A2 (aikido !158 released as v0.12.1; sqlengine MR from
  `ci/sccache-compiler-cache`): option (a). The 5 GiB is the S3 single-PUT
  limit behind the runner's presigned upload, so option (b) was dropped for
  good. The platform template gained `.aikido_rust_build_env` (sccache to
  S3, off until `AIKIDO_SCCACHE_BUCKET` is set; protected read-write and
  unprotected read-only key pairs pick the mode; credentials reach only the
  sccache server; `dev`/`test` debuginfo `line-tables-only` by default;
  `after_script` prints stats and a `/cache` persistence probe). sqlengine
  points at bucket `sqe-cargocache` (StorageGRID acc) through project CI
  variables and adds the block to `release-binaries`. Measured before, main
  pipeline 1606171: test job 27 min (7.5 setup, 12 compile, 2 tests, 3.8
  clippy), extra job 21.5 min (4.4 zipping the registry), release binaries
  43.5 min (28 thin-LTO link), `target/` 37 GiB. Open: lifecycle rule on
  the bucket (tenant admin), read-only key, reachability of the acc
  endpoint from the production runners.

## 4. Expected end state

| Loop | Today | After A | After B | After C |
|---|---|---|---|---|
| MR pipeline, wall | 72 min | about 35 | about 15 | about 10 |
| main pipeline, wall | 80 to 108 min | about 45 | about 20 | about 15 |
| `cargo test -p sqe-coordinator --no-run` after a one-line change | 5m50s | 5m50s | about 35 s | about 20 s |
| Cold `cargo test --workspace --no-run`, laptop | 8m14s | about 6m30s | about 5 min | about 4 min |
| Local disk | 147 GB, full | bounded by sweep | one dependency copy | same |

## 5. What not to spend time on

- Parallel rustc frontend and Cranelift: nightly only on 1.98; not a 2026 option for this toolchain.
- mold on macOS: unsupported. `ld64.lld` is already configured and is the right macOS linker.
- sccache on the laptop: measured 3 percent hits on the Defender-scanned filesystem; leave it disabled locally, use it in CI.
- Caching `target/` as a GitLab zip: the archive is over the 5 GiB limit and each attempt costs 10 minutes.
- Bazel or Buck2: no; cargo is not the bottleneck, the topology is.

## 6. Measurement protocol

Run before and after each MR in this program:

```
# cold build of all test binaries with a timings report
cargo clean && cargo test --workspace --no-run --timings
# incremental fan-out
touch crates/sqe-core/src/lib.rs && time cargo test --workspace --no-run
touch crates/sqe-coordinator/src/lib.rs && time cargo test -p sqe-coordinator --no-run
# feature-set drift between selections (should collapse to one hash after B1)
for s in "--workspace" "-p sqe-coordinator" "-p sqe-bench -p sqe-coordinator"; do
  cargo tree -e features --prefix none ${=s} | grep ' feature "' | sort -u | md5
done
# CI: read the job trace section timestamps (restore_cache, step_script, archive_cache)
```
