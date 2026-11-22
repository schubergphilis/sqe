## Why

SQE's row in the Iceberg compatibility matrix (`docs/evidence/iceberg-matrix-state.json`, 159 of 162 against the upstream rubric) is backed by evidence that only this repository can check: Rust integration tests that reach into SQE internals. The upstream matrix ([Neuw84/iceberg-matrix](https://github.com/Neuw84/iceberg-matrix)) verifies its engines differently. Each engine has a `tests/<engine>_feature_tests.py` that runs black box against a shared Polaris + RustFS stack, one test per feature, and reports whether the published rating holds.

A score that an outsider cannot reproduce is a claim, not a measurement. This change adds an SQE suite in the upstream shape. It talks to a running SQE only through its client protocols, and the Iceberg tables are proven by a second reader. Anyone can then run it, us included, and our own number becomes something we re-measure instead of something we maintain by hand.

## What Changes

- **`tests/matrix/sqe_feature_tests.py`**: one test per rubric cell (32 features, 54 version cells). It connects over the Trino HTTP protocol and Flight SQL, and uses upstream's environment variables (`ICEBERG_REST_*`, catalog `demo`, scope `PRINCIPAL_ROLE:ALL`) plus `SQE_HOST`, `SQE_PORT` and `SQE_IMAGE`.
- **Second witness**: write cells are confirmed by reading table metadata, manifests and delete files with PyIceberg, and from the raw metadata, manifests and Parquet / Puffin files where PyIceberg cannot read the v3 feature (a Spark cross-check follows). SQE reading back what SQE wrote does not count.
- **`tests/matrix/docker/`**: `docker-compose.sqe.yml` plus `start-sqe.sh` and `stop-sqe.sh`. These run SQE beside an upstream-style Polaris 1.x + RustFS stack, with its own compose project and ports so they do not collide with the dev stacks.
- **Reports**: JSON and Markdown, with a verdict per cell, the claimed level from the state file, the measured level, and a discrepancy list. The run exits non-zero on any discrepancy, error, or rubric cell without a test.
- **`tests/matrix/export_upstream.py`**: converts `iceberg-matrix-state.json` (singular `platform`) to the upstream engine file shape (`{"platforms":[...],"support":{...}}`), so a submission PR is generated, not hand-written.
- **Make target**: `make matrix-suite` brings the stack up, runs the suite against a given image and writes the reports to `target/matrix-suite/`.

Not changed: the state file's ratings. A discrepancy the suite finds is reported for a maintainer to decide on. It is never auto-applied, and a test is never adjusted to keep a score.

## Capabilities

### New Capabilities
- `iceberg-matrix-feature-suite`: a black-box, upstream-shaped feature suite that measures SQE against the matrix rubric and reports agreement with the published ratings.

### Modified Capabilities
<!-- iceberg-matrix-submission (in iceberg-matrix-parity) is not changed; this suite supplies the evidence it links to. -->

## Impact

- New Python-only code under `tests/matrix/`. No Rust changes, so no cargo build is needed to run it.
- Docker images: Polaris 1.x, RustFS, the SQE image under test, and optionally Spark for v3 witnesses.
- Open decision (maintainer): **how an outsider obtains an SQE image.** Harbor `chameleon/sqe` refuses anonymous pulls, and the public GitHub release is at v0.48.1. The options are a public registry project, publishing to a public registry with each release, or building from the public mirror. Until this is decided the suite is runnable by anyone with registry access or a source checkout (`SQE_IMAGE` takes a local tag).
