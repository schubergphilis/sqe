# Independent Iceberg feature-matrix suite

This suite measures SQE against the public Iceberg feature matrix without trusting
SQE's own claims. It starts a throwaway Polaris catalog, an S3-compatible object
store (RustFS) and the SQE image you name, runs one test per matrix cell, and
writes a report that says where the measured level and the claimed level differ.

It is a black box. The tests talk to SQE only through its client protocols (Trino
HTTP, and Flight SQL where a feature needs a session). They then check what really
landed on storage with readers that are not SQE: PyIceberg, the catalog's raw
`loadTable` JSON, raw Avro manifests, and Parquet and Puffin files read directly.
SQE reading back its own write is a cross-check only, never the proof.

## Quickstart

You need Docker, Python 3.11 or newer, and an SQE image (a published one, or one you
built).

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r tests/matrix/requirements.txt

make matrix-suite SQE_IMAGE=<your-sqe-image>
```

`make matrix-suite` starts the stack, runs the suite, and always stops the stack,
even when the suite fails. Its exit code is the suite's. Reports land in
`target/matrix-suite/` (`report.md` for people, plus a JSON file). Without `make`:

```bash
export SQE_IMAGE=<your-sqe-image>          # required
tests/matrix/docker/start-sqe.sh
python tests/matrix/sqe_feature_tests.py --out target/matrix-suite [--only FEATURE[:vN]]
tests/matrix/docker/stop-sqe.sh
```

`--only` is repeatable (`--only time-travel --only lineage:v3`). Cells you do not
select are reported as unmeasured, so a partial run never looks like a full one.

`make matrix-export` writes SQE's claimed levels in the upstream engine-file shape to
`target/matrix-export/sqe.json`, after checking them against the rubric.

## Environment variables

Image selection (read by `start-sqe.sh`): `SQE_IMAGE` (required), `POLARIS_IMAGE`
(default `apache/polaris:1.7.0`), `RUSTFS_VERSION`. Ports: `POLARIS_PORT` (48181),
`RUSTFS_PORT` (49000), `SQE_PORT` (48080, Trino HTTP). Flight SQL is on
`SQE_FLIGHT_PORT` (48051).

The suite reads the same names the upstream matrix project uses for its catalog:
`ICEBERG_REST_URI`, `ICEBERG_REST_WAREHOUSE`, `ICEBERG_REST_CREDENTIAL`,
`ICEBERG_REST_SCOPE`, `ICEBERG_REST_REALM`, `ICEBERG_S3_KEY_ID`, `ICEBERG_S3_SECRET`,
`ICEBERG_S3_REGION`, `ICEBERG_S3_ENDPOINT`. The catalog advertises one object-store
address to every client, so the host-side readers replace it with
`ICEBERG_S3_ENDPOINT`; set it to an empty string to keep the catalog's own address.

And SQE's own: `SQE_HOST`, `SQE_PORT`, `SQE_FLIGHT_PORT`, `SQE_USER`, `SQE_PASSWORD`,
`SQE_CATALOG`. `tests/matrix/docker/env.sh` sets every default.

## Reading the report

The cell list is the pinned rubric: one cell per feature and per format version it
applies to (54 cells). Each cell has a claimed level (from
`docs/evidence/iceberg-matrix-state.json`, which the suite only reads) and a
measured level.

How a test result becomes a measured level, exactly:

| Test result | Measured level |
|---|---|
| passes with no partial reason | full |
| passes and records a partial reason | partial |
| fails | none |
| skipped, or the harness itself errors | unmeasured |

A cell **agrees** only when the measured level equals the claimed level. The
sections of the report:

- **Measured**: cells that got a level, and how many of them agree.
- **Discrepancies**: measured cells where measured and claimed differ. The report
  lists them as proposed edits to the state file; it never applies them.
- **Unmeasured**: skipped cells or harness errors, each with its reason (for
  example a cloud catalog that needs credentials, or a cell that needs a reader
  this suite does not have yet). They do not count for or against any level.
- **Untested**: a rubric cell with no registered test. Any such cell fails the run.
- **Protocol gaps**: a feature that works over one SQE protocol and not the other.
  The level is the engine's capability over any protocol, so a gap does not lower
  it; it is listed as a bug to file.
- **Witness observations**: per cell, which assertion came from which reader.

A partial reason always names the capability that is missing. A fail always carries
the raw error or the value that did not match.

## What the stack changes from SQE's defaults

`tests/matrix/docker/sqe-config.toml` is the whole SQE configuration. One setting
is not the default: `maintenance.orphan_min_age_secs = 1`. `remove_orphan_files`
refuses a cutoff newer than that age (the default is one day), and the maintenance
test plants its own orphan file and needs a real delete inside one run. Everything
else is a minimal single-node setup with one catalog and path-style object storage.

## Known limits of this round

- No Spark reader. Where PyIceberg 0.12 cannot model a type (variant, geometry,
  unknown) the witness is the raw metadata JSON and the Parquet file. PyIceberg
  0.12 also cannot write format-version 3, so a version 3 fixture is a PyIceberg
  table upgraded by a raw catalog commit.
- The cloud catalogs (AWS Glue, Unity, Snowflake Horizon, Google) are skipped: they
  need provider credentials.
- Statements about speed (efficient field access, faster queries, bloom-filter
  skipping) cannot be observed from a client protocol and are recorded as
  observations, not asserted.
