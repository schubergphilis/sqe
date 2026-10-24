**STATUS 2026-10-24: Nessie, HMS and JDBC live catalog tests in CI (`test/catalog-live-ci-866`, #866).**

New job `catalog-live-tests` in `.gitlab-ci.yml`. It extends `aikido-rust-extra`, so it reuses the ci-deps image, its warm `/opt/sqe-deps/target` and the build env. Three `services:` sidecars, pinned by digest: `postgres:17-alpine`, `ghcr.io/projectnessie/nessie:0.108.3` (in-memory, no S3) and `apache/hive:standalone-metastore-4.1.0` (Derby). The script waits up to 180 s per port, because HMS runs schematool first. Then it runs `cargo test -p sqe-catalog --features hms,sql-postgres --test it -- --ignored nessie:: hms:: sql_postgres::`. The job runs on MR and main pipelines that touch `sqe-catalog`, `vendor/iceberg-rust`, `Cargo.lock`, `.gitlab-ci.yml` or the Nessie/HMS compose files.

Proved locally on 2026-10-24 against fresh containers with exactly the service configuration from the job:

- Nessie (no S3 settings) and HMS: `nessie::nessie_namespace_round_trip`, `hms::hms_namespace_round_trip` -> 2 passed.
- An empty Postgres: the three `sql_postgres::` tests -> 3 passed.

The MR pipeline is the proof for the runner side: whether these runners start `services:` and pull from ghcr.io. No other job here uses either.

Also fixed: the HMS compose healthcheck ran `cat < /dev/tcp/localhost/9083`. A Thrift server never speaks first, so the probe blocked until its 5 s timeout and the container stayed `health: starting` (confirmed: exit 124 against a running HMS). It now only opens the port. `docs/site/web/iceberg.json` says Nessie, HMS and JDBC are tested in CI, and that Hadoop is read-only by design. The rendered `docs/internal/design-archive/iceberg-matrix.md` was regenerated to match.

**NEXT:** #870 shredded variant pruning.
