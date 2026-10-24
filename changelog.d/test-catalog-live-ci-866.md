### Changed

- The Nessie, Hive Metastore and JDBC (PostgreSQL) catalog live tests run in CI (#866)

    A new `catalog-live-tests` job runs the five live round-trip tests against Postgres, Nessie and Hive Metastore service containers, pinned by digest, whenever `sqe-catalog`, the vendored iceberg-rust or the CI file changes. They used to run only by hand, so a regression waited for the next manual run. The Hadoop catalog stays read-only by design, and the site's catalog table now says so.

### Fixed

- The Hive Metastore compose stack turns healthy (#866)

    Its healthcheck read from the Thrift port, and a Thrift server never speaks first, so every probe hung until it timed out and `docker compose up --wait` never returned. The probe now only opens the port.
