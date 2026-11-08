**STATUS 2026-11-07: table-health gauges and alert rules, part B of #815 (`feat/table-health-alerts-815`, #815).**

What changed:

- `sqe-metrics-core`: new per-table gauges `sqe_table_data_files` and `sqe_table_manifests`.
- `maintenance_scheduler.rs`: `record_health_gauges` sets all five per-table gauges; the
  advisory and active paths both call it (they used to duplicate three setters). Manifest
  count comes from the current snapshot's manifest list. In active mode a failed manifest
  read leaves the last gauge value and logs a warning; advisory fails the table's
  analysis as before for other read errors.
- `maintenance.rs`: `current_manifest_count(table)`. Fixes `rewrite_manifests`, whose
  summary had `input_count = 0` and `output_count` = number of snapshot summary
  properties. Now it reports the manifests before and after.
- Finding, fixed here: `CALL system.rewrite_manifests` never merged anything. The
  iceberg-rust `RewriteManifestsAction` only rewrites when given `cluster_by`; without it
  the commit carried the same manifests. The real counts made this visible (3 in, 3 out).
  It now passes one cluster key, so each partition spec's data manifests merge, sealed at
  `commit.manifest.target-size-bytes`. `sql-reference/procedures.md` states this.
- `deploy/observability/alerts.yml`: group `sqe.table_health` with
  `SqeTableDeleteFileAmplification` and `SqeTableManifestCountHigh` (for 1h, warning).
  `alerts.test.yml` holds promtool unit tests; `make alerts-test` runs `promtool check
  rules` and `promtool test rules` from `prom/prometheus:v3.5.0` in Docker.
- Helm: `templates/prometheusrule.yaml` behind `prometheusRule.enabled` (default false),
  reading `files/alerts.yml`, a symlink to `deploy/observability/alerts.yml`, so there is one
  copy of the rules. `helm template` and `helm package` (alpine/helm:3.17.3) follow the
  symlink and render all 12 rules.
- Docs: `features/observability.md` "Table health and alert rules";
  `operations/go-live-checklist.md` names the chart option.

Tests:

- `make alerts-test`: check 12 rules SUCCESS, unit tests SUCCESS. The amplification test
  includes a small table (10 deletes / 2 data files) and a healthy large one that must not fire,
  and no alert before the 1h `for`.
- Gate: `make test-integration FILTER=maintenance_procedures_test` -> 14 passed, 0 failed.
  `rewrite_manifests_commits_and_returns_summary` runs three INSERTs and asserts
  `input_count` 3, `output_count` 1 and the row count; before the `cluster_by` fix it
  failed with 3 out.
- `RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --features test-sqlite --test it
  maintenance_scheduler_test` -> 13 passed (advisory tick asserts
  `sqe_table_data_files` 3 and `sqe_table_manifests` 1).
- `cargo clippy -p sqe-coordinator -p sqe-metrics-core --all-targets -- -D warnings` clean.

Limits: the gauges and alerts only cover tables the scheduler visits, so they need
`[maintenance] mode` advisory/active, `[maintenance.scheduler] enabled = true`, and the
table property `sqe.maintenance.enabled = 'true'`. Thresholds are starting points.

**NEXT:** #815 closes when this and !1682 merge. Then #875 (SET WRITE_BRANCH, CTAS split).
