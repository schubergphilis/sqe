**STATUS 2026-10-22: AWS Glue live tests re-run, V3 table round trip through Glue (`test/glue-v3-live-854`, #854).**

#854 asked to re-run the Glue live tests or downgrade `sqe:aws-glue-catalog`.
They were re-run, and a new end-to-end test creates a V3 table through Glue.

The new test found a bug: `CREATE TABLE ... TBLPROPERTIES ('format-version' = '3')`
failed on Glue with "Table properties should not contain reserved properties,
but got: [format-version]". `format_version_props_for_backend` already left
the property out for non-REST backends, but `merge_user_table_properties`
added the user's own option back. The matrix note that "format-version
forwarding is shared with Polaris" did not hold for Glue.

- `crates/sqe-write/src/write_handler.rs`: `create_table_properties` builds
  the CREATE TABLE / CTAS property map and drops every reserved Iceberg
  property for non-REST backends (Glue, S3 Tables, HMS, JDBC). The version
  still reaches the table through `TableCreation.format_version`, which
  `plan_create_table_schema` reads from the option. Unit test
  `a_user_format_version_option_is_dropped_for_non_rest_backends`.
- `crates/sqe-coordinator/tests/it/glue_v3_live_e2e.rs` (new,
  `#![cfg(feature = "glue")]`, `#[ignore = "needs:aws; ..."]`):
  `v3_table_round_trips_through_glue` runs CREATE SCHEMA, CREATE TABLE with
  format-version 3 and a `TIMESTAMP_NS` column, INSERT and SELECT through the
  `QueryHandler` on a Glue-configured coordinator, reloads the table from
  Glue (format-version 3), and checks the data files sit under the warehouse
  bucket. It drops the table and database before asserting.
- `crates/sqe-catalog/tests/it/backends_integration.rs`: the two Glue tests
  now say "last verified: 2026-10-22 (eu-central-1)".
- Matrix: both `aws-glue-catalog` caveats replaced by "local only, not in
  CI"; V3 notes describe the recorded run. Score unchanged (150/162).

Gate (eu-central-1, credentials from `.env`):
- `sqe-catalog` `backends_integration` ignored tests:
  `glue::live_glue_namespace_round_trip ... ok`,
  `s3_tables::list_namespaces_via_glue_rest ... ok` (the Unity, Nessie and
  Hadoop tests in the same run failed for lack of their services, as
  expected).
- `cargo test -p sqe-coordinator --features glue --test it glue_v3_live_e2e
  -- --ignored`: before the fix, failed at CREATE TABLE with the reserved
  property error; after, `v3_table_round_trips_through_glue ... ok` (5.5s).
- `cargo test -p sqe-write --lib format_version`: 4 passed.

**NEXT:** re-render `iceberg-matrix.md` and sync the site once this is on
main (#855).
