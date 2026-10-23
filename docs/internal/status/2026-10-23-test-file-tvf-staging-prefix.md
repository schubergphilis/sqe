**STATUS 2026-10-23: file TVFs under a staging-only prefix are covered by tests (`test/file-tvf-staging-prefix`).**

Portal #41 asked whether `read_parquet` / `read_csv` / `read_json` read `s3://staging/...` with the staging-only key, whether Ranger row filters apply, and whether paths outside `allowed_object_store_prefixes` are refused. Test-only change, no engine code:

- `tvf_catalog_placeholder_e2e::file_tvf_csv_and_json_read_inside_the_prefix_and_refuse_outside`: CSV and NDJSON inside `{catalog}` prefix return 3 rows; outside is an error naming `allowed_object_store_prefixes`. Gate: `make test-integration FILTER=file_tvf_csv_and_json` -> 1 passed.
- `access_control_e2e::file_tvf_row_filter_keys_on_the_function_name_not_the_path`: a Ranger row filter on table `read_csv()` (database `*`) cuts bob (engineer) to 1 row; `read_json` on the same rows returns 3; alice (analyst) gets 3. Gate: `scripts/access-control-test.sh file_tvf_row_filter_keys` -> 1 passed.

Open, for the portal to decide: Ranger cannot key a row filter on a file path. The prefix gate decides which files a user reads; a row filter applies per function name for the session's catalog/schema. A per-path filter needs a new Ranger resource shape.

Gotcha: `scripts/access-control-test.sh` must not run with `COMPOSE_PROJECT_NAME=sqlengine` (as `/tmp/sqe-env.sh` sets it); it then recreates the integration stack's rustfs/polaris from the AC compose file. Use `COMPOSE_PROJECT_NAME=polaris-ranger-keycloak`.

**NEXT:** unchanged
