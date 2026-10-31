**STATUS 2026-10-31: COMMENT ON a qualified catalog (`fix/comment-on-qualified-catalog`).**

Reported from the playground4 Databricks migration: `COMMENT ON COLUMN`
parses, but its commit fails with "Table does not exist" on a table `SELECT`
reads. Cause: `QueryHandler::handle_comment_on` committed through
`create_session_context`'s catalog (the session default) and dropped the
name's catalog qualifier. It now resolves the target with
`catalog_qualifier` + `show_catalog`, as `SHOW CREATE TABLE` does, and the
dispatch invalidates the session cache after the commit.

Verified: new live IT `catalog_discovery_test::comment_on_a_discovered_catalog_table`
failed before the fix with the reported 404 and passes after (COLUMN and
TABLE on `discovery_test_wh`, DESCRIBE shows the comment); the
discovery suite (10), `mcp_describe_schema_e2e` and the Hive DDL refusals
(29) pass.

**NEXT:** the other SQE asks from the same handover: `MERGE ... UPDATE SET *`
does not parse; `USE` does not hold across statements on the portal's
statement API (no session); about 30 statements a minute.
