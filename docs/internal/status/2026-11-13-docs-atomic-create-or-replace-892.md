**STATUS 2026-11-13: OpenSpec change `atomic-create-or-replace` for #892 (`docs/atomic-create-or-replace-892`).**

This change is design only; no code changed. `openspec/changes/atomic-create-or-replace/` holds:

- **proposal:** a schema- or partition-changing `CREATE OR REPLACE TABLE ... AS SELECT` becomes one catalog commit on the existing table, instead of a staged table followed by DROP + RENAME. The table keeps its UUID, history, refs and grants.
- **design:** the Iceberg Java replace semantics, compared with what SQE does today. It covers:
  - the update order;
  - the requirements that pin the predicted schema and spec ids;
  - the retry and failure policy;
  - the properties carried over from #896;
  - four assumptions (A1-A4) to probe against Polaris first;
  - ten user-visible decisions (U1-U10), each with its default.
- **spec:** scenarios for the gapless read, history, field ids, properties and tags, failures and refusals.
- **tasks:** probes, planner, CTAS wiring, tests (including the `information_schema_listing_race_e2e` extension that #892's Done-when asks for), and docs.

No changelog fragment: nothing user-visible changes until the implementation lands.

**NEXT:** review and settle U1-U10, then run the task group 1 probes against Polaris.
