## Context

How the streaming CTAS works today (`crates/sqe-write/src/write_handler.rs`, `handle_ctas` streaming path):

1. `create_table` registers the table in the REST catalog. Polaris writes `metadata/00000-*.json` and the table exists, with no snapshot.
2. `load_table` loads it as the caller.
3. `check_write_access` probes whether the caller may write. It sends a no-op commit with an impossible `assert-table-uuid` and reads the status: 403 refuses, 409 passes. Creating a table does not imply the right to write into it.
4. The data files are streamed to the table location, and a fast-append commit adds the snapshot and sets `main`.

Any failure after step 1 calls `ctas_rollback::roll_back`: delete the S3 prefix, then `drop_table` without purge, both as the caller. A refused drop leaves the entry, and the statement's error names it.

The vendored REST catalog (`vendor/iceberg-rust/crates/catalog/rest/src/catalog.rs`) builds `CreateTableRequest` with `stage_create: Some(false)` and has no staged path. `TableRequirement::NotExist` (`assert-create`) exists in the core crate.

## Goals and non-goals

- Goal: a CTAS that fails before its first commit leaves no catalog entry, whatever the caller's drop rights.
- Goal: the caller's write right is still checked, and a refusal is still an access-denied error with nothing registered.
- Goal: no service identity. The staged create and the commit run as the caller, like every SQE request.
- Non-goal: changing what a successful CTAS returns, emits or commits.

## Decisions

**D1. Staged create, then one create-commit.** `createTable` with `stage-create = true` returns `LoadTableResult` metadata (location, uuid, schema, spec, sort order, properties) and registers nothing. SQE writes the data files under that location. The commit sends `assert-create` and the updates that build the table from nothing (assign uuid, format version, schema, spec, sort order, location, properties) plus `add-snapshot` and `set-snapshot-ref main`, in one request. The table appears in the catalog only when that request succeeds.

**D2. The write check moves to the commit.** Step 3 above needs an existing table: its probe asserts the table's uuid. A staged table has none in the catalog, so the probe cannot run. The create-commit itself becomes the check.

- Caveat 1: the data files are written before the authorization answer. A caller without write right spends the write, then gets 403 at the commit. The cost is time and S3 traffic, not exposure: nothing is registered, and the cleanup deletes the prefix.
- To limit that cost, keep a cheap pre-check where one exists: for example a namespace-level probe, if Polaris offers one that matches the commit's authorization. Open, see Q1.

**D3. Cleanup is the prefix delete only.** With nothing registered, a failure before the commit needs only `delete_prefix` on the staged location. It is best-effort, as today.

- Caveat 2: the prefix delete may itself be refused. If the staged create vends write-scoped credentials, the delete uses them. If it vends none, or read-only ones, data files stay in S3 with no catalog entry pointing at them. That is the same as today's prefix-delete failure, and the orphan-files procedure covers it. The entry, which is what blocks a retry of the same name, no longer exists.

**D4. Fallback.** When the catalog refuses `stage-create` (a non-Polaris REST server, or an older one), SQE keeps today's register-first path and its drop rollback. Detection is per request on the error status, not a config flag. Whether that needs a config switch is open (Q3).

**D5. Vendor change upstream first.** Before patching `vendor/iceberg-rust`, check apache/iceberg-rust for staged-create or create-transaction support. If it exists, take it; if not, keep the vendor patch minimal: a `stage_create` flag on the request path, and a commit that may carry `assert-create`. Propose it upstream.

## Open questions

- **Q1. What Polaris authorizes on a staged-create commit.** The staged create is authorized as a create (Polaris has `CREATE_TABLE_STAGED`, and a variant with write delegation). The commit that carries `assert-create` registers the table. Which operation Polaris authorizes it as, and which Ranger access types that op needs, decides whether a caller with CREATE and MODIFY on the namespace can complete a CTAS at all. It must be read from Polaris source and confirmed on the test stack before any code is written. If the commit needs more than today's create plus write, this change would refuse CTAS for callers that succeed today.
- **Q2. Credentials on a staged create.** Whether Polaris vends write credentials on `stage-create` with `X-Iceberg-Access-Delegation: vended-credentials`, and whether they cover the data files and the prefix delete.
- **Q3. Fallback detection.** Whether a per-request fallback on an error status is reliable enough, or a catalog-level setting is needed.
- **Q4. Concurrency.** Two CTAS on the same name both stage, and one commit wins on `assert-create`. The loser gets a conflict after writing its files, and its prefix cleanup must not touch the winner's files. Polaris assigns locations per table uuid or per name; which one decides whether the two prefixes can overlap.

## Risks

- Q1 answered the wrong way turns an orphan-entry problem into a refusal for a whole class of callers. That is why Q1 gates the work.
- A caller without write right now writes the data before the 403 (caveat 1).
