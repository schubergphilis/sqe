## 0. Gate (before any code)

- [ ] 0.1 Read Polaris source: the operation and Ranger access types for a commit that carries `assert-create` on a staged table (design Q1)
- [ ] 0.2 Confirm on the Polaris test stack: a principal with CREATE and MODIFY on a namespace stages a create and completes the create-commit; one with CREATE only is refused at the commit and nothing is registered
- [ ] 0.3 Confirm whether a staged create vends write credentials, and what they cover (Q2)
- [ ] 0.4 Check apache/iceberg-rust for staged-create or create-transaction support (D5)
- [ ] 0.5 Owner decision to proceed, with the answers to 0.1 to 0.4

## 1. Vendor and catalog

- [ ] 1.1 Staged create on the REST catalog (`stage-create = true`), returning metadata and credentials without registering (D1)
- [ ] 1.2 A commit that may carry `assert-create` with the create updates and the first snapshot (D1)
- [ ] 1.3 `SessionCatalog` staged create, with the same delegation and credential handling as `create_table`

## 2. CTAS path

- [ ] 2.1 Streaming CTAS uses the staged create and one create-commit; no catalog entry before the commit
- [ ] 2.2 Failure before the commit deletes the staged prefix only (D3)
- [ ] 2.3 A refused `stage-create` falls back to register-first with the drop rollback (D4)
- [ ] 2.4 A commit refused with 403 surfaces as access denied, "nothing was written", with nothing registered

## 3. Tests

- [ ] 3.1 wiremock: the `stage-create` request, the create-commit shape with `assert-create`, no DELETE on failure
- [ ] 3.2 wiremock: fallback when `stage-create` is refused
- [ ] 3.3 Polaris test stack: refused CTAS leaves no table entry; successful CTAS unchanged (rows, snapshot, lineage `version` facet)
- [ ] 3.4 Concurrent CTAS on one name: one wins, the loser's cleanup leaves the winner's files (Q4)

## 4. Docs

- [ ] 4.1 Writes page: what a failed CTAS leaves behind, with and without staged create
