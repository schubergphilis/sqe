# Phase 3: Tag-based masking Implementation Plan (MVP = 3a enforcement)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax. This plan is intended to be executed in a FRESH session (Phase 3 is a multi-week subsystem; do not try to finish it in the session that wrote the plan).

**Goal (3a MVP):** Enforce tag-based column masking — a column tagged `PII` (in Iceberg metadata) gets the mask that Ranger's tag policy assigns to `PII` — reusing the existing `PolicyEnforcer`/`PlanRewriter`. Tagging DDL (`ALTER TABLE ... SET TAGS`) is Phase 3b (deferred); for the 3a demo, set the tag property via a Polaris `updateProperties` call.

**Architecture (settled — `docs/ranger-tag-storage-decision.md`):**
- **Tag-to-column association** = Iceberg/Polaris table property `sqe.column-tags` (JSON `{"<col>": ["TAG", ...]}`), read from `TableMetadata.properties()` via `TableMetadataCache`. SQE's source of truth. NOT Ranger tag-resource associations / Atlas.
- **Mask-per-tag rule** = Ranger `tag` service `tagPolicies`, which flow into the hive service's download bundle ONLY when the hive service is linked to a tag service (see Prerequisite). SQE reads `tagPolicies` from the bundle it already downloads.
- SQE joins the two itself: column tags (Iceberg) x tagPolicies (Ranger) -> per-column masks / row filters -> merged into `ResolvedPolicy` -> existing rewriter. NO new enforcement engine, NO new distribution concern (tag-derived masks are the same masks/filters Phase 1/2A/2B already const-fold and ship safely).

**THE SPINE: full table identity.** This has caused two bugs already (Phase 1 `database` match; the `info_schema` full-vs-last-component leak). `TableMetadataCache` is keyed by the FULL `catalog . full-namespace . table`. But `PolicyStore::resolve(user, table_name, namespace)` only carries the REDUCED last-component namespace and cannot reconstruct the full identity for multi-level namespaces. The full `TableReference` lives in the REWRITER (`plan_rewriter.rs` `table_refs: HashMap<String, TableReference>`). Therefore the tag read + join happens in the REWRITER (which has the full identity + an injected `TagSource`), NOT in `PolicyStore::resolve`. The `TagSource` trait takes the FULL identity. A multi-level-namespace test for the tag path is mandatory.

**Dependency inversion:** `sqe-policy` must NOT depend on `sqe-catalog`. Define `trait TagSource` in `sqe-policy`; implement it in `sqe-coordinator`/`sqe-catalog` (backed by `TableMetadataCache`) and inject `Arc<dyn TagSource>` into the rewriter. Mirrors how `OpaStore`/`RangerStore`/metrics are injected.

**Mask-collision precedence (LOCKED):** `ResolvedPolicy.column_masks` is `HashMap<col, MaskType>` (last-writer-wins = undefined if both a resource data-mask and a tag-derived mask hit one column). Rule: a **resource (column-specific) data-mask policy WINS over a tag-derived mask** for the same column (more specific beats more general). Row filters from both sources AND together. Restricted columns always win (dropped). Implement deterministically; document. (Ranger's own engine is tag-first then resource with deny-overrides; we choose resource-mask-wins for column masks because it is the more specific grant and avoids surprising a per-column policy.)

**Cache lag note:** a tag change is a Polaris `updateProperties` commit; it does NOT trigger SQE's `invalidate_all` (that fires only on SQE GRANT/REVOKE). Tag edits therefore lag by the policy cache TTL. Acceptable for MVP; note it. (Phase 3b tagging DDL can invalidate.)

**Branch:** `feat/tag-based-masking` off `main` (has Phase 1+2A+2B). Never push to main; MR -> main.

**Gates:** `cargo build --all`; `cargo clippy --all-targets --all-features -- -D warnings`; `cargo test --all` (only the known env-flaky `oidc_m2m` / `channel_pool` network tests may fail).

---

## Prerequisite (Ranger setup — verify empirically, the stack is up)
The hive download bundle currently has NO `tagPolicies` (confirmed by curl: present=False), even though a `tag` service-def (id 100) and a `tag` service instance exist. The hive service must be LINKED to the tag service for tag policies to flow into its bundle. Before building the parser:
- [ ] Link the hive service to the `tag` service (set the hive service's tag-service config, e.g. `tag.download.auth.users` / the service `tagService` field — confirm the exact Ranger 2.8 mechanism). Then create a tag mask policy on the `tag` service (e.g. tag `PII` -> `MASK_SHOW_LAST_4` for some role) and re-curl `download/hive` to CONFIRM `tagPolicies` now appears in the bundle. Capture the real JSON shape of a `tagPolicies` entry (it nests `policies` under a `serviceDef` for `tag`; the items are the same dataMask/rowFilter shapes keyed by a `tag` resource). Model the parser from THAT captured JSON, not from memory. Bake the link + tag policy into `quickstart/polaris-ranger-keycloak/ranger/bootstrap-ranger.sh`.

---

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `crates/sqe-policy/src/ranger_store.rs` | parse `tagPolicies` from the bundle; expose resolved tag masks/filters for (user, tag-set) | Modify |
| `crates/sqe-policy/src/tag_source.rs` | `trait TagSource` (full-identity -> column->tags) + a no-op impl | Create |
| `crates/sqe-policy/src/plan_rewriter.rs` | hold `Option<Arc<dyn TagSource>>`; read column tags (full identity), join with tag policies, merge into ResolvedPolicy with the precedence rule | Modify |
| `crates/sqe-policy/src/lib.rs` | `pub mod tag_source;` | Modify |
| `crates/sqe-coordinator/src/tag_source_impl.rs` (or in session_context) | `TagSource` impl over `TableMetadataCache` reading `sqe.column-tags` | Create/Modify |
| `crates/sqe-coordinator/src/policy_wiring.rs` + rewriter construction | inject the `TagSource` impl into the rewriter | Modify |
| `crates/sqe-policy/tests/rewriter_integration.rs` | executable tag-masking tests incl. multi-level namespace | Modify |
| `quickstart/polaris-ranger-keycloak/` | tag-service link + tag policy + demo | Modify |

---

## Task 1: Parse `tagPolicies` from the Ranger bundle
**Files:** `crates/sqe-policy/src/ranger_store.rs`.
- [ ] Using the REAL captured JSON from the Prerequisite, add serde structs for the `tagPolicies` block (a `ServicePolicies`-shaped object for the `tag` service: `policies[]` with `policyType` 1/2, `resources` carrying a `tag` resource value, `dataMaskPolicyItems`/`rowFilterPolicyItems` with users/roles/groups + dataMaskInfo/rowFilterInfo). Add `tag_policies: Option<...>` to the bundle model.
- [ ] Add a pure function `resolve_tag_policies(bundle, user, tags: &HashSet<String>) -> (HashMap<col-agnostic tag, MaskType>, Vec<Expr row filters>)` — wait: tag policies are keyed by TAG, not column. So produce, for each TAG the user's roles match, the `MaskType` (via the existing `map_mask`, reusing Task 2A `map_mask`) and any row-filter Expr (via `parse_sql_predicate`, passing the session identity per 2B). Return `HashMap<tag, MaskType>` + `Vec<(tag, Expr)>`. Match items on user.username OR token roles (same `item_matches`).
- [ ] Unit tests with the captured JSON: tag `PII` -> Nullify for role analyst; non-matching role -> none; unsupported mask type -> fail-closed (restrict signal).
- [ ] Commit: `feat(policy): parse Ranger tagPolicies from the bundle`.

## Task 2: `TagSource` trait + no-op impl
**Files:** Create `crates/sqe-policy/src/tag_source.rs`; modify `lib.rs`.
- [ ] `pub trait TagSource: Send + Sync { fn column_tags(&self, catalog: Option<&str>, namespace: &[String], table: &str) -> HashMap<String, Vec<String>>; }` — takes the FULL identity (catalog + full multi-level namespace vec + table). Returns column -> tags. A `NoopTagSource` returning empty (default when tags disabled).
- [ ] Doc the contract: implementations read the `sqe.column-tags` JSON property from the table metadata; missing/unparseable -> empty (fail-safe: no tags = no extra masking; that's correct, tags only ADD restrictions).
- [ ] Commit: `feat(policy): TagSource trait (full-identity column tags)`.

## Task 3: `TagSource` impl over `TableMetadataCache`
**Files:** `crates/sqe-coordinator` (new `tag_source_impl.rs` or in session_context).
- [ ] Implement `TagSource` backed by the `TableMetadataCache`: build the full `TableIdent`/lookup from (catalog, namespace, table), load the cached `Table`, read `table.metadata().properties().get("sqe.column-tags")`, parse the JSON `{"col":["tag"]}`. On any miss/parse error -> empty map (log debug). Reuse the SAME identity-resolution the scan path uses so the cache key matches (do NOT re-derive a reduced namespace).
- [ ] Unit/integration test: a Table whose properties carry `sqe.column-tags` -> correct column->tags map; absent property -> empty.
- [ ] Commit: `feat(coordinator): TagSource over TableMetadataCache (sqe.column-tags)`.

## Task 4: Join in the rewriter + precedence + wiring
**Files:** `crates/sqe-policy/src/plan_rewriter.rs`, `crates/sqe-coordinator/src/policy_wiring.rs` (+ rewriter construction).
- [ ] Give `PolicyPlanRewriter` an `Option<Arc<dyn TagSource>>` (builder `with_tag_source`). For each `TableScan`, using the FULL `TableReference` already in `table_refs`: (1) `tag_source.column_tags(full identity)` -> column->tags; (2) gather the union tag-set; (3) ask the store for tag-derived masks/filters for (user, tag-set) [Task 1]; (4) map each tagged column to its tag's mask; (5) MERGE into the `ResolvedPolicy` already returned by `store.resolve`, applying the precedence rule (resource column-mask WINS over tag mask; row filters AND; restricted always wins).
- [ ] Wire the `TagSource` impl into the rewriter where `build_policy_enforcer` constructs `PolicyPlanRewriter` (pass the cache-backed impl; `NoopTagSource` when no catalog/cache).
- [ ] Commit: `feat(policy): merge tag-derived masks/filters in the rewriter (resource-mask precedence)`.

## Task 5: Executable tests (incl. multi-level namespace — the recurring gap)
**Files:** `crates/sqe-policy/tests/rewriter_integration.rs`.
- [ ] A fake `TagSource` returning `{"ssn": ["PII"]}`; a store/bundle with tag `PII` -> Nullify for the user's role; over the QUALIFIED multilevel scan: assert `ssn` is masked (NULL) end to end.
- [ ] Multi-level-namespace test: full `cat.ns1.ns2.employees`; assert the TagSource is called with the FULL namespace (`["ns1","ns2"]`), not the last component, and the mask applies. (This is the exact identity gap that leaked twice before.)
- [ ] Precedence test: a column with BOTH a resource Nullify mask and a tag Hash mask -> resource Nullify wins (deterministic).
- [ ] Commit: `test(policy): tag-based masking incl. multi-level namespace + precedence`.

## Task 6: Quickstart live demo + gates + MR
**Files:** `quickstart/polaris-ranger-keycloak/*`, docs.
- [ ] Set `sqe.column-tags = {"ssn":["PII"]}` on `sales_wh.sales.pii` (or orders) via a Polaris `updateProperties` curl (no DDL needed for 3a). Ensure the hive service is tag-linked + a tag `PII` -> `MASK_SHOW_LAST_4` policy exists (Prerequisite). 
- [ ] Live: as a user whose role the tag policy targets, `SELECT ssn FROM ...` returns the masked form; an exempt user sees raw. Add to test.sh.
- [ ] Full gates; update `docs/fine-grained-policy.md`/`nextsteps.md`/README roadmap (Phase 3a tag masking shipped; 3b tagging DDL + Iceberg->Ranger sync remain).
- [ ] Push `feat/tag-based-masking`; open MR -> main.

---

## Out of scope (Phase 3b / later)
- Tagging DDL (`ALTER TABLE ... SET TAGS`) -> Polaris `updateProperties` + cache invalidation on tag change.
- Optional one-way Iceberg->Ranger tag-association sync for Spark/Kyuubi tag enforcement.
- Tag inheritance/propagation through views/derived columns beyond the natural scan-level apply.
- Object (table/namespace-level) tags; only column tags in 3a.
