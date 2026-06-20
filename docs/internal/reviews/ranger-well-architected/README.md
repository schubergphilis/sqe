# Well-Architected Review: Apache Ranger Fine-Grained Governance

This is a Well-Architected review of the new Apache Ranger fine-grained governance in SQE, run 2026-06-20 across the four pillars: Security, Reliability, Performance, and Operational Excellence. 5 findings were fixed on branch `review/ranger-well-architected` (commit 722d1b2); the rest are filed here as one markdown file each, ready to become GitLab issues.

## Top priority

[`view-bypass-policy`](P0-view-bypass-policy.md) (P0) is the most urgent finding. Policy enforcement runs on the unoptimized plan before views are inlined, so any row filter, column mask, or column restriction on a base table is skipped when the table is read through a view. An analyst with no grant can read a governed table raw through a view. Fix this first, on a dedicated branch with view+policy integration tests.

## Findings

| ID | Pillar | Severity | Status | One-line |
|---|---|---|---|---|
| [view-bypass-policy](P0-view-bypass-policy.md) | Security | P0/Critical | Open | Views bypass all policy: enforcement runs before view inlining |
| [empty-projection-failopen](P0-empty-projection-failopen.md) | Security | P0/Critical | Resolved in commit 722d1b2 | Empty mask/restrict projection returned the raw scan |
| [partialmask-non-ascii-leak](HIGH-partialmask-non-ascii-leak.md) | Security | High | Resolved in commit 722d1b2 | Full mask passed non-ASCII PII through unmasked |
| [policy-expr-body-log-leak](HIGH-policy-expr-body-log-leak.md) | Operational Excellence | High | Resolved in commit 722d1b2 | WARN logs leaked row-filter and mask template literals |
| [tag-mask-cache-miss-failopen](HIGH-tag-mask-cache-miss-failopen.md) | Reliability + Security | High | Open | Tag masks/filters silently skipped on cache miss or disabled cache |
| [stale-tags-cross-token](HIGH-stale-tags-cross-token.md) | Reliability | High | Open | Token-keyed cache leaves stale tags for other users after a write |
| [resolve-tags-no-bundle-cache](HIGH-resolve-tags-no-bundle-cache.md) | Performance | High | Open | resolve_tags re-downloads the bundle per query per tagged table |
| [tagpolicies-shape-unvalidated](HIGH-tagpolicies-shape-unvalidated.md) | Operational Excellence | High | Open | Tag masking validated only against a hand-authored fixture |
| [no-ranger-metrics](HIGH-no-ranger-metrics.md) | Operational Excellence | High | Open | RangerStore emits zero metrics; breaker-open deny is invisible |
| [no-policy-decision-audit](HIGH-no-policy-decision-audit.md) | Operational Excellence | High | Open | No policy-decision audit field; deny-all looks like an empty result |
| [no-show-effective-policy-or-tags](HIGH-no-show-effective-policy-or-tags.md) | Operational Excellence | High | Open | No SHOW EFFECTIVE POLICY or SHOW TAGS; masking un-introspectable |
| [datamask-column-isexcludes-failopen](MED-datamask-column-isexcludes-failopen.md) | Security | Medium | Resolved in commit 722d1b2 | Datamask column loop ignored column.isExcludes complement |
| [ranger-config-zero-validation](MED-ranger-config-zero-validation.md) | Operational Excellence | Medium | Resolved in commit 722d1b2 | Zero config numerics caused silent fleet-wide deny-all |
| [concurrent-set-tags-lost-update](MED-concurrent-set-tags-lost-update.md) | Reliability | Medium | Open | Concurrent SET TAGS silently lose updates (last-writer-wins) |
| [namespace-flatten-no-diagnostic](MED-namespace-flatten-no-diagnostic.md) | Operational Excellence | Medium | Open | Namespace flattening silently misses policies, no diagnostic |
| [untested-io-and-failclosed-branches](MED-untested-io-and-failclosed-branches.md) | Operational Excellence | Medium | Open | I/O glue and fail-closed branches have no automated tests |
| [mask-str-per-cell-allocations](MED-mask-str-per-cell-allocations.md) | Performance | Medium | Open | mask_str does two heap allocations per masked cell |
| [hash-mask-unsalted-sha256-default](LOW-hash-mask-unsalted-sha256-default.md) | Security | Low | Open | Hash mask defaults to unsalted SHA-256 (rainbow-table risk) |
| [external-ranger-edits-lag-ttl](LOW-external-ranger-edits-lag-ttl.md) | Reliability | Low | Open | External Ranger Admin edits not honored until cache TTL |
| [parse-sql-predicate-fresh-context](LOW-parse-sql-predicate-fresh-context.md) | Performance | Low | Open | Fresh SessionContext per parse repeats in the tag path |
| [is-role-in-session-per-row-alloc](LOW-is-role-in-session-per-row-alloc.md) | Performance | Low | Open | is_role_in_session array branch allocates a String per row |
| [dead-roles-clone](LOW-dead-roles-clone.md) | Performance | Low | Open | Dead roles clone in the rewriter |
| [apply-tag-ops-map-clone](LOW-apply-tag-ops-map-clone.md) | Performance | Low | Open | apply_tag_ops clones the whole tag map (DDL path only) |
| [properties-for-linear-scan](LOW-properties-for-linear-scan.md) | Performance | Low | Open | properties_for linear-scans the metadata cache per query |
| [magic-strings-policytype-custom](LOW-magic-strings-policytype-custom.md) | Operational Excellence | Low | Open | policyType ints and CUSTOM discriminator duplicated |
| [set-tags-parser-weak-diagnostics](LOW-set-tags-parser-weak-diagnostics.md) | Operational Excellence | Low | Open | SET TAGS parser gives weak, context-free diagnostics |
| [group-bound-warn-spam-no-metric](LOW-group-bound-warn-spam-no-metric.md) | Operational Excellence | Low | Open | Group-bound items emit a WARN burst per cache miss, no counter |
| [breaker-half-open-thundering-herd](LOW-breaker-half-open-thundering-herd.md) | Reliability | Low | Open | Breaker half-open admits all concurrent probes |

## Verified correct (no change needed)

These were checked and confirmed clean during the review:

- **parse_sql_predicate injection**: the fresh `SessionContext` rejects cross-table and subquery references; the two-pass parse rejects trailing garbage.
- **validate_identifier**: the resource map is serde-JSON, not URL-interpolated.
- **masked-column predicate pushdown**: the mask projection sits above the scan, so predicates do not push down onto raw values.
- **precedence rules**: resource-mask-wins, restricted-stays-restricted, and unmappable-fail-closed are all well tested.
- **fetch_bundle plus circuit breaker fail-closed**: every failure denies.
- **per-table partial-failure isolation**: one table's failure denies only that table.
- **is_role_in_session membership correctness**: `Vec::contains` on unsorted roles plus const-fold distribution is correct.
- **GROUP grantee rejection**: rejected as designed.
