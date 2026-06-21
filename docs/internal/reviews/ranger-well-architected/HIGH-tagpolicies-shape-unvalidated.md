# Tag masking path validated only against a hand-authored fixture, not live Ranger

- **ID:** tagpolicies-shape-unvalidated
- **Pillar:** Operational Excellence
- **Severity:** High
- **Status:** Open (placeholder test wired; live capture + fixture pending the Ranger-backend validation run). A `#[ignore]`-d in-module test `resolve_tag_policies_against_live_sample` (crates/sqe-policy/src/ranger_store.rs) deserializes `src/testdata/tag_bundle_live_sample.json` and asserts a non-empty `(masks, filters)`. The fixture currently ships as an empty placeholder; dropping in a bundle captured from a real Ranger (`GET /service/plugins/policies/download/<service>`, with at least one tag-linked datamask and one rowfilter) and removing the `#[ignore]` makes the gate active, no code change needed. The opt-in gate / startup WARN from the proposed fix was intentionally NOT added (it would disable the working feature).
- **Files:** crates/sqe-policy/src/ranger_store.rs:26,435-536,947-953

## Problem
The entire tag masking and row-filter path is built and tested only against a hand-authored `TAG_BUNDLE` fixture, whose shape the authors themselves flag (`TODO(phase3)`) as unconfirmed against real Ranger output. If the live `tagPolicies` JSON differs, `bundle.tag_policies` deserializes to `None` and `resolve_tag_policies` returns empty. A PII-tagged column then returns raw with no error and no warning.

A security path is shipping without integration validation against the system it integrates with.

## Proposed fix
Capture a real `policies/download/<service>` bundle with at least one tag-linked datamask and one rowfilter. Add it as a fixture and assert a non-empty result. Gate tag masking behind opt-in plus a startup WARN until validated.

## Acceptance criteria
A test deserializes a captured-from-live tag bundle and asserts non-empty `(masks, filters)`.
