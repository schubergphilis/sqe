# Decision: where tag associations are stored (Phase 3 tag-based masking)

Status: DECIDED (2026-06-19). Scopes the storage layer for tag-based masking
(the Snowflake tag-masking parity pillar). Pairs with
`docs/ranger-fine-grained-service-type.md` and `docs/fine-grained-policy.md`.

## The two halves of a tag system

Tags split into two independently-stored things. Conflating them is the usual
mistake.

1. **The mask-per-tag RULE** ("any column tagged `PII` -> mask show-last-4").
2. **The tag-to-column ASSOCIATION** ("column `sales.customers.ssn` has tag `PII`").

## Decision

- **Rule (1) lives in Apache Ranger.** Ranger's `tag` service holds tag-based
  mask / row-filter policies, and SQE's `RangerStore` download bundle already
  returns `tagPolicies`. No change to the policy-source model: the same rules
  are shared with Spark/Kyuubi, exactly like our resource policies.

- **Association (2) lives in Iceberg/Polaris table metadata** as a single
  namespaced table property `sqe.column-tags`, a JSON object mapping column name
  to a list of tags:
  ```
  sqe.column-tags = {"ssn": ["PII"], "amount": ["FINANCIAL"]}
  ```
  Stored in `TableMetadata.properties` (an arbitrary key/value map that Polaris
  persists and SQE already reads via `table.metadata()`). Written by a SQE
  tagging DDL/API (an `ALTER TABLE ... SET` that edits this one property).

- **Cross-engine (Spark) tag enforcement is an OPTIONAL one-way sync**, not a
  dependency: a Phase-3.1 job mirrors `sqe.column-tags` into Ranger's tag store
  (`/service/tags/...`) so Kyuubi's Ranger plugin honors the same column tags.
  SQE never depends on this sync being run.

## Why Iceberg/Polaris properties for the association, not the Ranger tag store

Four deciding factors, in order:

1. **Federated catalogs.** Per `docs/ranger-fine-grained-service-type.md`, SQE's
   engine-side enforcement is the ONLY fine-grained layer that covers external /
   federated catalogs (Polaris cannot gate those). Tags-as-table-properties work
   for ANY Iceberg table SQE can read, federated included. Populating Ranger's
   tag store per federated resource (with exact name matching) is fragile and
   would leave federated tables untagged.

2. **No Atlas, no tagsync.** Ranger tag associations normally come from Apache
   Atlas via tagsync. There is no Atlas for Polaris/Iceberg here, so a
   Ranger-only association store would sit empty unless something pushes to it.
   Iceberg properties need no Atlas and no tagsync.

3. **Tags travel with the data.** Properties live in the table metadata, so they
   survive clone, replicate, and rename. Ranger associations are keyed by
   resource name and break on rename/move.

4. **SQE reads it natively.** SQE already loads `table.metadata()` on every scan;
   reading one extra property is trivial. No new client, no new store.

The mask-per-tag RULE (the genuinely valuable shared-with-Spark part) still lives
in Ranger, so the cross-engine policy story is preserved. Only the association
source of truth moves to the data.

## Tradeoff accepted

Spark/Kyuubi read tag ASSOCIATIONS from Ranger's tag store, not from Iceberg
properties. So out of the box, Spark will not honor `sqe.column-tags`-sourced
tags. That is what the optional Iceberg -> Ranger sync (above) is for, and it is
only needed when Spark must enforce the same column tags. For SQE-native
governance (the common case, and the only option for federated catalogs), no
sync is required.

## Storage format detail

- One property `sqe.column-tags` (a single JSON blob), not one property per
  column. Atomic to read/write, no per-column-property support needed (Iceberg
  per-column metadata beyond `doc` is not in the spec). Column `doc` is left for
  human descriptions.
- Tag names are opaque strings that must match the resource/tag names used in
  Ranger `tagPolicies`.
- iceberg-rust 0.8.0: `TableMetadata::properties() -> &HashMap<String,String>`
  for the read; the write goes through the catalog's update-table-properties
  path (the same mechanism CTAS/ALTER use). Confirm the exact iceberg-rust write
  API at implementation time.

## Resolution path at query time (Phase 3 build)

1. On scan, read `sqe.column-tags` from the table metadata -> map column -> tags.
2. From the `RangerStore` bundle `tagPolicies`, resolve the mask/row-filter for
   each tag that applies to the user's roles (same matching as resource policies).
3. Feed the resulting masks/filters into the existing `PolicyEnforcer` /
   `PlanRewriter`. Resource policies still win on conflict per Ranger ordering.

This reuses the entire enforcement path already shipped in Phase 1/2A; Phase 3 is
the tag SOURCE + the tag-policy resolution, not new enforcement.
