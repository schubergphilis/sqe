# policyType ints and CUSTOM discriminator are magic strings duplicated across the path

- **ID:** magic-strings-policytype-custom
- **Pillar:** Operational Excellence
- **Severity:** Low
- **Status:** Open
- **Files:** crates/sqe-policy/src/ranger_store.rs:342,370,465,511 (policyType 1/2), :273,475 ("CUSTOM" duplicated)

## Problem
The `policyType` ints and the CUSTOM discriminator are repeated. The CUSTOM special-case at :475 duplicates knowledge in `map_mask`, so a mask-type change must be made twice or the tag path diverges from the resource path. (`PROP_KEY` is well-centralized, which is good.)

## Proposed fix
Add `const POLICY_TYPE_DATAMASK=1` and `const POLICY_TYPE_ROWFILTER=2`. Route CUSTOM detection through one shared helper.

## Acceptance criteria
No bare `1`/`2` policyType literals and no duplicated "CUSTOM" guards.
