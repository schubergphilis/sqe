# Hash mask defaults to unsalted SHA-256 (rainbow-table risk)

- **ID:** hash-mask-unsalted-sha256-default
- **Pillar:** Security
- **Severity:** Low
- **Status:** Open
- **Files:** crates/sqe-policy/src/plan_rewriter.rs:38,482-501; crates/sqe-coordinator/src/policy_wiring.rs:26-30; crates/sqe-policy/src/sha256_udf.rs:35-49

## Problem
When `mask_key` is empty, `MaskType::Hash` uses plain unsalted SHA-256. Low-entropy columns (SSN, phone, small enums) are brute-forceable via rainbow tables built from the hashed output. Known as issue #37. HMAC is supported when keyed.

## Proposed fix
Require a non-empty `mask_key` when `engine=ranger` and any Hash mask can be produced, or default-deny Hash without a key.

## Acceptance criteria
Config validation rejects `ranger` plus Hash usage with an empty `mask_key`.
