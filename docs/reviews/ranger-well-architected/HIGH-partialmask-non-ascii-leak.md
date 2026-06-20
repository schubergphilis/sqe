# Partial mask passes non-ASCII characters through unmasked (PII leak)

- **ID:** partialmask-non-ascii-leak
- **Pillar:** Security
- **Severity:** High
- **Status:** Resolved in commit 722d1b2
- **Files:** crates/sqe-policy/src/mask_udf.rs:48-62

## Problem
`mask_str` used ASCII-only character classes (`is_ascii_uppercase`, `is_ascii_lowercase`, `is_ascii_digit`). The else branch passed any non-ASCII character through unchanged. A full MASK over Cyrillic, CJK, or accented PII therefore returned the original characters.

For an EU-sovereignty system this is a near-total leak: names like "Иван" or accented Latin names are exactly the data the mask is meant to protect, and they came back in the clear.

## Proposed fix
Use Unicode-aware character classes: `is_alphabetic` mapped to upper or lower by `is_uppercase`; `is_numeric` mapped to a digit placeholder; punctuation and whitespace pass through.

## Acceptance criteria
`mask_str("Иван", 0, 0, ...)` and CJK input no longer return the originals.
