# SET TAGS parser gives weak, context-free diagnostics

- **ID:** set-tags-parser-weak-diagnostics
- **Pillar:** Operational Excellence
- **Severity:** Low
- **Status:** Open
- **Files:** crates/sqe-sql/src/tags.rs:99-261

## Problem
The bespoke SET TAGS char-scanner gives uneven operator-facing errors. `strip_parens` on a missing paren says "expected parentheses" with no column context. An unterminated quote in `split_identifier` silently truncates rather than erroring.

## Proposed fix
Include the offending column or token in errors. Add tests for unbalanced quotes and trailing garbage.

## Acceptance criteria
`SET TAGS (email = ('PII'` returns an error naming the column and the missing `)`.
