**STATUS 2026-11-16: two line budgets raised after merge skew on `main` (`fix/sqe-write-budget-merge-skew`).**

`main` fails `cargo xtask budgets --check` (bc72796fc):

- `sqe-write: 31305 lines, over its ceiling of 31300`. The #900 and #899 merges each fit on their own; merged together they went 5 lines over.
- `crates/sqe-catalog/src/rest_catalog.rs: 8011 lines, over its ceiling of 8000`. The #900 and #903 merges, the same way.

Every branch's pre-push hook runs the same check, so no branch can push until this is fixed.

The new ceilings are the ones `--update` would record (next step boundary plus one step: 33000 for the crate at 1000-line steps, 8500 for the file at 250-line steps). Neither is slack by the check's own rule.

Gate: `cargo xtask budgets --check` exits 0 on this branch.

**NEXT:** none; merge before any branch that needs to push.
