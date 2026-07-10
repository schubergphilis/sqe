## Summary

One or two sentences on what this PR does and why. Lead with the
user-visible change if there is one.

## What changed

A bullet list of the concrete changes. File-level granularity is fine
when it helps reviewers.

-
-
-

## Test plan

How you tested this. Specific commands, expected outputs, links to
benchmark results if relevant. Reviewers will run these.

```bash
cargo test --all
cargo clippy --all-targets --all-features -- -D warnings
```

- [ ] `cargo fmt --all -- --check` clean
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` clean
- [ ] `cargo test --all` passes
- [ ] Integration / e2e tests where relevant
- [ ] Benchmarks where relevant
- [ ] Docs updated (`README.md`, `docs/`, `openspec/changes/*/tasks.md`)
- [ ] Matrix updated if a cell flipped
  (`docs/iceberg-matrix-state.json`)

## Why this approach

Optional. If there were alternatives you considered and rejected,
note them so future readers and reviewers do not have to rediscover
the trade-offs.

## Linked issues

Closes #
