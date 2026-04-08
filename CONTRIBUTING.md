# Contributing to SQE

Thank you for your interest in contributing to the Sovereign Query Engine!

## How to Contribute

1. **Fork the repository** and create a feature branch from `main`
2. **Never push directly to main** — all changes go through merge requests
3. **One logical change per MR** — keep MRs focused and reviewable

## Commit Format

We use [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` — new feature or capability
- `fix:` — bug fix
- `docs:` — documentation only
- `chore:` — maintenance, dependencies, CI
- `refactor:` — code restructuring without behavior change
- `test:` — adding or updating tests
- `perf:` — performance improvement

Examples:
```
feat: add AWS Glue catalog backend
fix: handle expired JWT tokens in bearer auth
docs: update pluggable catalogs design spec
```

## Testing Requirements

All contributions must include appropriate tests. Before submitting an MR:

```bash
# Static analysis (must pass with zero warnings)
cargo clippy --all-targets --all-features -- -D warnings

# Unit tests
cargo test --all

# Security advisory scan
cargo audit

# Dependency policy check
cargo deny check advisories

# Integration tests (requires running quickstart stack: Polaris + S3)
scripts/integration-test.sh
```

### Test Suite Overview

| Suite | Location | Requires |
|---|---|---|
| Unit tests | `crates/*/src/` (`#[cfg(test)]` modules) | Nothing |
| Integration tests | `tests/` | Polaris + S3 stack |
| E2E tests | `scripts/e2e-test.sh` | Full stack |
| Benchmarks | `sqe-bench` crate | Polaris + S3 stack |

## Code Review

- All MRs require at least one review before merge
- CI must pass (clippy, tests, audit, deny)
- Benchmark-sensitive changes should include benchmark results

## License

By contributing to SQE, you agree that your contributions will be licensed
under the [Apache License 2.0](LICENSE).
