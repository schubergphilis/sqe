# Contributing to SQE

Thank you for your interest in contributing to the Sovereign Query Engine.

## Reporting Issues

Open a GitHub issue for bugs, feature requests, or questions. Include:

- **Bug reports**: Steps to reproduce, expected vs actual behavior, SQE version, OS, Rust version
- **Feature requests**: What you want, why you need it, and how you think it should work
- **Questions**: Check existing issues and docs first. If the answer is not there, open an issue

## How to Contribute

1. **Fork the repository** and create a feature branch from `main`
2. **Never push directly to main** -- all changes go through pull requests
3. **One logical change per PR** -- keep pull requests focused and reviewable

### Branch naming

Use a prefix that describes the type of change:

- `feat/` -- new feature or capability
- `fix/` -- bug fix
- `refactor/` -- code restructuring without behavior change
- `docs/` -- documentation only
- `test/` -- adding or updating tests
- `perf/` -- performance improvement

### Workflow

```bash
# Fork and clone
git clone https://github.com/YOUR_USERNAME/sqe.git
cd sqe

# Create a feature branch
git checkout -b feat/my-feature

# Make changes, then commit
git add -p
git commit -m "feat: add my feature"

# Push and open a pull request
git push -u origin feat/my-feature
```

Then open a pull request on GitHub against `main`.

## Commit Format

We use [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` -- new feature or capability
- `fix:` -- bug fix
- `docs:` -- documentation only
- `chore:` -- maintenance, dependencies, CI
- `refactor:` -- code restructuring without behavior change
- `test:` -- adding or updating tests
- `perf:` -- performance improvement

Examples:
```
feat: add AWS Glue catalog backend
fix: handle expired JWT tokens in bearer auth
docs: update pluggable catalogs design spec
```

## Code Style

SQE follows standard Rust conventions:

- **Formatting**: Run `cargo fmt --all` before committing. CI enforces this.
- **Linting**: `cargo clippy --all-targets --all-features -- -D warnings` must pass with zero warnings.
- **No unsafe code** without a comment explaining why it is necessary.
- **Error handling**: Use `Result` and the project's error types. No `.unwrap()` in production code.

## Testing Requirements

All contributions must include appropriate tests. Before submitting a PR:

```bash
# Format check
cargo fmt --all -- --check

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

- All PRs require at least one review before merge
- CI must pass (fmt, clippy, tests, audit, deny)
- Benchmark-sensitive changes should include benchmark results

## Documentation

If your change affects user-facing behavior, update the relevant docs. Key files:

- `README.md` -- roadmap checklist and feature overview
- `docs/features.md` -- detailed feature comparison
- `docs/trino-compatibility.md` -- Trino SQL compatibility matrix

## License

By contributing to SQE, you agree that your contributions will be licensed
under the [Apache License 2.0](LICENSE).
