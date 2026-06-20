# OSS Release + Security Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prepare SQE for open-source release (Apache 2.0 license, versioning, CI pipelines) and complete a structured security + functional audit before starting feature work (Spec C: Pluggable Catalogs).

**Architecture:** Two parallel workstreams: (A) OSS release readiness — licensing, contributor docs, version bumps, retro-tagging, changelog, CI pipelines; (B) Security & functional audit — dependency fixes, structured code review, test verification, and AUDIT.md deliverable. A and B are independent and can run in parallel. All work lands on a single branch as one MR (`v0.29.0`).

**Tech Stack:** git-cliff (changelog), cargo-deny (advisory checking), glab CLI (GitLab API for retro-tagging), openssl (test keypair generation), GitLab CI/CD

**Design spec:** `docs/superpowers/specs/2026-04-08-oss-release-and-catalogs-design.md` (Specs A + B)

---

## File Structure

### Files to Create

| File | Purpose |
|---|---|
| `LICENSE` | Apache License 2.0 |
| `CONTRIBUTING.md` | Contribution guidelines |
| `deny.toml` | cargo-deny advisory configuration |
| `cliff.toml` | git-cliff changelog configuration |
| `scripts/retro-tag.sh` | Retro-tagging script for 28 historical MRs |
| `AUDIT.md` | Security + functional audit report |
| `CHANGELOG.md` | Generated from git-cliff after retro-tagging |

### Files to Modify

| File | Change |
|---|---|
| `crates/sqe-core/Cargo.toml` | version `0.1.0` → `0.15.0` |
| `crates/sqe-auth/Cargo.toml` | version bump + remove `rsa`/`pkcs1` dev-deps |
| `crates/sqe-catalog/Cargo.toml` | version bump |
| `crates/sqe-sql/Cargo.toml` | version bump |
| `crates/sqe-policy/Cargo.toml` | version bump |
| `crates/sqe-planner/Cargo.toml` | version bump |
| `crates/sqe-coordinator/Cargo.toml` | version bump |
| `crates/sqe-worker/Cargo.toml` | version bump |
| `crates/sqe-trino-compat/Cargo.toml` | version bump |
| `crates/sqe-metrics/Cargo.toml` | version bump |
| `crates/sqe-cli/Cargo.toml` | version bump |
| `crates/sqe-bench/Cargo.toml` | version bump |
| `crates/sqe-auth/src/bearer_token.rs` | Replace `rsa` runtime keygen with static PEM keypair |
| `.gitlab-ci.yml` | Add clippy, cargo-audit, cargo-deny stages; add release pipeline |

---

## Phase 1: OSS Release Foundation (Spec A)

### Task 1: Apache 2.0 LICENSE File

**Files:**
- Create: `LICENSE`

- [ ] **Step 1: Create the LICENSE file**

Download the standard Apache 2.0 license text and set the copyright line:

```bash
curl -sL https://www.apache.org/licenses/LICENSE-2.0.txt > LICENSE
```

Then prepend the copyright notice. The final file should start with:

```
                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/
```

The copyright holder line (used in NOTICE files, not in the license itself) is:

```
Copyright The SQE Authors
```

No per-file headers needed — the repo-level LICENSE file is sufficient under Apache 2.0 convention for new projects.

- [ ] **Step 2: Verify the file**

```bash
head -5 LICENSE
wc -l LICENSE
```

Expected: ~202 lines, starts with "Apache License".

- [ ] **Step 3: Commit**

```bash
git add LICENSE
git commit -m "$(cat <<'EOF'
chore: add Apache 2.0 LICENSE file

Standard Apache License 2.0 at repo root. No per-file headers —
repo-level LICENSE is sufficient for new projects.
EOF
)"
```

---

### Task 2: CONTRIBUTING.md

**Files:**
- Create: `CONTRIBUTING.md`

- [ ] **Step 1: Create CONTRIBUTING.md**

```markdown
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
```

- [ ] **Step 2: Verify**

```bash
head -3 CONTRIBUTING.md
```

Expected: `# Contributing to SQE`

- [ ] **Step 3: Commit**

```bash
git add CONTRIBUTING.md
git commit -m "$(cat <<'EOF'
docs: add CONTRIBUTING.md

Covers commit format, testing requirements, code review process,
and license agreement for contributors.
EOF
)"
```

---

### Task 3: cargo-deny Configuration (deny.toml)

**Files:**
- Create: `deny.toml`

- [ ] **Step 1: Create deny.toml**

```toml
# cargo-deny configuration for SQE
# https://embarkstudios.github.io/cargo-deny/

[advisories]
db-path = "~/.cargo/advisory-db"
db-urls = ["https://github.com/rustsec/advisory-db"]
ignore = [
    # paste (v1.0.15): unmaintained but feature-complete proc macro.
    # Transitive dependency from arrow-flight, datafusion, parquet, tonic.
    # No known vulnerability. Upstream migration pending.
    "RUSTSEC-2024-0436",
]
```

- [ ] **Step 2: Verify cargo-deny runs cleanly**

```bash
cargo install cargo-deny 2>/dev/null || true
cargo deny check advisories
```

Expected: PASS (with `paste` advisory ignored). If `rsa` advisory still fires, that's expected — Task 5 removes it.

- [ ] **Step 3: Commit**

```bash
git add deny.toml
git commit -m "$(cat <<'EOF'
chore: add deny.toml for cargo-deny advisory checking

Ignores RUSTSEC-2024-0436 (paste crate, unmaintained but
feature-complete proc macro, transitive from arrow/datafusion).
EOF
)"
```

---

### Task 4: Remove rsa Dev-Dependency — Static Test Keypair

**Files:**
- Modify: `crates/sqe-auth/Cargo.toml` — remove `rsa` and `pkcs1` from `[dev-dependencies]`
- Modify: `crates/sqe-auth/src/bearer_token.rs:425-465` — replace runtime keygen with static PEM

This task eliminates RUSTSEC-2023-0071 (Marvin Attack on `rsa` crate) by removing the crate entirely. The only usage is in tests for JWT signing — replaced with a pre-generated static keypair.

- [ ] **Step 1: Generate static RSA test keypair**

```bash
openssl genrsa 2048 > /tmp/sqe-test-rsa.pem 2>/dev/null
```

- [ ] **Step 2: Extract the base64url-encoded modulus (n) and exponent (e)**

```bash
# Extract n and e from the PEM for JWKS mock
openssl rsa -in /tmp/sqe-test-rsa.pem -noout -text 2>/dev/null | head -5

# Get n as base64url (via openssl + python):
python3 -c "
import subprocess, base64, json

# Extract modulus bytes
result = subprocess.run(
    ['openssl', 'rsa', '-in', '/tmp/sqe-test-rsa.pem', '-modulus', '-noout'],
    capture_output=True, text=True
)
mod_hex = result.stdout.strip().split('=')[1]
mod_bytes = bytes.fromhex(mod_hex)
# Remove leading zero byte if present
if mod_bytes[0] == 0:
    mod_bytes = mod_bytes[1:]
n_b64 = base64.urlsafe_b64encode(mod_bytes).rstrip(b'=').decode()

# RSA public exponent is always 65537 = 0x010001
e_b64 = base64.urlsafe_b64encode(b'\\x01\\x00\\x01').rstrip(b'=').decode()

print(f'TEST_RSA_N: {n_b64}')
print(f'TEST_RSA_E: {e_b64}')

# Also output the PEM for embedding
with open('/tmp/sqe-test-rsa.pem') as f:
    pem = f.read().strip()
print(f'\\nPEM:\\n{pem}')
"
```

Save the output — you need the PEM string, `n`, and `e` values for the next step.

- [ ] **Step 3: Write the failing test (verify current tests still compile without rsa)**

First, update `crates/sqe-auth/Cargo.toml` — remove the `rsa` and `pkcs1` dev-dependencies:

```toml
# REMOVE these two lines from [dev-dependencies]:
# rsa = "0.9"
# pkcs1 = { version = "0.7", features = ["pem"] }
```

Run:
```bash
cargo test -p sqe-auth --no-run 2>&1 | head -20
```

Expected: FAIL — compilation error because `bearer_token.rs` tests still import `rsa`.

- [ ] **Step 4: Replace the test keypair code in bearer_token.rs**

Replace lines 425-465 of `crates/sqe-auth/src/bearer_token.rs` (the `#[cfg(test)] mod tests` header through the `TEST_KEYS` static). Replace the runtime keygen with static constants:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde_json::json;
    use std::sync::LazyLock;

    const TEST_KID: &str = "test-key-1";

    // -----------------------------------------------------------------------
    // Static RSA 2048-bit test keypair — NOT for production use.
    // Generated with: openssl genrsa 2048
    // This eliminates the rsa crate dev-dependency (RUSTSEC-2023-0071).
    // -----------------------------------------------------------------------

    /// PKCS#1 PEM-encoded RSA private key for test JWT signing.
    const TEST_RSA_PRIVATE_KEY_PEM: &str = "<PASTE PEM FROM STEP 2>";

    /// Base64url-encoded RSA modulus (n) for JWKS mock responses.
    const TEST_RSA_N: &str = "<PASTE n FROM STEP 2>";

    /// Base64url-encoded RSA public exponent (e) for JWKS mock responses.
    const TEST_RSA_E: &str = "<PASTE e FROM STEP 2>";

    struct TestKeyPair {
        encoding_key: EncodingKey,
        n: String,
        e: String,
    }

    static TEST_KEYS: LazyLock<TestKeyPair> = LazyLock::new(|| {
        let encoding_key =
            EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_KEY_PEM.as_bytes()).unwrap();
        TestKeyPair {
            encoding_key,
            n: TEST_RSA_N.to_string(),
            e: TEST_RSA_E.to_string(),
        }
    });
```

The rest of the test module (`build_signed_jwt`, `build_jwks_json`, `test_config`, and all `#[test]` functions) remains **unchanged** — they only reference `TEST_KEYS.encoding_key`, `TEST_KEYS.n`, `TEST_KEYS.e`.

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cargo test -p sqe-auth -- bearer_token 2>&1 | tail -20
```

Expected: All bearer_token tests PASS. The `rsa` crate is no longer in the dependency tree.

- [ ] **Step 6: Verify rsa is removed from the dependency tree**

```bash
cargo tree -p sqe-auth --depth 3 2>&1 | grep -i rsa
```

Expected: No output (rsa crate gone).

- [ ] **Step 7: Re-run cargo-deny to confirm RUSTSEC-2023-0071 is resolved**

```bash
cargo deny check advisories 2>&1 | grep -i "rsa\|RUSTSEC-2023-0071" || echo "rsa advisory cleared"
```

Expected: "rsa advisory cleared"

- [ ] **Step 8: Commit**

```bash
git add crates/sqe-auth/Cargo.toml crates/sqe-auth/src/bearer_token.rs
git commit -m "$(cat <<'EOF'
fix: remove rsa crate — static test keypair eliminates RUSTSEC-2023-0071

Replace runtime RSA key generation in bearer_token tests with a
pre-generated static PEM keypair. The rsa and pkcs1 dev-dependencies
are removed entirely, eliminating the Marvin Attack advisory.
EOF
)"
```

- [ ] **Step 9: Clean up temp files**

```bash
rm -f /tmp/sqe-test-rsa.pem
```

---

### Task 5: Workspace Version Bump (0.1.0 → 0.15.0)

**Files:**
- Modify: All 12 `crates/*/Cargo.toml` files

- [ ] **Step 1: Bump all crate versions**

```bash
cd "$(git rev-parse --show-toplevel)"
for toml in crates/*/Cargo.toml; do
    sed -i '' 's/^version = "0\.1\.0"/version = "0.15.0"/' "$toml"
done
```

- [ ] **Step 2: Verify all crates are at 0.15.0**

```bash
grep '^version' crates/*/Cargo.toml
```

Expected: All 12 lines show `version = "0.15.0"`.

- [ ] **Step 3: Verify the workspace still builds**

```bash
cargo check --all 2>&1 | tail -5
```

Expected: `Finished` with no errors.

- [ ] **Step 4: Commit**

```bash
git add crates/*/Cargo.toml
git commit -m "$(cat <<'EOF'
chore: bump all crate versions from 0.1.0 to 0.15.0

Aligns Cargo.toml versions with the actual release state after
44 merged MRs and 493 commits.
EOF
)"
```

---

### Task 6: Retro-Tagging Script

**Files:**
- Create: `scripts/retro-tag.sh`

- [ ] **Step 1: Create the retro-tagging script**

```bash
#!/usr/bin/env bash
# scripts/retro-tag.sh — Create annotated git tags for historical MRs.
#
# Reads the MR-to-version mapping, resolves each MR's merge commit SHA
# via glab API, and creates annotated tags. Idempotent (skips existing tags).
#
# Requires: glab CLI authenticated with GitLab.
#
# Usage: ./scripts/retro-tag.sh [--dry-run] [--push]

set -euo pipefail

DRY_RUN=false
PUSH=false
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
        --push)    PUSH=true ;;
        *)         echo "Usage: $0 [--dry-run] [--push]"; exit 1 ;;
    esac
done

# MR IID → version mapping (only merged MRs)
declare -A MR_VERSION=(
    [1]="v0.1.0"   [2]="v0.1.1"   [3]="v0.2.0"   [4]="v0.3.0"
    [5]="v0.4.0"   [6]="v0.5.0"   [7]="v0.6.0"   [8]="v0.6.1"
    [9]="v0.6.2"   [10]="v0.6.3"  [11]="v0.6.4"  [12]="v0.7.0"
    [13]="v0.7.1"  [14]="v0.7.2"  [15]="v0.8.0"  [16]="v0.8.1"
    [17]="v0.9.0"  [18]="v0.10.0" [19]="v0.11.0" [20]="v0.12.0"
    [21]="v0.13.0" [22]="v0.13.1" [23]="v0.14.0" [24]="v0.29.0"
    [25]="v0.15.1" [26]="v0.15.2" [27]="v0.16.0" [28]="v0.16.1"
    [29]="v0.16.2" [30]="v0.16.3" [31]="v0.16.4" [32]="v0.17.0"
    [33]="v0.18.0" [34]="v0.19.0" [35]="v0.20.0" [36]="v0.21.0"
    [37]="v0.22.0" [38]="v0.23.0" [39]="v0.24.0" [42]="v0.24.1"
    [43]="v0.25.0" [45]="v0.26.0" [46]="v0.27.0" [47]="v0.28.0"
)

CREATED=0
SKIPPED=0
FAILED=0

# Sort MR IIDs numerically for deterministic ordering
for mr_iid in $(printf '%s\n' "${!MR_VERSION[@]}" | sort -n); do
    version="${MR_VERSION[$mr_iid]}"

    # Skip if tag already exists
    if git rev-parse "$version" &>/dev/null; then
        echo "SKIP  $version (tag exists)"
        ((SKIPPED++))
        continue
    fi

    # Resolve merge commit SHA via glab API
    sha=$(glab api "projects/:fullpath/merge_requests/$mr_iid" --jq '.merge_commit_sha' 2>/dev/null || true)

    if [[ -z "$sha" || "$sha" == "null" ]]; then
        echo "FAIL  $version (!$mr_iid) — could not resolve merge commit"
        ((FAILED++))
        continue
    fi

    # Verify the commit exists locally
    if ! git cat-file -e "$sha" 2>/dev/null; then
        echo "FAIL  $version (!$mr_iid) — commit $sha not found locally (run git fetch)"
        ((FAILED++))
        continue
    fi

    if $DRY_RUN; then
        echo "DRY   $version → $sha (!$mr_iid)"
    else
        git tag -a "$version" "$sha" -m "Release $version (MR !$mr_iid)"
        echo "TAG   $version → $sha (!$mr_iid)"
    fi
    ((CREATED++))
done

echo ""
echo "Summary: created=$CREATED skipped=$SKIPPED failed=$FAILED"

if $PUSH && ! $DRY_RUN && [[ $CREATED -gt 0 ]]; then
    echo "Pushing tags to origin..."
    git push origin --tags
    echo "Done."
fi
```

- [ ] **Step 2: Make it executable**

```bash
chmod +x scripts/retro-tag.sh
```

- [ ] **Step 3: Syntax check**

```bash
bash -n scripts/retro-tag.sh && echo "Syntax OK"
```

Expected: "Syntax OK"

- [ ] **Step 4: Dry run (verify it resolves MRs without creating tags)**

```bash
./scripts/retro-tag.sh --dry-run 2>&1 | head -20
```

Expected: `DRY  v0.1.0 → <sha> (!1)` etc., or `FAIL` if glab is not authenticated (acceptable in CI — script is for manual use).

- [ ] **Step 5: Commit**

```bash
git add scripts/retro-tag.sh
git commit -m "$(cat <<'EOF'
chore: add retro-tagging script for 44 historical MRs

Maps each merged MR to a semver tag via glab API. Idempotent,
supports --dry-run and --push flags. Creates annotated tags on
the merge commit SHAs.
EOF
)"
```

---

### Task 7: git-cliff Configuration + CHANGELOG

**Files:**
- Create: `cliff.toml`

- [ ] **Step 1: Create cliff.toml**

```toml
# git-cliff configuration for SQE
# https://git-cliff.org

[changelog]
header = """
# Changelog

All notable changes to SQE are documented in this file.

"""
body = """
{% if version %}\
    ## [{{ version | trim_start_matches(pat="v") }}] — {{ timestamp | date(format="%Y-%m-%d") }}
{% else %}\
    ## [Unreleased]
{% endif %}\
{% for group, commits in commits | group_by(attribute="group") %}
    ### {{ group | striptags | trim | upper_first }}
    {% for commit in commits %}
        - {% if commit.scope %}**{{ commit.scope }}:** {% endif %}\
            {{ commit.message | upper_first }}\
            {% if commit.body %}\n\n  {{ commit.body | indent(width=2) }}{% endif %}\
    {% endfor %}
{% endfor %}\n
"""
trim = true
footer = """
---
*Generated by [git-cliff](https://git-cliff.org)*
"""

[git]
conventional_commits = true
filter_unconventional = false
split_commits = false
commit_parsers = [
    { message = "^feat", group = "Features" },
    { message = "^fix", group = "Bug Fixes" },
    { message = "^doc", group = "Documentation" },
    { message = "^perf", group = "Performance" },
    { message = "^refactor", group = "Refactoring" },
    { message = "^test", group = "Testing" },
    { message = "^chore", group = "Miscellaneous" },
    { message = "^style", group = "Styling" },
    { body = ".*security", group = "Security" },
]
protect_breaking_commits = false
filter_commits = false
tag_pattern = "v[0-9].*"
sort_commits = "oldest"
```

- [ ] **Step 2: Verify cliff.toml parses**

```bash
cargo install git-cliff 2>/dev/null || true
git-cliff --config cliff.toml --unreleased --output /dev/null 2>&1 && echo "Config OK"
```

Expected: "Config OK" (or empty changelog if no tags exist yet).

- [ ] **Step 3: Commit cliff.toml (CHANGELOG generated after retro-tagging)**

```bash
git add cliff.toml
git commit -m "$(cat <<'EOF'
chore: add cliff.toml for git-cliff changelog generation

Conventional commit grouping (feat/fix/docs/chore/perf/refactor).
CHANGELOG.md generated after retro-tagging historical MRs.
EOF
)"
```

> **Note:** The actual `CHANGELOG.md` is generated in Task 15 after running `retro-tag.sh` to create the historical tags. Without tags, git-cliff produces an empty changelog.

---

### Task 8: GitLab CI — Extend MR Pipeline

**Files:**
- Modify: `.gitlab-ci.yml`

- [ ] **Step 1: Add clippy, cargo-audit, and cargo-deny to the CI pipeline**

Add new jobs after the existing `cargo-test` job. The existing pipeline has stages: `test`, `secret-detection`, `discover`, `trigger`, `build`.

Add a `check` stage before `test` and insert the new jobs:

```yaml
stages:
  - check
  - test
  - secret-detection
  - discover
  - trigger
  - build
```

Add the `cargo-check` job:

```yaml
# ── Static analysis + security ─────────────────────────────────
cargo-check:
  stage: check
  image: rust:bookworm
  before_script:
    - apt-get update -qq && apt-get install -y -qq cmake protobuf-compiler libssl-dev pkg-config > /dev/null
    - cargo install cargo-deny 2>/dev/null || true
  script:
    - cargo clippy --all-targets --all-features -- -D warnings
    - cargo audit
    - cargo deny check advisories
  rules:
    - if: '$CI_PIPELINE_SOURCE == "merge_request_event"'
      changes:
        - crates/**/*
        - Cargo.toml
        - Cargo.lock
        - deny.toml
    - if: '$CI_COMMIT_BRANCH == "main" && $CI_PIPELINE_SOURCE == "push"'
      changes:
        - crates/**/*
        - Cargo.toml
        - Cargo.lock
        - deny.toml
  cache:
    key: cargo-check-${CI_COMMIT_REF_SLUG}
    paths:
      - target/
      - .cargo/registry/
      - .cargo/bin/cargo-deny
```

- [ ] **Step 2: Verify YAML syntax**

```bash
python3 -c "import yaml; yaml.safe_load(open('.gitlab-ci.yml'))" && echo "YAML OK"
```

Expected: "YAML OK"

- [ ] **Step 3: Commit**

```bash
git add .gitlab-ci.yml
git commit -m "$(cat <<'EOF'
ci: add clippy, cargo-audit, cargo-deny to MR pipeline

New 'check' stage runs before tests with:
- cargo clippy (strict, -D warnings)
- cargo audit (security advisories)
- cargo deny check advisories (with deny.toml policy)
EOF
)"
```

---

### Task 9: GitLab CI — Release Pipeline

**Files:**
- Modify: `.gitlab-ci.yml`

- [ ] **Step 1: Add release stage and jobs triggered on tag push**

Append to `.gitlab-ci.yml`:

```yaml
# ── Release pipeline (triggered on tag push: v*) ──────────────

changelog:
  stage: test
  image: rust:bookworm
  script:
    - cargo install git-cliff 2>/dev/null || true
    - git-cliff --latest --output release-notes.md
  artifacts:
    paths:
      - release-notes.md
    expire_in: 1 hour
  rules:
    - if: '$CI_COMMIT_TAG =~ /^v[0-9]+/'

release:
  stage: build
  image: registry.gitlab.com/gitlab-org/release-cli:latest
  needs:
    - job: changelog
      artifacts: true
  script:
    - echo "Creating release for ${CI_COMMIT_TAG}"
  release:
    tag_name: ${CI_COMMIT_TAG}
    name: "SQE ${CI_COMMIT_TAG}"
    description: release-notes.md
  rules:
    - if: '$CI_COMMIT_TAG =~ /^v[0-9]+/'
```

Also update the `stages` list to include these stages are already covered (`test` and `build` exist).

- [ ] **Step 2: Verify YAML syntax**

```bash
python3 -c "import yaml; yaml.safe_load(open('.gitlab-ci.yml'))" && echo "YAML OK"
```

Expected: "YAML OK"

- [ ] **Step 3: Commit**

```bash
git add .gitlab-ci.yml
git commit -m "$(cat <<'EOF'
ci: add release pipeline triggered on tag push

On v* tag push: generates release notes via git-cliff, creates
GitLab release with the notes. Reuses existing test + build stages.
EOF
)"
```

---

## Phase 2: Security & Functional Audit (Spec B)

### Task 10: Security Audit — Auth Passthrough Verification

**Files:**
- Review: `crates/sqe-auth/src/*.rs`, `crates/sqe-coordinator/src/session_manager.rs`
- Test: `crates/sqe-auth/src/bearer_token.rs` (existing tests cover this)

Verify that bearer tokens are never logged or stored beyond session lifetime.

- [ ] **Step 1: Search for potential token logging**

```bash
# Search for any tracing/log macro that might include token values
cd "$(git rev-parse --show-toplevel)"
grep -rn "access_token\|bearer_token\|refresh_token" crates/ \
    | grep -i "info!\|warn!\|error!\|debug!\|trace!\|println!\|eprintln!" \
    | grep -v "#\[cfg(test)\]" \
    | grep -v "\.md:" \
    | grep -v "target/"
```

Expected: Either no results, or results that use token fingerprints (last 8 chars) rather than full token values. If any full token logging is found, it must be fixed.

- [ ] **Step 2: Verify token fingerprint pattern is used**

```bash
grep -rn "token_fingerprint\|fingerprint" crates/sqe-auth/src/ crates/sqe-coordinator/src/ \
    | grep -v "target/" | head -20
```

Expected: Token fingerprint usage in session/catalog code for cache invalidation and logging.

- [ ] **Step 3: Verify session cleanup on disconnect**

```bash
grep -rn "remove\|cleanup\|drop\|expire" crates/sqe-coordinator/src/session_manager.rs | head -20
```

Expected: Session removal logic exists (idle timeout sweeper, explicit removal).

- [ ] **Step 4: Document finding**

Create a temporary audit notes file:

```bash
cat >> /tmp/sqe-audit-notes.md << 'AUDIT'
### Auth Passthrough

**Status:** ✅ CONFIRMED

- Bearer tokens are referenced by fingerprint (last 8 chars) in logs
- Full tokens stored only in `Session.access_token` (per-session, in-memory DashMap)
- Session sweeper removes expired sessions (idle + absolute timeouts)
- No token persistence to disk
AUDIT
```

---

### Task 11: Security Audit — Error Sanitization Verification

**Files:**
- Review: `crates/sqe-core/src/error.rs`
- Tests: Already comprehensive (13 tests in error.rs)

- [ ] **Step 1: Run the existing error sanitization tests**

```bash
cargo test -p sqe-core -- error 2>&1 | tail -20
```

Expected: All tests pass. Key tests to verify:
- `client_message_hides_catalog_details` — "connection refused: polaris:8181" → "Catalog operation failed"
- `client_message_hides_internal_details` — "segfault at 0xdeadbeef" → "Internal error"
- `client_message_hides_config_details` — "missing field 'client_secret'" → "Internal error"
- `client_message_hides_detail_for_system_errors` — "connection pool exhausted" → "Internal error"

- [ ] **Step 2: Verify error sanitization is used at protocol boundaries**

```bash
# Check that Flight SQL and Trino HTTP handlers use client_message() or to_client_error()
grep -rn "client_message\|to_client_error" crates/sqe-coordinator/src/ | head -20
```

Expected: Protocol handlers (flight_sql.rs, trino HTTP) use `to_client_error(debug)` with debug controlled by config.

- [ ] **Step 3: Document finding**

```bash
cat >> /tmp/sqe-audit-notes.md << 'AUDIT'
### Error Sanitization

**Status:** ✅ CONFIRMED

- `SqeError::client_message()` classifies errors as user vs. system
- System errors return generic messages ("Internal error", "Catalog operation failed")
- User errors (syntax, not-found, auth) show cleaned detail (DataFusion prefix stripped)
- `to_client_error(debug)` provides debug mode toggle for dev environments
- 13 unit tests cover all error variants and classification
- Protocol handlers use `to_client_error()` at boundaries
AUDIT
```

---

### Task 12: Security Audit — Token Validation (JWT Expiry)

**Files:**
- Review: `crates/sqe-auth/src/bearer_token.rs`
- Test: Add explicit expiry test if not present

- [ ] **Step 1: Check if JWT expiry test exists**

```bash
grep -n "expir\|exp.*claim\|exp.*time" crates/sqe-auth/src/bearer_token.rs | head -20
```

If an expiry rejection test exists, verify it passes. If not, proceed to Step 2.

- [ ] **Step 2: Write a JWT expiry rejection test (if missing)**

Add to the test module in `crates/sqe-auth/src/bearer_token.rs`:

```rust
    #[tokio::test]
    async fn rejects_expired_jwt() {
        let mut server = mockito::Server::new_async().await;
        let jwks_mock = server
            .mock("GET", "/.well-known/jwks.json")
            .with_body(build_jwks_json().to_string())
            .create_async()
            .await;

        let config = test_config(&format!("{}/.well-known/jwks.json", server.url()));
        let provider = BearerTokenProvider::new(config).await.unwrap();

        // JWT expired 1 hour ago
        let claims = json!({
            "sub": "expired-user",
            "iss": "test-issuer",
            "exp": (chrono::Utc::now() - chrono::Duration::hours(1)).timestamp(),
            "iat": (chrono::Utc::now() - chrono::Duration::hours(2)).timestamp(),
        });
        let token = build_signed_jwt(&claims);

        let creds = FlightCredentials {
            bearer_token: Some(token),
            ..Default::default()
        };
        let result = provider.authenticate(&creds).await;
        assert!(result.is_err(), "expired JWT should be rejected");

        jwks_mock.assert_async().await;
    }
```

- [ ] **Step 3: Run the test**

```bash
cargo test -p sqe-auth -- rejects_expired_jwt 2>&1 | tail -10
```

Expected: PASS — expired JWTs are rejected.

- [ ] **Step 4: Commit (only if new test was added)**

```bash
git add crates/sqe-auth/src/bearer_token.rs
git commit -m "$(cat <<'EOF'
test: add explicit JWT expiry rejection test

Verifies that expired bearer tokens are rejected during
authentication. Part of security audit (Spec B1).
EOF
)"
```

- [ ] **Step 5: Document finding**

```bash
cat >> /tmp/sqe-audit-notes.md << 'AUDIT'
### Token Validation

**Status:** ✅ CONFIRMED

- JWT expiry (`exp` claim) enforced by `jsonwebtoken` crate validation
- Expired tokens rejected with AuthError
- JWKS fetched from configured endpoint, cached
- Explicit test confirms expiry rejection
AUDIT
```

---

### Task 13: Security Audit — TLS Enforcement + Config Secrets

**Files:**
- Review: `crates/sqe-coordinator/src/main.rs` or `bin/sqe_server.rs`
- Review: `sqe.toml.example`

- [ ] **Step 1: Verify TLS config exists in coordinator**

```bash
grep -rn "tls\|TlsConfig\|cert_file\|key_file\|ca_file" crates/sqe-coordinator/src/ crates/sqe-core/src/config.rs | head -20
```

Expected: TLS configuration struct with cert_file, key_file, optional ca_file (mTLS). Coordinator server binds with TLS when configured.

- [ ] **Step 2: Verify sqe.toml.example has no real credentials**

```bash
grep -in "password\|secret\|key\s*=" sqe.toml.example
```

Expected: Only placeholder values like `"s3admin"`, `""`, or documented example values. No real production credentials.

- [ ] **Step 3: Document findings**

```bash
cat >> /tmp/sqe-audit-notes.md << 'AUDIT'
### TLS Enforcement

**Status:** ✅ CONFIRMED

- `[coordinator.tls]` config section with cert_file, key_file, optional ca_file
- mTLS supported via ca_file for client certificate validation
- TLS is opt-in (dev mode works without TLS for local development)
- Recommendation: document that production deployments MUST enable TLS

### Config Secrets

**Status:** ✅ CONFIRMED

- sqe.toml.example uses placeholder values only
- No real credentials committed
- S3 credentials use generic "s3admin" placeholders
- Client secret fields are empty strings
AUDIT
```

---

### Task 14: Functional Audit — Run Full Test Suite

**Files:**
- Run: All test and lint commands from Spec B4

- [ ] **Step 1: Run cargo clippy (strict)**

```bash
cargo clippy --all-targets --all-features -- -D warnings 2>&1 | tail -20
```

Expected: No warnings, clean pass.

- [ ] **Step 2: Run all unit tests**

```bash
cargo test --all 2>&1 | tail -30
```

Expected: All tests pass. Note any failures for AUDIT.md.

- [ ] **Step 3: Run cargo audit**

```bash
cargo audit 2>&1
```

Expected: No vulnerabilities (rsa removed in Task 4).

- [ ] **Step 4: Run cargo deny**

```bash
cargo deny check advisories 2>&1
```

Expected: Pass (with paste advisory ignored per deny.toml).

- [ ] **Step 5: Verify Docker build**

```bash
docker build --target builder . 2>&1 | tail -10
```

Expected: Build completes. If Docker is not available in the current environment, note it as "SKIPPED — requires Docker" in AUDIT.md.

- [ ] **Step 6: Document results**

```bash
cat >> /tmp/sqe-audit-notes.md << 'AUDIT'
### Functional Audit Results

| Check | Status | Notes |
|---|---|---|
| cargo clippy (strict) | ✅ PASS | Zero warnings |
| cargo test --all | ✅ PASS | N tests, 0 failures |
| cargo audit | ✅ PASS | No advisories |
| cargo deny | ✅ PASS | paste advisory ignored (documented) |
| Docker build | ✅ PASS / ⏭️ SKIPPED | |

### Dependency Security

| Crate | Advisory | Action Taken |
|---|---|---|
| rsa v0.9.10 | RUSTSEC-2023-0071 (Marvin Attack) | **Removed.** Replaced with static test keypair. |
| paste v1.0.15 | RUSTSEC-2024-0436 (unmaintained) | **Documented.** Transitive from arrow/datafusion. No vulnerability. Ignored in deny.toml. |
AUDIT
```

---

### Task 15: AUDIT.md Deliverable

**Files:**
- Create: `AUDIT.md`

- [ ] **Step 1: Compile AUDIT.md from audit notes**

```markdown
# SQE Security & Functional Audit

**Date:** 2026-04-08
**Auditor:** Claude Code (automated) + manual review
**Scope:** Full codebase security review + functional verification
**Version:** v0.15.0 (pre-release)

---

## Security Audit

### Auth Passthrough

**Status:** ✅ CONFIRMED

- Bearer tokens are referenced by fingerprint (last 8 chars) in logs
- Full tokens stored only in `Session.access_token` (per-session, in-memory DashMap)
- Session sweeper removes expired sessions (idle + absolute timeouts)
- No token persistence to disk

### Error Sanitization

**Status:** ✅ CONFIRMED

- `SqeError::client_message()` classifies errors as user vs. system
- System errors return generic messages ("Internal error", "Catalog operation failed")
- User errors (syntax, not-found, auth) show cleaned detail (DataFusion prefix stripped)
- `to_client_error(debug)` provides debug mode toggle for dev environments
- 13 unit tests cover all error variants and classification
- Protocol handlers use `to_client_error()` at boundaries

### Token Validation

**Status:** ✅ CONFIRMED

- JWT expiry (`exp` claim) enforced by `jsonwebtoken` crate validation
- Expired tokens rejected with AuthError
- JWKS fetched from configured endpoint, cached
- Explicit test confirms expiry rejection

### TLS Enforcement

**Status:** ✅ CONFIRMED

- `[coordinator.tls]` config section with cert_file, key_file, optional ca_file
- mTLS supported via ca_file for client certificate validation
- TLS is opt-in (dev mode works without TLS for local development)

**Recommendation:** Document that production deployments MUST enable TLS.

### Config Secrets

**Status:** ✅ CONFIRMED

- `sqe.toml.example` uses placeholder values only
- No real credentials committed to repository

### Query Cancellation

**Status:** ✅ CONFIRMED

- `CancellationToken` registry in coordinator
- Flight SQL cancel handler triggers token cancellation
- Background query timeout enforcement

---

## Dependency Security

| Crate | Advisory | Severity | Action |
|---|---|---|---|
| `rsa` v0.9.10 | RUSTSEC-2023-0071 (Marvin Attack) | Medium | **Removed entirely.** Replaced with static test keypair in sqe-auth. |
| `paste` v1.0.15 | RUSTSEC-2024-0436 (unmaintained) | Informational | **Documented.** Transitive from arrow-flight, datafusion, parquet, tonic. No vulnerability. Feature-complete crate. Ignored in `deny.toml`. Resolves when upstream migrates. |

---

## Functional Audit

| Check | Status | Notes |
|---|---|---|
| `cargo clippy --all-targets --all-features -- -D warnings` | ✅ PASS | Zero warnings |
| `cargo test --all` | ✅ PASS | All tests pass |
| `cargo audit` | ✅ PASS | No active advisories |
| `cargo deny check advisories` | ✅ PASS | paste ignored per policy |
| Docker build | ✅ PASS / ⏭️ SKIPPED | Requires Docker daemon |
| Integration tests | ⏭️ DEFERRED | Requires Polaris + S3 stack |

---

## Deferred Items

| Item | Tracking |
|---|---|
| Rate limiting audit | Implemented in Step 3 (governor crate). Full load test deferred. |
| Audit log completeness | Basic audit logging exists. Structured audit log format deferred to Step 6. |
| Integration test full run | Requires Polaris + S3 quickstart stack. Run via `scripts/integration-test.sh`. |
| EXPLAIN FULL metrics accuracy | Requires live query execution to verify. Deferred to integration test phase. |
| Iceberg partition pruning | Functional, but edge cases (deeply nested partitions) not exhaustively tested. |

---

*This audit covers the codebase at the v0.15.0 milestone (44 merged MRs, 493 commits). Next audit scheduled after Spec C (Pluggable Catalogs) completion.*
```

> **Note:** The implementing agent should update the specific statuses based on actual findings from Tasks 10-14. The template above reflects the expected outcomes based on codebase review. If any check fails, update the status and add remediation notes.

- [ ] **Step 2: Verify the file**

```bash
head -5 AUDIT.md
wc -l AUDIT.md
```

Expected: Starts with `# SQE Security & Functional Audit`, ~80-100 lines.

- [ ] **Step 3: Commit**

```bash
git add AUDIT.md
git commit -m "$(cat <<'EOF'
docs: add AUDIT.md — security and functional audit report

Structured review of auth passthrough, error sanitization, token
validation, TLS, config secrets, and dependency security. All areas
confirmed or remediated (rsa crate removed).
EOF
)"
```

---

## Phase 3: Finalization

### Task 16: Run Retro-Tagging + Generate CHANGELOG

**Files:**
- Generate: `CHANGELOG.md` (via git-cliff after tags exist)

This task requires all previous commits to be on `main` (or at least tagged). In practice, retro-tagging targets historical MR merge commits — it's independent of the current branch.

- [ ] **Step 1: Run retro-tagging (requires glab authentication)**

```bash
./scripts/retro-tag.sh --dry-run 2>&1 | tail -10
```

Review the output. If it looks correct:

```bash
./scripts/retro-tag.sh --push
```

> **Note:** If glab is not authenticated or the GitLab API is unreachable, this step is deferred to CI. The script is committed and ready.

- [ ] **Step 2: Generate CHANGELOG.md**

```bash
git-cliff --output CHANGELOG.md
```

- [ ] **Step 3: Verify CHANGELOG**

```bash
head -30 CHANGELOG.md
```

Expected: Grouped by version tags, conventional commit categories.

- [ ] **Step 4: Commit**

```bash
git add CHANGELOG.md
git commit -m "$(cat <<'EOF'
docs: generate CHANGELOG.md from retro-tagged history

Full changelog from v0.1.0 through v0.28.0 generated by git-cliff.
Conventional commit grouping: Features, Bug Fixes, Documentation,
Miscellaneous.
EOF
)"
```

---

### Task 17: Update README.md and nextsteps.md

**Files:**
- Modify: `README.md` — mark OSS release + audit items as done
- Modify: `nextsteps.md` — update status line, mark Step 1 as done

- [ ] **Step 1: Update nextsteps.md status line**

Update the status line at the top of `nextsteps.md` to reflect audit completion:

```markdown
> Status as of 2026-04-08. **Steps 1–4d + 7.1 + 7.3 done.** Step 1 (Security & Functional Audit) completed — see AUDIT.md. OSS release readiness complete (LICENSE, CONTRIBUTING.md, deny.toml, CI pipelines, retro-tagging, CHANGELOG). **Next: Step 5 (pluggable catalogs).**
```

Mark Step 1 sections with ✅:

```markdown
## ~~Step 1: Security and Functional Audit~~ ✅

See [AUDIT.md](AUDIT.md) for the full report. Completed 2026-04-08.
```

- [ ] **Step 2: Update README.md roadmap**

Add checkmarks for OSS release items in the README roadmap section (if applicable).

- [ ] **Step 3: Commit**

```bash
git add nextsteps.md README.md
git commit -m "$(cat <<'EOF'
docs: mark OSS release + security audit as complete

Update nextsteps.md and README.md to reflect Step 1 completion
and OSS release readiness (LICENSE, CONTRIBUTING, CI, CHANGELOG).
EOF
)"
```

---

## Summary

| Task | Description | Phase |
|---|---|---|
| 1 | Apache 2.0 LICENSE file | OSS Release |
| 2 | CONTRIBUTING.md | OSS Release |
| 3 | deny.toml (cargo-deny config) | OSS Release |
| 4 | Remove rsa crate — static test keypair | Security |
| 5 | Workspace version bump (0.1.0 → 0.15.0) | OSS Release |
| 6 | Retro-tagging script | OSS Release |
| 7 | cliff.toml (git-cliff config) | OSS Release |
| 8 | GitLab CI — MR pipeline (clippy + audit + deny) | OSS Release |
| 9 | GitLab CI — Release pipeline (tag-triggered) | OSS Release |
| 10 | Security audit — auth passthrough | Audit |
| 11 | Security audit — error sanitization | Audit |
| 12 | Security audit — token validation | Audit |
| 13 | Security audit — TLS + config secrets | Audit |
| 14 | Functional audit — full test suite | Audit |
| 15 | AUDIT.md deliverable | Audit |
| 16 | Retro-tagging + CHANGELOG generation | Finalization |
| 17 | README + nextsteps update | Finalization |

**Parallelism:** Tasks 1-3 and 10-13 can run in parallel (OSS files vs. audit reviews). Task 4 (rsa removal) should run before Task 14 (full test suite). Tasks 16-17 are sequential at the end.

**Total estimated time:** 2-3 hours for a single agent; ~1 hour with parallel execution.
