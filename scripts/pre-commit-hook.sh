#!/usr/bin/env bash
# Rust checks run from .pre-commit-config.yaml. One subcommand per hook:
#
#   rustfmt  FILE...  staged .rs files must not add rustfmt drift (ratchet)
#   lockfile          Cargo.lock matches the manifests (cargo metadata --locked)
#   hakari            crates/workspace-hack is in sync with Cargo.lock
#   deny              cargo-deny advisories, bans, sources (deny.toml)
#   audit             cargo-audit RUSTSEC advisories (.cargo/audit.toml)
#   machete           no unused dependencies (cargo-machete)
#   clippy            clippy on the crates changed since origin/main, warnings denied
#   clippy-all        clippy on the whole workspace, warnings denied
#
# rustfmt is a ratchet, not a full check: about 275 files on main carry
# rustfmt drift, and reformatting them all in one change would bury every
# real edit (`cargo fmt --all` stays off limits). A staged file fails when
# rustfmt reports a hunk that its HEAD version does not have, so new code
# must be formatted and so must an edit inside an already-drifted hunk. A
# new file must be clean. Formatting a touched file in full is always fine:
#   rustfmt --edition 2021 <file>
set -euo pipefail

need() {
    command -v "$1" >/dev/null 2>&1 && return 0
    echo "$1 not found. Install with: $2" >&2
    exit 1
}

# The rustfmt hunks of the source read from stdin, one per line: the hunk
# body with its line-number header dropped, so a hunk that only moved
# still matches.
fmt_hunks() {
    { rustfmt --edition 2021 --check 2>/dev/null || true; } | awk '
        /^Diff in / { if (h != "") print h; h = "@"; next }
        { h = h "\x1f" $0 }
        END { if (h != "") print h }'
}

check_rustfmt() {
    need rustfmt "rustup component add rustfmt"
    local failed=0 file staged head new
    for file in "$@"; do
        # Read the index, not the working tree: the commit is what counts.
        staged=$(git show ":$file" | fmt_hunks)
        [ -n "$staged" ] || continue
        if git cat-file -e "HEAD:$file" 2>/dev/null; then
            head=$(git show "HEAD:$file" | fmt_hunks)
        else
            head=""
        fi
        new=$(grep -Fxv -f <(printf '%s\n' "$head") <<<"$staged" || true)
        if [ -n "$new" ]; then
            echo "$file: rustfmt changes not present at HEAD:" >&2
            printf '%s\n' "$new" | tr '\037' '\n' | sed 's/^@$/---/' >&2
            failed=1
        fi
    done
    if [ "$failed" -ne 0 ]; then
        echo "Fix with: rustfmt --edition 2021 <file> (then re-stage)" >&2
        exit 1
    fi
}

clippy_workspace() {
    cargo clippy --workspace --all-targets --exclude sqe-cli -- -D warnings
    cargo clippy --package sqe-cli --all-targets --no-default-features -- -D warnings
}

# Clippy on the crates the branch changes since it left origin/main
# (CLIPPY_BASE overrides the base). A change outside crates/ and xtask/
# that can move every crate's lints (Cargo.toml, Cargo.lock, .cargo/,
# clippy.toml, rust-toolchain.toml) lints the whole workspace, as does a
# missing base. Dependents of a changed crate are not linted: the MR
# pipeline compiles the workspace, and `make lint` lints all of it.
check_clippy() {
    local base files crates=() crate
    base=${CLIPPY_BASE:-$(git merge-base HEAD origin/main 2>/dev/null || true)}
    if [ -z "$base" ]; then
        echo "clippy: no merge base with origin/main, linting the workspace" >&2
        clippy_workspace
        return
    fi
    files=$(git diff --name-only "$base" HEAD)
    if grep -qE '^(Cargo\.(toml|lock)|\.cargo/|clippy\.toml|rust-toolchain\.toml)' <<<"$files"; then
        echo "clippy: workspace-wide change, linting the workspace" >&2
        clippy_workspace
        return
    fi
    while read -r crate; do
        [ -n "$crate" ] && [ -f "$crate/Cargo.toml" ] && crates+=("$(basename "$crate")")
    done < <(grep -oE '^(crates/[^/]+|xtask)/' <<<"$files" | sed 's:/$::' | sort -u)
    if [ "${#crates[@]}" -eq 0 ]; then
        echo "clippy: no crate changed since $(git rev-parse --short "$base")" >&2
        return
    fi
    echo "clippy: ${crates[*]}" >&2
    local args=()
    for crate in "${crates[@]}"; do
        [ "$crate" = sqe-cli ] || args+=(--package "$crate")
    done
    if [ "${#args[@]}" -gt 0 ]; then
        cargo clippy "${args[@]}" --all-targets -- -D warnings
    fi
    if printf '%s\n' "${crates[@]}" | grep -qx sqe-cli; then
        cargo clippy --package sqe-cli --all-targets --no-default-features -- -D warnings
    fi
}

case "${1:-}" in
    rustfmt)
        shift
        check_rustfmt "$@"
        ;;
    lockfile)
        cargo metadata --locked --format-version 1 >/dev/null
        ;;
    hakari)
        need cargo-hakari "cargo install cargo-hakari --locked"
        cargo hakari generate --diff
        ;;
    deny)
        need cargo-deny "cargo install cargo-deny --locked"
        cargo deny check advisories bans sources
        ;;
    audit)
        need cargo-audit "cargo install cargo-audit --locked"
        cargo audit
        ;;
    machete)
        need cargo-machete "cargo install cargo-machete --locked"
        cargo machete --with-metadata crates/sqe-*
        ;;
    clippy)
        check_clippy
        ;;
    clippy-all)
        clippy_workspace
        ;;
    *)
        echo "usage: $0 {rustfmt FILE...|lockfile|hakari|deny|audit|machete|clippy|clippy-all}" >&2
        exit 2
        ;;
esac
