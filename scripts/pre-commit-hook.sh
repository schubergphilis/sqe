#!/usr/bin/env bash
# Rust checks run from .pre-commit-config.yaml. One subcommand per hook:
#
#   rustfmt  FILE...  staged .rs files must not add rustfmt drift (ratchet)
#   lockfile          Cargo.lock matches the manifests (cargo metadata --locked)
#   hakari            crates/workspace-hack is in sync with Cargo.lock
#   deny              cargo-deny advisories, bans, sources (deny.toml)
#   audit             cargo-audit RUSTSEC advisories (.cargo/audit.toml)
#   machete           no unused dependencies (cargo-machete)
#   clippy            clippy on the whole workspace, warnings denied
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
        cargo clippy --workspace --all-targets --exclude sqe-cli -- -D warnings
        cargo clippy --package sqe-cli --all-targets --no-default-features -- -D warnings
        ;;
    *)
        echo "usage: $0 {rustfmt FILE...|lockfile|hakari|deny|audit|machete|clippy}" >&2
        exit 2
        ;;
esac
