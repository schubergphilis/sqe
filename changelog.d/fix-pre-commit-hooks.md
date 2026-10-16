### Changed

- Pre-commit hooks check Rust formatting, the supply chain, shell scripts, and clippy

    Besides gitleaks and the file checks, a commit now runs a rustfmt
    ratchet on staged `.rs` files: it fails on formatting drift that HEAD
    does not already have, so the existing drift does not block commits.
    Staging `Cargo.toml`, `Cargo.lock`, `deny.toml` or `.cargo/audit.toml`
    runs the lockfile, `cargo hakari`, `cargo deny` and `cargo audit` checks;
    a changed crate manifest also runs `cargo machete`. shellcheck runs at
    error severity, and clippy (warnings denied) runs at pre-push.
    `make pre-commit-repo` runs the repo-wide checks without staging
    anything. Re-run `pre-commit install` to add the pre-push hook.

### Fixed

- Clippy passes on the 1.99 toolchain

    Ten lints new with the toolchain bump (`manual_is_multiple_of`,
    `chunks_exact` with a constant size, one argument count) are fixed.
