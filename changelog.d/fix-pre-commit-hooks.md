### Changed

- Pre-commit hooks check Rust formatting, the supply chain, shell scripts, and clippy

    Besides gitleaks and the file checks, a commit now runs a rustfmt
    ratchet on staged `.rs` files: it fails on formatting drift that HEAD
    does not already have, so the existing drift does not block commits.
    Staging `Cargo.toml`, `Cargo.lock`, `deny.toml` or `.cargo/audit.toml`
    runs the lockfile, `cargo hakari`, `cargo deny` and `cargo audit` checks;
    a changed crate manifest also runs `cargo machete`. shellcheck runs at
    warning severity, and clippy (warnings denied) runs at pre-push.
    The whole tree passes every hook, so `make pre-commit-all`
    (`pre-commit run --all-files`, both stages) is a usable gate: the
    whitespace fixers skip generated output (benchmark reports, captured
    plans, rendered diagrams, test data, the vendored mermaid bundle).
    Re-run `pre-commit install` to add the pre-push hook.

### Fixed

- Clippy passes on the 1.99 toolchain

    Ten lints new with the toolchain bump (`manual_is_multiple_of`,
    `chunks_exact` with a constant size, one argument count) are fixed.

- Shell scripts pass shellcheck at warning severity

    Unchecked `cd`s exit on failure, dead variables are removed, and
    `local x=$(...)` captures are split. The trino-compat captures keep
    their old continue-on-failure behaviour with `|| true`; the AWS
    quickstarts now stop when `aws sts get-caller-identity` fails
    instead of deploying with an empty account.
