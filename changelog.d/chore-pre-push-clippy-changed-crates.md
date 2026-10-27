### Changed

- The pre-push clippy hook lints only the crates a branch changed

    The hook linted the whole workspace on every push, about three minutes
    even for a one-crate change. It now lints the crates changed since the
    branch left `origin/main` (18 s warm for a four-crate change). A change
    to `Cargo.toml`, `Cargo.lock`, `.cargo/`, `clippy.toml` or
    `rust-toolchain.toml` still lints the whole workspace, and so does a
    branch with no merge base. `make lint` lints the whole workspace;
    `make pre-commit-all` now calls it.
