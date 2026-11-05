### Changed

- Toolchain refresh: Docker build images on Rust 1.99.1, Cargo.lock refreshed within semver bounds

    The `Dockerfile` and `Dockerfile.bench` builder images move from `rust:1.99.0` to `rust:1.99.1`,
    matching `rust-toolchain.toml`, and the `cargo-chef` image digest is refreshed. `make
    lock-maintenance` moved 65 dependencies to their newest semver-compatible release (aws-sdk-*,
    aws-lc-rs, tonic 0.14.9, icu 2.4, arrow-flight 59.3.2 and others); the workspace-hack needed no
    change. The vendored `datafusion-physical-plan` TopK dynamic filter calls `try_update` instead of
    the `fetch_update` that Rust 1.99 deprecated, which removes the one warning in a release build,
    and its `rust-version` is raised to 1.99 to match. Major upgrades (arrow/parquet 60, sqlparser
    0.63) wait for a DataFusion release that uses them.
