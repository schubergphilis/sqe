### Fixed

- A bare `cargo test` no longer aborts on a stack overflow

    `.cargo/config.toml` sets `RUST_MIN_STACK` to 32 MiB for the processes
    cargo starts, as `make test` and CI already did. Debug builds of the
    coordinator Flight SQL tests overflowed Rust's 2 MiB test-thread default
    and took the whole test binary down. Deployed binaries are unaffected:
    the server runtime sizes its own threads.
