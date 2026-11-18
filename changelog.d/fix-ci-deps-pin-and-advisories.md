### Fixed

- MR pipelines pass again: the dependency image pin, the advisory review
  dates and an unused `sqe-cli` dependency

    The pinned `ci-deps` digest no longer existed in the registry, so
    `aikido-test-rust`, `aikido-rust-extra` and `catalog-live-tests` failed
    to pull their image. The pin now names the image built from the current
    `Cargo.lock`, toolchain and `vendor/`. The three chrono, chrono-tz and
    humantime advisory ignores in `deny.toml` carry their review date in the
    form the advisory check reads. `sqe-cli` no longer depends on
    `sqe-policy`, which it stopped using, so `deps-check` passes on main.
