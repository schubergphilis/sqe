### Fixed

- CI test jobs start again after the dependency image was rebuilt

    `aikido-test-rust` and `aikido-rust-extra` failed on every merge request with
    `manifest unknown`: the pinned `ci-deps` digest no longer existed in the registry
    after the dependency refresh rebuilt the image. The pin now points at
    `ci-deps:lock-a73c967e88a4`, built from the current `Cargo.lock`, toolchain and
    `vendor/` tree.
