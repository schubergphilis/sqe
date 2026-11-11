### Fixed

- CI test jobs reuse the prebuilt dependency image again

    `aikido-test-rust` and `aikido-rust-extra` pinned a `ci-deps` image built with rustc
    1.98.1. The toolchain moved to 1.99.1 on 2026-10-18, so cargo rejected every baked
    artifact and both jobs compiled all 679 crates from `proc-macro2` on every merge
    request, 12 to 24 minutes depending on the runner. The pin now points at the image
    built from the current `Cargo.lock`, toolchain and `vendor/` tree.
