### Fixed

- CI test jobs reuse the dependency image built with Rust 1.100

    The Rust jobs still pinned the `ci-deps` image built with rustc 1.99.1, so after
    the toolchain bump every job rebuilt all dependencies. The pin now points at the
    image built on main from 87a6f695 with rustc 1.100.0.
