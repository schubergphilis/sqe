### Fixed

- CI test jobs reuse the dependency image built for the current `vendor/` tree

    The #897 merge changed the vendored iceberg-rust, so the `ci-deps` image pinned the
    same day no longer matched it and every crate downstream of `vendor/` would rebuild
    in each merge request. The pin now points at the image built from de95a8ed.
