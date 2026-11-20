### Fixed

- SAST: the catalog-scope test's ES256 key no longer fails `aikido-sast`

    `crates/sqe-coordinator/tests/it/catalog_scope_e2e.rs` embeds a published
    test keypair that signs only test JWTs. It now carries a `nosemgrep` for
    `detected-private-key`, which turned the v0.55.2 tag pipeline red.
