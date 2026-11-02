### Fixed

- The SQLite write-rig tests pass again now that Iceberg scans return Utf8View

    `feat/iceberg-string-view` made Iceberg scans read strings as Utf8View.
    The rig's `id_v_pairs` helper still required Utf8 and panicked, failing
    eleven `write_handler::tests::committing` tests on main since that merge.
    The helper now accepts both string types. Test-only; no runtime change.
