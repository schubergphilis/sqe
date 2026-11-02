**STATUS 2026-11-02: SQLite write-rig helper accepts Utf8View (`fix/rig-string-view`).**

Main's `aikido-test-rust` job has failed since 3b318f204 (`feat/iceberg-string-view`).
Eleven `write_handler::tests::committing` tests, which only compile under
`--features test-sqlite`, panicked in `test_support::sqlite_rig::id_v_pairs`
with "v is Utf8, got Utf8View". Iceberg scans now read strings as Utf8View, and
the helper only downcast to `StringArray`. It now matches on the column type
and reads either Utf8 or Utf8View. Every MR rebased on main since then
inherited the same eleven failures (!1619, !1620).

**NEXT:** unchanged
