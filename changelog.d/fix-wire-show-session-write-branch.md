### Fixed

- The Trino wire test expects `iceberg.write_branch` in `SHOW SESSION`

    `use_and_session_statements_echo_headers` still expected `SHOW SESSION` to list one property, and failed on main since `iceberg.write_branch` joined the allow-list. It now asserts both names in order, the replayed `ZSTD` value and an unset write branch; the golden transcript carries the second row. Tests only.
