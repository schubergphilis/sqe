### Fixed

- `CREATE OR REPLACE VIEW` no longer drops another engine's SQL text without asking (#900)

    A replace writes a new view version with only the `sqe` representation. When
    the current version also holds a text in another dialect (for example
    `spark`, written by a migrator or by Spark itself), that engine could no
    longer read the view, and nobody was told. The replace is now refused with
    an error that names each dialect it would drop. Add
    `WITH (drop_dialects = 'spark')` (a comma-separated list, case-insensitive)
    to replace the view anyway. The option is a statement option and is not
    stored as a view property. Views whose current version has only `sqe` text
    are replaced as before.

    Polaris already refused such a replace, but only as an HTTP 500 that did
    not say how to proceed (`replace.drop-dialect.allowed=false`). With the
    option, SQE sets `replace.drop-dialect.allowed = true` in the replace commit
    and removes it again in a second commit, unless the view already had it.
