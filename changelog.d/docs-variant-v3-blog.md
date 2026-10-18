### Changed

- Blog: "JSON columns are variants now", on V3 variant storage, Spark interop and shredded reads

    The book no longer says shredded variant files are unsupported
    (`sql-reference/ddl.md`, `features/iceberg.md`). The dlt blog post's
    ADBC example passes `autocommit=True`, which its DML needs.
