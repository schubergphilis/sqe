### Documentation

- ADBC bulk ingest commits at once, also without autocommit

    The README and the book said an ADBC connection without autocommit keeps
    bulk ingest on the transaction branch until `commit()`. It does not: the
    ADBC Flight SQL driver never sends the transaction id with an ingest, so
    `adbc_ingest` commits to `main` at once. The server joins an ingest to a
    transaction only when the id is on the command. The docs now say so and
    show the staging-table workaround.
