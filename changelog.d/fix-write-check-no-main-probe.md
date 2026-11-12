### Fixed

- An INSERT-only grantee can write the first rows into a table that has no snapshot yet

    The pre-write check sends the catalog a commit whose requirement cannot match. For
    a table without a `main` ref it sent an empty update list, which Polaris authorizes
    as `UPDATE_TABLE` and so demands `table-properties-write`, a privilege an INSERT
    grant does not carry. The first INSERT into a fresh table was refused with "no write
    access". The check now drops the absent `main` ref instead, a no-op that the
    data-write privilege covers. Regression since the write check covered every write.
