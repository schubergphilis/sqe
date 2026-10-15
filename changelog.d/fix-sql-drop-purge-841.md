### Fixed

- `DROP TABLE ... PURGE` no longer drops the table while silently keeping its data (#841)

    The SQL path ignored `PURGE`: the table left the catalog and every data file
    stayed in storage, with nothing telling the caller. The statement is now
    refused with `NotSupported` before anything is dropped, and the error points
    to `CALL system.drop_table(table => ..., purge => true)`. `DROP TABLE`
    without `PURGE` is unchanged. The DDL reference no longer claims `PURGE`
    deletes data files.
