### Security

- `CREATE OR REPLACE TABLE ... AS SELECT`, a new-table CTAS and `CALL system.rewrite_data_files` now check write access before writing any file (#880 follow-up)

    The write check that INSERT, DELETE, UPDATE and MERGE run up front (a commit Polaris cannot apply, plus a storage probe with the vended keys) did not run on these three paths. A reader's replace or compaction wrote data files before the commit refused them; with read-only vended keys it failed on the first `PutObject` with an unclear error. Now each is refused with `ACCESS_DENIED` ("no write access") before planning any write. A principal that may create a table but not write data into it is refused the CTAS, and the table it created is rolled back.
