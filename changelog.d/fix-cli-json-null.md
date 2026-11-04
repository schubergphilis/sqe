### Fixed

- `sqe-cli -f json` prints SQL NULL as `null` instead of `""`, and the table shows `NULL` on every backend

    The Flight and embedded backends rendered a NULL cell as an empty string, so `-f json` could not tell NULL from `''` and the table showed a blank. The HTTP backend printed the text `NULL`, which a `'NULL'` string matched. The result now carries which cells are NULL: `-f json` prints `null`, the table prints `NULL`, and CSV/TSV print an empty field on all three backends (HTTP printed `NULL` there before). The embedded backend no longer swallows an Arrow formatting error into an empty result; it reports it.
