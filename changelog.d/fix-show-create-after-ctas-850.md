### Fixed

- `SHOW CREATE TABLE` right after a CTAS, INSERT or ALTER lists the table's columns (#850)

    Every table commit drops the namespace's cached table list, and `SHOW CREATE TABLE` read `information_schema.columns` without refreshing that list first. The first `SHOW CREATE TABLE` after a write came back with an empty column list until some other query re-listed the namespace. It now refreshes the list before the lookup, as `SHOW COLUMNS` and `DESCRIBE` already did.
