### Fixed

- `CREATE TABLE ... TBLPROPERTIES ('format-version' = '3')` works on Glue, S3 Tables, HMS and JDBC catalogs (#854)

    The native catalogs reject the reserved `format-version` property, and a
    user's own option was passed through to them. CREATE TABLE and CTAS now
    drop reserved properties for non-REST backends; the version still reaches
    the table. A live test creates, writes and reads a V3 table through AWS
    Glue.
