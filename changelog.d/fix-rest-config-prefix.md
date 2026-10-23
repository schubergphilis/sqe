### Fixed

- Writes and views work on REST catalogs whose path prefix is not the warehouse name (#873)

    SQE's own REST calls (the table access check before a write, ETag
    revalidation, view DDL, schema updates) put the warehouse name in the
    URL, where the Iceberg REST spec puts the `prefix` the server returns
    from `GET /v1/config`. Polaris advertises the warehouse name, so only
    other servers broke: on Nessie every `INSERT` failed with
    "Authorization check failed for table". SQE now uses the advertised
    prefix, the same one the Iceberg client uses for reads.
