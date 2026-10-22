### Fixed

- Iceberg V3 `unknown` struct members are read and written (#859)

    A `STRUCT<a INT, u VOID>` column, also as a list element, used to be
    refused on write, and SQE could not read a Spark table with one. The
    parquet writer now leaves every `unknown` field out of the data file at
    any depth, as the spec requires, and the coordinator scan and the worker
    put the missing members back as NULLs. Spark 4.1 + Iceberg 1.12 reads
    SQE's rows and SQE reads Spark's. An `unknown` list element, an
    `unknown` map key or value, and a struct whose members are all
    `unknown` are still refused on write.
