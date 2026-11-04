### Fixed

- A write the object store refuses reports `ACCESS_DENIED`, not `EXECUTION_FAILED` (#880)

    With credential vending, a reader's `DELETE`, `UPDATE` or `MERGE` gets a
    read-only credential and fails on its first upload. The write path wrapped
    the store's reply (`PermissionDenied (persistent) ... S3Error AccessDenied`)
    in a typed `ExecutionFailed`, which no text classifier ever read, so the
    client saw an execution failure. A typed `ExecutionFailed` whose text
    carries the store's refusal (OpenDAL `PermissionDenied`, S3 `AccessDenied`)
    now reports `AccessDenied` (gRPC `PERMISSION_DENIED`, Trino
    `PERMISSION_DENIED`); an expired or invalid engine key still reports
    `StorageCredentialsUnavailable`. Other typed codes are unchanged. The
    statement is still refused only once the store says no; refusing it
    before the first file is written is part 1 of #880.
