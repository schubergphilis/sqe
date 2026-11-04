### Fixed

- Quack: a write the object store refused is reported as an access denial

    Over the Quack protocol the error routing read the raw error code, so a
    refused `PutObject` (built as an execution failure and only refined to
    `ACCESS_DENIED` by the error's code) reached the client as `SQE-EXEC`
    with the store's reply in full. It now routes on the refined code and
    is answered `SQE-POLICY: access denied`, like every other denial; the
    detail stays in the server log.
