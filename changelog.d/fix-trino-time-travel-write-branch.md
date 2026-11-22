### Fixed

- Trino HTTP accepts `FOR VERSION AS OF`, `FOR TIMESTAMP AS OF` and `FOR INCREMENTAL BETWEEN`, as Flight SQL does

    These clauses used to fail on the Trino path with "Expected: one of UPDATE or SHARE", because the front door parsed the statement before the clause was stripped. They are now stripped only to classify the statement, and the executor gets the SQL as the client wrote it.

- `SET WRITE_BRANCH` and `RESET WRITE_BRANCH` work over Trino HTTP

    They used to fail with "session not found in manager". The branch now travels as the session property `iceberg.write_branch`: SQE returns it in `X-Trino-Set-Session`, the client sends it back in `X-Trino-Session`, and the next requests write to that branch. `SET SESSION iceberg.write_branch` works too, and `SHOW SESSION` lists the property. A transaction branch name (`sqe-txn-`) is refused however it arrives.

- `USE [catalog.]schema` works over Trino HTTP

    It failed with "USE: session not found in manager" on a server that also serves Flight SQL. A Trino request runs on a session built from its headers, which the session manager never stores. The scope goes back to the client as `X-Trino-Set-Catalog` and `X-Trino-Set-Schema`, so the coordinator now leaves the stored scope alone for such a session instead of failing.

- Time travel on an unqualified table name reads the table in the session schema, on Flight SQL and Trino HTTP

    `SELECT ... FROM t FOR VERSION AS OF ...` looked `t` up in the namespace `default` whatever schema the session had selected with `USE` or `X-Trino-Schema`.
