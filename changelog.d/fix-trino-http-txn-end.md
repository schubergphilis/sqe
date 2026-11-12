### Fixed

- COMMIT and ROLLBACK over the Trino HTTP endpoint no longer fail after they took effect (#820)

    With a session manager wired (hybrid mode: Flight SQL and Trino HTTP on one
    coordinator), COMMIT published the transaction's branch, or ROLLBACK
    dropped it, and then returned "transaction control: session not found in
    manager": the HTTP session is built per request and the manager never
    held it. A client saw a failed COMMIT that had published. Ending a
    transaction now succeeds when the manager holds no session to clear;
    BEGIN on a session the manager does not hold still fails.
