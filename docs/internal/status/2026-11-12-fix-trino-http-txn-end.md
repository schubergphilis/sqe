**STATUS 2026-11-12: COMMIT / ROLLBACK over Trino HTTP no longer report an error after they took effect (`fix/trino-http-txn-end`, #820).**

Found by the dbx-migrator writer probe (2026-11-11, v0.53.0, official `trino`
0.340.0 client): in hybrid mode `end_transaction` published or dropped the
branch, then `set_session_transaction(None)` failed because the Trino HTTP
session is per request and the session manager (wired for Flight SQL) never
held it. The client got "transaction control: session not found in manager"
for a COMMIT that had published. Ending is now idempotent for a session the
manager does not hold; starting (`BEGIN`) still needs a stored session.
Test: `transaction_end_unmanaged_session_test` in the coordinator's
integration harness (no docker stack).

**NEXT:** unchanged
