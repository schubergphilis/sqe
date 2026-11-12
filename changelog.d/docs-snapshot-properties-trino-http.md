### Changed

- `write-snapshot-properties` spec: Trino HTTP takes snapshot properties inside a transaction only

    A Trino HTTP session is built per request and never held, so `SET SNAPSHOT_PROPERTIES` outside a transaction is refused there. Inside one the properties live on the transaction, which the client replays in `X-Trino-Transaction-Id`.
