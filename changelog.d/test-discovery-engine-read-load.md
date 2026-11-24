### Changed

- Load-level tests for engine reads on discovered warehouses

    `tests/it/discovery_engine_load.rs` loads a table through a discovered warehouse against a mock catalog and checks the requests. A proved warehouse loads with the engine's keys: no `X-Iceberg-Access-Delegation` header, one `/credentials` call with the engine token, and the configured OAuth scope sent unchanged. An engine 403, a different engine prefix or a warehouse outside `discovery_engine_warehouses` loads with the person's delegated keys and makes no `/credentials` call. Two proved warehouses share one engine token, and a warehouse outside the session's catalog scope makes no call. Tests only; no engine change.
