### Changed

- Test-stack checks of engine reads on discovered warehouses

    `catalog_discovery_test.rs` gains five live tests on Polaris. Each test creates its own principals and warehouses. A person with metadata access only reads a discovered warehouse through the engine, and fails with the opt-in off. A warehouse created during the run is readable with no restart. An INSERT stays on the person's delegation even when the engine may write. A signed catalog scope refuses a warehouse outside it before any proof call. The docs now say that the `/v1/config` proof shows both calls reach the same catalog, not that the engine holds a grant. The engine principal needs a catalog role on every allowlisted warehouse. Without one, reads fail with `ENGINE_CREDENTIALS_FAILED` and do not fall back to the person's keys. `bootstrap-test.sh` creates a `warehouse-engine` bucket. Tests and docs only; no engine change.
