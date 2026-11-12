### Fixed

- `make test-distributed` on a fresh stack no longer fails eleven catalog-discovery tests with "Unable to find warehouse discovery_test_wh"

    `scripts/bootstrap-distributed.sh` kept its own copy of the catalog setup and created only
    `test_warehouse`. It now runs `scripts/bootstrap-test.sh` for the buckets, warehouses and
    namespaces, so both suites start from the same catalog state, and then waits for the
    coordinator and the workers as before.
