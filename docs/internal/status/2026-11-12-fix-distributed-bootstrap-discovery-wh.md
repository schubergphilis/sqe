**STATUS 2026-11-12: distributed bootstrap reuses the single-node catalog setup (`fix/distributed-bootstrap-discovery-wh`).**

A full sweep on a fresh distributed stack failed 11 tests (`catalog_discovery_test::*` and two `tvf_catalog_placeholder_e2e` file-TVF tests) with "Unable to find warehouse discovery_test_wh": `bootstrap-distributed.sh` created only `test_warehouse`, `warehouse` bucket and `default` namespace, while `bootstrap-test.sh` also creates `test_ns`, the `warehouse-discovery` bucket and `discovery_test_wh`/`disc_ns`. Earlier distributed runs passed only because the stack still held the single-node bootstrap's state. The distributed script now calls `bootstrap-test.sh` (same ports, idempotent) and keeps only its coordinator/worker waits; its catalog-admin grant, which sent a body without the `grant` wrapper, goes with the removed copy.

**NEXT:** none.
