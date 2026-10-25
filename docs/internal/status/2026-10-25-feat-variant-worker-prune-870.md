**STATUS 2026-10-25: distributed workers prune row groups on shredded variant fields (`feat/variant-worker-prune-870`, #870).**

!1537-!1540 pruned shredded variant row groups on the coordinator's own scan only. A distributed scan ran the same query on a worker that read every row group: the worker has no variant functions, so the filter never reached it.

The coordinator now resolves each `VariantPrune` to its Iceberg field id and sends the list in the scan ticket (`variant_prunes_b64`, JSON). The ticket is version 5 only when the field is set. A ticket without prunes keeps its old version. The worker decodes the list and runs `VariantRowGroupPruner` after the #369 statistics pruning, on the row groups the morsel range left. If the list does not decode, the worker logs a warning and reads every row group, so the result stays correct.

Workers must be upgraded before the coordinator: a v4 worker refuses a v5 ticket, and the worker hello already refuses a worker older than the coordinator.

Gate: `make test-integration FILTER=prunes_shredded_row_groups_on_a_worker`. The test runs an in-process worker built from the current tree. It checks that the pruned and the `+ 0` baseline queries return the same rows, that the worker got the scan, and that the worker shipped fewer than half the rows when the prune applied.

Left open on #870: data-file pruning from variant manifest bounds, typed read without unshredding, nested variant shredding (and the read path that drops `typed_value` from nested shredded variants), Spark multi-row-group evidence, and the two caveats in `docs/evidence/iceberg-matrix-state.json`.

**NEXT:** #870 nested variant shredding plus the nested read fix.
