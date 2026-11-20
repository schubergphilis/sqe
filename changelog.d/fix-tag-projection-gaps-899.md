### Fixed

- End-to-end test for catalog-scoped tag projection (#899)

    Under `[policy.ranger] catalog_scoped = true`, a column tag set through SQE lands on the `<catalog>.<namespace>` Ranger resource, and the unscoped `<namespace>` resource left from before is cleared. Until now this was unit-tested only; it is now asserted against a live Ranger.
