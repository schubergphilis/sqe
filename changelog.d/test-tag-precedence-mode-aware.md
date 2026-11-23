### Changed

- The access-control live suite asserts each `policy.mask_precedence` mode

    `make test-access-control` can now run under `SQE_POLICY__MASK_PRECEDENCE=tag`, `resource` or `refuse`, so the `refuse` mode a deployment ships is tested end to end rather than only the default. `tag_mask_beats_resource_mask_live` checks the outcome of the mode it runs under: the tag mask wins under `tag`, the resource mask under `resource`, and the read is refused under `refuse`. `refuse_at_write_mask_conflict` uses an explicit `tag` handler for its `tag` step, so it no longer depends on the suite's mode. Tests only; no engine change.
