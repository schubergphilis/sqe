### Changed

- Catalog DDL (`CatalogOps`) moved from `sqe-coordinator` to `sqe-write`

    `catalog_ops` and `tag_source_impl` now live in `sqe-write`, next to the
    write handler, so the embedded CLI can run `ALTER TABLE` through the same
    code as a coordinator. The coordinator re-exports both modules under their
    old paths and passes its Polaris catalog discovery in through the
    existing `CatalogDiscovery` hook. No behaviour change.
