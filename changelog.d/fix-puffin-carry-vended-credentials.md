### Fixed

- Compaction and `rewrite_manifests` keep Puffin NDV statistics on a credential-vending catalog

    After a rewrite, the previous snapshot's statistics are carried onto the
    new snapshot. The sidecar was read and written through the table the
    commit returned, but a REST commit's response carries no storage config,
    so on a vending catalog without a static warehouse key the carry failed
    and, being best effort, dropped the statistics without an error. The
    carry now uses the `FileIO` of a fresh table load, which holds the
    caller's vended credential.
