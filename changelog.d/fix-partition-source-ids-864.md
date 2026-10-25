### Fixed

- Iceberg V3 tables whose partition spec names a field's source as `source-ids` load (#864)

    The V3 spec lets a partition field list its sources as `source-ids` instead of `source-id`. SQE only read `source-id`, so such a table failed to load. A one-element `source-ids` now reads like `source-id`.
