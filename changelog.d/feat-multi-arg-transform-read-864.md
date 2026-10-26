### Added

- Iceberg tables with multi-argument or unknown transforms are readable (#864)

    A partition or sort field that lists several `source-ids`, or names a
    transform SQE does not know, no longer stops the table from loading. As
    the V3 spec asks of readers, SQE reads such a table and ignores the
    field when it prunes files: a predicate on a source column scans every
    file. The field keeps its original transform name and source ids when
    SQE writes the metadata back, and manifests decode with the partition
    type their writer recorded. Writes into such a table are refused, as
    the spec requires of writers: no data or delete file commits under a
    spec with an unknown transform, and `ALTER TABLE ... PARTITION FIELD`
    asks to drop that field first. `$partitions` and `$files` hide a
    multi-argument field's value when any of its source columns is masked
    or restricted.
