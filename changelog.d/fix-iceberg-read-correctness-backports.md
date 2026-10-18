### Fixed

- Iceberg read correctness fixes from upstream iceberg-rust

    The vendored iceberg-rust picks up 18 apache fixes and 2 RisingWave
    fixes. Page-index pruning no longer drops rows for INT32/INT64
    decimals, and it now prunes FIXED_LEN_BYTE_ARRAY decimals.
    Nested struct columns added by schema evolution read as NULL, and
    evolved nested structs are matched by field id, not position.
    `<`, `<=`, `NOT IN` and `NOT STARTS_WITH` no longer match rows in files
    written before the column existed. A `NOT` predicate on a scan task
    built outside the table scan no longer prunes row groups that hold
    matching rows or aborts page-index pruning. Day transforms of pre-epoch timestamps and
    decimal and binary `truncate` literals are now correct. Float partition
    values are formatted like iceberg-java. UUID columns round-trip and
    carry the Arrow `arrow.uuid` extension, so Flight SQL clients such as
    pyarrow return UUID values instead of 16 raw bytes.
    Snapshot summaries count delete files the delete filter dropped.
