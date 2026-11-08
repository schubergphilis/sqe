### Changed

- Long strings in shredded variants get truncated manifest bounds (#870)

    A shredded variant path whose strings are longer than 16 characters
    used to get no manifest bound above 64 bytes. SQE now writes the bounds
    the way Iceberg Java does: the lower bound is the 16-character prefix and
    the upper bound is cut and rounded up. A filter such as
    `variant_get(v, '$.s', 'string') = '...'` on long text values now skips
    the files whose range excludes it.
