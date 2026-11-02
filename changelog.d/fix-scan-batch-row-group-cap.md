### Fixed

- Worker scans of files with small row groups no longer over-allocate decode buffers

    The 8192-row scan batch is now capped at the file's largest row group.
    A batch never spans row groups, so a file of 1024-row groups was decoded
    into buffers eight times larger than each batch and charged to the scan
    memory budget at that size.
