**STATUS 2026-11-02: worker scan batches capped at the file's largest row group (`fix/scan-batch-row-group-cap`).**

`fix/scan-batch-size` set the worker's parquet reader to 8192-row batches. The
reader sizes its decode buffers for the full batch size, but a batch never
spans row groups, so a file of 1024-row groups decoded 1024-row batches in
8192-row buffers and charged the scan budget 8x what they held. On main that
failed `credential_refresh_mid_scan`: each batch asked for 65,632 bytes from a
64 KiB decode lane, and an item larger than its lane is refused outright
("item too large for budget 'scan'"). The batch size is now
`min(8192, largest row group)`, so files with row groups of 8192 rows or more
read exactly as before.

**NEXT:** unchanged
