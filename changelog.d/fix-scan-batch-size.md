### Fixed

- Iceberg and worker scans emit full-size batches

    The Iceberg scan and the worker's parquet reader decoded 1024-row batches, the parquet crate's default, instead of DataFusion's 8192. Every operator above the scan paid its per-batch cost eight times over. On a 150M-row `GROUP BY` the scan now produces 18 K batches instead of 146 K and the query uses 25% less CPU.
