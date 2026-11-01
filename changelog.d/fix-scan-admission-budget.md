### Fixed

- Iceberg scans reserve less memory, leaving more for sorts and joins

    The streaming reader now reserves 2x a read task's projected bytes instead of 4x, and data fetched ahead of decoding is capped at 10% of the memory limit (at least 64 MiB) per scan. A fetch over the cap waits instead of failing. On a 150M-row "latest row per key" query with a 3 GB limit, the sort spills 99 times instead of 165, and a plain scan peaks at 0.74 GiB instead of 1.22 GiB.
