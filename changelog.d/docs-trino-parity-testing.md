### Changed

- Docs: Trino compatibility is stated as measured parity, not matrix coverage

    The book's testing page documents the Trino parity harness (queries,
    Iceberg scenarios, Trino functions, both protocols) and the BI driver
    probes. The Trino compatibility pages and the README quote the strict
    2026-11-17 baseline against Trino 484 (HTTP: 137 of 153 queries, 20 of 20
    Iceberg scenarios, 121 of 296 functions) instead of "~96% coverage". New
    blog post "153 of 153 was a rounding error" on what the lenient check hid.
