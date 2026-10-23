### Fixed

- Distributed scans reported 0 `physicalInputBytes`

    A scan that ran on workers had no `bytes_scanned` counter, so the audit record, `system.runtime.queries` and Trino's `physicalInputBytes` showed 0 fetched bytes for it. `DistributedScanExec` now counts the file bytes of each fragment it was served (the morsel's byte range, or its whole files) once the worker returns the first batch or ends an empty stream. A failed fragment counts nothing. `processedBytes` was already counted.
