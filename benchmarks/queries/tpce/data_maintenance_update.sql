-- name: Data Maintenance — Update zero-volume daily market rows
-- requires: write_via_benchmark
-- description: Write portion of Data Maintenance: set default volume on zero-volume market data
-- timeout: 30s

-- The Data Maintenance transaction fixes stale or anomalous reference data.
-- This query sets a minimum volume of 1 on daily_market rows with zero volume.
UPDATE daily_market
SET dm_vol = 1
WHERE dm_vol = 0;
