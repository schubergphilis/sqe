-- name: Data Maintenance — Delete stale news items
-- requires: write_via_benchmark
-- description: Write portion of Data Maintenance: purge old news items
-- timeout: 30s

-- The Data Maintenance transaction cleans up stale reference data.
-- This query removes old news items that are no longer current.
DELETE FROM news_item
WHERE ni_dts < DATE '2001-01-01';
