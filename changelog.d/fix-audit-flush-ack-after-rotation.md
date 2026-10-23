### Fixed

- Audit `flush()` returns only after the writer has rotated the file

    The audit writer acknowledged a flush before its size check ran, and kept draining queued records into the file it was about to rotate. A record logged right after `flush()` could therefore land in the old generation, so a rotated file held more than its limit. The writer now stops draining at a flush and acknowledges it after rotation. `both_format_rotates_the_ocsf_file_at_the_same_generation` failed intermittently in CI because of this.
