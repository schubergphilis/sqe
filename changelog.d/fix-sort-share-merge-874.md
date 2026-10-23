### Fixed

- A partitioned sort under a tight query quota no longer fails at random in its final merge (#874)

    Each query's sorts split the spillable bytes evenly between their
    buffers and merge reservations. A merge growing while it built output
    batches was refused like a buffer, but it cannot spill, so the query
    failed whenever sibling partitions had not finished yet. Merges still
    count towards the split, which leaves them room, but are no longer
    refused by it; the query quota still bounds them.
