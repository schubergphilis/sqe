### Fixed

- The join-fallback rerun test no longer races a slow CI runner

    `a_query_refused_by_a_full_pool_reruns_once_there_is_room` released the
    other query's memory 300 ms after the start. When the first attempt
    reached its reservation later than that, nothing was refused and the
    test failed with "the query ran again". The memory is now released
    after the query was refused and holds the rerun gate.
