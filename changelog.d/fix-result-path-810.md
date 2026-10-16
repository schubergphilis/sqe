### Removed

- The unused multi-endpoint result builder in the Flight SQL service

    `build_flight_info_distributed` and `QueryResultLocation` described
    results fetched straight from workers, but nothing called them, and
    they could never apply: workers only scan, and every join, aggregate
    and sort runs on the coordinator, so a final result lives only there.
    Results stay one stream from the coordinator (#810).
