### Changed

- Web UI design refresh: summaries, action banners, sortable tables, dark theme

    Each tab opens with a one-sentence summary and shows a banner with an
    action when the memory pool is above 80 %, query slots are nearly full,
    queries failed in the last hour, or a worker is unhealthy. The six stat
    cards became one metric strip, tabs are grouped (Engine, Workload), table
    headers sort and keep their order across the refresh, elapsed and scanned
    columns carry inline bars, states show as pills, the query detail splits
    queued, planning and execution time, and the page follows the system's
    light or dark setting. Still one self-contained file with no external
    assets.
