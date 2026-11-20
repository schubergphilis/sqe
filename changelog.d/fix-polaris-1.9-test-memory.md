### Fixed

- Cap the test stack's Polaris container at 3 GB of memory

    The Polaris image sizes its heap at 80% of the memory it can see. With no
    limit that is 80% of the whole Docker VM, and on Polaris 1.9 the heap grew
    past 6 GB of uncollected garbage during `make test-integration` until the
    VM's OOM killer stopped the JVM mid-suite. Every later test then failed to
    fetch a token. With the cap the JVM collects garbage in time.
