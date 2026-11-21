### Fixed

- Cap Polaris at 3 GB of memory in the quickstarts

    The same cap the integration test stack got: the Polaris image sizes its
    heap at 80% of the Docker VM, and the per-request S3 FileIO garbage can
    grow until the VM's OOM killer stops the JVM. Applies to the five
    quickstarts that run Polaris.
