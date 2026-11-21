**STATUS 2026-11-20: Polaris memory cap in the integration stack (`fix/polaris-1.9-test-memory`).**

On Polaris 1.9.0 `make test-integration` failed (220 passed, 214 failed). The
Polaris container's log stopped without an error at the point the failures
started; every later test failed on a refused token request to
`localhost:18181`. The cause was the Docker VM's OOM killer: the image's
run-java defaults (`-XX:MaxRAMPercentage=80.0 -XX:+UseParallelGC`) size the
heap at 80% of the VM (about 12.5 GB on a 16 GB VM shared with other stacks).
ParallelGC let the old generation fill to 6.2 GB before a full collection, so
the VM ran out first. A forced GC left about 620 MB live, mostly AWS SDK
attribute maps and exception stack traces.

`docker-compose.test.yml` now sets `mem_limit: 3g` on the polaris service, so
the heap tops out near 2.4 GB and the JVM collects in time. If the live set
itself keeps growing, the JVM now exits with an explicit OutOfMemoryError
(the image sets `-XX:+ExitOnOutOfMemoryError`) instead of vanishing.

**Follow-up 2026-11-21: not a leak.** Three gate runs, two on the same
container without a restart: after a forced GC the old generation stayed at
45-170 MB and did not grow from run to run. The 620 MB reading above did not
reproduce. The heap pressure is garbage. Every REST request builds and closes
a catalog, an `S3FileIO` and an S3 client. In iceberg-aws 1.12.0,
`S3FileIO`, `S3InputStream` and `S3OutputStream` declare `finalize()` and
capture a stack trace, so their object graphs survive one extra GC and get
promoted. A heap dump showed them held only through `java.lang.ref.Finalizer`,
and the log had no "Unclosed" warnings, so nothing is left open. Uncapped, the
80%-of-VM heap lets this garbage reach the Docker VM limit before a full
collection. The 3 GB cap is the fix (container peak about 2.0 GiB).

**NEXT:** none. An upstream note to Iceberg about finalizer-driven heap
pressure is optional; nothing was filed.
