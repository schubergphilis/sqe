**STATUS 2026-11-21: Polaris memory cap in the quickstarts (`fix/quickstart-polaris-mem-limit`).**

Follows `fix/polaris-1.9-test-memory`, which capped Polaris in
`docker-compose.test.yml`. The five quickstarts that run Polaris
(polaris-ranger-keycloak, polaris-ranger-service-principal,
polaris-keycloak-user-token, polaris-keycloak-client-id, engine-bench) get the
same `mem_limit: 3g`.

Investigation (three gate runs on one container): Polaris 1.9 does not leak.
After a forced GC the old generation is 45-170 MB and plateaus across runs.
The heap pressure is garbage: every REST request builds a catalog, an
`S3FileIO` and an S3 client and closes them, but `S3FileIO`, `S3InputStream`
and `S3OutputStream` in iceberg-aws 1.12.0 declare `finalize()` and capture a
stack trace, so their graphs survive an extra GC and get promoted. With the
image's `MaxRAMPercentage=80` an uncapped JVM lets that reach the Docker VM's
limit before a full collection.

**NEXT:** none; an upstream note (heap pressure from iceberg-aws finalizers)
is optional.
