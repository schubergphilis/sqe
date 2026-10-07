# Shared CI controls migration

Reusable implementations now live in Aikido's `consumer-controls.yml` and scripts.
SQE keeps its application inputs, feature matrix, test-layout check, public-doc
leak scan and grant-profile drift verification.

- Ordinary MRs: blocking tests, dependency checks and SAST; no release binary,
  malware or image-validation build. Set `SQE_FORCE_IMAGE_BUILD=1` to exercise the
  complete dev-release binary/artifact/image chain explicitly.
- Main: release binaries -> malware gate -> image assembly; all blocking test
  stage jobs complete before publishing. Image CVEs are checked once by digest
  before mirroring/signing. Clippy stays advisory in the extra feature-check job.
- Tags: the existing release image/signing flow and GitLab release notes remain.
- Schedules: advisory expiry, vendored supply-chain watch and deployed `:stable`
  image rescan. No image build, publish, binary build or version bump. Monthly
  review retains `SQE_SCHEDULE=monthly-vuln-review`.

Audit/deny run once in the blocking dependency job. The copied Kaniko script,
container scan job, SBOM script override and seven local control implementations
are removed. Source scripts and includes pin the same immutable Aikido commit;
CI_JOB_TOKEN must be permitted to read that project. Merge Aikido before this MR.

For local advisory checks, use the reviewed Aikido checkout:

```sh
make check-advisory-ignores AIKIDO_SHARED_HOME=/path/to/aikido
```

The dependency image still uses `/opt/sqe-deps/target`, preserving existing cache
compatibility. Cargo determines freshness; workspace artifacts are removed by
workspace metadata rather than SQE-specific filename patterns. The shared Helm
checker matches complete selectors and namespaces and includes Pods/CronJobs.
