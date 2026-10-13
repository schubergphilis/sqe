# Runtime base review 2026-10-13 (#800)

Pinned digest: `cgr.dev/chainguard/glibc-dynamic@sha256:2a3f1ec8a825dfbb1211b5835a9b8b3fb8d76c7bd22a189b1f84157ed2028293` (upstream `:latest` on 2026-10-13; same digest Renovate proposes in the container-images update).

Replaces `sha256:82edc253a57efee78d0fb504e11a93b7c74687b1b736110ad3a2a4f3edf632ab` (glibc-2.44 2.44-r7).

| Check | Old base (r7) | New base |
|---|---|---|
| glibc packages (syft) | glibc-2.44, glibc-2.44-locale-posix, ld-linux-2.44 at 2.44-r7 | same packages at 2.44-r8 |
| Grype, fresh DB, linux/amd64 | CVE-2026-8674, CVE-2026-89092, CVE-2026-86805 (Medium), CVE-2026-95818 (Low), GHSA-fmf4-pr35-46c2, GHSA-gwqp-9qgw-c5pv; all fixed in 2.44-r8 | no vulnerabilities found |
| Grype, fresh DB, linux/arm64 | same as amd64 | no vulnerabilities found |
| Trivy, LOW and above | n/a | no findings |
| v0.51.0 `sqe-server` / `sqe-cli` `--version` on the new base (amd64) | n/a | start, print 0.51.0 |

Scope: unfiltered scans of the runtime base only. The full SQE image is scanned by the `aikido-image-scan-built` gate on merge. No suppressions were added; `.grype.yaml` stays `ignore: []`.
