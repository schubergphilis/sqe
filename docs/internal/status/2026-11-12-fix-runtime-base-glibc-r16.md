# fix/runtime-base-glibc-r16 (2026-11-12)

Main pipeline 1711327 failed `aikido-image-scan-built`: Chainguard released
glibc 2.44-r16, so grype now reports CVE-2026-84243 on the r13 base as fixable.
The `.grype.yaml` exception covered only `glibc-2.44`, not `ld-linux-2.44` or
`glibc-2.44-locale-posix`.

The runtime base digest moves in Dockerfile, Dockerfile.bench and Dockerfile.ci
(glibc 2.44-r16 and ca-certificates-bundle 20261020 on amd64 and arm64), and the
exception is removed. Grype 0.116.0 reports no vulnerabilities on the new base
for either architecture. Evidence:
`docs/evidence/security/runtime-base-review-2026-11-12.md`.

The v0.53.0 image was never published because its tag pipeline (1702189) failed
the same scan on the older base. The next release tag from main publishes.

**NEXT:** cut the next release from main once this merges; check that
`aikido-release-image-scan` passes and the image reaches Harbor.
