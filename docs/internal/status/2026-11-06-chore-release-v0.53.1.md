**STATUS 2026-11-06: v0.53.1 replaces the unpublished v0.53.0 image (`chore/release-v0.53.1`).**

The v0.53.0 tag pipeline (1702189) built the image, but `aikido-release-image-scan`
(grype, fail on high) failed on the runtime base: `ca-certificates-bundle`
20260909-r2 (CVE-2026-91403 Critical, fixed in r3) and `glibc-2.44` 2.44-r8
(CVE-2026-84243 High, no fix anywhere). Publish, sign and `:latest` were skipped,
so Harbor stayed on v0.52.0. The GitLab Release for v0.53.0 exists, with no image.

This branch bumps the Chainguard `glibc-dynamic` digest in `Dockerfile`,
`Dockerfile.ci` and `Dockerfile.bench` to `sha256:671a6210...` (ca-certificates
fixed, glibc 2.44-r13) and adds a `.grype.yaml` exception for CVE-2026-84243 on
`glibc-2.44` with `fix-state: unknown`. Wolfi apk matches without a fix report
`unknown`, not `not-fixed`; a `not-fixed` entry did not suppress the match.
Verified locally with grype 0.116 `--fail-on high` on amd64 and arm64: exit 0
on the new digest, exit 2 on the old one.

**NEXT:** drop the CVE-2026-84243 exception and bump the digest once Chainguard
ships a fixed glibc 2.44.
