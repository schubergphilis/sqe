**STATUS 2026-10-13: runtime base on glibc 2.44-r8; the #800 advisories are fixed (`fix/glibc-base-800`, #800).**

Chainguard shipped glibc 2.44-r8, which fixes CVE-2026-8674, CVE-2026-89092, CVE-2026-86805 and CVE-2026-95818. `Dockerfile`, `Dockerfile.ci` and `Dockerfile.bench` now pin `glibc-dynamic@sha256:2a3f1ec8…`. Unfiltered Grype (amd64 and arm64) and Trivy scans of that base are clean, and the v0.51.0 binaries start on it. Evidence: `docs/evidence/security/runtime-base-review-2026-10-13.md`.

Split out of the Renovate container-images MR !1261 (same digest), which stays held for the Polaris 1.8 bug. That rebase drops this hunk.

**NEXT:** SF100 memory safety (#788, #784, #755); #812 on top of !1415.
