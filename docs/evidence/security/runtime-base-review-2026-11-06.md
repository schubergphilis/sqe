# Runtime base review 2026-11-06

Pinned digest: `cgr.dev/chainguard/glibc-dynamic@sha256:671a621016fb33812136dea6182b1db957996d3c8c618187aaf86abd9b14af53` (upstream `:latest` on 2026-11-06).

Replaces `sha256:2a3f1ec8a825dfbb1211b5835a9b8b3fb8d76c7bd22a189b1f84157ed2028293` (glibc-2.44 2.44-r8, ca-certificates-bundle 20260909-r2).

Why: the v0.53.0 tag pipeline (1702189) stopped at `aikido-release-image-scan`, so the image was never published to Harbor. Grype reported the Critical CVE-2026-91403 (and the Medium CVE-2026-111943, GHSA-9h75-rf66-wh3r, GO-2026-6809) in ca-certificates-bundle 20260909-r2, fixed in r3, and the High CVE-2026-84243 in glibc-2.44 2.44-r8, with no fix.

| Check | Old base | New base |
|---|---|---|
| Grype 0.116.0, current DB, linux/amd64 | ca-certificates-bundle 20260909-r2: CVE-2026-91403 (Critical), CVE-2026-111943 (Medium), GHSA-9h75-rf66-wh3r, GO-2026-6809; glibc-2.44 2.44-r8: CVE-2026-84243 (High) | glibc-2.44 2.44-r13: CVE-2026-84243 (High), no fixed version |
| Grype 0.116.0, current DB, linux/arm64 | n/a | same as amd64 |

Accepted: CVE-2026-84243 in `.grype.yaml` (review by 2026-11-20, owner sqe-maintainers). No fixed glibc exists in any Chainguard base yet. Remove the entry when Renovate proposes a digest whose glibc has the fix.
