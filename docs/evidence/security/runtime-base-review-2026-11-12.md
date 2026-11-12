# Runtime base review 2026-11-12

Pinned digest: `cgr.dev/chainguard/glibc-dynamic@sha256:cb7aa4681bef604e99ad3fb6fb365330a3ca24fcee56df0b60b27ac4983409eb` (upstream `:latest` on 2026-11-12).

Replaces `sha256:671a621016fb33812136dea6182b1db957996d3c8c618187aaf86abd9b14af53` (glibc-2.44 2.44-r13, ca-certificates-bundle 20260909-r3), reviewed in [runtime-base-review-2026-11-06.md](runtime-base-review-2026-11-06.md).

Why: main pipeline 1711327 failed `aikido-image-scan-built`. Chainguard now ships glibc 2.44-r16, which fixes CVE-2026-84243 (High) and GHSA-8rvm-662m-jvqv, so grype reports the old r13 as fixable. The `.grype.yaml` exception matched only the `glibc-2.44` package, and grype also flagged `ld-linux-2.44` and `glibc-2.44-locale-posix`.

| Check | Old base | New base |
|---|---|---|
| Installed packages (`/lib/apk/db/installed`), linux/amd64 and linux/arm64 | glibc-2.44, ld-linux-2.44, glibc-2.44-locale-posix 2.44-r13 | all three 2.44-r16; ca-certificates-bundle 20261020-r0 |
| Grype 0.116.0, current DB, `--fail-on high`, no ignore file, linux/amd64 | CVE-2026-84243 (High), GHSA-8rvm-662m-jvqv, fixed in 2.44-r16 | No vulnerabilities found, exit 0 |
| Same, linux/arm64 | same as amd64 | No vulnerabilities found, exit 0 |

Accepted exceptions: none. The CVE-2026-84243 entry is removed from `.grype.yaml`.
