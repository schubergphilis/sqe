### Security

- Runtime base with glibc 2.44-r16 (CVE-2026-84243)

    The Chainguard glibc-dynamic base moves to the 2026-11-12 digest, which
    ships glibc 2.44-r16 and ca-certificates-bundle 20261020. That fixes the
    High CVE-2026-84243 the 2026-11-06 base still carried, and the `.grype.yaml`
    exception for it is gone: the image scan passes with no accepted findings.
