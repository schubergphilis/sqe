### Security

- Runtime base with ca-certificates-bundle 20260909-r3 (CVE-2026-91403)

    The v0.53.0 release image was held back by the image scan: its Chainguard
    glibc-dynamic base carried ca-certificates-bundle 20260909-r2 (Critical
    CVE-2026-91403). The base moves to the 2026-11-06 digest. CVE-2026-84243
    (glibc 2.44, High, no fix yet) is accepted in `.grype.yaml` until 2026-11-20.
