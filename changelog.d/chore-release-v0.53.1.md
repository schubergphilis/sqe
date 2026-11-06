### Security

- Runtime image base bumped to the current Chainguard `glibc-dynamic` digest, which ships the fixed `ca-certificates-bundle` (CVE-2026-91403, Critical)

    The v0.53.0 release scan failed on the old base (`ca-certificates-bundle` 20260909-r2), so v0.53.0
    was tagged but its image was never published. `Dockerfile`, `Dockerfile.ci` and `Dockerfile.bench`
    move together. `.grype.yaml` gains one scoped exception, CVE-2026-84243 on `glibc-2.44`, which has
    no fixed glibc in any distribution yet; it only matches while grype reports no fix, so it stops
    applying once Chainguard ships one.
