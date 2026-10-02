# Runtime base advisory review — !1234

Proposed digest: `cgr.dev/chainguard/glibc-dynamic@sha256:82edc253a57efee78d0fb504e11a93b7c74687b1b736110ad3a2a4f3edf632ab`.

Read the package database from a stopped temporary container, then removed that container. glibc-2.44, ld-linux-2.44 and locale-posix are 2.44-r7. Existing integration services were untouched.

Ran `grype registry:<digest> -o json` from a directory without .grype.yaml. The arm64 base scan has no High/Critical matches and no CVE-2026-19499. Remove its old 2026-10-06 exception with this image update; the image gate remains active.

Remaining matches: CVE-2026-8674, CVE-2026-89092 and CVE-2026-86805 (Medium), CVE-2026-95818 (Low). No fix versions reported; track them in #800 without adding suppressions. This is a runtime-base scan, not full SQE image or multiarchitecture qualification. Full image gates remain CI's responsibility.
