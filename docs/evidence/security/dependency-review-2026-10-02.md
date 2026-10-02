# Dependency security review — 2026-10-02 (#789/#799)

## JWT claim validation and crypto provider

A new real RS256/JWKS regression reproduces acceptance of a signed string `nbf` on jsonwebtoken 9.3.1. The upstream advisory [CVE-2026-25537](https://github.com/Keats/jsonwebtoken/security/advisories/GHSA-h395-gr6q-cpjc) identifies malformed optional registered claims as affected below 10.3.0. SQE requires `exp` but makes `nbf` optional, so the reproduced path is relevant. This does not permit forging signatures.

Upgrade to jsonwebtoken 10.4 with exactly one explicit `aws_lc_rs` feature. Existing !1208 failed runtime signature verification because it selected neither crypto provider; CI job 5477490 records 18 provider panics. The new signed-token regression rejects string/boolean/array/object `nbf`; existing tests preserve missing optional nbf and valid RSA tokens, and reject future nbf, expired tokens, bad signatures, wrong algorithms/audiences and JWKS rotation failures.

The initially tested RustCrypto provider introduced `rsa 0.9.10`, which has unpatched RUSTSEC-2023-0071 and failed cargo-deny's advisory gate. AWS-LC passes without introducing that dependency or accepting another advisory. It requires native C compilation: local macOS compilation/signature tests pass; Linux CI must verify its build environment as well.

Validation on the final AWS-LC choice: all 286 authentication library tests pass; auth/test Clippy with `-D warnings` passes; `cargo deny check advisories licenses` passes; configured `cargo audit` reports zero active vulnerabilities. The baseline malformed-nbf regression fails before upgrading. The yanked yoke-derive 0.8.3 is replaced by 0.8.4.

## Resolved and outstanding advisory exceptions

Remove the RSA audit ignore because rsa is absent from the final lockfile. Remove both PyO3 ignores: the lockfile now contains patched pyo3 0.29.2. No review dates are extended.

Unfiltered audit still reports **RUSTSEC-2026-0194/0195 against quick-xml 0.39.4**, plus the unmaintained paste warning. Existing exceptions remain: XML review 2026-10-25, paste review 2026-12-25. object_store 0.14.2 is now available with quick-xml ^0.41, correcting the old registry-availability comment. DataFusion 55 still uses object_store 0.13's traits; upgrading SQE alone would mix incompatible object-store APIs. Coordinate that migration with !1133; do not claim these XML risks are fixed.

## Runtime base

The existing container update !1234 proposes digest `82edc253a57efee78d0fb504e11a93b7c74687b1b736110ad3a2a4f3edf632ab`. Direct package inspection confirms glibc-2.44 2.44-r7. An unfiltered arm64 Grype scan reports no High/Critical matches and no CVE-2026-19499, so the obsolete 2026-10-06 glibc exception can be removed with that update.

Four unpatched findings remain in this base: CVE-2026-8674, CVE-2026-89092, CVE-2026-86805 (Medium) and CVE-2026-95818 (Low). They are tracked in #800. No suppressions were added. This base scan does not qualify the full SQE image or every architecture.

#789's private acknowledgment/review controls and the coordinated XML migration remain open; this review does not fabricate acknowledgment or re-accept any deadline.
