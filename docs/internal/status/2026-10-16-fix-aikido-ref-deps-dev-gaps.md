**STATUS 2026-10-16: aikido pin carries reviewed licenses for base64, flatbuffers and lz4_flex (`fix/aikido-ref-deps-dev-gaps`).**

The license gate listed eight REVIEW lines, "license evidence missing", for four components: base64 0.22.1 and 0.23.1, flatbuffers 25.12.19 and lz4_flex 0.14.0. deps.dev returns 404 for all three packages, so retries never clear it. aikido !229 adds a `reviewed_evidence` entry per exact version, each sourced from the LICENSE file at the publish commit in the crate's `.cargo_vcs_info.json`. The five include refs and `AIKIDO_SHARED_REF` move together to the !229 merge commit `96527c4a`; between the old and new ref aikido changed only `policy/license-policy.json`. `sbom_license_check.py --strict` on the four components: review=4 before, allowed=4 after.

**NEXT:** unchanged: `fix/trino-json-cast` (`CAST(ARRAY/MAP AS JSON)` returns a quoted display string).
