# CI and release credentials: inventory and rotation log

Internal. Every credential the SQE pipeline and release process use, who owns it, when it expires, and a log of rotations. The credentials an SQE *deployment* uses are in the book's `operations/secret-rotation.md`; this page covers only the build and release side.

Values never go in this file or in `.gitlab-ci.yml`. They live as masked, protected CI/CD variables (protected: only pipelines on `main` and `v*` tags see them).

## Inventory

| Credential | Where it lives | Used by | Scope / rights | Expires | Owner |
|---|---|---|---|---|---|
| `AIKIDO_GROUP_TOKEN` (group access token) | Group CI/CD variable, protected, masked | aikido-triage, supply-chain-watch, vuln-review (`GITLAB_TOKEN`), aikido platform scripts, Renovate MRs | `api`, `write_repository` on the group | 2027-01-31 | Group maintainers (aikido platform) |
| `REGISTRY_USERNAME` / `REGISTRY_PASSWORD` (Harbor robot account) | Group CI/CD variables, protected; password masked | aikido-publish-image / aikido-publish-release-image (mirror to Harbor) | Push to the Harbor project | Set in Harbor | Group maintainers |
| `AIKIDO_SBOM_DEPLOY_TOKEN` (+ `AIKIDO_SBOM_PROJECT_ID`) | Group CI/CD variables, protected; token masked | SBOM publication | Deploy token on the SBOM project | Set on the token | Group maintainers |
| `CI_JOB_TOKEN`, `CI_REGISTRY_PASSWORD` | Issued by GitLab per job | Registry pushes, API calls inside a job | That job only | End of the job | GitLab (nothing to rotate) |
| Sigstore OIDC token (`SIGSTORE_ID_TOKEN`) | Issued by GitLab per job | aikido-sign-image (keyless cosign) | Signing identity bound to the pipeline | Minutes | GitLab / Sigstore (nothing to rotate) |
| GitHub mirror push | Maintainer's own SSH key or `gh` login | Manual `git push github main` and tags | Push to schubergphilis/sqe | Per personal policy | The pushing maintainer |
| sccache bucket keys | Not configured (removed 2026-09-07) | none | none | none | none |

Project-level CI/CD variables: none. Project access tokens and deploy tokens: none (checked 2026-09-26).

## Rotation

- **Cadence:** every token at least yearly and before its expiry date; immediately when a maintainer with access leaves or a value may have leaked.
- **How:** create the new token or robot secret first, update the CI/CD variable, run one pipeline that uses it (the weekly schedule exercises `AIKIDO_GROUP_TOKEN`; a release exercises the Harbor robot), then revoke the old one.
- **Test of one:** after each rotation, record the pipeline that proved the new value works.

## Rotation log

| Date | Credential | Action | Proved by (pipeline) | By |
|---|---|---|---|---|
| 2026-09-26 | all | Inventory created; no rotation performed yet. `AIKIDO_GROUP_TOKEN` expires 2027-01-31, rotate before then. | none | maintainers |
