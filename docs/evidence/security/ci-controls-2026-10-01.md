# CI supply-chain and merge controls — 2026-10-01

## #776 — immutable dependency image

The two test-job overrides used the mutable `$CI_REGISTRY_IMAGE/ci-deps:main` image. They now use the registry-reported image reference with an immutable manifest digest:

`registry-sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/ci-deps:main@sha256:ca76cfbb51768ed5ba75482893d159fa0773da5f9c97f5e9344dc1a4fb4a1c66`

The project registry API reports this digest for repository 411, tag main, image created 2026-09-27T16:55:20.649Z. Both test jobs use the same reviewed manifest. Digest pinning prevents a moved tag from changing an existing CI definition; it does not prove the contents are vulnerability-free. Refresh the checked-in digest when a new dependency image is reviewed. The main tag is retained for update discovery but the digest determines the pulled contents.

GitLab CI lint (including the five pinned Aikido includes) returned valid=true, no errors and no warnings. The merged YAML contains the pinned image in both `aikido-test-rust` and `aikido-rust-extra`. `git diff --check` passed. Actual runner image pull and jobs are verified by MR CI.

## #777 and #778 — project controls applied

Read-only inspection confirmed that successful pipelines were not required, no approval rules existed, and main was protected. Six active members have Developer-or-higher access, so independent review has eligible reviewers.

Applied and read back using the GitLab project API:

| Control | Before | After |
|---|---|---|
| Successful pipeline required | false | true |
| Required approvals on protected branches | 0 | 1 (any eligible approver) |
| MR author may approve | false | false |
| MR committers excluded from approval | false | true |
| Approvals reset on push | true | true |
| Per-MR approver/rule overrides prevented | false | true |

The approval rule covers every protected branch, including main. New MRs need an independent eligible reviewer and a successful pipeline. This is project-level configuration, so it was applied directly rather than represented as a source-code-only fix. The scanner-managed issues should clear on the next Aikido rescan; API readback verifies the controls now.

API semantics: https://docs.gitlab.com/api/merge_request_approvals/ . This review does not re-accept outstanding dependency advisories in #789; their existing expiry dates remain.
