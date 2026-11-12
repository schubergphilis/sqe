**STATUS 2026-11-12: main-branch pipeline trimmed (`fix/trim-main-pipeline`).**

- Before: every merge ran `aikido-test-rust` (21-45 min), `aikido-rust-extra` (3-6 min), `release-binaries` in the release profile (27-51 min), `catalog-live-tests`, `aikido-cargo-lock`, `helm-chart-check`, `ci-config-sanity`, and the next merge cancelled it half way. That was mostly a repeat of the MR pipeline and it held the shared runner's CPU and memory.
- After: main runs the `.pre` guardrails (secret scan, SAST, SBOM, licence, malware), `deps-check` (fresh advisory feed) and a manual `release-binaries` (`allow_failure: true`, so the pipeline stays green). `SQE_BUILD_IMAGE=1` on a main pipeline builds without the click. `aikido-build` now `needs:` `release-binaries`, so pressing play builds, scans and publishes `:main` in one go. Tags are unchanged: `aikido-release-image` builds the release image from the tag.
- Test-rule overrides copy the template's MR arms (rules cannot be appended to an included job's own list); re-check them when the aikido ref is bumped.
- Trade-off: clippy and the feature matrix run only on MRs labelled `ci::full` and in the local pre-push hook; a semantic merge conflict between two green MRs is no longer caught on main by CI. `make test-integration` locally stays the integration gate.

**NEXT:** after merge, check the first main pipeline: test jobs absent, `release-binaries` manual, and pressing play runs `malware-scan` and the `.post` image chain.
