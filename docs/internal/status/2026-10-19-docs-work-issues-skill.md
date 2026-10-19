**STATUS 2026-10-19: `/work-issues` skill for working the GitLab backlog (`docs/work-issues-skill`).**

`.claude/skills/work-issues/SKILL.md`: triage into done / code / split / settings / park, read-only investigation agents in parallel, then fix -> `make test-integration FILTER=` -> after-work files -> commit -> push -> MR, one branch at a time from `origin/main`, without waiting for review. The user merges.

Written as a checklist first, then run literally on #865 (!1485), and corrected where the run deviated: worktrees reuse the main checkout's `target/` and the `sqlengine` compose project through `/tmp/sqe-env.sh`; `git switch --no-track` so a bare push cannot target main; the rustfmt ratchet is checked per file, never by running rustfmt; the gate log is read for the non-zero `test result:` line; agent-written SQL may use another engine's syntax; the gate is rerun after the last edit. Timings from that run: warm test build 3m49, edit -> gate 46-58 s, push with pre-push clippy 3m09.

Triage on the same day found #849 still open although its slices have shipped: VARIANT columns and JSON as variant (990d954c, 83a00215, bc40ee0e), `variant_get` (#857, 0a6dce67) and shredded variant reads (9399f04f). `typeof` on a variant was only seen in a Spark interop test (`variant_spark_interop.rs`), so it is unverified; proposed for closing after that check.

**NEXT:** unchanged
