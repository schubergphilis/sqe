---
name: work-issues
description: Work GitLab issues through to merge requests with one build tree - triage a batch, investigate in parallel, then fix, gate, push and open an MR one branch at a time. Use when the user says "work issues", "pick up #N", "clear the backlog", "do the next lane", or types /work-issues [#N ...].
---

# Work issues

**Investigate in parallel, build in series.** Read-only agents cost nothing;
cargo serialises every build on one lock and `make test-integration` binds
fixed ports, so one tree, one builder, one test stack. Don't wait for review
between MRs: push, open the MR, start the next branch from `origin/main`.

The user merges. Never merge, never set auto-merge, never push to main.

Measured on the first run (#865, test-only change): warm test build 3m49 on
a shared `target/`; edit -> gate cycle 46-58 s; pre-push clippy on first push.

## 0. Environment (once per session)

CLAUDE.md says not to create worktrees; this section is for when the harness
(T3) has already put you in one. A worktree has no `target/` and its directory name
becomes the compose project, so without this you pay a cold 24-crate build, a
second multi-GB `target/`, and a port clash with the running test stack:

```bash
MAIN=$(git worktree list --porcelain | awk '/^worktree /{print $2; exit}')
printf 'export CARGO_TARGET_DIR=%s/target\nexport COMPOSE_PROJECT_NAME=sqlengine\n' "$MAIN" > /tmp/sqe-env.sh
docker ps --format '{{.Names}}' | grep -E '^sqlengine-(polaris|rustfs)-1$'   # stack up?
pgrep -fl 'cargo|rustc'                                                      # another builder?
```

Shell state does not persist between tool calls: start **every** command that
runs cargo, make or git push with `. /tmp/sqe-env.sh &&`. One forgotten
`cargo xtask` built a fresh `target/` in the worktree.

The shell is zsh: `git add $P` does not word-split (name paths literally),
and an unquoted glob that matches nothing aborts the command
(`grep --include='*.rs'`, not `--include=*.rs`).

Kick off the slow work in the background while step 2 runs:
`. /tmp/sqe-env.sh && cargo test -p sqe-coordinator --no-run`.

## 1. Triage (before touching code)

```bash
glab issue view N --comments
git fetch origin main -q
git log --oneline origin/main --grep="#N"      # already shipped?
grep -rln "#N" changelog.d docs/internal/status
glab mr list --search "N"                       # MR already open?
```

One bucket per issue; show the buckets to the user before going on:

| Bucket | Signal | Action |
|---|---|---|
| **done** | commits reference it **and** every scope item / "Done when" is traced to a commit or test (follow-ups often land under another issue number) | propose closing with a comment listing the commits; close only on the user's OK |
| **code** | fits one MR | continue |
| **split** | "Done when" items of different size | do the small items now, say what stays open |
| **settings** | GitLab project settings (approvals, pipelines) | hand to the user |
| **park** | `blocked::upstream`, external accounts, SF100 rig | leave |

Group **code** issues by concern into themed branches (`fix/`, `feat/`,
`test/`, `docs/`, `refactor/`), not one per issue.

## 2. Investigate (parallel, read-only)

One `Explore` agent per issue, all in one message. Ask each for: root cause
with file:line, the smallest fix, the test that proves it (written in the
neighbouring test's style), and a `FILTER=` substring that matches only the
new tests.

Agent output is a lead, not a fact:
- Open the cited lines before building on a claim.
- Agent-written SQL may use another engine's syntax (it wrote Iceberg-Spark
  `DROP TAG IF EXISTS t`; SQE takes `DROP TAG t IF EXISTS`). The gate catches
  this; a mismatch with a documented upstream form is itself a finding.

Investigation usually finds more than the issue says. **Fix the findings**;
a "found, not fixed" list is not a deliverable. A finding about the code the
current MR already touches (same feature, same files) goes into that MR,
even when the MR started as tests only. A different concern becomes the next
branch of this batch, worked right after. Only a finding too big for the
batch becomes a drafted follow-up issue (filed on the user's OK).

Confirm each finding by running it before fixing it: a failing test, or a
throwaway probe test that runs the variants and prints the outcomes. Wrap
each case in `catch_unwind` so one panic does not hide the rest, and end the
probe with a `panic!` so the gate shows its output. A probe often finds the
issue is bigger than the read-only report said (#865: "binary defaults
refused" was really "every quoted DEFAULT on a non-string column panics
CREATE TABLE"). Turn the probe into the real test, then delete it.

While there, grep `openspec/changes/*/tasks.md` for open tasks in the same
area: a one-assertion task that fits the branch is worth taking.

## 3. Fix (serial, one branch at a time)

```bash
git fetch origin main -q
git switch --no-track -c <prefix>/<slug>-<N> origin/main
```

`--no-track`: otherwise the upstream is `origin/main` and a bare `git push`
targets main. Never branch from the current `t3/` or feature branch.

- Edit with the Edit tool; match the neighbouring code and test style
  (integration tests: `#[ignore = "needs:polaris"]`, `crate::common::setup_handler()`).
- rustfmt is a **ratchet**: main carries drift, a file fails only when it
  gains hunks. Check your own lines with
  `git add <file> && scripts/pre-commit-hook.sh rustfmt <file>`. Never run
  `rustfmt` or `cargo fmt` on the file or workspace; fix your hunks by hand
  (binding the SQL to a `let` keeps calls on one line).
- `cargo check-server` for code changes; tests build in the gate.
- Before pushing, clippy just the crates you changed:
  `cargo clippy -p <crate> --all-targets -- -D warnings` (seconds when warm).
  The pre-push hook lints the whole workspace in ~3 min and a lint it
  catches (e.g. `manual_is_multiple_of`) costs a second full run.

## 4. Gate

```bash
. /tmp/sqe-env.sh && caffeinate -i make test-integration FILTER=<substring> > /tmp/gate-N.log 2>&1; echo "gate=$?"
sed 's/\x1b\[[0-9;]*m//g' /tmp/gate-N.log | grep -aE "test result: .* [1-9][0-9]* (passed|failed)|panicked|^test .*::"
```

- `FILTER` is a substring. The run prints several `test result:` lines (one
  per test binary, most `0 passed`); confirm the non-zero line and that the
  expected test names appear. A FILTER that matches nothing is a false green.
- Never pipe the gate into `tail`/`grep` for its exit code; capture `$?` first.
- The stack stays up between runs; `make test-integration-down` at batch end.
  RustFS 403 / "check key failed" after the Mac slept = clock skew: down, rerun.
- Rerun the gate after the **last** edit, even a formatting one; the commit
  message quotes this run.
- CI has no integration job. A green pipeline is not the gate; this is.

## 5. After-work files

1. `README.md` roadmap - only if a roadmap item moves.
2. `docs/internal/status/YYYY-MM-DD-<branch-slug>.md`: what changed, the gate
   command and result, what is left open, extra findings, `**NEXT:**`.
3. `changelog.d/<branch-slug>.md`; validate with `python3 scripts/changelog-collect.py --check`.
4. `openspec/changes/*/tasks.md` - tick what the change completes.
5. `[MATRIX]` issues: update `docs/evidence/iceberg-matrix-state.json`
   (evidence test names, caveats), then
   `. /tmp/sqe-env.sh && cargo xtask matrix-report --print-only`. Claim only
   what a test asserts (setting merge-on-read is not proof that deletion
   vectors were written).

## 6. Commit, push, MR (separate steps)

```bash
git add <path> <path> ...                       # named paths; never -a, never stash
. /tmp/sqe-env.sh && pre-commit run > /tmp/pc.log 2>&1; echo "pc=$?"
git commit -m "<type>(<scope>): <summary> (#N)" # signing may need Touch ID; check it succeeded
git log --format='%G? %h' -1                     # G or U = signed
. /tmp/sqe-env.sh && git push -u origin HEAD     # pre-push runs clippy: run in background
# clippy refused? fix, amend (the commit is not pushed yet), push again
# don't edit .rs files while it runs: it lints the working tree
glab mr create --source-branch <branch> --target-branch main --title "<commit subject>" \
  --description "<what, Part of/Closes #N, gate command -> result, left open, findings>"
```

`Closes #N` only when every "Done when" item is met; otherwise `Part of #N`.
Then the next branch, straight away.

## 7. Report

Per MR: link, issue, the gate (command, passed/failed, test names), what is
left open, extra findings with confirmed/read-only. Then the issues parked,
the ones proposed for closing, and the ones handed to the user. A failure is
reported as a failure, with its output.
