# docs/ctas-staged-create-proposal

**NEXT:** the owner decides on the gate in `openspec/changes/ctas-staged-create/tasks.md`
section 0. First answer Q1: which operation Polaris authorizes for a staged-create
commit. No code until then.

## Done

- `openspec/changes/ctas-staged-create/`: proposal, design, spec and tasks for
  a streaming CTAS that stages its create and registers the table only in the
  commit that adds its first snapshot.
- The design states two caveats. First, the write check moves to that commit,
  so data is written before a refusal. Second, the prefix delete can still fail.
  It also states open questions: Polaris authorization of the commit,
  credentials on a staged create, fallback detection and concurrency.

## Depends on

- The `ctas_rollback` module from `fix/ctas-rollback-orphan-visible`, which
  the proposal refers to.
