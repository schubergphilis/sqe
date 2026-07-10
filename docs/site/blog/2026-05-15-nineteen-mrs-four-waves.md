---
title: "Nineteen MRs, four waves, and the failure modes of agent batches at scale"
description: "Two days after the nine-PR audit pass, we ran a bigger one. 130 issues filed, 19 themed MRs merged across four waves. Same workflow, more failure modes. Watchdog stalls, a reboot mid-wave, a broken main, and config.rs as the conflict magnet. Here is what actually happened."
pubDate: "2026-05-15"
author: "Jacob Verhoeks"
tags:
  - "developer-experience"
  - "agentic-ai"
  - "code-review"
  - "git"
  - "security"
---



*May 15, 2026*

Two days ago we shipped the nine-PR audit pass. Eighteen issues, nine themed branches, two structural conflicts. Four minutes of friction across a Sunday afternoon.

Today we ran the same workflow at roughly seven times the scale. 130 issues filed by a separate audit pass, four waves of parallel agents, nineteen themed merge requests. 108 commits landed on main. The shape of the work was identical. The failure modes were different.

This post is about what broke and why.

## The numbers

Wave 1 ran four agents in parallel: critical policy correctness, tests infrastructure, auth hardening, Trino and Flight protocol completeness. 24 commits. Four MRs. All four merged within a few hours.

Wave 2 ran five: worker-side auth (the part wave 1 had carved off as separate scope), SecretString migration, scheduler isolation, write-path correctness, and the DELETE/UPDATE half of the policy enforcer that wave 1 had deferred. 26 commits. Five MRs.

Wave 3 ran five themes in two batches: async hygiene, auth and session config, code-quality refactor, caching and perf, build hygiene and observability. 33 commits. Five MRs.

Wave 4 ran five themes in two batches: remaining correctness, test coverage, miscellaneous hygiene tail, operator tunability, type-safety polish. 30 commits. Five MRs.

Total: nineteen MRs, 108 commits, roughly 110 issues closed. One issue (#2, an existing branch) was already mid-flight and stayed out of scope.

## What changed from nine to nineteen

The themed-branches principle held. Conflicts came in the structural bucket. Five of the nineteen MRs hit conflicts on rebase. All five resolved in under fifteen minutes each, no semantic surprises.

What did not hold was the watchdog assumption.

The agent harness kills a subagent that produces no streaming output for ten minutes. That number was fine for the nine-PR pass because each branch was small. At wave 3 it killed all five batch-1 agents simultaneously. Each agent was sitting on a cold `cargo check --all` that takes about two minutes per crate on this workspace. Multiplied across seven issues per agent, the agent could spend twelve minutes inside a single cargo invocation. Watchdog fires. Agent dies. Three of the five had managed one commit before they were killed. Two had zero commits, only dirty edits.

The recovery shape was:

- Push the three salvageable partial branches to origin so the commits survived the worktree teardown.
- Remove the five dead worktrees, freeing 60 GiB of cargo target dirs.
- Re-dispatch with a leaner brief.

The leaner brief changed three things. First, `cargo check -p <crate>` instead of `cargo check --all`. Single-crate checks finish in 7 to 45 seconds. Watchdog has nothing to worry about. Second, commit and push after every issue, not at the end. A watchdog kill mid-run now loses at most one issue's edits. Third, stagger three agents at a time instead of five. Lower contention on the shared cargo cache, fewer simultaneous worktrees to GC if something goes wrong.

The leaner brief shipped the rest of wave 3 cleanly and ran wave 4 with no stalls.

## config.rs as the conflict magnet

`crates/sqe-core/src/config.rs` got touched by twelve of the nineteen MRs. Every theme that added a tunable knob ended up there. Every theme that added a struct field ended up in `CoordinatorConfig::default()` too. The Debug impl listed every field. The TOML test fixtures had to mention every field. Four anchor points per theme, all in the same file.

The result: every wave's rebase had at least one config.rs conflict. The pattern was always the same.

```
<<<<<<< HEAD
    /// HTTP/2 / gRPC transport tuning. Lifts the receiver windows off
    /// tonic's 64 KB default.
    #[serde(default)]
    pub transport: GrpcTransportConfig,
=======
    /// gRPC connect timeout (seconds) used when dispatching scan tasks.
    #[serde(default = "default_worker_connect_timeout")]
    pub worker_connect_timeout_secs: u64,
>>>>>>> 85e4eaf (fix(coordinator): add connect+rpc timeouts to dispatch_to_worker)
```

Both branches add a field. Same anchor. Different concerns. The resolution is always "keep both, the order does not matter." One minute of editing, zero minutes of thinking.

The interesting bit is what *would* have made it semantic. If two branches had both renamed `worker_secret`, or both moved the field into a substruct, or both changed its type from `String` to `SecretString`, the rebase would have been a real merge. That last one almost happened. The SecretString migration (#100, in wave 2 MR !203) flipped the field type. The next four MRs to touch `coordinator.worker_secret` all assumed `String`. They compiled in isolation. They broke on rebase. We caught it in wave 3, fixed the callsites with `.expose().to_string()`, and noted it for a follow-up that types the consumer signatures properly.

That is the only semantic conflict we had across the campaign. It is also the only one that mattered.

## The wave that left main broken

Wave 2's SecretString migration was technically correct. The struct field changed type. The deserializer kept the same TOML schema. Unit tests in `sqe-core` and `sqe-auth` passed. The branch merged.

What we missed: the `with_worker_secret(secret: String)` builder in `sqe-coordinator/src/distributed_scan.rs` still expected `String`. The callsite was `self.config.coordinator.worker_secret.clone()`. After the type flip, `.clone()` returns `SecretString`, not `String`. The coordinator failed to compile.

CI on the SecretString branch did not catch it because the merge-result pipeline only ran SAST checks. The branch pipeline did not run a full workspace `cargo check`. The first agent of the next wave hit the error during its own work and flagged it in its MR description. We patched the callsites as a bundled fix inside the next branch (`fix/auth-session-config`, MR !205) along with the moka invalidation fix it was actually scoped for.

This is a CI gap, not a workflow gap. The fix is a build job that runs `cargo build --workspace --all-targets` on every MR pipeline, not just SAST. The themed-branches pattern surfaced the problem cleanly because the next agent's first build failed loudly. With a single mega-branch we would have noticed when the audit-test stage broke and would have spent longer untangling which issue introduced the regression.

## The reboot

Mid-wave 2 we lost the laptop. Reboot, fresh shell, no cargo on PATH, no glab on PATH, five live worktrees with cargo running inside them.

The state after reboot:
- Three worktrees had one committed fix each, the rest as dirty in-progress edits.
- Two worktrees had no commits, only dirty edits.
- No agent was alive. The harness state was gone with the process.

The recovery decision was the obvious one. Commit-on-disk survives reboot. Dirty trees do not. Push the three branches that had any commit so the work was durable on origin. Trash the rest. Re-dispatch the five themes fresh, each told what its predecessor had committed so the new agent would not redo it.

Three of the five new agents continued cleanly. Two were starting from zero anyway. The campaign lost about thirty minutes of agent time and zero commits. Worth it.

The lesson is the same one git has been teaching for fifteen years: if it is not committed, it does not exist. Agent-managed worktrees do not change that.

## Cron-based status polling

Each wave had a status cron firing every two minutes. The prompt was "for each active worktree, report branch / commits-ahead / dirty-file-count / branch-on-remote / MR-iid, compact table." It self-cancelled when every active branch had an MR.

The cron was useful when an agent was running for an hour and I had no other signal. It was wasteful when agents finished in fifteen minutes. The harness already wakes me up on agent completion. Polling between completions adds nothing except cache misses on a 1500-token conversation window.

The pattern that worked: schedule the cron at dispatch time, cancel it the moment the last MR opens. Let agent completion notifications drive the interesting transitions. The cron is just a tripwire for agents that go silent.

## The metric

Nineteen MRs. 108 commits. Five rebases for conflicts. Four were pure structural (config.rs anchor collisions, test-block anchor collisions, sibling-method anchor collisions). One was semantic and load-bearing (the SecretString migration not threading through builder signatures).

Twelve hours of wall time, including the watchdog-stall recovery and the reboot. Probably eight hours of actual agent runtime. The rest was orchestration, conflict resolution, and pushing merge buttons.

Compare to a human pass of the same backlog. 130 issues, even at one solid hour per issue, is 130 hours. Three engineer-weeks. The agents got it to a day. The orchestrator (me) was a one-person reviewer queue, not the bottleneck on writing the code.

## What this means for the next round

Three things we will do next time.

**Brief agents with the leaner cadence from the start.** No `cargo check --all`. Single-crate checks. Push after every issue, not at the end. The watchdog stays a tripwire instead of a wall.

**Run a workspace build on every MR pipeline.** SAST and unit tests are not enough. The SecretString hole was visible to `cargo build` and invisible to everything else CI was running. The cost is two minutes per pipeline. The cost of finding it from the next agent's failure is worse.

**Scope themes to avoid load-bearing type migrations.** SecretString-touches-eight-sites was a real concern that needed real attention. Dropping it into a five-issue branch was wrong. Migrations that cross signature boundaries should be their own theme with their own follow-up audit of consumer sites. Pair them with a "consumer-update" branch that lands second.

The themed-branches model held under load. It is the right shape for AI-assisted batches in the 5-to-20 issue range. At 130 issues it needs better failure-mode handling than a 9-issue pass, but the underlying decomposition is the same. Theme by concern. Sequence by dependency. Accept structural conflicts as routine. Reserve human attention for the semantic ones, which are still the only ones that need it.

130 issues, 19 MRs, 108 commits, one day. One semantic conflict, caught at the next agent's first build. Four structural conflicts, twelve minutes of edits between them.

That is the workflow at this scale.
