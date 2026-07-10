---
title: "Nine PRs, two merge conflicts, and the value of themed branches"
description: "Sunday afternoon: an audit dropped eighteen issues into the tracker. Monday morning we had nine merge requests open. Two of them hit conflicts on rebase. Neither one mattered. Here is why that was deliberate, not lucky."
pubDate: "2026-05-13"
author: "Jacob Verhoeks"
tags:
  - "developer-experience"
  - "code-review"
  - "git"
  - "security"
---



*May 13, 2026*

Sunday afternoon we ran a structured audit pass against the coordinator. The output: eighteen open issues. Four CRITICAL, eight HIGH, five MEDIUM, two unlabeled. Fail-open OPA backend. JWT audience opt-in. Secret literals leaking through the audit log. Path traversal in the file TVFs. N+1 planner invocations on JDBC `getTables`. The usual mix of "should never have shipped that way" and "we noticed but moved on."

The interesting part is what happened next. Nine themed merge requests went up by Monday morning. Two of them hit merge conflicts on rebase. Both conflicts resolved in under five minutes. The work landed clean.

This post is about the workflow that made the second part trivial. Not the audit itself. The audit was the easy bit.

## The shape of the work

The eighteen issues spanned five concerns. Auth correctness. Cache invalidation scope. Error classification. TVF security. Admin gating for catalog DDL. A few were genuinely independent (RwLock cleanup, Debug-derive hardening). Most clustered.

The naive option is one branch per issue, eighteen branches, eighteen reviews. The cost adds up. Each branch needs its own description, its own pipeline run, its own context-switch in the reviewer's head. For a security pass where most fixes share a config file or a dispatch site, eighteen tiny MRs becomes paperwork that crowds out the signal.

The other naive option is one mega-branch. Eighteen fixes in one diff. Reviewer has to load the entire context. Rollback granularity is gone. Bisect becomes useless if any one fix turns out to regress something.

We picked the middle. Nine themed branches. Each one took a coherent slice:

| Theme | Branch | Issues |
|---|---|---|
| Fail-closed defaults | `fix/security-fail-closed-defaults` | #4, #5, #6 |
| Metadata browse path | `fix/metadata-browse-perf-and-injection` | #7, #9, #15 |
| OIDC plumbing | `fix/auth-token-hardening` | #13, #14, #17 |
| Secret hygiene + JWT audience | `fix/secret-debug-derives-and-jwt-aud` | #16, #8 |
| Cache invalidation scope | `fix/scope-session-cache-invalidation` | #11 |
| Error classification | `fix/catalog-error-classification` | #12 |
| RwLock removal | `fix/remove-pointless-rwlock-restcatalog` | #18 |
| TVF allowlist | `fix/tvf-path-and-host-allowlists` | #10 |
| Admin gate | `fix/admin-gate-attach-and-secrets` | #3 |

The branches share a substrate. Most of them touch `sqe-core/src/config.rs`. Several touch the auth crate. The metadata-browse fix and the admin-gate fix both extend `QueryHandler` in `query_handler.rs`. By construction, the later branches were going to step on the earlier ones once those earlier ones merged.

That is the part we want to think about.

## The bet behind themed branches

The bet is simple. Merge conflicts come in two flavours. Structural and semantic.

A *structural* conflict is when two branches add code at the same anchor point. Both add a test at the end of `mod tests`. Both add a method right after `write_handler()`. Both add a field to a config struct. Git sees both edits hit the same line range, raises a conflict marker, and asks the human which version to keep. The human reads two snippets and almost always answers "both, here is the order." A structural conflict takes one minute to resolve and zero minutes to verify, because the two changes have no logical interaction.

A *semantic* conflict is the other thing. Two branches change the same function for incompatible reasons. One renames a parameter that the other relies on. One inlines a helper that the other adds a new caller to. Git either silently produces wrong code or flags a small textual conflict that hides a large logical one. Resolving takes thinking, not editing. Tests catch some of these. The rest are the ones that ship.

Themed branches push the conflicts into the structural bucket. When each branch owns a coherent concern, two branches almost never touch the same logic for different reasons. They touch the same file, sure. The audit log redaction (theme: fail-closed) and the audit log call sites (theme: secret-leak) both live in `sqe-metrics/src/audit.rs`. Different concerns. Different code paths through the file. Different anchors.

If you instead theme branches by *file*, every concern that touches that file fights every other concern through the same diff. Semantic conflicts everywhere. The reviewer has to mentally untangle which line belongs to which intention. This is the trap.

## Where the conflicts actually landed

Seven of the nine branches merged in order with no conflict. Each was a clean fast-forward over its predecessor. The eighth and ninth (TVF allowlist and admin gate) opened after the first seven had landed on main. Both hit conflicts on rebase.

The first conflict, on `crates/sqe-core/src/config.rs`:

```
<<<<<<< HEAD
    // --- Provider env-var override (issue #14 regression test) ---
    static ENV_LOCK: ...
    #[test] fn provider_client_secret_env_override_beats_toml() { ... }
    #[test] fn provider_client_secret_env_override_token_exchange_optional() { ... }
=======
    // --- TvfPolicy (issue #10 regression tests) ---
    #[test] fn tvf_default_allows_object_store_schemes() { ... }
    #[test] fn tvf_default_rejects_local_absolute_paths() { ... }
    #[test] fn tvf_default_rejects_arbitrary_http_hosts() { ... }
    [...]
>>>>>>> f5b748a (fix(catalog): TVF path & host allowlist)
```

Two branches added tests at the same anchor: right before the closing `}` of `mod tests`. The OIDC-plumbing branch added two tests for the new `[[auth.providers]]` env-var override. The TVF-allowlist branch added seven tests for the new path/host policy. Git flagged it because the byte ranges overlapped.

The resolution: keep both blocks. They test different code. They run in any order. They share the file but not the logic. One minute of editing.

The second conflict, on `crates/sqe-coordinator/src/query_handler.rs`, had the same shape:

```
<<<<<<< HEAD
    pub async fn session_catalog(&self, ...) -> Result<Arc<SessionCatalog>> { ... }
    pub async fn list_metadata_tables(&self, ...) -> Result<Vec<(String, String)>> { ... }
=======
    fn require_admin(&self, session: &Session, statement: &str) -> Result<()> { ... }
>>>>>>> 740ac6e (fix(auth): admin gate for ATTACH/CREATE SECRET)
```

Three methods to insert at the same anchor: right after `write_handler()`. The metadata-browse branch added two helpers for the new direct-catalog walk. The admin-gate branch added one helper for role checking. Different concerns. Different call sites. Three minutes of editing to merge them in declaration order.

That was the worst of it.

## The drive-by that Git auto-resolved

The RwLock-removal branch and the TVF-allowlist branch both made the same drive-by clippy fix. Rust 1.92 promoted `clippy::type_complexity` and started flagging an existing test fixture in `hf_tree_cache.rs`:

```rust
struct MockHttp {
    responses: Mutex<Vec<(String, Vec<u8>, Vec<(String, String)>)>>,
    calls: Mutex<Vec<String>>,
}
```

Both branches introduced a `type MockResponse = (String, Vec<u8>, Vec<(String, String)>);` alias. Identical content. Identical placement. When the second branch rebased over the first, Git auto-merged the duplicate without raising a conflict marker. Same text on both sides counts as agreement.

This is the boring case. It is also the most common case in a well-themed workflow. Two branches reach for the same small cleanup because the same lint blocks them both. The repo ends up with the right code regardless of which branch lands first.

## Where this falls apart

Themed branches are not magic. They fail in three predictable ways.

**Touching the same function for different reasons.** The cache invalidation fix and the admin gate both edited the dispatch site in `query_handler.rs::execute`. The invalidation fix swapped `invalidate_all_session_caches()` for `invalidate_session_cache(&session.user.username)` at twelve call sites. The admin gate added a `require_admin(...)?` line at five call sites. Two of those call sites overlap (CREATE TABLE under CTAS, and the ATTACH path that the admin gate now blocks). The audit-time refactor that sequenced these branches put the invalidation fix first because it had no security gate to insert. The admin gate rebased clean because the invalidation fix had already settled. If they had been built in the other order the conflict would have been semantic, and we would have needed to think about whether a non-admin should still trigger a cache invalidation before the gate kicked in. (Answer: yes, defensively, but it is a real question.)

**Renaming or moving a function the other branch depends on.** None of the nine branches did this. If one had, the structural-conflict guarantee would have broken. The fix is to identify the rename up-front, land it as its own branch, and rebase the dependents.

**Forgetting that pure-test diffs still count.** Both conflict cases here were in test files, not production code. That is fine; tests are code. But it means the "themed branches have no semantic overlap" guarantee depends on the tests being themed the same way. If you scattered eighteen test additions across one giant tests-only branch you would have semantic conflicts on every other rebase, because the tests would reach for the same fixtures and helpers built for different intentions.

## The metric to remember

For this audit batch the number is 7/9 fast-forward merges, 2/9 structural conflicts under five minutes each, 0/9 semantic conflicts. Roughly four minutes of total conflict-resolution time across eighteen issues.

Compare to the unthemed alternative. One mega-branch with all eighteen fixes would have one diff, no conflicts. It would also have one reviewer trying to hold all eighteen concerns in their head for the duration of the review, no granular bisect, and a rollback story that means "revert everything." Eighteen single-issue branches would have eighteen pipelines and eighteen review queues with the same set of structural conflicts in worse positions, because the granularity matches the noise rather than the signal.

Nine themed branches sit in the middle on every axis we care about. Review effort proportional to concern, not to issue count. Rollback granular at the concern level. Bisect useful. Conflicts predictable and trivial.

## The part the AI helped with

This was a pair-programming session with Claude Code. The model triaged the eighteen issues into themes, drafted the patches, ran the test and clippy loops, and opened the MRs. Two prompts shaped the session.

First: "what you think is good" for scope. The model picked nine themes after reading the issue bodies. The themes mostly matched what I would have picked, with one exception. The model put the JWT audience-required fix in the same branch as the Debug-derive hardening because both touched `sqe-auth` and both addressed secret-leak adjacent issues. I had been leaning toward one branch per HIGH-severity issue. The model's grouping was better. The audience fix needed twelve test-fixture updates across the bearer-token tests; the Debug-derive fix needed manual `Debug` impls on three structs. Different code, same concern (secret hygiene), one branch handled both with one set of test runs and one CI pipeline.

Second: "themed PRs" instead of "one PR per issue" for the workflow. That decision is the one this post is about.

The two structural conflicts at the end of the session were the model's blind spot. When PR7 (RwLock) and PR8 (TVF) both reached for the same `MockResponse` clippy fix, the model did not flag the duplication during PR8's authorship. When PR8 and PR9 both added test blocks at the end of `mod tests` in the same config file, the model did not flag the upcoming conflict at PR9 authorship time either. Git caught both later. Five minutes of resolution.

A human reviewer scanning the nine MRs in sequence would probably have caught the `MockResponse` duplicate before submission. The conflict markers in test modules are harder to anticipate because the relevant anchor (end of `mod tests`) is implicit. Both are cheap when they happen. Neither is worth restructuring the workflow to avoid.

## What this means for AI-assisted batches

If you let an AI generate eighteen security-audit fixes in one session, you will end up with overlapping branches. The structural conflicts are unavoidable and cheap. The semantic conflicts are avoidable, and avoiding them is what theming buys you.

We are going to do more of these. The mental model: theme by concern, sequence by dependency, accept structural conflicts as routine. Reserve human attention for the semantic ones, which are the only ones that needed it anyway.

Eighteen issues. Nine PRs. Two structural conflicts. Zero semantic ones. Four minutes of friction across a Sunday afternoon's worth of audit work. That is the workflow.
