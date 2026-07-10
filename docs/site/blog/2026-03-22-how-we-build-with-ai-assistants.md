---
title: "How We Build Software with AI Assistants"
description: "From brainstorm to production in four phases — structured AI collaboration that produces better software than either alone."
pubDate: "2026-03-22"
author: "Jacob Verhoeks"
tags:
  - "ai"
  - "development-process"
  - "claude"
  - "productivity"
---



*We did not just use AI to write code. We used it to think, plan, specify, and build. A structured loop that produces better software than either of us would alone.*

---

## The problem with "vibe coding"

There is a pattern that has become common with AI coding assistants. You describe what you want. The AI generates code. You paste it in. It does not quite work. You go back and forth. Eventually you have something that runs. Maybe.

This works for scripts and prototypes. It does not work for building a distributed SQL engine with 10 crates, 330+ tests, security controls, and a multi-phase roadmap.

The problem is not the AI. The problem is skipping the phases that make software engineering work: understanding requirements, designing architecture, specifying behaviour, and building incrementally with verification. When you skip straight to "write me the code," you get code that solves the wrong problem precisely.

We needed a process where AI amplifies engineering discipline, not bypasses it.

---

## The four phases

Every feature in SQE, from the initial query engine to TLS support to the planned semantic AI layer, follows the same loop:

```
1. Brainstorm  →  2. Plan & Review  →  3. Specify (OpenSpec)  →  4. Build in Parts
     ↑                                                                    |
     └────────────── feedback from implementation ────────────────────────┘
```

It is not waterfall. Phases overlap. Implementation reveals design issues that loop back to the spec. But the phases exist. Skipping them is how you get tangled code.

### Phase 1: Brainstorm with the human

Every feature starts as a conversation. Not "build me X," which is an instruction, not a brainstorm. Instead: "we need to solve Y, here is the context, what are the options?"

When we started the security hardening work, the conversation was not "add TLS and rate limiting." It was:

> "SQE is going open-source under Apache 2.0. It was built for our internal stack: Keycloak, Polaris, S3. Before we release it, what does 'production-ready' mean for a SQL engine exposed to the network? What is missing?"

The AI does not decide what to build. The human does. But the AI is excellent at enumerating what "production-ready" means for this category of software: TLS enforcement, rate limiting, query timeouts, session lifecycle, audit logging, error sanitisation, config validation, health endpoints. Each with trade-offs.

**What the brainstorm produces:**
- A shared understanding of the problem
- A list of capabilities needed (not tasks, capabilities)
- Key design constraints ("no breaking changes for existing users," "TLS optional for dev mode")
- What is explicitly out of scope ("Kubernetes Helm charts are a separate concern")

**What the brainstorm does NOT produce:**
- Code
- File names
- Implementation details

This phase is 15-30 minutes of conversation. It is the most valuable 30 minutes of the entire feature lifecycle. Misunderstanding the problem here cascades through everything downstream.

### Phase 2: Plan and review

Once we agree on what to build, the AI produces a structured plan. Not a vague outline. A concrete architecture document with:

- **Architecture diagram** showing how new components fit with existing ones
- **Trait definitions** (in Rust, this means the interface contracts)
- **Data flows**: what data moves where, in what format
- **Config examples**: what the user will actually write in their config file
- **Key design decisions**: each with the decision, the alternatives considered, and the rationale

The plan is a document, not a conversation. It lives in the repo at `docs/superpowers/plans/`. It gets reviewed by the human, critically. This is where you catch architectural mistakes before they become 500-line refactors.

A real example from the security hardening plan:

> **Decision:** Rate limiting uses `governor` crate with per-user token bucket + global bucket as second gate.
> **Alternative:** Per-connection limiting at the tonic transport layer.
> **Rationale:** Per-user is more meaningful. A user with 10 connections should not get 10x the rate limit. Governor's keyed rate limiter with DashMap gives O(1) lookup per request.

Review is adversarial. The human asks "what happens when...?" questions:
- What happens when the rate limit fires mid-query? (Error returned, session preserved, no data corruption.)
- What happens when TLS cert files are missing at startup? (Validation fails before the server binds. Fail-fast, not fail-silent.)
- What happens when a worker dies during a distributed query? (Coordinator re-assigns fragments or falls back to local execution.)

Every "what happens when" that does not have a good answer becomes a design item.

### Phase 3: Specify with OpenSpec

This is the part most people skip. It is also the part that makes everything else work.

OpenSpec is our specification format. Each feature change gets a directory with four files:

```
openspec/changes/oss-security-hardening/
├── proposal.md    — Why, what changes, success criteria, rollback strategy
├── design.md      — Architecture, trait definitions, data flows, decisions
├── specs/
│   └── security-controls/
│       └── spec.md  — GIVEN/WHEN/THEN scenarios for each behaviour
└── tasks.md       — Numbered task checklist, broken into sub-phases
```

**The proposal** answers "why are we doing this?" and "what does success look like?" It also has a rollback strategy. For the OIDC rename, that was "deprecated config keys accepted for one release with a WARN log."

**The design** is the architecture from Phase 2, refined and committed to the repo.

**The specs** are GIVEN/WHEN/THEN scenarios that describe expected behaviour:

```
GIVEN a coordinator configured with [coordinator.tls] cert_file and key_file
WHEN a Flight SQL client connects without TLS
THEN the connection is rejected

GIVEN a coordinator with TLS configured and ca_file set
WHEN a client connects with a valid client certificate signed by the CA
THEN the connection succeeds and the client CN is available for mTLS auth

GIVEN a coordinator with no TLS configuration
WHEN the server starts
THEN it binds in plaintext mode and logs a warning
```

These specs are the contract. They are what you verify against when the code is done. They are also what the AI reads when implementing. Each spec scenario maps to a test case.

**The tasks** are the implementation checklist. Each task is small enough to implement in one pass, specific enough to verify, and numbered for tracking:

```
## 5. Rate Limiting

- [ ] 5.1 Add [rate_limit] config: enabled, per_user_queries_per_minute, global_queries_per_minute
- [ ] 5.2 Integrate governor crate; implement per-user token bucket
- [ ] 5.3 Add global bucket as second gate
- [ ] 5.4 On limit exceeded: return RESOURCE_EXHAUSTED error; do not drop session
- [ ] 5.5 Unit tests: rate limit fires after configured threshold; passes below threshold
```

The task list is the interface between specification and implementation. Each unchecked box is a unit of work. Each checked box is a verified capability.

### Phase 4: Build in parts

This is where the AI writes code. Not all at once. The task list drives execution:

1. Pick the next unchecked task
2. Read the relevant context (design doc, existing code)
3. Implement the smallest change that completes the task
4. Run tests
5. Check the box
6. Move to the next task

For SQE, we often run tasks in parallel. Multiple AI agents work on independent tasks simultaneously. The security hardening change had six agents running in parallel: config validation, error sanitisation, rate limiting, query timeouts, session lifecycle, and query cancellation. Each in its own scope, touching different files, converging into one coherent branch.

**The key discipline: each task is verified before moving on.** Not "I think this works." `cargo test` passes. `cargo check` compiles. The box gets checked. If a task reveals a design issue, we pause implementation and loop back to the spec.

This happened during the query cancellation work. The initial design assumed the Flight SQL cancel handler could extract a query ID from the `ActionCancelQueryRequest`. In practice, the FlightInfo encoding made this non-trivial. Rather than hack around it, we noted the limitation, implemented the registry foundation, and deferred the wiring to a follow-up. The task got checked with an honest scope note, not a half-working hack.

---

## What this looks like in practice

A concrete timeline from the security hardening feature:

**Hour 0-1: Brainstorm.** "What does production-ready mean for an open-source SQL engine?" Enumerated 11 capability areas. Agreed to defer Kubernetes/Helm. Agreed TLS should be optional (not enforced) to keep dev experience smooth.

**Hour 1-3: Plan and review.** Architecture document produced. Reviewed rate limiting approach (governor vs. custom). Reviewed error sanitisation (`client_message()` pattern). Reviewed session lifecycle (idle + absolute timeouts). Several "what happens when" rounds.

**Hour 3-4: OpenSpec.** Proposal, design, specs, and 51 tasks written. Each task small enough for one implementation pass. Tasks grouped by section (rename, config, TLS, rate limiting, timeouts, sessions, cancellation, audit, errors, health).

**Hour 4-8: Implementation.** I handled Sections 1-2 (renames, doc updates, judgment-heavy). Six AI agents ran Sections 3, 5-10 in parallel (implementation-heavy). I fixed compilation errors from agent output, verified tests, resolved conflicts between agents touching shared files.

**Result:** 51/51 tasks complete. 331 tests passing. 30 files changed, +1700 lines. One MR.

Total elapsed: roughly one working day for what would traditionally be 2-3 weeks of engineering work.

---

## Why this process works

### It separates thinking from typing

The brainstorm and plan phases are thinking. The implementation phase is typing. AI assistants are extraordinary at typing, generating syntactically correct, idiomatically consistent code at high speed. They are less good at deciding what to type. The four-phase process keeps the human in the "what" and "why" while the AI handles the "how."

### Specs are a shared language

GIVEN/WHEN/THEN scenarios are unambiguous. When the AI reads "GIVEN a coordinator with TLS configured, WHEN a client connects without TLS, THEN the connection is rejected," it knows exactly what to implement and how to test it. No interpretation gap. No "I thought you meant..."

### Tasks are atomic

Each task in the checklist is small enough to implement correctly in one pass. "5.2 Integrate governor crate; implement per-user token bucket" is one file, one struct, one test. The AI does not need to hold the entire feature in context. Just the current task and the design doc.

### Verification is continuous

Every task gets tested before the box is checked. The feedback loop is tight: implement, test, verify, next. Bugs are caught in the task where they are introduced, not three tasks later when something else breaks.

### The loop is explicit

When implementation reveals a design issue, and it always does, there is a defined path back. You do not hack around the issue and move on. You pause, update the spec if needed, and resume. The OpenSpec format makes this natural. The task file is a living document, not a frozen plan.

---

## The OpenSpec format

For those interested in the format itself:

```
openspec/
├── config.yaml           — project-level config
└── changes/
    └── feature-name/
        ├── .openspec.yaml  — change metadata (status, schema)
        ├── proposal.md     — why, what, success criteria, rollback
        ├── design.md       — architecture, traits, data flows, decisions
        ├── specs/
        │   └── domain/
        │       └── spec.md — GIVEN/WHEN/THEN scenarios
        └── tasks.md        — numbered implementation checklist
```

Each change has a status (`in-progress`, `complete`) and a task count. The CLI (`openspec status`, `openspec instructions apply`) drives the workflow. It reads the specs and tasks, shows progress, and provides implementation instructions.

The format is designed to be:
- **Machine-readable**: AI assistants can parse the tasks, check progress, and know what to implement next
- **Human-reviewable**: proposals and designs are Markdown, readable in any editor or GitLab MR
- **Incremental**: tasks can be checked off one at a time, progress is visible, pausing and resuming is natural
- **Version-controlled**: everything lives in the repo, changes are tracked in git, reviews happen in MRs

---

## The numbers

Across SQE's development so far:

| Change | Tasks | Completed | Method |
|--------|-------|-----------|--------|
| Core engine (sqe-core-engine) | 103 | 99 (4 blocked upstream) | Phased over ~6 weeks |
| Docker packaging | 29 | 29 | Single session |
| OSS security hardening | 51 | 51 | Single day, 6 parallel agents |
| Pluggable auth | 59 | 0 (designed, not started) | -- |
| Pluggable catalogs | 83 | 0 (designed, not started) | -- |
| Semantic AI layer | 89 | 0 (designed, not started) | -- |

414 tasks total. 179 implemented. 231 designed and ready to build. 4 blocked on upstream.

Every completed task has a passing test. Every checked box means the code compiles, the test passes, and the capability works.

---

## What we would tell teams starting this

**Start with the brainstorm, not the prompt.** If you are typing "build me a rate limiter" into an AI assistant, you have skipped three phases. Start with "we need to protect our server from abuse, what does that mean for our architecture?"

**Write the spec before the code.** It feels slow. It is not. A 30-minute spec saves hours of rework. GIVEN/WHEN/THEN scenarios are the fastest way to align human and AI on expected behaviour.

**Make tasks atomic.** If a task takes more than 15 minutes to implement, split it. The AI's context window is finite. Small tasks fit cleanly. Large tasks produce large, tangled, hard-to-review diffs.

**Verify continuously.** Do not batch up 10 tasks and then test. Test after each one. The earlier you catch a bug, the less context you need to fix it.

**Let the loop work.** Implementation will reveal design issues. That is not failure, that is the process working. Update the spec, note the limitation, move on. The spec is a living document, not a contract.

**Use parallel agents for independent work.** Six sections of security hardening, each touching different files, each with clear inputs and outputs. Six agents, one coherent result. This is where AI assistants shine. Not at doing one thing faster, but at doing six things simultaneously.

---

## Conclusion

We did not build SQE by telling an AI "write me a SQL engine." We built it by brainstorming what we needed, planning the architecture, specifying the behaviour, and building it in verified pieces. The AI is the most productive engineering partner we have ever had. But only because we gave it the structure to work within.

The process is the product. The code is a side effect.

---

*SQE is open-source under Apache 2.0. The OpenSpec format and development process described here are part of the repository. See `openspec/` for the full specification library.*
