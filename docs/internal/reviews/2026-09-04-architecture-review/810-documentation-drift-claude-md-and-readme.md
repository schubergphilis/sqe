# 810: CLAUDE.md and README describe an engine that no longer exists

- Scope: CLAUDE.md, README.md, nextsteps.md, docs/site production guide
- Severity: low
- Category: maintainability
- Effort: S
- Status: fixed (branch `fix/config-startup-code-structure`, 2026-09-05)

## Resolution (2026-09-05)

Fixed on `fix/config-startup-code-structure`. CLAUDE.md crate table matches
the workspace members (minus workspace-hack/xtask), names `sqe-server`, lists
the five client surfaces, and mentions OPA/Cedar only as historical. README
quick start runs `sqe-server --config`.


## Problem

CLAUDE.md is the first file every agent and contributor reads, and several
of its load-bearing statements are false against the current tree.

- `CLAUDE.md:9` "the full engine implementation across 10 crates"; the table
  at `:24-36` lists 11; `cargo metadata` lists 22 workspace members (21 crates
  plus `workspace-hack`). Missing from the table: sqe-cli, sqe-lineage,
  sqe-compaction, sqe-spill, sqe-trino-functions, sqe-quack-wire,
  sqe-quack-server, sqe-quack-client, sqe-bench, xtask.
- `CLAUDE.md:7,20,29,75,85` describe the policy backend as "OPA/Cedar" and
  "OPA (Rego) or Cedar (pluggable)". `crates/sqe-core/src/config.rs:3225-3233`
  `enum PolicyEngine { Passthrough, InMemory, Ranger, .. }`; there is no
  Cedar or OPA implementation in `crates/` (`grep -rli 'cedar\|rego\|OpaStore' crates/*/src`
  finds only unrelated hits). The shipped backends are Passthrough,
  InMemory, Apache Ranger and Polaris grants.
- `CLAUDE.md:18` "Client (JDBC/Flight SQL) -> Coordinator -> Workers"; the
  actual client surfaces are Flight SQL, Trino HTTP, MCP, Quack (DuckDB
  RPC) and the web UI, and the shipped binary is `sqe-server`, not a
  coordinator binary.
- `README.md:17` and `:209` tell users to run `cargo run --release --bin sqe-coordinator`,
  a binary without health endpoints that is not shipped in the image (see 801).
- `nextsteps.md:1-3` has the title line twice.
- `docs/site/book/src/operations/production-guide.md:24` describes the slim
  image feature set incorrectly (see 809, item 3).

The README crate table (`README.md:291-308`) is current and lists 18 crates;
CLAUDE.md was not updated with it.

## Why it matters

- Agents plan work from CLAUDE.md. An agent asked to "add a Cedar policy"
  will look for a Cedar backend that does not exist; an agent asked to
  touch "the coordinator binary" will edit the dead one.
- The published book repeats the OPA/Cedar claim in its design notes; the
  docs leak-scan does not check for stale architecture claims.

## Proposed change

1. Regenerate the CLAUDE.md crate table from `cargo metadata` (an `xtask
   crate-table` that prints the markdown; a test compares it to the file).
2. Replace "OPA/Cedar" with "Ranger, Polaris grants, in-memory, passthrough"
   in CLAUDE.md `:7,20,29,75,85`, and add a line stating that OPA/Cedar were
   the original design and were not built.
3. Update the architecture line to list the five client surfaces and name
   `sqe-server` as the binary. Update README `:17,:209`.
4. Fix `nextsteps.md` duplicate title.
5. Add the CLAUDE.md crate-count check to the existing docs leak-scan job.

## Acceptance criteria

- `grep -n 'OPA\|Cedar' CLAUDE.md` returns only the historical note.
- The CLAUDE.md crate table has one row per `cargo metadata` member except `workspace-hack` and `xtask`.
- `grep -rn 'bin sqe-coordinator' README.md docs/site` returns nothing.

## Related

- 801, 809.
- June 2026 codebase analysis, Medium item 21 (README table; done for README, not for CLAUDE.md).
