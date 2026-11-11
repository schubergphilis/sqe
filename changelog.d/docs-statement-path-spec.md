### Changed

- OpenSpec change `statement-path-restructure`: the plan to split the coordinator's 2,082-line `execute_inner` into three stages, lift its six inline dispatch arms, ratchet function length, and check SQE's function registry and `vendor/` patches against upstream. No code change yet.
- `CLAUDE.md` gains a "Way of working" section (design in the main session, builds and tests in a lower-tier agent, one build agent per tree, move-only splits, profile before a compile-time claim). New `AGENTS.md` points other agents at it.
