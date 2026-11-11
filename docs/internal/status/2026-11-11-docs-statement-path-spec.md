**STATUS 2026-11-11: statement-path restructure specified; way of working written down (`docs/statement-path-spec`, openspec `statement-path-restructure`).**

`openspec/changes/statement-path-restructure/` holds the proposal, design, three capability specs (`statement-pipeline`, `code-size-budgets`, `upstream-parity`) and 32 tasks. The design keeps the dispatch match flat and exhaustive, moves the six inline arms out as pure moves, and carries the captured plan through `&mut` so a timed-out statement still audits its tables. No code changed. `CLAUDE.md` gains a "Way of working" section and corrects two stale lines (`docs/internal` is on the public mirror; a second checkout is allowed for measurement and docs). `AGENTS.md` is new and points at `CLAUDE.md`.

Known red: `access_control_e2e::insert_does_not_confer_storage_relocation` fails on `main` (reproduced on 5d1f41e62 with a fresh stack). Task 1.1 bisects it; the access-control arms are not moved before it is green.

**NEXT:** task 1.1 (bisect the access-control failure), then 1.2 to 1.4 (audit-count tests, baseline profile) before the stage split in section 2.
