**STATUS 2026-11-12: write snapshot properties specified (`docs/write-snapshot-properties-spec`, openspec `write-snapshot-properties`).**

A copy job asked for a completion marker in the same commit as its data. Today it can only write a table property in a second commit, and a crash between the two leaves data without a marker. `openspec/changes/write-snapshot-properties/` holds the proposal, design, one capability spec and 24 tasks.

Main decisions:
- Properties set inside a transaction belong to it, which is race-free when clients share one bearer session. Outside a transaction they stay on the session until `RESET`.
- Every write's commit closure applies them. There is no central hook.
- An empty write commits an empty snapshot when properties are set.
- Reserved keys and prefixes are refused.
- The `t$snapshots` JSON escaping is fixed first.

No code changed.

**NEXT:** task 1.1 (serde_json for `t$snapshots`), then 1.2 (does the Trino HTTP session reach the session manager) before the statement work in section 2.
