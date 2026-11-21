**STATUS 2026-11-21: the table-tag inheritance docs say plainly that it is enforced by SQE only (`docs/tag-inheritance-sqe-only-924`).**

!1910 (columns inherit their table's tags) merged with design decision D5: the Ranger tag projection keeps projecting own column tags. The docs said this in passing. They now say it plainly, so nobody reads it as a two-engine guarantee: "inherited tags are enforced by SQE only; the Ranger tag projection (and so Spark) still sees own tags".

- `openspec/changes/tag-inheritance/design.md` (D5 and Risks), `proposal.md`, `tasks.md` 3.1: the statement, plus the follow-up #935.
- `changelog.d/feat-tag-inheritance-table-924.md` and the book's fine-grained access control page: the same statement.
- #935 filed: project effective tags into Ranger with the same Databricks-style override (part of #924).

This is no regression for Spark: table tags never masked columns before. #924 is not done until #935 lands.

Docs only; `python3 scripts/changelog-collect.py --check` ok.

**NEXT:** #924 part 2, schema and catalog tag inheritance with cached parent reads; then #935.
