**STATUS 2026-10-12: status entries move to one file per change (`docs/status-fragments`).**

Every merge request used to prepend a `STATUS` paragraph to line 3 of
`nextsteps.md` (126 commits in the month before), so any two open branches
conflicted there. Each change now adds its own file under
`docs/internal/status/`; `nextsteps.md` keeps its history frozen below a
pointer. Same day: openspec reconciled with the code (!1390; two superseded
changes archived), and the combined priority list is #827. New P2 bugs from
that pass: #829 (`SET WRITE_BRANCH` is ignored outside a transaction) and #828
(column `DEFAULT` never applied).

**NEXT:** dbt-sqe per-connection transactions (openspec `single-table-transactions` 5.4); then token refresh-ahead (#816) and re-resolve on 401 (#803). #829 is a candidate to go first.
