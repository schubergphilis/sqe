# Status log

One file per finished change, named `YYYY-MM-DD-<branch-slug>.md` (for
`feat/txn-audit-824`, `2026-10-11-feat-txn-audit-824.md`). Each merge request
adds its own file, so concurrent merge requests never touch the same lines.
That was the problem with prepending a `STATUS` paragraph to `nextsteps.md`:
every pair of open branches conflicted there.

The newest file's `**NEXT:**` line is the current pointer. To read the log:

```bash
ls -r docs/internal/status/2*.md | head -5 | xargs cat
```

Entries before 2026-10-12 stay where they were, frozen in `nextsteps.md`.

## Template

```markdown
**STATUS YYYY-MM-DD: <one-line summary> (`<branch>`, #<issue>, openspec <task>).**

What changed, what it fixes, limits.

**NEXT:** <what comes next, or "unchanged">
```
