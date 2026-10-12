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

The entries from 2026-08-22 to 2026-10-11 were split out of `nextsteps.md` on 2026-10-12. Their file names carry the old same-day ordering code (`2026-10-11-dc-...`), and their text is unchanged apart from the blockquote markers. The old Step 1–6 roadmap is in `legacy-roadmap-steps.md`.

## Template

```markdown
**STATUS YYYY-MM-DD: <one-line summary> (`<branch>`, #<issue>, openspec <task>).**

What changed, what it fixes, limits.

**NEXT:** <what comes next, or "unchanged">
```
