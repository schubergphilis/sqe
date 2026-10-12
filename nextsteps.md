# SQE — Next Steps

Status entries live in [`docs/internal/status/`](docs/internal/status/README.md), one file per change. The newest file's `**NEXT:**` line is the current pointer:

```bash
ls -r docs/internal/status/2*.md | head -5 | xargs cat
```

Do not add entries here. Every merge request used to prepend one to this file, so any two open branches conflicted. The 149 entries this file held up to 2026-10-11 are now individual files in that directory, and the old Step 1–6 roadmap is in `docs/internal/status/legacy-roadmap-steps.md`. The live roadmap is in `README.md`.
