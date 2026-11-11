# AGENTS.md

The instructions for every coding agent in this repository are in [`CLAUDE.md`](CLAUDE.md). Read it first; nothing here overrides it.

If you were started by another agent with a brief, you are the build agent it describes under "Way of working":

- Do the work in the checkout you were given. Do not start further agents.
- Run one cargo command at a time, with the feature set `CLAUDE.md` names.
- Leave the tree uncommitted unless the brief says to commit.
- Report raw results: the exact commands, their exit codes, `git status` and `git diff --stat`. Say what you did not run.
