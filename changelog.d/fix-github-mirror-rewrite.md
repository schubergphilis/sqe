### Security

- The GitHub mirror no longer carries the internal GitLab host or group path in commit messages

    `make mirror-github` (dry run) and `make mirror-github PUSH=1` replace the
    manual `git push github main`. `scripts/mirror-github.sh` fetches main and
    the tags from origin into a scratch repository and rewrites only the
    commit and tag messages with git filter-repo: merge-from-URL lines, "See
    merge request <group path>!N" trailers, other internal URLs, the group path
    and the company abbreviation. Files and authors are unchanged, and the
    script checks that the rewritten tip tree equals origin's. The rewrite is
    deterministic, so later pushes are fast-forwards. Signatures are dropped on
    the mirror. The host and group path come from the origin URL, so the script
    does not name them.
