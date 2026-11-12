#!/usr/bin/env python3
"""Tag `vX.Y.Z` on main when a `release/vX.Y.Z` merge request merges.

Runs in the main pipeline (`sqe-tag-release` in .gitlab-ci.yml). The release
branch already carries the version bump, the Cargo.lock refresh, the Helm chart
and the folded changelog (`scripts/release.sh` on that branch), so this job
only creates the tag, on the merge commit that is actually on main. The tag
pipeline then builds and publishes the release image.

Refuses, with a non-zero exit, when the workspace version on main does not
match the branch name, or when the tag already exists on another commit. An
existing tag on this same commit is a re-run and passes.

Needs AIKIDO_GROUP_TOKEN (api scope, allowed to create protected `v*` tags).
`--dry-run` prints the plan without the token or any write.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

# GitLab's default merge commit message: "Merge branch 'release/v0.54.0' into 'main'".
MERGE_MESSAGE = re.compile(r"^Merge branch 'release/v(\d+\.\d+\.\d+)' into '[^']+'", re.M)


def release_version(message: str) -> str | None:
    """The version named by a release-branch merge commit, or None."""
    m = MERGE_MESSAGE.search(message)
    return m.group(1) if m else None


def workspace_version(cargo_toml: str) -> str | None:
    """`version` under `[workspace.package]` in the root manifest."""
    in_section = False
    for line in cargo_toml.splitlines():
        stripped = line.strip()
        if stripped.startswith("["):
            in_section = stripped == "[workspace.package]"
            continue
        if in_section:
            m = re.match(r'version\s*=\s*"([^"]+)"', stripped)
            if m:
                return m.group(1)
    return None


def _request(method: str, url: str, token: str, data: dict | None = None):
    body = urllib.parse.urlencode(data).encode() if data else None
    req = urllib.request.Request(url, data=body, method=method, headers={"PRIVATE-TOKEN": token})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--root", default=os.environ.get("CI_PROJECT_DIR", "."))
    args = ap.parse_args(argv)

    message = os.environ.get("CI_COMMIT_MESSAGE", "")
    want = release_version(message)
    if want is None:
        print("[tag-release] not a release/vX.Y.Z merge; nothing to tag")
        return 0

    with open(os.path.join(args.root, "Cargo.toml"), encoding="utf-8") as f:
        have = workspace_version(f.read())
    if have != want:
        print(
            f"[tag-release] ERROR: branch says {want} but [workspace.package] version is "
            f"{have}. Run scripts/release.sh {want} on the release branch before merging.",
            file=sys.stderr,
        )
        return 1

    tag = f"v{want}"
    sha = os.environ["CI_COMMIT_SHA"]
    if args.dry_run:
        print(f"[tag-release] DRY-RUN: would create {tag} at {sha}")
        return 0

    token = os.environ.get("AIKIDO_GROUP_TOKEN", "")
    if not token:
        print("[tag-release] ERROR: AIKIDO_GROUP_TOKEN is not set", file=sys.stderr)
        return 1
    tags_url = f"{os.environ['CI_API_V4_URL']}/projects/{os.environ['CI_PROJECT_ID']}/repository/tags"

    try:
        existing = _request("GET", f"{tags_url}/{urllib.parse.quote(tag, safe='')}", token)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
        existing = None
    if existing is not None:
        target = existing.get("commit", {}).get("id")
        if target == sha:
            print(f"[tag-release] {tag} already points at {sha}; nothing to do")
            return 0
        print(f"[tag-release] ERROR: {tag} already exists at {target}, not {sha}", file=sys.stderr)
        return 1

    try:
        _request("POST", tags_url, token, {"tag_name": tag, "ref": sha, "message": f"Release {tag}"})
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace")[:500]
        print(f"[tag-release] ERROR: creating {tag} failed: HTTP {e.code} {detail}", file=sys.stderr)
        return 1
    print(f"[tag-release] created {tag} at {sha}; the tag pipeline builds and publishes the release")
    return 0


if __name__ == "__main__":
    sys.exit(main())
