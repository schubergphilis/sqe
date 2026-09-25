#!/usr/bin/env python3
"""Weekly supply-chain watch. Fails (a red scheduled pipeline) when:

1. A vendored crate has an advisory nobody reviewed. cargo audit and cargo
   deny match registry crates only; the path crates under vendor/ (iceberg*,
   datafusion-iceberg) are invisible to both. Every crate name found under
   vendor/ is looked up on OSV (crates.io ecosystem, all versions: a vendored
   fork carries upstream's bugs whatever its version string says). A reviewed
   advisory goes into vendor/reviewed-advisories.txt with the usual
   `# review-by YYYY-MM-DD owner=<name>` tag.
2. A Renovate MR has been open longer than MAX_MR_AGE_DAYS: a dependency
   update, possibly a security fix, is stuck. Needs GITLAB_TOKEN (read_api);
   skipped with a notice when unset, e.g. locally.

Stdlib only, so it runs in a bare python image.
"""
import datetime
import json
import os
import pathlib
import sys
import tomllib
import urllib.request

MAX_MR_AGE_DAYS = 14
REVIEWED = pathlib.Path("vendor/reviewed-advisories.txt")


def vendored_crates(root=pathlib.Path("vendor")):
    names = set()
    for manifest in root.rglob("Cargo.toml"):
        if "target" in manifest.parts:
            continue
        pkg = tomllib.loads(manifest.read_text()).get("package", {})
        if isinstance(pkg.get("name"), str):
            names.add(pkg["name"])
    return sorted(names)


def reviewed_ids(path=REVIEWED):
    if not path.exists():
        return set()
    return {line.split("#", 1)[0].strip() for line in path.read_text().splitlines()} - {""}


def osv_vulns(names):
    body = {"queries": [{"package": {"name": n, "ecosystem": "crates.io"}} for n in names]}
    req = urllib.request.Request(
        "https://api.osv.dev/v1/querybatch",
        json.dumps(body).encode(),
        {"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        results = json.load(r)["results"]
    return {n: [v["id"] for v in res.get("vulns", [])] for n, res in zip(names, results)}


def unreviewed(vulns, reviewed):
    return {n: [i for i in ids if i not in reviewed] for n, ids in vulns.items() if set(ids) - reviewed}


def stale_renovate_mrs(mrs, now, max_days=MAX_MR_AGE_DAYS):
    out = []
    for mr in mrs:
        if not mr["source_branch"].startswith("renovate/"):
            continue
        created = datetime.datetime.fromisoformat(mr["created_at"].replace("Z", "+00:00"))
        age = (now - created).days
        if age > max_days:
            out.append((mr["iid"], mr["title"], age))
    return out


def gitlab_open_mrs(token):
    api = os.environ.get("CI_API_V4_URL", "")
    project = os.environ.get("CI_PROJECT_ID", "")
    req = urllib.request.Request(
        f"{api}/projects/{project}/merge_requests?state=opened&per_page=100",
        headers={"PRIVATE-TOKEN": token},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def main():
    failed = False

    names = vendored_crates()
    bad = unreviewed(osv_vulns(names), reviewed_ids())
    for name, ids in bad.items():
        print(f"vendored crate {name}: unreviewed advisories {', '.join(ids)}")
    failed |= bool(bad)
    print(f"==> {len(names)} vendored crates checked on OSV, {len(bad)} with unreviewed advisories")

    token = os.environ.get("GITLAB_TOKEN", "")
    if token:
        now = datetime.datetime.now(datetime.timezone.utc)
        stale = stale_renovate_mrs(gitlab_open_mrs(token), now)
        for iid, title, age in stale:
            print(f"!{iid} open {age} days: {title}")
        failed |= bool(stale)
        print(f"==> {len(stale)} Renovate MRs older than {MAX_MR_AGE_DAYS} days")
    else:
        print("GITLAB_TOKEN unset: Renovate MR age not checked")

    return 1 if failed else 0


def _selftest():
    assert unreviewed({"iceberg": ["GHSA-1", "RUSTSEC-2"]}, {"GHSA-1"}) == {"iceberg": ["RUSTSEC-2"]}
    assert unreviewed({"iceberg": ["GHSA-1"]}, {"GHSA-1"}) == {}
    now = datetime.datetime(2026, 9, 25, tzinfo=datetime.timezone.utc)
    mrs = [
        {"iid": 1, "title": "old", "source_branch": "renovate/x", "created_at": "2026-09-01T00:00:00Z"},
        {"iid": 2, "title": "new", "source_branch": "renovate/y", "created_at": "2026-09-20T00:00:00Z"},
        {"iid": 3, "title": "human", "source_branch": "feat/z", "created_at": "2026-01-01T00:00:00Z"},
    ]
    assert stale_renovate_mrs(mrs, now) == [(1, "old", 24)]


if __name__ == "__main__":
    _selftest()
    sys.exit(main())
