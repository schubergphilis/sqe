#!/usr/bin/env python3
"""Monthly vulnerability review: file one dated issue that a maintainer works
through and closes as the record of the review.

The issue lists what needs a decision this month, built only from data this
repository controls: open `aikido` triage issues (as links, never their
titles, which carry scanner text), every accepted advisory with its owner and
review date (those due within 30 days flagged), and Renovate MRs stuck past
14 days. It ends in a checklist and a "reviewed by" line.

Idempotent: if this month's issue exists it does nothing. Needs GITLAB_TOKEN
(api), CI_API_V4_URL, CI_PROJECT_ID; prints the body and stops without them.
Stdlib only.
"""
import datetime
import importlib.util
import json
import os
import pathlib
import sys
import urllib.parse
import urllib.request

HERE = pathlib.Path(__file__).resolve().parent
DUE_SOON_DAYS = 30


def _load(name, file):
    spec = importlib.util.spec_from_file_location(name, HERE / file)
    assert spec and spec.loader, f"cannot load {file}"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ignores = _load("check_advisory_ignores", "check-advisory-ignores.py")
watch = _load("supply_chain_watch", "supply-chain-watch.py")


def accepted_advisories():
    rows = []
    for path in ignores.FILES:
        p = pathlib.Path(path)
        if not p.exists():
            continue
        for _n, line, adv in ignores.entry_lines(p.read_text()):
            tag = ignores.TAG.search(line)
            rows.append((adv, path, tag.group(1) if tag else "MISSING", tag.group(2) if tag else "-"))
    return rows


def body(month, today, advisories, triage_iids, stale):
    soon = today + datetime.timedelta(days=DUE_SOON_DAYS)
    out = [f"Monthly vulnerability review for **{month}**. Work through the list, "
           "record decisions in the linked issues or MRs, then close this issue.", ""]
    out.append(f"## Open triage issues ({len(triage_iids)})")
    out += [f"- #{i}" for i in triage_iids] or ["- none"]
    out += ["", f"## Accepted advisories ({len(advisories)})", "",
            "| Advisory | File | Review by | Owner |", "|---|---|---|---|"]
    for adv, path, date, owner in sorted(advisories, key=lambda r: r[2]):
        due = date != "MISSING" and datetime.date.fromisoformat(date) <= soon
        out.append(f"| {adv} | `{path}` | {'**' + date + '** (due)' if due else date} | {owner} |")
    out += ["", f"## Renovate MRs open over {watch.MAX_MR_AGE_DAYS} days ({len(stale)})"]
    out += [f"- !{iid} ({age} days)" for iid, _t, age in stale] or ["- none"]
    out += ["", "## Checklist", "",
            "- [ ] Every open triage issue has an owner and a target date",
            "- [ ] Every advisory due within 30 days is fixed or re-accepted with a new date",
            "- [ ] Stuck Renovate MRs are merged, closed, or have a reason",
            "- [ ] Private vulnerability reports received this month are acknowledged (SECURITY.md targets)",
            "", "Reviewed by: @", ""]
    return "\n".join(out)


def api(token, method, path, data=None):
    url = f"{os.environ['CI_API_V4_URL']}/projects/{os.environ['CI_PROJECT_ID']}{path}"
    req = urllib.request.Request(url, method=method, headers={"PRIVATE-TOKEN": token})
    if data is not None:
        req.data = urllib.parse.urlencode(data).encode()
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def main():
    today = datetime.date.today()
    month = today.strftime("%Y-%m")
    title = f"Vulnerability review {month}"
    token = os.environ.get("GITLAB_TOKEN", "")
    if not token:
        print(body(month, today, accepted_advisories(), [], []))
        print("GITLAB_TOKEN unset: printed only")
        return 0
    existing = api(token, "GET", "/issues?" + urllib.parse.urlencode({"search": title, "in": "title"}))
    if any(i["title"] == title for i in existing):
        print(f"{title}: already filed")
        return 0
    triage = [i["iid"] for i in api(token, "GET", "/issues?state=opened&labels=aikido&per_page=100")]
    now = datetime.datetime.now(datetime.timezone.utc)
    stale = watch.stale_renovate_mrs(watch.gitlab_open_mrs(token), now)
    issue = api(token, "POST", "/issues", {
        "title": title,
        "labels": "security,vulnerability-review",
        "description": body(month, today, accepted_advisories(), sorted(triage), stale),
    })
    print(f"filed #{issue['iid']}: {title}")
    return 0


def _selftest():
    b = body("2026-09", datetime.date(2026, 9, 25),
             [("RUSTSEC-1", "deny.toml", "2026-10-01", "o"), ("RUSTSEC-2", "deny.toml", "2027-01-01", "o")],
             [7], [(3, "x", 20)])
    assert "**2026-10-01** (due)" in b and "| 2027-01-01 |" in b
    assert "- #7" in b and "- !3 (20 days)" in b and "Reviewed by: @" in b


if __name__ == "__main__":
    _selftest()
    sys.exit(main())
