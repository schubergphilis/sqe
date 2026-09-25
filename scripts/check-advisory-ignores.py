#!/usr/bin/env python3
"""Every accepted advisory must carry an owner and a review date.

Scans the ignore lists of deny.toml, .cargo/audit.toml and .grype.yaml. Each
line that names an ignored RUSTSEC-/CVE-/GHSA- id must end in a comment of
the form

    # review-by YYYY-MM-DD owner=<name>

Fails when the comment is missing or the date has passed, so an accepted risk
comes back up for a decision instead of staying ignored forever. Stdlib only:
runs in ci-config-sanity's python:alpine image.
"""
import datetime
import re
import sys

FILES = ["deny.toml", ".cargo/audit.toml", ".grype.yaml"]
ID = re.compile(r"(RUSTSEC-\d{4}-\d{4}|CVE-\d{4}-\d+|GHSA-[\w-]+)")
TAG = re.compile(r"#\s*review-by\s+(\d{4}-\d{2}-\d{2})\s+owner=(\S+)")


def entry_lines(text):
    """(lineno, line, id) for lines that ARE ignore entries, not comments about them."""
    for n, line in enumerate(text.splitlines(), 1):
        m = ID.search(line.split("#", 1)[0])
        if m:
            yield n, line, m.group(1)


def check(path, text, today):
    errors = []
    for n, line, adv in entry_lines(text):
        tag = TAG.search(line)
        if not tag:
            errors.append(f"{path}:{n}: {adv} has no '# review-by YYYY-MM-DD owner=<name>'")
        elif datetime.date.fromisoformat(tag.group(1)) < today:
            errors.append(f"{path}:{n}: {adv} review date {tag.group(1)} has passed (owner {tag.group(2)})")
    return errors


def main():
    today = datetime.date.today()
    errors = []
    for path in FILES:
        try:
            with open(path) as f:
                errors += check(path, f.read(), today)
        except FileNotFoundError:
            pass
    for e in errors:
        print(e)
    if errors:
        print("Re-assess each advisory: fix it, or extend the date with the reason updated.")
        return 1
    print("==> advisory ignores ok")
    return 0


def _selftest():
    d = datetime.date(2026, 9, 25)
    ok = '    "RUSTSEC-2024-0436",  # review-by 2026-12-25 owner=sqe-maintainers\n'
    assert check("x", ok, d) == []
    assert check("x", ok, datetime.date(2027, 1, 1))  # expired
    assert check("x", '    "RUSTSEC-2024-0436",\n', d)  # untagged
    assert check("x", "# RUSTSEC-2023-0071 is NOT listed here\n", d) == []  # prose
    assert check("x", "  - vulnerability: CVE-2026-19499  # review-by 2026-10-06 owner=a\n", d) == []


if __name__ == "__main__":
    _selftest()
    sys.exit(main())
