#!/usr/bin/env python3
"""Fold changelog.d/ fragments into the [Unreleased] section of CHANGELOG.md.

Every merge request writes its entry to changelog.d/<branch-slug>.md instead of
CHANGELOG.md, so two open merge requests never edit the same lines. A release
(scripts/release.sh) runs this to move the fragments into CHANGELOG.md and
delete them. See changelog.d/README.md for the fragment format.

    scripts/changelog-collect.py           # fold and delete the fragments
    scripts/changelog-collect.py --check   # validate only, change nothing
"""

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FRAGMENTS = ROOT / "changelog.d"
CHANGELOG = ROOT / "CHANGELOG.md"
SECTIONS = ["Added", "Changed", "Deprecated", "Removed", "Fixed", "Security"]
HEADING = re.compile(r"^### (.+?)\s*$")


def parse_fragment(path):
    """Return {section: entry text} for one fragment, or raise ValueError."""
    entries, section, lines = {}, None, []

    def flush():
        if section is None:
            return
        text = "\n".join(lines).strip("\n")
        if not text:
            raise ValueError(f"{path.name}: '### {section}' has no entry")
        entries[section] = text

    for line in path.read_text().splitlines():
        m = HEADING.match(line)
        if m:
            flush()
            section, lines = m.group(1), []
            if section not in SECTIONS:
                raise ValueError(
                    f"{path.name}: unknown section '### {section}' "
                    f"(use one of {', '.join(SECTIONS)})"
                )
            if section in entries:
                raise ValueError(f"{path.name}: '### {section}' appears twice")
        elif section is None:
            if line.strip():
                raise ValueError(f"{path.name}: text before the first '### ' heading")
        else:
            lines.append(line)
    flush()
    if not entries:
        raise ValueError(f"{path.name}: no '### <Section>' heading")
    return entries


def fragments():
    return sorted(p for p in FRAGMENTS.glob("*.md") if p.name != "README.md")


def fold(changelog, collected):
    """Insert the collected entries at the top of each [Unreleased] section."""
    lines = changelog.splitlines()
    try:
        start = lines.index("## [Unreleased]")
    except ValueError:
        sys.exit("error: CHANGELOG.md has no '## [Unreleased]' heading")
    end = next(
        (i for i in range(start + 1, len(lines)) if lines[i].startswith("## [")),
        len(lines),
    )
    body = lines[start + 1 : end]

    # Split the existing [Unreleased] body into its ### sections.
    existing, order, current = {}, [], None
    for line in body:
        m = HEADING.match(line)
        if m:
            current = m.group(1)
            order.append(current)
            existing[current] = []
        elif current is not None:
            existing[current].append(line)

    for name in collected:
        if name not in existing:
            existing[name] = []
            order.append(name)
    order.sort(key=lambda n: SECTIONS.index(n) if n in SECTIONS else len(SECTIONS))

    out = [""]
    for name in order:
        old = "\n".join(existing[name]).strip("\n")
        parts = collected.get(name, []) + ([old] if old else [])
        out.append(f"### {name}")
        out.append("")
        if parts:
            out.append("\n\n".join(parts))
            out.append("")
    return "\n".join(lines[: start + 1] + out + lines[end:]) + "\n"


def main():
    ap = argparse.ArgumentParser(description="Fold changelog.d/ fragments into CHANGELOG.md.")
    ap.add_argument("--check", action="store_true", help="validate only")
    args = ap.parse_args()

    collected, errors = {}, []
    paths = fragments()
    for path in paths:
        try:
            for section, text in parse_fragment(path).items():
                collected.setdefault(section, []).append(text)
        except ValueError as e:
            errors.append(str(e))
    if errors:
        sys.exit("error: " + "\nerror: ".join(errors))
    if args.check:
        print(f"changelog.d: {len(paths)} fragment(s) ok")
        return
    if not paths:
        print("changelog.d: no fragments")
        return

    CHANGELOG.write_text(fold(CHANGELOG.read_text(), collected))
    for path in paths:
        path.unlink()
    print(f"changelog.d: folded {len(paths)} fragment(s) into CHANGELOG.md")


if __name__ == "__main__":
    main()
