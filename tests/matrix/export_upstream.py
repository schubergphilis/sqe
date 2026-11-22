#!/usr/bin/env python3
"""Export SQE's claimed matrix state in the upstream engine-file shape.

Reads docs/evidence/iceberg-matrix-state.json and writes
{"platforms": [...], "support": {"<platform>:<feature>:<version>": {...}}},
the shape of the per-engine files in the public iceberg-matrix project.

  export_upstream.py [--out FILE]   write the engine file (default: stdout)
  export_upstream.py --check        verify the export against the rubric and the state

--check asserts that every rubric cell (docs/evidence/iceberg-matrix-rubric.json) has
exactly one exported entry, that its level equals the state file's level, that no
exported entry is outside the rubric, and that the score computed with the rubric
weights (full 3, partial 2, none 0) equals the total the state file reports.
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "docs/evidence/iceberg-matrix-state.json"
RUBRIC = ROOT / "docs/evidence/iceberg-matrix-rubric.json"
WEIGHT = {"full": 3, "partial": 2, "unknown": 1, "none": 0}
KEEP = ("level", "notes", "caveats", "links")


def rubric_cells(rubric):
    cells = []
    for f in rubric["features"]:
        for v in (2, 3):
            if v < int(str(f["introducedIn"]).lstrip("v")):
                continue
            if f.get("removedIn") and v >= int(str(f["removedIn"]).lstrip("v")):
                continue
            cells.append(f"{f['id']}:v{v}")
    return cells


def export(state):
    platform = state["platform"]
    pid = platform["id"]
    support = {k: {x: v[x] for x in KEEP if x in v} for k, v in state["support"].items()}
    return {"platforms": [platform], "support": support}, pid


def check(state, rubric, exported, pid):
    problems = []
    cells = rubric_cells(rubric)
    if rubric.get("cells") is not None and rubric["cells"] != len(cells):
        problems.append(f"rubric says {rubric['cells']} cells, computed {len(cells)}")
    keys = list(exported["support"])
    if len(keys) != len(set(keys)):
        problems.append("duplicate exported keys")
    score = 0
    for c in cells:
        key = f"{pid}:{c}"
        n = keys.count(key)
        if n != 1:
            problems.append(f"{key}: {n} exported entries, expected exactly 1")
            continue
        level = exported["support"][key]["level"]
        want = state["support"].get(key, {}).get("level")
        if level != want:
            problems.append(f"{key}: exported {level!r}, state file {want!r}")
        if level not in WEIGHT:
            problems.append(f"{key}: unknown level {level!r}")
        score += WEIGHT.get(level, 0)
    extra = sorted(set(keys) - {f"{pid}:{c}" for c in cells})
    if extra:
        problems.append(f"exported entries outside the rubric: {extra}")
    total = state["score"]
    if isinstance(total, dict):
        if score != total["raw"]:
            problems.append(f"score {score}, state file says {total['raw']}")
        if 3 * len(cells) != total["max"]:
            problems.append(f"max {3 * len(cells)}, state file says {total['max']}")
    return problems, score, 3 * len(cells), len(cells)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", help="write the engine file here instead of stdout")
    ap.add_argument("--check", action="store_true", help="verify the export; exit 1 on a mismatch")
    args = ap.parse_args()
    state = json.loads(STATE.read_text())
    rubric = json.loads(RUBRIC.read_text())
    exported, pid = export(state)
    if args.check:
        problems, score, mx, n = check(state, rubric, exported, pid)
        for p in problems:
            print(f"MISMATCH {p}")
        print(f"{n} rubric cells, score {score} of {mx}, {len(problems)} problems")
        sys.exit(1 if problems else 0)
    text = json.dumps(exported, indent=2, ensure_ascii=False) + "\n"
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text)
        print(f"wrote {args.out}: {len(exported['support'])} cells")
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()
