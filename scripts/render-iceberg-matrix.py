#!/usr/bin/env python3
"""Regenerate docs/internal/design-archive/iceberg-matrix.md from docs/evidence/iceberg-matrix-state.json.

Run: python3 scripts/render-iceberg-matrix.py

The state file is the source of truth for SQE's cells. The rubric (features,
categories, which format versions each feature is scored on) and the peer
scores come from docs/evidence/iceberg-matrix-rubric.json, a pinned snapshot
of the upstream matrix written by scripts/render-iceberg-matrix-compare.py.
Both are local files, so this script runs offline.
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / "docs" / "evidence" / "iceberg-matrix-state.json"
RUBRIC = ROOT / "docs" / "evidence" / "iceberg-matrix-rubric.json"
SITE = ROOT / "docs" / "site" / "web" / "iceberg.json"
OUT = ROOT / "docs" / "internal" / "design-archive" / "iceberg-matrix.md"

VERSIONS = ["v2", "v3"]
SYM = {"full": "F", "partial": "P", "unknown": "?", "none": "."}
WEIGHT = {"full": 3, "partial": 2, "unknown": 1, "none": 0}
TOP_PEERS = 12
NOTE_CHARS = 160

CATEGORY_NAMES = {
    "row-level-operations": "Row-level operations",
    "table-management": "Table management",
    "partitioning": "Partitioning",
    "read-write": "Read / write",
    "catalog-support": "Catalog support",
    "v3-data-types": "V3 data types",
}


def scored_versions(feat: dict) -> list[str]:
    lo = VERSIONS.index(feat["introducedIn"])
    hi = VERSIONS.index(feat["removedIn"]) if feat.get("removedIn") else len(VERSIONS)
    return VERSIONS[lo:hi]


def short(text: str) -> str:
    text = " ".join((text or "").split()).replace("|", "\\|")
    if len(text) <= NOTE_CHARS:
        return text
    return text[:NOTE_CHARS].rsplit(" ", 1)[0] + " ..."


def main() -> None:
    d = json.loads(STATE.read_text())
    rubric = json.loads(RUBRIC.read_text())
    support = d["support"]
    features = rubric["features"]

    cells = [(f["id"], v) for f in features for v in scored_versions(f)]
    keys = {f"sqe:{fid}:{v}" for fid, v in cells}
    if keys != set(support):
        raise SystemExit(
            "state file and rubric snapshot disagree on the scored cells: "
            f"missing {sorted(keys - set(support))}, extra {sorted(set(support) - keys)}")
    raw = sum(WEIGHT[support[k]["level"]] for k in keys)
    max_score = 3 * len(cells)
    score = d["score"]
    if (score["raw"], score["max"]) != (raw, max_score):
        raise SystemExit(f"declared score {score['raw']}/{score['max']} != computed {raw}/{max_score}")
    pct = 100.0 * raw / max_score

    lines: list[str] = []
    add = lines.append

    add("# SQE Iceberg Compatibility Matrix")
    add("")
    add("SQE rated against the [icebergmatrix.org](https://icebergmatrix.org) rubric "
        "([Neuw84/iceberg-matrix](https://github.com/Neuw84/iceberg-matrix)).")
    add("")
    add(f"**Score: {raw}/{max_score} ({pct:.1f}%)** on {len(cells)} cells.")
    add("")
    add(f"- SQE cells: `docs/evidence/iceberg-matrix-state.json`, last changed {d.get('generatedAt', 'n/a')[:10]}.")
    add(f"- Rubric and peer scores: `docs/evidence/iceberg-matrix-rubric.json`, upstream "
        f"`{rubric['sourceCommit'][:10]}` fetched {rubric['fetchedAt']}.")
    add("- Regenerate: `python3 scripts/render-iceberg-matrix-compare.py` (refreshes the "
        "rubric snapshot and [the engine comparison](./iceberg-matrix-compare.md)), then "
        "`python3 scripts/render-iceberg-matrix.py`.")
    add("")
    add("---")
    add("")
    add("## Legend")
    add("")
    add("| Symbol | Level | Weight | Meaning |")
    add("|:---:|---|---:|---|")
    add("| F | full | 3 | Verified end-to-end; no significant limitations |")
    add("| P | partial | 2 | Some functionality works; caveats apply |")
    add("| ? | unknown | 1 | Library primitives exist; no end-to-end verification |")
    add("| . | none | 0 | Not implemented; planned or deferred |")
    add("| n/a | | | The rubric does not score this format version |")
    add("")
    add("A feature is scored on each format version from `introducedIn` up to, not "
        "including, `removedIn`.")
    add("")
    add("---")
    add("")
    add("## Peer rankings")
    add("")
    add(f"Every engine scored on the same {len(cells)} cells. SQE rates itself; the "
        "other engines are rated in the upstream repo. Top "
        f"{TOP_PEERS} of {len(rubric['peers'])} upstream entries; the full list is in "
        "[the comparison](./iceberg-matrix-compare.md).")
    add("")
    add("| Engine | Score | % |")
    add("|---|---:|---:|")
    sqe_row = f"| **SQE (self-rated)** | **{raw}/{max_score}** | **{pct:.1f}** |"
    inserted = False
    for p in rubric["peers"][:TOP_PEERS]:
        if not inserted and raw >= p["raw"]:
            add(sqe_row)
            inserted = True
        add(f"| {p['name']} | {p['raw']}/{p['max']} | {p['percent']:.1f} |")
    if not inserted:
        add(sqe_row)
    add("")
    add("---")
    add("")
    add("## Feature matrix")
    add("")
    by_cat: dict[str, list[dict]] = {}
    for f in features:
        by_cat.setdefault(f["category"], []).append(f)
    for cat, feats in by_cat.items():
        add(f"### {CATEGORY_NAMES.get(cat, cat)}")
        add("")
        add("| Feature | V2 | V3 | V2 notes | V3 notes |")
        add("|---|:---:|:---:|---|---|")
        for f in feats:
            versions = scored_versions(f)
            syms, notes = [], []
            for v in VERSIONS:
                cell = support.get(f"sqe:{f['id']}:{v}")
                if v not in versions:
                    syms.append("n/a")
                    notes.append("V3-only feature" if v == "v2" else "Not scored on V3")
                else:
                    syms.append(SYM[cell["level"]])
                    notes.append(short(cell.get("notes")))
            add(f"| {f['name']} | {syms[0]} | {syms[1]} | {notes[0]} | {notes[1]} |")
        add("")
    add("---")
    add("")
    add("## Open cells")
    add("")
    add("Every cell below full, with its caveats. Each has a `[MATRIX]` issue.")
    add("")
    for fid, v in cells:
        cell = support[f"sqe:{fid}:{v}"]
        if cell["level"] == "full":
            continue
        add(f"- **{fid} ({v})**, {cell['level']}: {' '.join((cell.get('notes') or '').split())}")
        for cv in cell.get("caveats", []):
            add(f"  - {cv}")
    add("")
    add("## Caveats on full cells")
    add("")
    for fid, v in cells:
        cell = support[f"sqe:{fid}:{v}"]
        if cell["level"] == "full" and cell.get("caveats"):
            for cv in cell["caveats"]:
                add(f"- **{fid} ({v})**: {cv}")
    add("")
    if SITE.exists():
        site = json.loads(SITE.read_text())
        if site.get("outsideRubric"):
            add("---")
            add("")
            add("## Catalogs outside the rubric")
            add("")
            add(site.get("outsideRubricNote", ""))
            add("")
            add("| Catalog | Support | Testing |")
            add("|---|---|---|")
            for row in site["outsideRubric"]:
                add(f"| {row['catalog']} | {row['support']} | {row['testing']} |")
            add("")
            add("Source: `docs/site/web/iceberg.json`.")
            add("")
    add("---")
    add("")
    add("## SQE differentiation")
    add("")
    add("Not captured in the rubric but material to picking SQE:")
    add("")
    add("- **OIDC bearer-token passthrough.** Every query runs as the authenticated user, "
        "with no service account.")
    add("- **Fine-grained access control in the engine.** Row filters, column masks and "
        "column restriction are applied by rewriting the logical plan before optimization "
        "(Ranger, Polaris grants).")
    add("- **Full SQL DML.** DELETE, UPDATE and MERGE in both copy-on-write and "
        "merge-on-read modes.")
    add("- **Arrow Flight SQL first, Trino HTTP compatible.** No JVM.")
    add("- **Benchmarks.** See `README.md` and `benchmarks/results/` for the current numbers.")
    add("")
    add("---")
    add("")
    add("## Contributing")
    add("")
    add("1. Make the change in code.")
    add("2. Update the matching entry in `docs/evidence/iceberg-matrix-state.json` "
        "(level, notes, evidence, caveats, `score`, `generatedAt`).")
    add("3. Run `cargo xtask matrix-report` to check the declared score. CI runs it with "
        "`--min-percent` in `ci/aikido-rust-extra.sh`.")
    add("4. Regenerate this file: `python3 scripts/render-iceberg-matrix.py`.")
    add("")
    add("## See also")
    add("")
    add("- [Engine comparison](./iceberg-matrix-compare.md)")
    add("- [Openspec change](../../../openspec/changes/iceberg-matrix-parity/proposal.md) "
        "with proposal, design, specs and tasks (the upstream submission is section 2.22 on)")
    add("- [Source roadmap](../plans/2026-04-24-iceberg-matrix-parity.md) with upstream "
        "research and deferral rationale")
    add("- [Matrix parity workflow](./matrix-parity-workflow.md)")

    OUT.write_text("\n".join(lines) + "\n")
    print(f"wrote {OUT} ({len(lines)} lines)")


if __name__ == "__main__":
    main()
