#!/usr/bin/env python3
"""Regenerate docs/iceberg-matrix.md from docs/iceberg-matrix-state.json.

Run: python3 scripts/render-iceberg-matrix.py

The state file is the source of truth. This script renders a readable
markdown view grouped by rubric category, with caveats pulled out.
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / "docs" / "iceberg-matrix-state.json"
OUT = ROOT / "docs" / "iceberg-matrix.md"

SYM = {"full": "F", "partial": "P", "unknown": "?", "none": "."}

CATEGORIES = [
    ("Row-level operations", [
        ("position-deletes", "Position Deletes"),
        ("equality-deletes", "Equality Deletes"),
        ("merge-on-read", "Merge-on-Read"),
        ("copy-on-write", "Copy-on-Write"),
    ]),
    ("Table management", [
        ("schema-evolution", "Schema Evolution"),
        ("type-promotion", "Type Promotion / Widening"),
        ("column-default-values", "Column Default Values"),
        ("table-creation", "Table Creation"),
        ("time-travel", "Time Travel / Snapshots"),
        ("table-maintenance", "Table Maintenance"),
        ("branching-tagging", "Branching & Tagging"),
    ]),
    ("Partitioning", [
        ("hidden-partitioning", "Hidden Partitioning"),
        ("partition-evolution", "Partition Evolution"),
        ("multi-arg-transforms", "Multi-Argument Transforms"),
    ]),
    ("Read / write", [
        ("read-support", "Read Support"),
        ("write-insert", "Write (INSERT)"),
        ("write-merge-update-delete", "Write (MERGE/UPDATE/DELETE)"),
        ("catalog-integration", "Catalog Integration"),
        ("statistics", "Statistics (Column Metrics)"),
        ("bloom-filters", "Bloom Filters & Puffin"),
    ]),
    ("Catalog support", [
        ("hive-metastore", "Hive Metastore"),
        ("aws-glue-catalog", "AWS Glue Catalog"),
        ("rest-catalog", "REST Catalog"),
        ("nessie", "Nessie"),
        ("polaris", "Polaris"),
        ("unity-catalog", "Unity Catalog"),
        ("snowflake-horizon-catalog", "Snowflake Horizon"),
        ("hadoop-catalog", "Hadoop Catalog"),
        ("jdbc-catalog", "JDBC Catalog"),
    ]),
    ("V3 data types", [
        ("variant-type", "Variant"),
        ("shredded-variant", "Shredded Variant"),
        ("geometry-type", "Geometry"),
        ("vector-type", "Vector / Embedding"),
        ("nanosecond-timestamps", "Nanosecond Timestamps"),
    ]),
    ("V3 advanced", [
        ("cdc-support", "Change Data Capture"),
        ("lineage", "Lineage Tracking"),
    ]),
]


def main() -> None:
    d = json.loads(STATE.read_text())
    score = d["score"]
    support = d["support"]

    lines: list[str] = []
    add = lines.append

    add("# SQE Iceberg Compatibility Matrix")
    add("")
    add("Current state of SQE against the [icebergmatrix.org](https://icebergmatrix.org) rubric, "
        "the de-facto reference engineers consult when picking an Iceberg engine. Data lives at "
        "[Neuw84/iceberg-matrix](https://github.com/Neuw84/iceberg-matrix).")
    add("")
    add(f"**Score: {score['raw']}/{score['max']} ({score['percent']}%)**  |  **Target: 156/189 (83%)**")
    add("")
    add(f"Last generated: {d.get('generatedAt', 'n/a')}  |  Source: `{d.get('generatedBy', 'manual')}`")
    add("")
    add("Regenerate: `python3 scripts/render-iceberg-matrix.py`. Source of truth: `docs/iceberg-matrix-state.json`.")
    add("")
    add("---")
    add("")
    add("## Legend")
    add("")
    add("| Symbol | Level | Meaning |")
    add("|:---:|---|---|")
    add("| F | full | Verified end-to-end; no significant limitations |")
    add("| P | partial | Some functionality works; caveats apply |")
    add("| ? | unknown | Library primitives exist; no end-to-end verification |")
    add("| . | none | Not implemented; planned or deferred |")
    add("")
    add("Each feature is scored against V2 and V3 of the Iceberg spec (63 cells total). "
        "Aggregate score weights: F=3, P=2, ?=1, .=0. Max 189.")
    add("")
    add("---")
    add("")
    add("## Peer rankings")
    add("")
    add("| Engine | Score | % |")
    add("|---|---:|---:|")
    add("| AWS EMR (Spark 7.12) | 180/189 | 95 |")
    add("| OSS Spark 4.1 | 175/189 | 93 |")
    add("| OSS Flink 2.2 | 153/189 | 81 |")
    add("| Snowflake | 134/189 | 71 |")
    add("| PyIceberg 0.11 | 130/189 | 69 |")
    add("| Databricks DBR 17.3 | 103/189 | 54 |")
    add(f"| **SQE (current)** | **{score['raw']}/{score['max']}** | **{score['percent']}** |")
    add("| DuckDB 1.5 | 85/189 | 45 |")
    add("| Daft | 77/189 | 41 |")
    add("| Athena v3 | 59/189 | 31 |")
    add("| ClickHouse 26.1 | 46/189 | 24 |")
    add("")
    add("Peer scores from icebergmatrix.org as of 2026-04-24.")
    add("")
    add("---")
    add("")
    add("## Feature matrix")
    add("")
    for cat_name, features in CATEGORIES:
        add(f"### {cat_name}")
        add("")
        add("| Feature | V2 | V3 | V2 notes | V3 notes |")
        add("|---|:---:|:---:|---|---|")
        for fid, label in features:
            v2 = support.get(f"sqe:{fid}:v2")
            v3 = support.get(f"sqe:{fid}:v3")
            if v2 is None and v3 is not None:
                v2_sym = "n/a"
                v2_note = "V3-only feature"
            elif v2 is not None:
                v2_sym = SYM[v2["level"]]
                v2_note = (v2.get("notes") or "")[:140]
            else:
                v2_sym = "-"
                v2_note = ""
            if v3 is not None:
                v3_sym = SYM[v3["level"]]
                v3_note = (v3.get("notes") or "")[:140]
            else:
                v3_sym = "-"
                v3_note = ""
            add(f"| {label} | {v2_sym} | {v3_sym} | {v2_note} | {v3_note} |")
        add("")
    add("---")
    add("")
    add("## Caveats")
    add("")
    add("Cells marked `partial` or `unknown` have specific gaps documented in "
        "`docs/iceberg-matrix-state.json` under `caveats`. Key ones:")
    add("")
    for key, v in support.items():
        if v["level"] in ("partial", "unknown") and v.get("caveats"):
            _, fid, ver = key.split(":")
            for cv in v["caveats"][:3]:
                add(f"- **{fid} ({ver})**: {cv}")
    add("")
    add("---")
    add("")
    add("## SQE differentiation")
    add("")
    add("Not captured in the rubric but material to picking SQE:")
    add("")
    add("- **OIDC bearer-token passthrough.** Every query runs as the authenticated user. "
        "No service account. No engine on the matrix offers this.")
    add("- **Full SQL DML via CoW `rewrite_files()`.** DuckDB has MoR-only writes, no MERGE. "
        "SQE has all three operations in both CoW and MoR modes.")
    add("- **Arrow Flight SQL primary + Trino HTTP compat.** Matches the protocol surface of "
        "Spark and Flink without a JVM.")
    add("- **Benchmarks vs Trino 465.** 5 of 7 suites faster at SF1. Latest SF0.1 run: 2.3x "
        "faster across TPC-H, TPC-DS, SSB, ClickBench (177/177 SQE pass, 170/177 byte-match "
        "Trino). See `benchmarks/results/*2026-04-24*.json`.")
    add("- **Security audit.** 43 of 43 findings resolved before OSS release.")
    add("")
    add("---")
    add("")
    add("## Contributing")
    add("")
    add("1. Make the change in code.")
    add("2. Update the matching entry in `docs/iceberg-matrix-state.json`.")
    add("3. Run `cargo xtask matrix-report` to verify the aggregate score.")
    add("4. Raise `MATRIX_MIN_PERCENT` in `.gitlab-ci.yml` if the new score clears the next "
        "1% threshold.")
    add("5. Regenerate this file: `python3 scripts/render-iceberg-matrix.py`.")
    add("")
    add("For the public matrix submission workflow see "
        "`openspec/changes/iceberg-matrix-parity/tasks.md` section 2.22 and beyond.")
    add("")
    add("## See also")
    add("")
    add("- [Full openspec change](../openspec/changes/iceberg-matrix-parity/proposal.md) "
        "with proposal, design, 8 spec files, and tasks")
    add("- [Source roadmap](./superpowers/plans/2026-04-24-iceberg-matrix-parity.md) "
        "with upstream research and deferral rationale")
    add("- [Matrix parity workflow](./matrix-parity-workflow.md) for per-phase branching "
        "conventions")
    add("- [Tracking issue body](./matrix-parity-tracking-issue.md)")

    OUT.write_text("\n".join(lines) + "\n")
    print(f"wrote {OUT} ({len(lines)} lines)")


if __name__ == "__main__":
    main()
