#!/usr/bin/env python3
"""Mirror an AWS Glue Data Catalog into SQE Hive sidecar manifests.

Writes <root>/<database>/<table>/_sqe/table.json for every Glue table,
carrying Glue's own columns, partition keys, SerDe and projection
properties. The manifests live wherever you point --root; the data stays
in place, because a manifest's storage_descriptor.location is validated
against the engine's TVF allowlist rather than against the manifest root.

Usage:
  python3 glue2sidecar.py --root /tmp/gluemirror [--database NAME] [--limit N]
"""
import argparse
import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor

MANIFEST_VERSION = 1


def aws(*args):
    p = subprocess.run(["aws", *args], capture_output=True, text=True, timeout=300)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip()[:200])
    return json.loads(p.stdout) if p.stdout.strip() else None


def databases():
    out = aws("glue", "get-databases", "--query", "DatabaseList[].Name", "--output", "json")
    return out or []


def tables(db):
    try:
        out = aws("glue", "get-tables", "--database-name", db, "--output", "json")
    except RuntimeError:
        return []
    return (out or {}).get("TableList", [])


def col(c):
    return {"name": c["Name"], "type": c.get("Type", "string"), "comment": c.get("Comment")}


def manifest_for(t):
    sd = t.get("StorageDescriptor") or {}
    si = sd.get("SerdeInfo") or {}
    return {
        "manifest_version": MANIFEST_VERSION,
        "table_type": t.get("TableType", "EXTERNAL_TABLE"),
        "database_name": t["DatabaseName"],
        "name": t["Name"],
        "owner": t.get("Owner"),
        "created_at": None,
        "description": t.get("Description"),
        "partition_keys": [col(c) for c in (t.get("PartitionKeys") or [])],
        "parameters": {k: v for k, v in (t.get("Parameters") or {}).items()},
        "storage_descriptor": {
            "location": (sd.get("Location") or "").rstrip("/") + "/",
            "input_format": sd.get("InputFormat"),
            "output_format": sd.get("OutputFormat"),
            "compressed": bool(sd.get("Compressed", False)),
            "serde_info": {
                "serialization_library": si.get("SerializationLibrary"),
                "parameters": dict(si.get("Parameters") or {}),
            },
            "columns": [col(c) for c in (sd.get("Columns") or [])],
            "sort_columns": [],
        },
        "sqe": {},
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--database")
    ap.add_argument("--table")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()

    dbs = [args.database] if args.database else databases()
    written = skipped = 0
    reasons = {}

    def do_db(db):
        nonlocal written, skipped
        out = []
        for t in tables(db):
            if args.table and t["Name"] != args.table:
                continue
            if t.get("TableType") == "VIRTUAL_VIEW":
                reasons.setdefault("virtual_view", 0)
                reasons["virtual_view"] += 1
                continue
            sd = t.get("StorageDescriptor") or {}
            if not sd.get("Location"):
                reasons.setdefault("no_location", 0)
                reasons["no_location"] += 1
                continue
            m = manifest_for(t)
            d = os.path.join(args.root, db, t["Name"], "_sqe")
            os.makedirs(d, exist_ok=True)
            with open(os.path.join(d, "table.json"), "w") as f:
                json.dump(m, f, indent=1)
            out.append(t["Name"])
        return db, out

    with ThreadPoolExecutor(max_workers=10) as ex:
        for _db, names in ex.map(do_db, dbs):
            written += len(names)
            if args.limit and written >= args.limit:
                break

    print(f"wrote {written} manifests under {args.root}")
    for k, v in reasons.items():
        print(f"  skipped {v}: {k}")


if __name__ == "__main__":
    main()
