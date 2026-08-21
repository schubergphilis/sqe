#!/usr/bin/env python3
"""Generate Hive-layout demo data for the hive-external-s3 quickstart.

Four tables, four formats, one shared schema, all laid out the way Hive and
Athena expect: `<table>/<key>=<value>/<file>`.

    events_parquet/dt=2026-01-01/region=eu/part-000.parquet   two partition keys
    events_csv/dt=2026-01-01/part-000.csv                     header + comma
    events_json/dt=2026-01-01/part-000.json                   one JSON object per line
    events_ndjson/part-000.ndjson                             unpartitioned

Every table gets its own top-level prefix on purpose. SQE cycle 1 lists a
table's LOCATION without filtering by file extension, so two formats sharing
one prefix would have the reader parsing the wrong file. See the README's
gotchas.

The data is random but seeded, so the same `--size-mb` always produces the
same bytes and the quickstart's row counts are stable across runs.

Usage:
    python3 gen_data.py --out ./data --size-mb 64
    python3 gen_data.py --out ./data --size-mb 5120   # the 5GB ceiling

Requires pyarrow for the Parquet table. CSV and JSON are written with the
standard library alone.
"""

import argparse
import json
import random
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

# The ceiling the quickstart documents. Anything larger is almost certainly a
# typo (`--size-mb 51200` for 50GB) and would fill a laptop's disk twice over:
# the generator writes the files locally before they are uploaded.
MAX_SIZE_MB = 5120

# One row of the shared schema, as bytes on disk, measured per format from a
# generated run rather than guessed: Snappy Parquet compresses these low
# cardinality columns hard, CSV is compact text, JSON repeats every key on
# every line. The split lands within about 10% of the requested total, which
# is all a demo needs; treat --size-mb as approximate.
BYTES_PER_ROW = {
    "parquet": 14,
    "csv": 26,
    "json": 72,
}

# How the requested total splits across the four tables. Parquet carries the
# bulk because it is the format a real deployment puts its volume in, and the
# text formats stay big enough to be more than a toy.
SHARE = {
    "events_parquet": 0.70,
    "events_csv": 0.20,
    "events_json": 0.05,
    "events_ndjson": 0.05,
}

DAYS = 4
REGIONS = ["eu", "us"]
KINDS = ["click", "view", "purchase", "refund"]
START_DAY = date(2026, 1, 1)

SCHEMA_SQL = "event_id BIGINT, user_id BIGINT, kind STRING, amount DOUBLE"


def partition_days():
    return [(START_DAY + timedelta(days=i)).isoformat() for i in range(DAYS)]


def rows_for(table, size_mb):
    """Row count for one table, from its byte share and format density."""
    fmt = "parquet" if table.endswith("parquet") else (
        "csv" if table.endswith("csv") else "json"
    )
    target_bytes = size_mb * 1024 * 1024 * SHARE[table]
    return max(1, int(target_bytes / BYTES_PER_ROW[fmt]))


def make_rows(rng, count, first_id):
    """A batch of rows as column lists, ready for pyarrow or text writing."""
    event_id = list(range(first_id, first_id + count))
    user_id = [rng.randint(1, 50_000) for _ in range(count)]
    kind = [KINDS[rng.randrange(len(KINDS))] for _ in range(count)]
    # Refunds are negative, so SUM(amount) per kind is a query with a real
    # answer rather than a uniform blob.
    amount = [
        round(rng.uniform(-50.0, 500.0) if k == "refund" else rng.uniform(0.5, 500.0), 2)
        for k in kind
    ]
    return {"event_id": event_id, "user_id": user_id, "kind": kind, "amount": amount}


def write_parquet(out, size_mb, rng):
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        sys.exit(
            "pyarrow is required for the Parquet table.\n"
            "  pip install pyarrow   (run.sh does this in ./.venv for you)"
        )

    total = rows_for("events_parquet", size_mb)
    days = partition_days()
    # One file per (dt, region) leaf. Many moderate files parallelize in the
    # scan; one giant file does not.
    per_leaf = max(1, total // (len(days) * len(REGIONS)))
    schema = pa.schema(
        [
            ("event_id", pa.int64()),
            ("user_id", pa.int64()),
            ("kind", pa.string()),
            ("amount", pa.float64()),
        ]
    )
    written = 0
    next_id = 1
    for dt in days:
        for region in REGIONS:
            leaf = out / "events_parquet" / f"dt={dt}" / f"region={region}"
            leaf.mkdir(parents=True, exist_ok=True)
            cols = make_rows(rng, per_leaf, next_id)
            next_id += per_leaf
            table = pa.Table.from_pydict(cols, schema=schema)
            pq.write_table(table, leaf / "part-000.parquet", compression="snappy")
            written += per_leaf
    return written


def write_csv(out, size_mb, rng):
    total = rows_for("events_csv", size_mb)
    days = partition_days()
    per_day = max(1, total // len(days))
    written = 0
    next_id = 1
    for dt in days:
        leaf = out / "events_csv" / f"dt={dt}"
        leaf.mkdir(parents=True, exist_ok=True)
        cols = make_rows(rng, per_day, next_id)
        next_id += per_day
        with open(leaf / "part-000.csv", "w", encoding="utf-8") as fh:
            # A header row, which is why the table declares
            # skip.header.line.count = 1. Hive TEXTFILE is headerless by
            # convention, so without that property the header would come back
            # as data.
            fh.write("event_id,user_id,kind,amount\n")
            for i in range(per_day):
                fh.write(
                    f"{cols['event_id'][i]},{cols['user_id'][i]},"
                    f"{cols['kind'][i]},{cols['amount'][i]}\n"
                )
        written += per_day
    return written


def write_json_lines(out, table, suffix, size_mb, rng, partitioned):
    """Line-delimited JSON, the only JSON framing Hive and SQE read.

    A `[{...},{...}]` array document is NOT readable by either. The two
    tables differ only in file extension and in whether they are partitioned.
    """
    total = rows_for(table, size_mb)
    targets = partition_days() if partitioned else [None]
    per_target = max(1, total // len(targets))
    written = 0
    next_id = 1
    for dt in targets:
        leaf = out / table / (f"dt={dt}" if dt else "")
        leaf.mkdir(parents=True, exist_ok=True)
        cols = make_rows(rng, per_target, next_id)
        next_id += per_target
        with open(leaf / f"part-000.{suffix}", "w", encoding="utf-8") as fh:
            for i in range(per_target):
                fh.write(
                    json.dumps(
                        {
                            "event_id": cols["event_id"][i],
                            "user_id": cols["user_id"][i],
                            "kind": cols["kind"][i],
                            "amount": cols["amount"][i],
                        }
                    )
                    + "\n"
                )
        written += per_target
    return written


def directory_bytes(path):
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default="./data", help="output directory")
    ap.add_argument(
        "--size-mb",
        type=int,
        default=64,
        help=f"approximate total size across all four tables (max {MAX_SIZE_MB})",
    )
    ap.add_argument("--seed", type=int, default=20260820, help="RNG seed")
    args = ap.parse_args()

    if args.size_mb < 1:
        sys.exit("--size-mb must be at least 1")
    if args.size_mb > MAX_SIZE_MB:
        sys.exit(f"--size-mb {args.size_mb} exceeds the {MAX_SIZE_MB} MB ceiling")

    out = Path(args.out)
    # A stale partial run would upload files the manifests do not describe, so
    # the generator owns this directory outright.
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    rng = random.Random(args.seed)
    counts = {
        "events_parquet": write_parquet(out, args.size_mb, rng),
        "events_csv": write_csv(out, args.size_mb, rng),
        "events_json": write_json_lines(
            out, "events_json", "json", args.size_mb, rng, partitioned=True
        ),
        "events_ndjson": write_json_lines(
            out, "events_ndjson", "ndjson", args.size_mb, rng, partitioned=False
        ),
    }

    manifest = {
        "size_mb_requested": args.size_mb,
        "seed": args.seed,
        "bytes_on_disk": directory_bytes(out),
        "row_counts": counts,
        "partition_days": partition_days(),
        "regions": REGIONS,
    }
    (out / "generated.json").write_text(json.dumps(manifest, indent=2) + "\n")

    for table, rows in counts.items():
        size = directory_bytes(out / table) / (1024 * 1024)
        print(f"  {table:<16} {rows:>12,} rows  {size:>8.1f} MB")
    print(f"  {'total':<16} {sum(counts.values()):>12,} rows  "
          f"{manifest['bytes_on_disk'] / (1024 * 1024):>8.1f} MB")


if __name__ == "__main__":
    main()
