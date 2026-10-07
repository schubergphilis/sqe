#!/usr/bin/env python3
"""Compare q01 with its pre-aggregation candidate on a bounded synthetic fixture.

Requires the Python duckdb package and an existing sqe-cli binary. Measures the
SQE embedded path with process startup included; this is not SF100 qualification.
"""
import argparse
import json
import math
from pathlib import Path
import re
import statistics
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=50_000)
    parser.add_argument('--keys', type=int, default=1_000)
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--sqe-cli', default='target/debug/sqe-cli')
    args = parser.parse_args()
    if min(args.rows, args.keys, args.repetitions) < 1:
        parser.error('rows, keys and repetitions must be positive')
    import duckdb
    root = Path(__file__).resolve().parents[2]
    queries = {
        'original': (root / 'benchmarks/queries/tpcbb/q01.sql').read_text(),
        'preaggregated': (root / 'benchmarks/experiments/tpcbb-q01-preaggregated.sql').read_text(),
    }
    with tempfile.TemporaryDirectory(prefix='sqe-preaggregation-') as folder:
        folder = Path(folder)
        conn = duckdb.connect()
        conn.execute("SET memory_limit='512MB'")
        conn.execute("CREATE TABLE item AS SELECT n::INTEGER i_item_sk, 'item-'||n i_item_id, 'description-'||n i_item_desc, 'category' i_category, 'class' i_class FROM range(1, ?) t(n)", [args.keys + 1])
        conn.execute("CREATE TABLE web_clickstreams AS SELECT (n%?+1)::INTEGER wcs_item_sk, CASE WHEN n%4=0 THEN NULL ELSE n END wcs_sales_sk FROM range(?) t(n)", [args.keys, args.rows])
        conn.execute("CREATE TABLE store_sales AS SELECT (n%?+1)::INTEGER ss_item_sk, CASE WHEN n%7=0 THEN NULL ELSE (n%100+1)/100.0 END::DOUBLE ss_net_paid FROM range(?) t(n)", [args.keys, args.rows])
        exports = {
            'item': "COPY item TO ? (FORMAT PARQUET)",
            'web_clickstreams': "COPY web_clickstreams TO ? (FORMAT PARQUET)",
            'store_sales': "COPY store_sales TO ? (FORMAT PARQUET)",
        }
        for name, statement in exports.items():
            path = str(folder / f'{name}.parquet')
            conn.execute(statement, [path])
            path = path.replace("'", "''")
            for key, sql in queries.items():
                queries[key] = re.sub(r'\b' + name + r'\b', lambda _: f"read_parquet('{path}')", sql)
        conn.close()

        def execute(sql):
            start = time.perf_counter()
            # nosemgrep: opt.aikido.semgrep-rules.python.lang.security.audit.dangerous-subprocess-use-audit -- intentional local SQE binary, argv list, no shell
            completed = subprocess.run(
                [args.sqe_cli, '--embedded', '--memory', '--memory-limit', '512MB',
                 '-f', 'json', '--execute=' + sql], capture_output=True, text=True, shell=False,
            )
            if completed.returncode:
                raise RuntimeError(completed.stderr)
            elapsed = (time.perf_counter() - start) * 1000
            return [json.loads(line) for line in completed.stdout.splitlines()], elapsed

        # Compare every group. ORDER BY ties may choose different groups at the
        # LIMIT boundary, and float accumulation order may differ slightly.
        results = []
        for sql in queries.values():
            rows, _ = execute(sql[:sql.rfind('ORDER BY')])
            rows.sort(key=lambda row: row['i_item_id'])
            results.append(rows)
        if len(results[0]) != len(results[1]):
            raise AssertionError('group counts differ')
        for before, after in zip(*results):
            for field in before:
                if field in ('total_net_revenue', 'avg_sale_amount'):
                    if before[field] != after[field] and not math.isclose(
                        float(before[field]), float(after[field]), rel_tol=1e-9, abs_tol=1e-8,
                    ):
                        raise AssertionError((field, before[field], after[field]))
                elif before[field] != after[field]:
                    raise AssertionError((field, before[field], after[field]))
        timings = {key: [] for key in queries}
        for repetition in range(args.repetitions):
            order = list(queries) if repetition % 2 == 0 else list(reversed(queries))
            for key in order:
                rows, elapsed = execute(queries[key])
                if len(rows) > 20:
                    raise AssertionError('global LIMIT was not preserved')
                timings[key].append(elapsed)
        medians = {key: statistics.median(values) for key, values in timings.items()}
        print(json.dumps({
            'engine': 'SQE embedded; process startup included',
            'fixture_rows_per_fact': args.rows, 'item_keys': args.keys,
            'full_result_groups_checked': len(results[0]),
            'measurements_ms': timings, 'medians_ms': medians,
            'speedup': medians['original'] / medians['preaggregated'],
        }, indent=2))


if __name__ == '__main__':
    main()
