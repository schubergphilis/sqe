#!/usr/bin/env python3
"""Compare q01 with its pre-aggregation candidate on a bounded synthetic fixture.

Requires the Python duckdb package and an existing sqe-cli binary. Measures the
SQE embedded path with process startup included; this is not SF100 qualification.
"""
import argparse
import json
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
        # The edge cases the rewrite must preserve:
        # - item: pairs of surrogate keys share an i_item_id (TPC-DS keeps
        #   revisions of one item), and every third revision has its own
        #   description, so a group can hold one key or two.
        # - Both facts reference keys past the item table (no item row), and
        #   some NULL item keys.
        # - store_sales has no rows for keys divisible by 17 (clicks without
        #   sales), and keys past the click range (sales without clicks).
        # - ss_net_paid is DECIMAL(7,2) like TPC-DS, with NULLs, negative
        #   values (every fifth key only has refunds, so its average is
        #   negative and rounds half away from zero), and an item (key 2)
        #   whose every value is NULL.
        # - One item (ROUNDING_KEY) has a single click and 2,000,001 sales,
        #   1,000,000 of them 0.01: its average is 0.0049999975, so AVG
        #   returns 0.00. A candidate that divides at scale 6 and then casts
        #   to two decimals rounds twice and returns 0.01.
        conn.execute("CREATE TABLE item AS SELECT n::INTEGER i_item_sk, 'item-'||(n//2) i_item_id, CASE WHEN n%3=0 THEN 'description-alt-'||n ELSE 'description-'||(n//2) END i_item_desc, 'category-'||(n%5) i_category, 'class' i_class FROM range(1, ?) t(n)", [args.keys + 1])
        conn.execute("CREATE TABLE web_clickstreams AS SELECT CASE WHEN n%11=0 THEN NULL ELSE n%?+1 END::INTEGER wcs_item_sk, CASE WHEN n%4=0 THEN NULL ELSE n END::BIGINT wcs_sales_sk FROM range(?) t(n)", [args.keys + 10, args.rows])
        conn.execute("CREATE TABLE store_sales AS SELECT CASE WHEN n%13=0 THEN NULL ELSE n%?+1 END::INTEGER ss_item_sk, CASE WHEN n%?+1=2 OR n%7=0 THEN NULL WHEN (n%?+1)%5=0 THEN -(n%997)/100 ELSE ((n%1000)-100)/100 END::DECIMAL(7,2) ss_net_paid FROM range(?) t(n) WHERE (n%?+1)%17<>0", [args.keys + 20, args.keys + 20, args.keys + 20, args.rows, args.keys + 20])
        rounding_key = args.keys + 1000
        conn.execute("INSERT INTO item VALUES (?, 'item-rounding', 'description-rounding', 'category-rounding', 'class')", [rounding_key])
        conn.execute("INSERT INTO web_clickstreams VALUES (?, 1)", [rounding_key])
        conn.execute("INSERT INTO store_sales SELECT ?, CASE WHEN n < 1000000 THEN 0.01 ELSE 0 END::DECIMAL(7,2) FROM range(2000001) t(n)", [rounding_key])
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

        # Compare every group, exactly: ORDER BY ties may choose different
        # groups at the LIMIT boundary, so the comparison drops the ORDER BY
        # and LIMIT. Decimal sums are exact, so there is no tolerance.
        # The explicit NULL flags date from when sqe-cli's JSON printed NULL
        # as ""; it prints null now, and the flags still do no harm.
        group = ('i_item_id', 'i_item_desc', 'i_category', 'i_class')
        results = []
        for sql in queries.values():
            body = sql[:sql.rfind('ORDER BY')]
            rows, _ = execute(
                'SELECT *, total_net_revenue IS NULL AS revenue_is_null, '
                f'avg_sale_amount IS NULL AS avg_is_null FROM ({body}) q'
            )
            rows.sort(key=lambda row: tuple(row[k] for k in group))
            results.append(rows)
        if len(results[0]) != len(results[1]):
            raise AssertionError(('group counts differ', len(results[0]), len(results[1])))
        for before, after in zip(*results):
            if before != after:
                raise AssertionError((before, after))
        types = []
        for sql in queries.values():
            body = sql[:sql.rfind('ORDER BY')]
            rows, _ = execute(
                'SELECT arrow_typeof(click_sales_count) c, arrow_typeof(total_net_revenue) s, '
                f'arrow_typeof(avg_sale_amount) a FROM ({body}) LIMIT 1'
            )
            types.append(rows)
        if types[0] != types[1]:
            raise AssertionError(('output types differ', types))
        rows = results[0]
        coverage = {
            'groups': len(rows),
            'null_revenue_groups': sum(r['revenue_is_null'] == 'true' for r in rows),
            'negative_revenue_groups': sum((r['total_net_revenue'] or '').startswith('-') for r in rows),
        }
        if not (coverage['null_revenue_groups'] and coverage['negative_revenue_groups']):
            raise AssertionError(('the fixture lost an edge case', coverage))
        rounding = [r['avg_sale_amount'] for r in rows if r['i_item_id'] == 'item-rounding']
        if rounding != ['0.00']:
            raise AssertionError(('the rounding group lost its single rounding', rounding))
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
            'output_types': types[0], 'coverage': coverage,
            'measurements_ms': timings, 'medians_ms': medians,
            'speedup': medians['original'] / medians['preaggregated'],
        }, indent=2))


if __name__ == '__main__':
    main()
