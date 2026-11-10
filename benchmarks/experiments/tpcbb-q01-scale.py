#!/usr/bin/env python3
"""Measure TPCBB q01 and its pre-aggregation candidate on generated data.

The exactness check lives in tpcbb-preaggregation.py (a synthetic fixture
with every edge case). This script measures scale: join input and output
rows, wall time and peak RSS of both queries, and whether the top 20 match.

Generate the data first, into one root, at the same scale:

    sqe-bench generate tpcds --scale 10 --output <root>
    sqe-bench generate tpcbb --scale 10 --output <root>

then, from the repository root:

    python3 benchmarks/experiments/tpcbb-q01-scale.py <root> sf10 \
        --sqe-cli target/release/sqe-cli --memory-limit 12GB --repetitions 3

Runs SQE embedded over read_parquet, process startup included, peak RSS from
/usr/bin/time (macOS `-l` or GNU `-v`). Repetitions alternate the order of the
two queries. The original's join output grows with the square of the rows per
item, so above SF10 give it a --timeout or skip it with --candidate-only.
"""
import argparse
import json
import platform
from pathlib import Path
import re
import statistics
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('root', help='output root of sqe-bench generate')
    parser.add_argument('scale', help='scale directory name, for example sf10')
    parser.add_argument('--sqe-cli', default='target/release/sqe-cli')
    parser.add_argument('--memory-limit', default='8GB')
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--timeout', type=int, default=3600, help='seconds per query')
    parser.add_argument('--candidate-only', action='store_true')
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error('repetitions must be positive')

    repo = Path(__file__).resolve().parents[2]
    tables = {
        'web_clickstreams': f'{args.root}/tpcbb/{args.scale}/web_clickstreams/',
        'store_sales': f'{args.root}/tpcds/{args.scale}/store_sales/',
        'item': f'{args.root}/tpcds/{args.scale}/item/',
    }
    for path in tables.values():
        if not Path(path).is_dir():
            parser.error(f'missing table directory {path}')

    def bind(sql):
        for name, path in tables.items():
            escaped = path.replace("'", "''")
            sql = re.sub(r'\b' + name + r'\b', lambda _: f"read_parquet('{escaped}')", sql)
        return sql

    rss_flag, rss_pattern, rss_unit = (
        ('-l', r'(\d+)\s+maximum resident set size', 1)
        if platform.system() == 'Darwin'
        else ('-v', r'Maximum resident set size \(kbytes\): (\d+)', 1024)
    )

    def run(sql):
        command = ['/usr/bin/time', rss_flag, args.sqe_cli, '--embedded', '--memory',
                   '--memory-limit', args.memory_limit, '-f', 'json', '--execute=' + sql]
        start = time.perf_counter()
        try:
            # nosemgrep: opt.aikido.semgrep-rules.python.lang.security.audit.dangerous-subprocess-use-audit -- intentional local SQE binary, argv list, no shell
            completed = subprocess.run(command, capture_output=True, text=True, shell=False,
                                       timeout=args.timeout)
        except subprocess.TimeoutExpired:
            return None, (time.perf_counter() - start) * 1000, None, f'timeout after {args.timeout}s'
        elapsed = (time.perf_counter() - start) * 1000
        match = re.search(rss_pattern, completed.stderr)
        rss = int(match.group(1)) * rss_unit if match else None
        if completed.returncode:
            return None, elapsed, rss, completed.stderr[-800:]
        rows = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
        return rows, elapsed, rss, None

    queries = {
        'original': bind((repo / 'benchmarks/queries/tpcbb/q01.sql').read_text()),
        'preaggregated': bind((repo / 'benchmarks/experiments/tpcbb-q01-preaggregated.sql').read_text()),
    }
    if args.candidate_only:
        del queries['original']

    # Exact join cardinality from per-item counts: the original joins every
    # sale click of an item with every store sale of it.
    cardinality_sql = bind("""
        WITH c AS (SELECT wcs_item_sk k, COUNT(*) n FROM web_clickstreams
                   WHERE wcs_sales_sk IS NOT NULL GROUP BY 1),
             s AS (SELECT ss_item_sk k, COUNT(*) n FROM store_sales GROUP BY 1),
             i AS (SELECT i_item_sk k FROM item)
        SELECT (SELECT COUNT(*) FROM web_clickstreams) AS wcs_rows,
               (SELECT SUM(n) FROM c) AS wcs_sale_rows,
               (SELECT SUM(n) FROM s) AS ss_rows,
               (SELECT COUNT(*) FROM i) AS item_rows,
               (SELECT COUNT(*) FROM c JOIN s USING (k) JOIN i USING (k)) AS joined_items,
               (SELECT SUM(c.n * s.n) FROM c JOIN s USING (k) JOIN i USING (k)) AS original_join_rows,
               (SELECT COUNT(*) FROM c) AS candidate_click_groups,
               (SELECT COUNT(*) FROM s) AS candidate_sales_groups
    """)
    cardinality, _, _, error = run(cardinality_sql)
    if error or not cardinality:
        sys.exit(f'cardinality query failed: {error}')

    report = {
        'engine': 'SQE embedded over read_parquet; process startup included',
        'scale': args.scale, 'memory_limit': args.memory_limit,
        'cardinality': cardinality[0], 'runs': {key: [] for key in queries},
    }
    results = {}
    for repetition in range(args.repetitions):
        order = list(queries) if repetition % 2 == 0 else list(reversed(queries))
        for key in order:
            rows, elapsed, rss, error = run(queries[key])
            report['runs'][key].append({'ms': round(elapsed), 'peak_rss_bytes': rss, 'error': error})
            if rows is not None:
                results[key] = rows
            print(key, repetition, round(elapsed), rss, (error or '')[:200], file=sys.stderr, flush=True)

    for key, runs in report['runs'].items():
        ok = [run_ for run_ in runs if not run_['error']]
        report[key] = {
            'ok_runs': len(ok),
            'median_ms': statistics.median(r['ms'] for r in ok) if ok else None,
            'max_peak_rss_bytes': max((r['peak_rss_bytes'] or 0) for r in ok) if ok else None,
        }
    if len(results) == 2:
        report['top20_equal'] = results['original'] == results['preaggregated']
        if not report['top20_equal']:
            report['top20'] = results
    medians = [report.get(key, {}).get('median_ms') for key in ('original', 'preaggregated')]
    if all(medians):
        report['speedup'] = round(medians[0] / medians[1], 1)
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
