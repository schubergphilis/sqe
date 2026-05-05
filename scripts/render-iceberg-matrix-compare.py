"""Render the public Iceberg matrix as a comparison Markdown doc.

Fetches platform support files from the upstream public matrix repo
(`Neuw84/iceberg-matrix`) using the `gh` CLI, joins them with our local
`docs/iceberg-matrix-state.json`, and writes a side-by-side comparison
table to `docs/iceberg-matrix-compare.md`.

Run from the repo root:

    python3 scripts/render-iceberg-matrix-compare.py

Requires `gh` CLI authenticated to GitHub. Caches the fetched JSONs
in `/tmp/iceberg-matrix-data/` for subsequent runs.
"""
import base64
import glob
import json
import os
import subprocess
import sys
from collections import OrderedDict

DATA_DIR = '/tmp/iceberg-matrix-data'
OUT = 'docs/iceberg-matrix-compare.md'
UPSTREAM_REPO = 'Neuw84/iceberg-matrix'
UPSTREAM_DATA_PATH = 'src/data/platforms'
UPSTREAM_FEATURES = 'src/data/features.json'

# Filenames that exist in the upstream platforms/ directory. Each one may
# describe several platforms (e.g. aws.json describes EMR, Glue, Athena, ...).
PLATFORM_FILES = [
    'oss.json', 'snowflake.json', 'databricks.json',
    'aws.json', 'aws-tables.json',
    'gcp.json', 'azure.json',
    'firehose.json', 'kafka-connect.json',
]


def fetch_upstream_data():
    """Pull the latest features.json + platform JSONs from the public matrix
    repo via `gh api` and cache them under DATA_DIR. Skips files that are
    already present so re-running is fast; delete DATA_DIR to force refresh.
    """
    os.makedirs(DATA_DIR, exist_ok=True)

    def _gh_get(path, dest):
        if os.path.exists(dest):
            return
        result = subprocess.run(
            ['gh', 'api', f'repos/{UPSTREAM_REPO}/contents/{path}'],
            capture_output=True, text=True, check=True,
        )
        payload = json.loads(result.stdout)
        with open(dest, 'w') as f:
            f.write(base64.b64decode(payload['content']).decode())

    try:
        _gh_get(UPSTREAM_FEATURES, f'{DATA_DIR}/features.json')
        for fname in PLATFORM_FILES:
            _gh_get(f'{UPSTREAM_DATA_PATH}/{fname}', f'{DATA_DIR}/{fname}')
    except subprocess.CalledProcessError as e:
        print(
            f'gh api call failed: {e.stderr}\n'
            f'Make sure `gh auth status` reports an authenticated session.',
            file=sys.stderr,
        )
        sys.exit(1)
    except FileNotFoundError:
        print(
            'gh CLI not found on PATH. Install it from https://cli.github.com '
            'and run `gh auth login`.',
            file=sys.stderr,
        )
        sys.exit(1)

GLYPH = {
    'full': 'F',
    'partial': 'P',
    '?': '?',
    'unknown': '?',
    'none': '.',
}
SCORE = {'full': 3, 'partial': 2, '?': 1, 'unknown': 1, 'none': 0}

# Categories that group features (for readable subtables)
CATEGORIES = OrderedDict([
    ('Read & write fundamentals', [
        'read-support', 'write-insert', 'write-merge-update-delete',
        'copy-on-write', 'merge-on-read',
        'position-deletes', 'equality-deletes',
    ]),
    ('Schema & table evolution', [
        'table-creation', 'schema-evolution', 'type-promotion',
        'column-default-values', 'hidden-partitioning',
        'partition-evolution', 'multi-arg-transforms',
    ]),
    ('Time, lineage, history', [
        'time-travel', 'branching-tagging', 'lineage', 'cdc-support',
    ]),
    ('Statistics & maintenance', [
        'statistics', 'bloom-filters', 'table-maintenance',
    ]),
    ('Catalog backends', [
        'catalog-integration', 'rest-catalog',
        'polaris', 'nessie', 'unity-catalog',
        'snowflake-horizon-catalog',
        'hive-metastore', 'aws-glue-catalog',
        'jdbc-catalog', 'hadoop-catalog',
    ]),
    ('V3 advanced types', [
        'nanosecond-timestamps', 'variant-type', 'shredded-variant',
        'geometry-type', 'vector-type',
    ]),
])

# Two column groups for readability: SQE first in each, then peers
# OSS group includes SQE so SQE is always at the start.
COL_GROUP_OSS = [
    'sqe',
    'spark', 'flink', 'pyiceberg', 'duckdb',
    'clickhouse', 'doris', 'daft', 'kafka-connect',
]
COL_GROUP_CLOUD = [
    'sqe',  # SQE first, repeated for cross-table comparison
    'snowflake', 'databricks',
    'aws-emr', 'aws-glue', 'aws-athena', 'aws-redshift-s3',
    'aws-managed-flink', 'aws-firehose',
    'google-bigquery', 'google-dataproc',
    'azure-fabric', 'azure-synapse',
]

# Short labels keep header rows tight
SHORT = {
    'sqe': 'SQE',
    'spark': 'Spark',
    'flink': 'Flink',
    'pyiceberg': 'PyIce',
    'duckdb': 'DuckDB',
    'clickhouse': 'CH',
    'doris': 'Doris',
    'daft': 'Daft',
    'kafka-connect': 'Kafka',
    'snowflake': 'Snow',
    'databricks': 'DBX',
    'aws-emr': 'EMR',
    'aws-glue': 'Glue',
    'aws-athena': 'Athena',
    'aws-redshift-s3': 'RS-S3',
    'aws-tables': 'S3-T',
    'aws-managed-flink': 'MFlink',
    'aws-firehose': 'Fire',
    'google-bigquery': 'BQ',
    'google-dataproc': 'DProc',
    'azure-fabric': 'Fabric',
    'azure-synapse': 'Syn',
}

def load_data():
    features = json.load(open(f'{DATA_DIR}/features.json'))
    feat_by_id = {f['id']: f for f in features['features']}

    platforms = {}
    for path in sorted(glob.glob(f'{DATA_DIR}/*.json')):
        name = os.path.basename(path)
        if name == 'features.json':
            continue
        data = json.load(open(path))
        if 'platforms' in data:
            for p in data['platforms']:
                platforms[p['id']] = {'name': p['name'], 'support': {}}
            for cell_id, cell in data['support'].items():
                pid = cell_id.split(':')[0]
                if pid in platforms:
                    platforms[pid]['support'][cell_id] = cell

    sqe = json.load(open('docs/iceberg-matrix-state.json'))
    platforms['sqe'] = {'name': sqe['platform']['name'], 'support': sqe['support']}
    return features, feat_by_id, platforms

def score_of(plat):
    return sum(SCORE.get(c.get('level', 'none'), 0) for c in plat['support'].values())

def render_scoreboard(platforms):
    rows = []
    for pid, p in platforms.items():
        s = score_of(p)
        pct = 100.0 * s / 189
        rows.append((pid, p['name'], s, pct))
    rows.sort(key=lambda r: -r[2])

    out = ['## Scoreboard\n']
    out.append('| Rank | Engine | Score | Coverage |')
    out.append('|---:|---|---:|---:|')
    for i, (pid, name, s, pct) in enumerate(rows, 1):
        marker = ' **(this engine)**' if pid == 'sqe' else ''
        out.append(f'| {i} | {name}{marker} | {s}/189 | {pct:.1f}% |')
    return '\n'.join(out)

def cell_glyph(plat, fid, version, pid):
    cell = plat['support'].get(f'{pid}:{fid}:{version}')
    if cell is None:
        return ''
    return GLYPH.get(cell.get('level', 'none'), '.')

def render_category_table(category, feature_ids, feat_by_id, platforms, col_ids,
                           feature_versions):
    """Render one category. Cells show V2/V3 glyphs.

    feature_versions: dict mapping feature_id -> set of versions present in any
    platform support file. We use this to show single-version cells properly
    when a feature has no V2 entry in the rubric (V3-only types).
    """
    header = ['Feature']
    sep = ['---']
    for cid in col_ids:
        header.append(SHORT.get(cid, cid))
        sep.append(':---:')

    lines = [
        f'### {category}',
        '',
        '| ' + ' | '.join(header) + ' |',
        '| ' + ' | '.join(sep) + ' |',
    ]
    for fid in feature_ids:
        feat = feat_by_id.get(fid)
        if not feat:
            continue
        versions = feature_versions.get(fid, {'v2', 'v3'})
        row = [feat['name']]
        for cid in col_ids:
            plat = platforms[cid]
            v2g = cell_glyph(plat, fid, 'v2', cid) or '.'
            v3g = cell_glyph(plat, fid, 'v3', cid) or '.'
            if 'v2' not in versions and 'v3' in versions:
                row.append(f'./{v3g}' if v3g else '.')
            elif 'v2' in versions and 'v3' not in versions:
                row.append(f'{v2g}/.' if v2g else '.')
            else:
                row.append(f'{v2g}/{v3g}')
        lines.append('| ' + ' | '.join(row) + ' |')
    return '\n'.join(lines)

def feature_versions(platforms):
    """Determine which versions each feature is rated under across the rubric."""
    out = {}
    for plat in platforms.values():
        for cell_id in plat['support']:
            parts = cell_id.split(':')
            if len(parts) == 3:
                _, fid, ver = parts
                out.setdefault(fid, set()).add(ver)
    return out

def main():
    fetch_upstream_data()
    _, feat_by_id, platforms = load_data()
    feat_versions = feature_versions(platforms)

    out = []
    out.append('# Public Iceberg Matrix Comparison')
    out.append('')
    out.append('Side-by-side comparison of every Iceberg engine on the public '
               'scoreboard at [icebergmatrix.org](https://icebergmatrix.org). '
               'Source rubric: [Neuw84/iceberg-matrix]'
               '(https://github.com/Neuw84/iceberg-matrix). SQE data lives in '
               '[`docs/iceberg-matrix-state.json`](./iceberg-matrix-state.json) '
               'and is rendered in detail in [`docs/iceberg-matrix.md`]'
               '(./iceberg-matrix.md).')
    out.append('')
    out.append('Each cell shows V2/V3 status. Glyphs:')
    out.append('')
    out.append('- `F` full: feature works end-to-end with no significant caveats.')
    out.append('- `P` partial: works with caveats; see per-engine cell notes for details.')
    out.append('- `?` unknown: library primitives exist but no end-to-end verification.')
    out.append('- `.` none: not implemented, blocked upstream, or not applicable.')
    out.append('')
    out.append('Cells like `./X` mean the feature is V3-only in the rubric (no V2 cell). '
               'Cells like `X/.` mean V2-only. Engines with no V3 column at all (e.g. '
               '`./.` row-wide) have not implemented V3 features yet.')
    out.append('')
    out.append(render_scoreboard(platforms))
    out.append('')
    sqe_score = score_of(platforms['sqe'])
    sqe_pct = 100.0 * sqe_score / 189
    out.append(f'SQE sits at **{sqe_score}/189 ({sqe_pct:.1f}%)**, fifth on the public '
               'scoreboard behind EMR Spark, AWS Glue Spark, OSS Spark, and Dataproc. '
               'SQE is the only entry in the top five that is not a Spark distribution.')
    out.append('')
    out.append('---')
    out.append('')
    out.append('## OSS engines comparison')
    out.append('')
    out.append('SQE first, then Spark, Flink, PyIceberg, DuckDB, ClickHouse, Doris, '
               'Daft, and Kafka Connect (OSS streaming connector).')
    out.append('')
    for cat, feat_ids in CATEGORIES.items():
        out.append(render_category_table(cat, feat_ids, feat_by_id, platforms,
                                         COL_GROUP_OSS, feat_versions))
        out.append('')

    out.append('---')
    out.append('')
    out.append('## Cloud-managed engines comparison')
    out.append('')
    out.append('SQE first, then Snowflake, Databricks, AWS, GCP, and Azure offerings. '
               'EMR / Glue / Dataproc are Spark-managed so they share the same upstream '
               'matrix data as OSS Spark with vendor-specific caveats.')
    out.append('')
    for cat, feat_ids in CATEGORIES.items():
        out.append(render_category_table(cat, feat_ids, feat_by_id, platforms,
                                         COL_GROUP_CLOUD, feat_versions))
        out.append('')

    out.append('---')
    out.append('')
    out.append('## How to read the cells')
    out.append('')
    out.append('A cell like `F/F` means full support on V2 and V3. `F/P` means full '
               'V2 with a known caveat on V3. `./F` means the feature is V3-only in '
               'the rubric. `./.` means neither version is supported (or not '
               'implemented yet). The exact caveats per engine live in the upstream '
               'platform JSONs at '
               '[`Neuw84/iceberg-matrix/src/data/platforms/`]'
               '(https://github.com/Neuw84/iceberg-matrix/tree/main/src/data/platforms); '
               'SQE caveats live in [`docs/iceberg-matrix-state.json`](./iceberg-matrix-state.json).')
    out.append('')
    out.append('## SQE specifics')
    out.append('')
    out.append('Every SQE cell maps to a concrete file path or test name in the repo. '
               'The full per-cell evidence and notes live in '
               '[`docs/iceberg-matrix.md`](./iceberg-matrix.md). The matrix engineering '
               'history sits in chapters 16b ("The Matrix and the Quiet Bug") and 16c '
               '("Following Through") of the ebook in `docs/ebook/`.')
    out.append('')
    out.append('Regenerate this comparison: '
               '`python3 scripts/render-iceberg-matrix-compare.py`. The script '
               'fetches the latest platform JSONs from '
               '`Neuw84/iceberg-matrix` via `gh api` and joins them with our '
               'own `docs/iceberg-matrix-state.json`.')
    out.append('')

    open(OUT, 'w').write('\n'.join(out))
    print(f'wrote {OUT} ({sum(len(l) for l in out)} chars)')

if __name__ == '__main__':
    main()
