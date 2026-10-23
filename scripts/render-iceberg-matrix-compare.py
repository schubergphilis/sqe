"""Snapshot the public Iceberg matrix and render the engine comparison.

Fetches the rubric (`src/data/features.json`) and every platform file under
`src/data/platforms/` from the upstream public matrix repo
(`Neuw84/iceberg-matrix`, MIT) with the `gh` CLI, then writes:

- `docs/evidence/iceberg-matrix-rubric.json`: the rubric features and every
  engine's score at that upstream commit. `render-iceberg-matrix.py` reads it,
  so the SQE page renders offline from a pinned snapshot.
- `docs/internal/design-archive/iceberg-matrix-compare.md`: the side-by-side
  comparison against SQE's `docs/evidence/iceberg-matrix-state.json`.

Every engine is scored on the same cells SQE is: one cell per feature and
format version the rubric applies to (`introducedIn` up to, not including,
`removedIn`). A cell an engine does not list counts as none.

Run from the repo root:

    python3 scripts/render-iceberg-matrix-compare.py [--ref <commit>]

`--ref` defaults to upstream `main`. Fetched files are cached per commit in
`/tmp/iceberg-matrix-data/<sha>/`.
"""
import argparse
import base64
import datetime
import json
import os
import subprocess
import sys
from collections import OrderedDict

DATA_ROOT = '/tmp/iceberg-matrix-data'
STATE = 'docs/evidence/iceberg-matrix-state.json'
RUBRIC_OUT = 'docs/evidence/iceberg-matrix-rubric.json'
OUT = 'docs/internal/design-archive/iceberg-matrix-compare.md'
UPSTREAM_REPO = 'Neuw84/iceberg-matrix'
UPSTREAM_FEATURES = 'src/data/features.json'
UPSTREAM_PLATFORMS = 'src/data/platforms/'

VERSIONS = ['v2', 'v3']
GLYPH = {'full': 'F', 'partial': 'P', 'unknown': '?', 'none': '.'}
SCORE = {'full': 3, 'partial': 2, 'unknown': 1, 'none': 0}

CATEGORY_NAMES = OrderedDict([
    ('row-level-operations', 'Row-level operations'),
    ('table-management', 'Table management'),
    ('partitioning', 'Partitioning'),
    ('read-write', 'Read / write'),
    ('catalog-support', 'Catalog support'),
    ('v3-data-types', 'V3 data types'),
])

# Comparison columns, keyed by engine key (see `engine_key`). SQE leads each
# group so every table reads against it.
COL_GROUP_OSS = ['sqe', 'spark', 'trino', 'flink', 'doris', 'duckdb',
                 'pyiceberg', 'daft', 'clickhouse', 'databend']
COL_GROUP_CLOUD = ['sqe', 'starburst', 'aws-emr', 'dremio', 'aws-glue',
                   'google-dataproc', 'snowflake', 'databricks',
                   'aws-athena', 'google-bigquery', 'azure-fabric']

SHORT = {
    'sqe': 'SQE', 'spark': 'Spark', 'trino': 'Trino', 'flink': 'Flink',
    'doris': 'Doris', 'duckdb': 'DuckDB', 'pyiceberg': 'PyIce', 'daft': 'Daft',
    'clickhouse': 'CH', 'databend': 'Dbend', 'starburst': 'Starb',
    'aws-emr': 'EMR', 'dremio': 'Dremio', 'aws-glue': 'Glue',
    'google-dataproc': 'DProc', 'snowflake': 'Snow', 'databricks': 'DBX',
    'aws-athena': 'Athena', 'google-bigquery': 'BQ', 'azure-fabric': 'Fabric',
}


def gh_json(path):
    try:
        result = subprocess.run(['gh', 'api', path], capture_output=True,
                                text=True, check=True)
    except FileNotFoundError:
        sys.exit('gh CLI not found on PATH. Install it from https://cli.github.com '
                 'and run `gh auth login`.')
    except subprocess.CalledProcessError as e:
        sys.exit(f'gh api {path} failed: {e.stderr}\n'
                 'Make sure `gh auth status` reports an authenticated session.')
    return json.loads(result.stdout)


def fetch_upstream(ref):
    """Resolve `ref` to a commit and cache the rubric and platform files."""
    sha = gh_json(f'repos/{UPSTREAM_REPO}/commits/{ref}')['sha']
    data_dir = os.path.join(DATA_ROOT, sha)
    os.makedirs(data_dir, exist_ok=True)
    tree = gh_json(f'repos/{UPSTREAM_REPO}/git/trees/{sha}?recursive=1')['tree']
    paths = [UPSTREAM_FEATURES] + sorted(
        t['path'] for t in tree
        if t['path'].startswith(UPSTREAM_PLATFORMS) and t['path'].endswith('.json'))
    for path in paths:
        dest = os.path.join(data_dir, path.replace('/', '__'))
        if os.path.exists(dest):
            continue
        payload = gh_json(f'repos/{UPSTREAM_REPO}/contents/{path}?ref={sha}')
        with open(dest, 'w') as f:
            f.write(base64.b64decode(payload['content']).decode())
    return sha, data_dir, paths


def rubric_cells(features):
    """(feature id, version) pairs the rubric scores, in rubric order."""
    cells = []
    for feat in features:
        lo = VERSIONS.index(feat['introducedIn'])
        hi = VERSIONS.index(feat['removedIn']) if feat.get('removedIn') else len(VERSIONS)
        cells += [(feat['id'], v) for v in VERSIONS[lo:hi]]
    return cells


def engine_key(path, pid):
    """Upstream ships some engines twice (AWS on S3 buckets and on S3 Tables,
    Snowflake managed and external). The S3-bucket and managed files keep the
    plain id; the others get a suffix."""
    if '/s3tables/' in path:
        return f'{pid}@s3tables', ' (S3 Tables)'
    if '/snowflake/external/' in path:
        return f'{pid}@external', ' (external)'
    return pid, ''


def score_of(support, pid, cells):
    return sum(SCORE.get(support.get(f'{pid}:{fid}:{v}', {}).get('level', 'none'), 0)
               for fid, v in cells)


def load(data_dir, paths):
    features = json.load(open(os.path.join(data_dir, UPSTREAM_FEATURES.replace('/', '__'))))
    features = features['features']
    cells = rubric_cells(features)
    engines = OrderedDict()
    for path in paths[1:]:
        data = json.load(open(os.path.join(data_dir, path.replace('/', '__'))))
        for p in data['platforms']:
            key, suffix = engine_key(path, p['id'])
            name = p['name']
            # Cloud names like "Managed Spark" or "Glue" need their vendor.
            vendor = p.get('vendor', '')
            if p.get('group') != '3rd Party' and vendor and vendor not in name:
                name = f'{vendor} {name}'
            engines[key] = {
                'id': p['id'], 'name': name + suffix, 'file': path,
                'support': {k: v for k, v in data['support'].items()
                            if k.startswith(p['id'] + ':')},
            }
    sqe = json.load(open(STATE))
    engines['sqe'] = {'id': 'sqe', 'name': 'SQE', 'file': STATE, 'support': sqe['support']}
    return features, cells, engines, sqe


def write_rubric(sha, features, cells, engines):
    max_score = 3 * len(cells)
    peers = []
    for key, e in engines.items():
        if key == 'sqe':
            continue
        raw = score_of(e['support'], e['id'], cells)
        peers.append({'key': key, 'name': e['name'], 'file': e['file'], 'raw': raw,
                      'max': max_score, 'percent': round(100.0 * raw / max_score, 1)})
    peers.sort(key=lambda p: (-p['raw'], p['name']))
    doc = OrderedDict([
        ('source', f'https://github.com/{UPSTREAM_REPO}'),
        ('sourceCommit', sha),
        ('sourceLicense', 'MIT'),
        ('fetchedAt', datetime.date.today().isoformat()),
        ('generatedBy', 'python3 scripts/render-iceberg-matrix-compare.py'),
        ('scoring', 'One cell per feature and applicable format version '
                    '(introducedIn up to, not including, removedIn). full=3, '
                    'partial=2, unknown=1, none=0; a cell an engine does not list '
                    'counts as none.'),
        ('cells', len(cells)),
        ('max', max_score),
        ('features', [OrderedDict((k, f[k]) for k in
                                  ('id', 'name', 'category', 'introducedIn', 'removedIn')
                                  if k in f) for f in features]),
        ('peers', peers),
    ])
    with open(RUBRIC_OUT, 'w') as f:
        json.dump(doc, f, indent=2)
        f.write('\n')
    return peers


def glyph(engine, fid, version):
    cell = engine['support'].get(f"{engine['id']}:{fid}:{version}")
    return GLYPH.get(cell.get('level', 'none'), '.') if cell else '.'


def render_category(title, feats, engines, cols):
    lines = [f'### {title}', '',
             '| Feature | ' + ' | '.join(SHORT.get(c, c) for c in cols) + ' |',
             '|---|' + ':---:|' * len(cols)]
    for feat in feats:
        lo = VERSIONS.index(feat['introducedIn'])
        hi = VERSIONS.index(feat['removedIn']) if feat.get('removedIn') else len(VERSIONS)
        row = [feat['name']]
        for c in cols:
            row.append('/'.join(glyph(engines[c], feat['id'], v) if lo <= i < hi else '-'
                                for i, v in enumerate(VERSIONS)))
        lines.append('| ' + ' | '.join(row) + ' |')
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ref', default='main', help='upstream commit or branch')
    args = parser.parse_args()

    sha, data_dir, paths = fetch_upstream(args.ref)
    features, cells, engines, sqe = load(data_dir, paths)
    if {tuple(k.split(':')[1:]) for k in sqe['support']} != set(cells):
        sys.exit(f'{STATE} does not cover exactly the rubric cells at {sha[:10]}; '
                 'rescore it before rendering.')
    peers = write_rubric(sha, features, cells, engines)
    max_score = 3 * len(cells)
    sqe_raw = score_of(sqe['support'], 'sqe', cells)

    rows = sorted(peers + [{'key': 'sqe', 'name': 'SQE', 'raw': sqe_raw}],
                  key=lambda p: (-p['raw'], p['key'] != 'sqe', p['name']))
    out = [
        '# Public Iceberg Matrix Comparison',
        '',
        'Side-by-side comparison of SQE with every engine on '
        '[icebergmatrix.org](https://icebergmatrix.org). Upstream data: '
        f'[Neuw84/iceberg-matrix@{sha[:10]}](https://github.com/{UPSTREAM_REPO}/tree/{sha}) '
        f'(MIT), fetched {datetime.date.today().isoformat()}. SQE data: '
        '[`docs/evidence/iceberg-matrix-state.json`](../../evidence/iceberg-matrix-state.json), '
        'rendered per cell in [`iceberg-matrix.md`](./iceberg-matrix.md).',
        '',
        f'Every engine is scored on the same {len(cells)} cells: one per feature and '
        'format version the rubric applies to. full=3, partial=2, unknown=1, none=0; '
        f'max {max_score}. SQE rates itself; the other engines are rated by the '
        'upstream maintainers and contributors.',
        '',
        'Cells read `V2/V3`: `F` full, `P` partial, `?` unknown, `.` none, `-` the '
        'rubric does not score that version.',
        '',
        '## Scoreboard',
        '',
        '| Rank | Engine | Score | % |',
        '|---:|---|---:|---:|',
    ]
    for i, r in enumerate(rows, 1):
        name = '**SQE (self-rated)**' if r['key'] == 'sqe' else r['name']
        out.append(f"| {i} | {name} | {r['raw']}/{max_score} | "
                   f"{100.0 * r['raw'] / max_score:.1f} |")
    out.append('')

    by_cat = OrderedDict((c, []) for c in CATEGORY_NAMES)
    for feat in features:
        by_cat.setdefault(feat['category'], []).append(feat)
    for title, cols in (('Open-source engines', COL_GROUP_OSS),
                        ('Managed and commercial engines', COL_GROUP_CLOUD)):
        out += ['---', '', f'## {title}', '']
        for cat, feats in by_cat.items():
            out.append(render_category(CATEGORY_NAMES.get(cat, cat), feats, engines, cols))
            out.append('')

    out += [
        '---',
        '',
        'Per-cell notes and caveats live in the upstream platform files under '
        f'[`src/data/platforms/`](https://github.com/{UPSTREAM_REPO}/tree/{sha}/src/data/platforms) '
        'and, for SQE, in the state file.',
        '',
        'Regenerate: `python3 scripts/render-iceberg-matrix-compare.py`, then '
        '`python3 scripts/render-iceberg-matrix.py`.',
        '',
    ]
    with open(OUT, 'w') as f:
        f.write('\n'.join(out))
    print(f'wrote {RUBRIC_OUT} and {OUT} (upstream {sha[:10]}, {len(peers)} engines)')


if __name__ == '__main__':
    main()
