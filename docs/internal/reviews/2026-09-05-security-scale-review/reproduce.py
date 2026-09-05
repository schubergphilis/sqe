"""Offline regression check for the 2026-09-05 security/scale review.

At review time this script compiled the vulnerable endpoint validator and the
cache normalization expression extracted verbatim from the source and showed
both defects. Both were fixed on 2026-09-05; the fixed validator depends on the
`url` crate, so extraction into a plain `rustc` harness no longer works.
The script now runs the production unit tests that encode the same
reproductions, then keeps the two dependency-free illustrations.
"""
from pathlib import Path
import gzip
import subprocess
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[4]

CHECKS = [
    ("900 endpoint allowlist rejects userinfo", "sqe-core", "tvf_http_destinations"),
    ("903 result cache keys keep SQL literal semantics", "sqe-coordinator", "different_sql_literals_never_reuse_cached_results"),
]
for label, package, test in CHECKS:
    subprocess.run(
        ["cargo", "test", "-q", "-p", package, "--lib", test],
        cwd=ROOT,
        check=True,
    )
    print(f"FIXED: {label} ({package}::{test})")

url = "https://allowed.example:password@blocked.example/"
assert urlsplit(url).hostname == "blocked.example"
print("Standard URL parser destination:", urlsplit(url).hostname)
raw = b'{"v":"' + b'a' * (8 * 1024 * 1024) + b'"}\n'
compressed = gzip.compress(raw)
assert len(gzip.decompress(compressed)) == len(raw)
print(f"Bounded gzip illustration: {len(compressed)} compressed bytes -> {len(raw)} decoded bytes")
