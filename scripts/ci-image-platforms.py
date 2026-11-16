#!/usr/bin/env python3
"""Check that a raw image manifest is a multi-arch list (#898).

Reads the raw manifest bytes on stdin, as `crane manifest <ref>` or
`skopeo inspect --raw docker://<ref>` print them, and fails unless it is an
OCI image index or a Docker manifest list that names every required platform.

    skopeo inspect --raw docker://repo/sqe:v1.2.3 \
      | python3 scripts/ci-image-platforms.py --ref repo/sqe:v1.2.3 \
          --digest "$(cat image-digest.txt)"

--digest also checks the sha256 of those bytes, so a copy that rewrote the
manifest (a mirror that flattened it to one platform, or converted its format)
fails even when it still lists both platforms.

Exit status: 0 when every check passes, 1 otherwise.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys

REQUIRED = ("linux/amd64", "linux/arm64")
LIST_TYPES = (
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
)


def platforms(manifest: dict) -> list[str]:
    """`os/arch[/variant]` of every entry, attestation entries left out."""
    out = []
    for m in manifest.get("manifests", []):
        p = m.get("platform") or {}
        if p.get("os") in (None, "unknown"):
            continue
        name = f"{p['os']}/{p.get('architecture', '?')}"
        if p.get("variant"):
            name += f"/{p['variant']}"
        out.append(name)
    return sorted(out)


def check(raw: bytes, digest: str | None) -> list[str]:
    """Every problem with `raw`; empty when it passes."""
    problems = []
    try:
        manifest = json.loads(raw)
    except ValueError as e:
        return [f"not a JSON manifest: {e}"]
    media = manifest.get("mediaType", "")
    if media not in LIST_TYPES and "manifests" not in manifest:
        problems.append(f"a single-platform manifest ({media or 'no mediaType'}), not a list")
    found = platforms(manifest)
    # An arm64 image usually carries variant v8; the variant does not matter here.
    os_arch = {"/".join(p.split("/")[:2]) for p in found}
    for want in REQUIRED:
        if want not in os_arch:
            problems.append(f"missing {want} (has: {', '.join(found) or 'none'})")
    if digest:
        actual = "sha256:" + hashlib.sha256(raw).hexdigest()
        if actual != digest:
            problems.append(f"digest {actual}, expected {digest}")
    return problems


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref", default="<stdin>", help="image reference, for the message only")
    ap.add_argument("--digest", help="expected sha256:<hex> of the raw manifest")
    args = ap.parse_args()

    raw = sys.stdin.buffer.read()
    problems = check(raw, args.digest)
    if problems:
        for p in problems:
            print(f"  !!  {args.ref}: {p}", file=sys.stderr)
        return 1
    print(f"  ok  {args.ref} [{', '.join(platforms(json.loads(raw)))}]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
