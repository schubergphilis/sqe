#!/usr/bin/env python3
"""Fill in SBOM licenses for crates that live in THIS repository.

Why this exists
---------------
The SBOM is produced by `syft dir:.`, whose Rust cataloger reads `Cargo.lock`.
`Cargo.lock` carries a name and a version and no license, so syft emits every
crate without one. `sbom_license_check.py` recovers the missing ones from
deps.dev, which works for anything published to crates.io and cannot work for a
crate that is not there: a workspace member, or a vendored fork.

The result was 30 components in REVIEW with "license evidence missing", and with
`--strict` that fails the gate. Every one of the 30 declares its license in its
own `Cargo.toml` -- 29 Apache-2.0 and `jiter` MIT. The license was never unknown,
it just had no route into the SBOM. This is that route.

What it deliberately does NOT do
--------------------------------
This is not a waiver and must never become one:

* Only components whose name AND version match a crate found on disk in this
  repository are touched. A crates.io dependency is never given a license here,
  even if the name matches something local, because the version has to match too.
* A component that already carries a license is left alone.
* The license value is read from that crate's `Cargo.toml`. Nothing is inferred,
  defaulted, or guessed. A local crate that declares no license stays missing and
  the gate keeps failing, which is the correct outcome.

Each injected license records CycloneDX evidence naming the manifest it came
from, so a reader can check the claim rather than trust it.

Usage
-----
    ci/sbom-license-from-cargo.py --sbom sbom.cdx.json [--root .]
    ci/sbom-license-from-cargo.py --selftest
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import tomllib

# Where a path-dependency crate can live. `crates/*` are the workspace members,
# `vendor/**` the forks we carry. Kept explicit rather than globbing the whole
# tree so a Cargo.toml under a test fixture cannot become a license source.
MANIFEST_GLOBS = (
    "Cargo.toml",
    "xtask/Cargo.toml",
    "crates/*/Cargo.toml",
    "vendor/*/Cargo.toml",
    "vendor/*/crates/*/Cargo.toml",
    "vendor/*/crates/*/*/Cargo.toml",
    "vendor/*/crates/*/*/*/Cargo.toml",
)


def workspace_license(root_manifest: dict) -> str | None:
    """The `[workspace.package] license` a member can inherit."""
    return (
        root_manifest.get("workspace", {}).get("package", {}).get("license")
    )


def crate_license(pkg: dict, inherited: str | None) -> str | None:
    """The license a single `[package]` declares.

    `license = "Apache-2.0"` is a plain string. `license.workspace = true` is a
    table and means "use the workspace's". Anything else is treated as absent
    rather than coerced, so an unexpected shape fails loudly at the gate instead
    of silently becoming Apache-2.0.
    """
    value = pkg.get("license")
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, dict) and value.get("workspace") is True:
        return inherited
    return None


def crate_version(pkg: dict, inherited: str | None) -> str | None:
    """Same inheritance rules as the license, for `version`."""
    value = pkg.get("version")
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, dict) and value.get("workspace") is True:
        return inherited
    return None


def nearest_workspace(manifest: pathlib.Path, root: pathlib.Path) -> dict:
    """The `[workspace.package]` table a manifest inherits from.

    Cargo resolves `version.workspace = true` against the NEAREST enclosing
    workspace root, not the outermost one. That distinction is load-bearing here:
    `vendor/iceberg-rust/Cargo.toml` declares its own
    `[workspace.package] version = "0.8.0"`, so the vendored crates are 0.8.0
    while this repo's own crates are 0.37.0. Walking to the repo root instead
    stamped every vendored crate 0.37.0, the SBOM's `iceberg@0.8.0` then matched
    nothing, and the gate stayed red with no visible reason.
    """
    for parent in manifest.parents:
        candidate = parent / "Cargo.toml"
        if candidate != manifest and candidate.is_file():
            try:
                data = tomllib.loads(candidate.read_text())
            except (OSError, tomllib.TOMLDecodeError):
                continue
            if "workspace" in data:
                return data.get("workspace", {}).get("package", {}) or {}
        if parent == root:
            break
    return {}


def local_crates(root: pathlib.Path) -> dict[tuple[str, str], tuple[str, str]]:
    """Map (crate name, version) -> (license, manifest path) for local crates.

    Keyed on name AND version on purpose. A crates.io crate that happens to
    share a name with a local one is only matched when the version matches too,
    which for a workspace member means its own workspace's version.
    """
    found: dict[tuple[str, str], tuple[str, str]] = {}
    for pattern in MANIFEST_GLOBS:
        for manifest in sorted(root.glob(pattern)):
            if "/target/" in str(manifest):
                continue
            try:
                data = tomllib.loads(manifest.read_text())
            except (OSError, tomllib.TOMLDecodeError):
                continue
            pkg = data.get("package")
            if not isinstance(pkg, dict):
                continue  # a virtual manifest declares no package
            name = pkg.get("name")
            if not isinstance(name, str):
                continue
            inherited = nearest_workspace(manifest, root)
            version = crate_version(pkg, inherited.get("version"))
            license_id = crate_license(pkg, inherited.get("license"))
            if not version or not license_id:
                continue
            found[(name, version)] = (
                license_id,
                str(manifest.relative_to(root)) if manifest.is_absolute() else str(manifest),
            )
    return found


def enrich(sbom: dict, crates: dict[tuple[str, str], tuple[str, str]]) -> list[str]:
    """Add licenses to components that match a local crate. Returns what changed."""
    filled: list[str] = []
    for comp in sbom.get("components") or []:
        if comp.get("licenses"):
            continue
        name = comp.get("name")
        version = comp.get("version")
        if not isinstance(name, str) or not isinstance(version, str):
            continue
        match = crates.get((name, version))
        if not match:
            continue
        license_id, manifest = match
        comp["licenses"] = [{"license": {"id": license_id}}]
        # Evidence so the claim is checkable. `identity` is the CycloneDX 1.6
        # place for "how do you know", and the concludedValue names the file.
        comp.setdefault("evidence", {}).setdefault("identity", []).append(
            {
                "field": "purl",
                "confidence": 1,
                "concludedValue": f"license from {manifest}",
                "methods": [
                    {
                        "technique": "source-code-analysis",
                        "confidence": 1,
                        "value": f"{manifest} declares license = \"{license_id}\"",
                    }
                ],
            }
        )
        filled.append(f"{name}@{version} -> {license_id} ({manifest})")
    return filled


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sbom", help="CycloneDX JSON SBOM to modify in place")
    ap.add_argument("--root", default=".", help="repository root (default: .)")
    ap.add_argument(
        "--selftest", action="store_true", help="run the built-in checks and exit"
    )
    args = ap.parse_args(argv)

    if args.selftest:
        return selftest()
    if not args.sbom:
        ap.error("--sbom is required unless --selftest is given")

    root = pathlib.Path(args.root).resolve()
    crates = local_crates(root)
    print(f"[license-from-cargo] {len(crates)} local crates with a declared license")

    sbom_path = pathlib.Path(args.sbom)
    sbom = json.loads(sbom_path.read_text())
    filled = enrich(sbom, crates)
    sbom_path.write_text(json.dumps(sbom, indent=2))

    for line in filled:
        print(f"[license-from-cargo] {line}")
    remaining = [
        f"{c.get('name')}@{c.get('version')}"
        for c in (sbom.get("components") or [])
        if not c.get("licenses")
    ]
    print(
        f"[license-from-cargo] filled {len(filled)}; "
        f"{len(remaining)} components still have no license in the SBOM "
        f"(deps.dev resolves published crates later in the gate)"
    )
    return 0


def selftest() -> int:
    """Checks that the narrowness rules actually hold.

    These are the ones worth having: each asserts something the script must
    REFUSE to do. A version-blind match or a defaulted license would turn this
    from evidence into a blanket waiver, and neither would be visible in a
    passing pipeline.
    """
    crates = {
        ("sqe-core", "0.37.0"): ("Apache-2.0", "crates/sqe-core/Cargo.toml"),
        ("jiter", "0.15.0"): ("MIT", "vendor/jiter/Cargo.toml"),
    }

    # A local crate at the matching version gets its declared license.
    sbom: dict = {"components": [{"name": "sqe-core", "version": "0.37.0"}]}
    assert len(enrich(sbom, crates)) == 1
    component: dict = sbom["components"][0]
    assert component["licenses"] == [{"license": {"id": "Apache-2.0"}}]
    assert component["evidence"]["identity"][0]["confidence"] == 1

    # The license is the crate's own, not a repo-wide default: jiter is MIT.
    sbom = {"components": [{"name": "jiter", "version": "0.15.0"}]}
    enrich(sbom, crates)
    assert sbom["components"][0]["licenses"] == [{"license": {"id": "MIT"}}]

    # A DIFFERENT version of the same name is not touched. This is the guard
    # against a crates.io package inheriting a local crate's license.
    sbom = {"components": [{"name": "jiter", "version": "0.99.0"}]}
    assert enrich(sbom, crates) == []
    assert "licenses" not in sbom["components"][0]

    # An unrelated crate is not touched.
    sbom = {"components": [{"name": "serde", "version": "1.0.0"}]}
    assert enrich(sbom, crates) == []
    assert "licenses" not in sbom["components"][0]

    # An existing license is never overwritten, even for a local crate.
    sbom = {
        "components": [
            {
                "name": "sqe-core",
                "version": "0.37.0",
                "licenses": [{"license": {"id": "MIT"}}],
            }
        ]
    }
    assert enrich(sbom, crates) == []
    assert sbom["components"][0]["licenses"] == [{"license": {"id": "MIT"}}]

    # A local crate that declares NO license stays missing, so the gate keeps
    # failing rather than being quietly satisfied.
    assert crate_license({"name": "x"}, None) is None
    assert crate_license({"license": "  "}, "Apache-2.0") is None
    # Only `workspace = true` inherits. Any other table shape is absent.
    assert crate_license({"license": {"workspace": True}}, "Apache-2.0") == "Apache-2.0"
    assert crate_license({"license": {"path": "LICENSE"}}, "Apache-2.0") is None
    assert crate_license({"license": {"workspace": False}}, "Apache-2.0") is None

    # Inheritance resolves against the NEAREST workspace, not the outermost.
    #
    # This is the one that was actually wrong. A vendored fork carries its own
    # `[workspace.package] version`, so walking to the repo root stamped every
    # vendored crate with THIS repo's version. The names still matched, the
    # versions did not, nothing got filled, and the gate stayed red with no
    # visible reason. Caught only by comparing against the versions CI reported.
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        tree = pathlib.Path(tmp)
        (tree / "Cargo.toml").write_text(
            '[workspace]\nmembers = ["crates/*"]\n'
            '[workspace.package]\nversion = "0.37.0"\nlicense = "Apache-2.0"\n'
        )
        (tree / "crates" / "own").mkdir(parents=True)
        (tree / "crates" / "own" / "Cargo.toml").write_text(
            '[package]\nname = "own-crate"\nversion.workspace = true\n'
            "license.workspace = true\n"
        )
        vendored = tree / "vendor" / "forked"
        (vendored / "crates" / "inner").mkdir(parents=True)
        (vendored / "Cargo.toml").write_text(
            '[workspace]\nmembers = ["crates/*"]\n'
            '[workspace.package]\nversion = "0.8.0"\nlicense = "MIT"\n'
        )
        (vendored / "crates" / "inner" / "Cargo.toml").write_text(
            '[package]\nname = "inner-crate"\nversion.workspace = true\n'
            "license.workspace = true\n"
        )
        found = local_crates(tree)
        assert ("own-crate", "0.37.0") in found, found
        assert found[("own-crate", "0.37.0")][0] == "Apache-2.0"
        # The vendored crate takes its OWN workspace's version and license.
        assert ("inner-crate", "0.8.0") in found, found
        assert found[("inner-crate", "0.8.0")][0] == "MIT"
        assert ("inner-crate", "0.37.0") not in found

    print("[license-from-cargo] selftest OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
