### Security

- Removed internal infrastructure references from the tree; `make leaks` scans for them

    The repository is mirrored to a public GitHub repository, `docs/internal`
    included. CI includes and images now use GitLab predefined variables
    (`$CI_REGISTRY_IMAGE`, `$CI_PROJECT_ROOT_NAMESPACE`) instead of the
    internal registry host and group path; `Cargo.toml` and `QUICKSTART.md`
    point at the public repository. Local home paths, personal AWS profile
    names, a real AWS account id and internal dataset names are gone from
    docs, tests, benchmark output and the playground Iceberg metadata (the
    avro manifests are re-encoded, not byte-patched). `make leaks` runs
    `make leak-scan` and then `scripts/leak-scan-repo.sh` over every tracked
    and untracked non-ignored file: internal hosts and group paths, the
    company abbreviation, company mail hosts, home paths in text and binary
    files, AWS profile names and account ids, private patterns from a
    gitignored `.leak-scan-extra` file or `$LEAK_SCAN_EXTRA`, and gitleaks.
    Git history still holds the old values.
