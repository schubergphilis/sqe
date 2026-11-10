#!/usr/bin/env bash
# Repo-wide leak scan: `make leaks`.
#
# The whole tree is mirrored to a public GitHub repository, docs/internal
# included, so nothing tracked may name internal infrastructure. Checks every
# tracked file (vendor/ excluded) for:
#
#   1. internal identifiers: internal GitLab hosts and registries, the internal
#      group path, the company abbreviation, company mail/web hosts other than
#      the public security contact
#   2. local home-directory paths (/Users/<name>/...), text AND binary files:
#      generated Iceberg metadata/avro embeds absolute paths
#   3. personal AWS profile names and real AWS account ids
#   4. extra private patterns, one ERE per line, from the gitignored
#      .leak-scan-extra file or $LEAK_SCAN_EXTRA (a masked CI variable), for
#      names that must not appear in this script either
#   5. credentials: gitleaks over the working tree with .gitleaks.toml
#
# Patterns use [.] instead of \. so the script itself does not spell out the
# hosts it looks for. Exit 1 on any hit.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

fail=0
EXCLUDE=(':!vendor' ':!scripts/leak-scan-repo.sh')
# --untracked: a new file is checked before its first commit (ignored files
# such as target/ and .env stay out).
git() { if [[ $1 == grep ]]; then shift; command git grep --untracked "$@"; else command git "$@"; fi; }

report() {
  local title=$1 hits=$2
  if [[ -n "$hits" ]]; then
    echo "LEAKS: $title" >&2
    printf '%s\n' "$hits" | cut -c1-220 >&2
    echo >&2
    fail=1
  fi
}

scan() { # scan <title> <ERE> [extra pathspecs...]
  local title=$1 re=$2; shift 2
  local hits
  hits=$(git grep -n -I -E "$re" -- . "${EXCLUDE[@]}" "$@" || true)
  report "$title" "$hits"
}

# 1. Internal identifiers.
scan "internal GitLab host or registry" \
  '[a-z0-9-]+[.]gitlab[.][a-z0-9-]+[.][a-z]{2,}|registry-sbp'
scan "internal group path" 'vpf-data-ai'
scan "company abbreviation (SBP)" '(^|[^A-Za-z0-9_])SBP([^A-Za-z0-9_]|$)'
# SECURITY.md carries the public vulnerability-report contact on purpose.
# .asset is the internal asset-registry manifest; its contact field is read by
# the platform tooling.
scan "company host or mail address" 'schubergphilis[.]com' \
  ':!SECURITY.md' ':!.asset'

# 2. Local home paths. /Users/you/ and /Users/<name>/ are documentation
#    placeholders.
home_re='/Users/[A-Za-z0-9._-]+/|-Users-[a-z0-9]+-git-'
hits=$(git grep -n -I -E "$home_re" -- . "${EXCLUDE[@]}" \
  | grep -vE '/Users/(you|me|user|username|example|runner)/' || true)
report "local home-directory path" "$hits"
hits=$(git grep -l -a -E "$home_re" -- . "${EXCLUDE[@]}" \
  | while read -r f; do grep -Iq . "$f" 2>/dev/null || echo "$f (binary)"; done || true)
report "local home-directory path in a binary file" "$hits"

# 3. Personal AWS profiles and real account ids (placeholders allowed).
scan "personal AWS profile name" 'jacob(admin|builder)'
hits=$(git grep -n -I -iE \
  '(account|arn:aws[a-z-]*:[a-z0-9-]*:[a-z0-9-]*:|dkr[.]ecr)[^0-9]{0,20}[0-9]{12}([^0-9]|$)' \
  -- . "${EXCLUDE[@]}" | grep -vE '123456789012|000000000000|111111111111|210987654321' || true)
report "AWS account id" "$hits"

# 4. Private extra patterns.
extra=()
if [[ -f .leak-scan-extra ]]; then
  while IFS= read -r line; do
    [[ -z "$line" || "$line" == \#* ]] || extra+=("$line")
  done < .leak-scan-extra
fi
if [[ -n "${LEAK_SCAN_EXTRA:-}" ]]; then
  while IFS= read -r line; do
    [[ -z "$line" ]] || extra+=("$line")
  done <<< "$LEAK_SCAN_EXTRA"
fi
for re in "${extra[@]+"${extra[@]}"}"; do
  hits=$(git grep -n -I -iE "$re" -- . "${EXCLUDE[@]}" || true)
  report "private pattern (${#re} chars)" "$hits"
done

# 5. Credentials. Homebrew's gitleaks 8.30.1 bottle is a silent no-op
#    (gitleaks/gitleaks#2170), so prefer the pre-commit-built binary.
if command -v pre-commit >/dev/null 2>&1 && [[ -f .pre-commit-config.yaml ]]; then
  if ! pre-commit run gitleaks --all-files; then
    echo "LEAKS: gitleaks found credentials (above)" >&2
    fail=1
  fi
elif command -v gitleaks >/dev/null 2>&1; then
  if ! gitleaks dir --config .gitleaks.toml --redact --no-banner .; then
    echo "LEAKS: gitleaks found credentials (above)" >&2
    fail=1
  fi
else
  echo "leaks: WARNING gitleaks not installed; credential scan skipped" >&2
fi

if [[ $fail -ne 0 ]]; then
  echo "leaks: FAILED" >&2
  exit 1
fi
echo "leaks: tracked tree clean (${#extra[@]} private patterns)"
