#!/usr/bin/env bash
set -euo pipefail
ROOT="${1:-docs/site}"
# SECURITY-ONLY. Cosmetic patterns (MR !/feat/chore/region/amazonaws/crate paths) are
# intentionally NOT here -- they are kept in source and rewritten by the getsqe sync.
# 12-digit branch is boundary-anchored so a 12-digit *substring* of a longer
# run (IEEE-754 fractions, 19-digit Iceberg snapshot ids) is NOT flagged --
# only a standalone 12-digit token (a genuine AWS account id) trips it.
PATTERN='(^|[^0-9])[0-9]{12}([^0-9]|$)|sbp\.gitlab\.schubergphilis\.com|vpf-data-ai/chameleon|jacobadmin|jacobbuilder'
if grep -rnaIE "$PATTERN" "$ROOT" \
     --include='*.md' --include='*.toml' --include='*.json' --include='*.svg' \
   | grep -vE '123456789012|000000000000'; then
  echo "LEAK-SCAN: docs/site contains secrets/PII (hits above)" >&2
  exit 1
fi
# Committed quickstart configs and test fixtures must not carry a real AWS
# account id either (#741: quickstart/glue-readonly/sqe.toml shipped one).
# Only the account-id branch applies here; the hostname/user patterns above
# are legitimate in developer-facing files.
ACCOUNT_ID='(^|[^0-9])[0-9]{12}([^0-9]|$)'
if grep -rnaIE "$ACCOUNT_ID" quickstart tests \
     --exclude-dir=.venv --exclude-dir=node_modules --exclude-dir=target \
     --include='*.md' --include='*.toml' --include='*.json' --include='*.yml' \
     --include='*.yaml' --include='*.sh' --include='*.py' --include='*.rs' \
     --include='*.sql' --include='*.env' --include='*.properties' \
   | grep -vE '123456789012|000000000000'; then
  echo "LEAK-SCAN: quickstart/ or tests/ contains an AWS account id (hits above)" >&2
  exit 1
fi
echo "leak-scan: $ROOT, quickstart/ and tests/ clean"
