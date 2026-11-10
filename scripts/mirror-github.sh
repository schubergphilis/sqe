#!/usr/bin/env bash
# Mirror main and the release tags to the public GitHub repository, with the
# internal GitLab host and group path removed from commit and tag messages.
#
#   scripts/mirror-github.sh            rewrite, verify, show what would change
#   scripts/mirror-github.sh --push     ...and push (main + tags, forced)
#
# GitLab history stays as it is. The script fetches main and the tags from
# origin into a scratch bare repository and rewrites them there with
# git filter-repo. filter-repo is deterministic: the same input history and
# options give the same commit ids, so every later run reproduces the ids that
# are already on GitHub and a normal mirror push is a fast-forward. Keep the
# replacement rules below stable; changing one rewrites every commit it touches
# (and everything after it) on the next push.
#
# The host and group path are read from the origin URL at run time, so this
# file does not name them (`make leaks` scans it like any other file).
#
# What is rewritten (messages only; trees and authors are untouched):
#   "Merge branch 'x' of <internal url> into y"  -> "Merge branch 'x' into y"
#   "See merge request <group path>!123"          -> "See merge request !123"
#   any other <internal url> / host / group path  -> neutral text
#   the company abbreviation                      -> "company"
# Signatures are dropped (fast-export cannot keep them on rewritten commits).
set -euo pipefail

PUSH=0
[[ "${1:-}" == "--push" ]] && PUSH=1
MIRROR_REMOTE=${MIRROR_REMOTE:-github}

cd "$(git rev-parse --show-toplevel)"
command -v git-filter-repo >/dev/null || { echo "mirror: git filter-repo is not installed" >&2; exit 1; }

origin_url=$(git remote get-url origin)
github_url=$(git remote get-url "$MIRROR_REMOTE")
# ssh://git@host:port/group/path.git | git@host:group/path.git | https://host/group/path.git
host=$(sed -E 's#^[a-z+]+://##; s#^[^@/]*@##; s#[:/].*$##' <<<"$origin_url")
path=$(sed -E 's#^[a-z+]+://[^/]+/##; s#^[^@/]*@[^:]+:##; s#\.git$##' <<<"$origin_url")
top=${path%%/*}
[[ -n "$host" && -n "$path" && "$path" == */* ]] || { echo "mirror: cannot parse origin url" >&2; exit 1; }

re_esc() { sed -E 's/[][\.^$*+?(){}|/]/\\&/g' <<<"$1"; }
H=$(re_esc "$host"); P=$(re_esc "$path"); T=$(re_esc "$top")

work=$(mktemp -d "${TMPDIR:-/tmp}/sqe-mirror.XXXXXX")
# MIRROR_KEEP=1 keeps the rewritten repository for inspection.
if [[ "${MIRROR_KEEP:-0}" == 1 ]]; then echo "    workdir: $work"; else trap 'rm -rf "$work"' EXIT; fi
repo=$work/repo.git
rules=$work/messages.txt

# Order matters: the specific forms first, the catch-alls last.
# (?i) because GitLab group paths are case-insensitive and were typed both ways.
cat >"$rules" <<EOF
regex:(?i)[ \t]+of[ \t]+(?:https?|ssh)://[^ \t\n]*${H}[^ \t\n]*==>
regex:(?i)See merge request ${P}!==>See merge request !
regex:(?i)(?:https?|ssh)://[^ \t\n]*${H}[^ \t\n]*==>the internal repository
regex:(?i)${P}==>sqe
regex:(?i)${T}/==>
regex:(?i)${H}==>the internal GitLab
regex:(?<![A-Za-z0-9_])S[B]P(?![A-Za-z0-9_])==>company
EOF

echo "==> Fetching main and tags from origin"
git init -q --bare "$repo"
git -C "$repo" fetch -q --no-tags "$origin_url" \
  '+refs/heads/main:refs/heads/main' '+refs/tags/*:refs/tags/*'

src_tree=$(git -C "$repo" rev-parse 'main^{tree}')

echo "==> Rewriting messages"
git -C "$repo" filter-repo --force --quiet --replace-message "$rules"

echo "==> Verifying"
leaks=$(git -C "$repo" log --all --format=%B | grep -ciE "${H}|${P}|(^|[^a-z0-9-])${T}/|(^|[^A-Za-z0-9_])S[B]P([^A-Za-z0-9_]|$)" || true)
leaks_tags=$(git -C "$repo" for-each-ref refs/tags --format='%(contents)' | grep -ciE "${H}|${P}" || true)
if [[ "$leaks" != 0 || "$leaks_tags" != 0 ]]; then
  echo "mirror: $leaks commit-message and $leaks_tags tag-message lines still match; not pushing" >&2
  exit 1
fi
# The rewrite must not change any file: compare the tip trees.
new_tree=$(git -C "$repo" rev-parse 'main^{tree}')
[[ "$src_tree" == "$new_tree" ]] || {
  echo "mirror: rewritten main tree $new_tree differs from origin main $src_tree" >&2; exit 1; }

new_main=$(git -C "$repo" rev-parse main)
cur_main=$(git ls-remote "$github_url" refs/heads/main | cut -f1)
echo "    commits: $(git -C "$repo" rev-list --count main), tags: $(git -C "$repo" tag | wc -l | tr -d ' ')"
echo "    $MIRROR_REMOTE main: ${cur_main:-<none>} -> $new_main"
if [[ -n "$cur_main" ]] && git -C "$repo" cat-file -e "$cur_main^{commit}" 2>/dev/null \
   && git -C "$repo" merge-base --is-ancestor "$cur_main" main; then
  echo "    fast-forward"
else
  echo "    NOT a fast-forward (first rewritten push, or the rules changed)"
fi

if [[ $PUSH -eq 0 ]]; then
  echo "dry run; rerun with --push to update $MIRROR_REMOTE"
  exit 0
fi
echo "==> Pushing to $MIRROR_REMOTE"
git -C "$repo" push --force "$github_url" main:refs/heads/main
git -C "$repo" push --force "$github_url" 'refs/tags/*:refs/tags/*'
echo "mirror: $MIRROR_REMOTE main = $new_main"
