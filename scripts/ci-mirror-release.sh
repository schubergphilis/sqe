#!/usr/bin/env bash
# Mirror the multi-arch release image to Harbor, keeping its digest (#898).
#
# Usage: scripts/ci-mirror-release.sh <source-ref> <digest> <dest-repo> <tag>...
#
# Copies <source-ref> to <dest-repo>:<tag> for every tag with
# `skopeo copy --all --preserve-digests`, then reads each tag back and fails
# unless it lists linux/amd64 and linux/arm64 and its digest is <digest>
# (image-digest.txt from aikido-release-image). Without --all skopeo copies
# only the runner's platform and Harbor gets an amd64 image under a new digest.
#
# Credentials, when set: SRC_CREDS and DEST_CREDS as user:password.

set -euo pipefail

SRC="${1:?usage: $0 <source-ref> <digest> <dest-repo> <tag>...}"
DIGEST="${2:?digest}"
DEST="${3:?dest repo}"
shift 3
[[ $# -gt 0 ]] || { echo "error: no tags given" >&2; exit 1; }
[[ "$DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "error: '$DIGEST' is not a sha256 digest" >&2; exit 1; }

HERE="$(cd "$(dirname "$0")" && pwd)"
SKOPEO="${SKOPEO:-skopeo}"

src_auth=(); dest_auth=(); read_auth=()
if [[ -n "${SRC_CREDS:-}" ]]; then src_auth=(--src-creds "$SRC_CREDS"); fi
if [[ -n "${DEST_CREDS:-}" ]]; then
    dest_auth=(--dest-creds "$DEST_CREDS")
    read_auth=(--creds "$DEST_CREDS")
fi

status=0
for tag in "$@"; do
    ref="$DEST:$tag"
    "$SKOPEO" copy --all --preserve-digests \
        ${src_auth[@]+"${src_auth[@]}"} ${dest_auth[@]+"${dest_auth[@]}"} \
        "docker://$SRC" "docker://$ref"
    if "$SKOPEO" inspect --raw ${read_auth[@]+"${read_auth[@]}"} "docker://$ref" \
        | python3 -I "$HERE/ci-image-platforms.py" --ref "$ref" --digest "$DIGEST"; then
        echo "mirrored -> $ref"
    else
        echo "MIRROR FAILED -> $ref" >&2
        status=1
    fi
done
exit $status
