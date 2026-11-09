#!/usr/bin/env bash
# Build the embedder Lambda Docker image.
#
# Tag scheme: the first 12 hex chars of sha256(pyproject + uv.lock + this
# Dockerfile + the locked requirements + the src/ tree), with a `-dirty`
# suffix when the working tree has uncommitted changes. A content-derived tag
# means an unchanged build re-uses the image already in ECR, and any change
# to the inputs rolls a new tag instead of silently overwriting the old one.
#
# Usage:
#   bash scripts/build_embedder_image.sh
#   IMAGE_NAME=my-embedder bash scripts/build_embedder_image.sh
#
# Writes the computed tag to ${TMPDIR:-/tmp}/onebucket_embedder_tag for the deploy script.
# Embedder build pre-bakes BGE-small + SPLADE models — first build downloads
# ~700 MB and may take 5–15 min on residential bandwidth.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

DOCKERFILE="lambda/embedder.Dockerfile"
REQ_LOCK="lambda/requirements-embedder.txt"
[ -f "$DOCKERFILE" ] || { echo "ERROR: $DOCKERFILE not found" >&2; exit 1; }
[ -f "$REQ_LOCK" ] || { echo "ERROR: $REQ_LOCK not found — run scripts/lock_lambda_deps.sh first" >&2; exit 1; }

# The locked requirements are part of the tag hash: a dependency bump must
# roll the image tag, or ECR would keep serving the previous build.
TAG_INPUT_FILES=(
    pyproject.toml
    uv.lock
    "$DOCKERFILE"
    "$REQ_LOCK"
)
SRC_HASH=$(find src -type f -name '*.py' -print0 | sort -z | xargs -0 shasum -a 256 | shasum -a 256 | awk '{print $1}')

CONTENT_HASH=$(
    {
        for f in "${TAG_INPUT_FILES[@]}"; do
            [ -f "$f" ] && shasum -a 256 "$f" || true
        done
        echo "src=${SRC_HASH}"
    } | shasum -a 256 | awk '{print $1}'
)

TAG="${CONTENT_HASH:0:12}"
if [ -n "$(git status --porcelain 2>/dev/null || true)" ]; then
    TAG="${TAG}-dirty"
fi

IMAGE_NAME="${IMAGE_NAME:-onebucket-embedder}"
FULL_TAG="${IMAGE_NAME}:${TAG}"

echo "==> Building ${FULL_TAG} (linux/amd64; pre-bakes ~700MB of model weights; first build 15–30 min on Mac M-series due to QEMU cross-arch emulation)"
# --platform=linux/amd64: same rationale as the chunker build script. Lambda's
# default architecture is x86_64; building arm64 on Mac M-series creates wheel-
# availability issues for hf-xet and forces an explicit Lambda Architecture
# config. Cross-arch builds via QEMU are slower than native (~2–3× hit on the
# embedder build). Acceptable one-time cost for deploy reliability.
#
# --provenance=false --sbom=false: same rationale as the chunker build script.
# BuildKit's default attestation manifests turn the push into an OCI manifest
# list, which AWS Lambda rejects with "image manifest, config or layer media
# type ... is not supported." Disabling produces a plain single-manifest image.
docker build --platform=linux/amd64 --provenance=false --sbom=false -f "$DOCKERFILE" -t "${FULL_TAG}" .
echo "==> Built ${FULL_TAG}"
echo "${FULL_TAG}" > "${TMPDIR:-/tmp}/onebucket_embedder_tag"
