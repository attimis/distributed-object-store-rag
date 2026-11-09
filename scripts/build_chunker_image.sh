#!/usr/bin/env bash
# Build the chunker Lambda Docker image.
#
# Tag scheme: the first 12 hex chars of sha256(pyproject + uv.lock + this
# Dockerfile + the locked requirements + the src/ tree), with a `-dirty`
# suffix when the working tree has uncommitted changes. A content-derived tag
# means an unchanged build re-uses the image already in ECR, and any change
# to the inputs rolls a new tag instead of silently overwriting the old one.
#
# Usage:
#   bash scripts/build_chunker_image.sh
#   IMAGE_NAME=my-chunker bash scripts/build_chunker_image.sh
#
# Writes the computed tag to ${TMPDIR:-/tmp}/onebucket_chunker_tag for the deploy script.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

DOCKERFILE="lambda/chunker.Dockerfile"
REQ_LOCK="lambda/requirements-chunker.txt"
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

IMAGE_NAME="${IMAGE_NAME:-onebucket-chunker}"
FULL_TAG="${IMAGE_NAME}:${TAG}"

echo "==> Building ${FULL_TAG} (linux/amd64; matches Lambda default architecture)"
# --platform=linux/amd64: Lambda's default Architecture is x86_64; Mac M-series
# Docker defaults to linux/arm64 which would (a) require gcc to compile hf-xet
# from source (no aarch64-manylinux wheel on PyPI) and (b) require explicitly
# setting `Architectures: ["arm64"]` on the Lambda function. The x86_64 path is
# simpler and matches the deploy script's defaults. Cross-arch builds via
# Docker Desktop's QEMU emulation are slower than native — expect 3–5 min for
# the chunker image on first build.
#
# --provenance=false --sbom=false: BuildKit defaults to attaching SLSA
# provenance + SBOM attestations, which produces an OCI manifest list (one
# manifest for the image + one for the attestations). AWS Lambda only accepts
# single-manifest Docker v2 images — feeding it a manifest list raises
# "The image manifest, config or layer media type ... is not supported."
# Disabling attestations produces the plain single manifest Lambda expects.
docker build --platform=linux/amd64 --provenance=false --sbom=false -f "$DOCKERFILE" -t "${FULL_TAG}" .
echo "==> Built ${FULL_TAG}"
echo "${FULL_TAG}" > "${TMPDIR:-/tmp}/onebucket_chunker_tag"
