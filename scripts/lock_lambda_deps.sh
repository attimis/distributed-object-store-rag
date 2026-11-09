#!/usr/bin/env bash
# Regenerate Lambda image locked requirements from the .in source files.
#
# Run this whenever you bump a dep in lambda/requirements-{chunker,embedder}.in
# or you want to refresh transitive pins. Commit the resulting .txt files —
# they are the source of truth for `pip install` inside the Lambda Dockerfiles,
# and they make the image content hash deterministic across machines.
#
# Why this and not pip-tools / Poetry / `uv lock`?
# - The chunker image is a *strict subset* of the project's full dep tree
#   (no fastembed/lancedb/mcp/pyarrow/numpy). uv.lock locks the full project
#   set, so pulling from it would over-install.
# - `uv pip compile` reads a flat `.in` requirements file and produces a fully-
#   pinned, transitively-resolved `.txt` — same model as pip-tools, but without
#   needing a separate Python toolchain to be installed.
#
# Targeted Python: 3.12 (matches public.ecr.aws/lambda/python:3.12).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

command -v uv >/dev/null || {
    echo "ERROR: uv not found on PATH. Install: curl -LsSf https://astral.sh/uv/install.sh | sh" >&2
    exit 1
}

for variant in chunker embedder; do
    in_file="lambda/requirements-${variant}.in"
    out_file="lambda/requirements-${variant}.txt"
    [ -f "$in_file" ] || { echo "ERROR: $in_file missing" >&2; exit 1; }
    echo "==> Compiling $out_file"
    uv pip compile "$in_file" \
        --python-version 3.12 \
        --output-file "$out_file" \
        --quiet
done

echo "==> Done. Commit lambda/requirements-{chunker,embedder}.txt."
