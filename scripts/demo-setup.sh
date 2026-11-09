#!/usr/bin/env bash
# Bash entry point for the OneBucket RAG Tiering Demo setup.
#
# Forwards all arguments to scripts/demo_setup.py via `uv run`. Set as the
# single command an operator runs after the OneBucket console is
# provisioned. Idempotent — safe to re-run.
#
# Common invocations:
#     bash scripts/demo-setup.sh
#     bash scripts/demo-setup.sh --force-rebuild
#     bash scripts/demo-setup.sh --skip-daemon
#     bash scripts/demo-setup.sh --tickers AAPL,MSFT
#
# Run `bash scripts/demo-setup.sh --help` for the full flag list.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

if ! command -v uv >/dev/null 2>&1; then
    echo "FATAL: 'uv' not found on PATH." >&2
    echo "  → Install from https://docs.astral.sh/uv/ then re-run." >&2
    exit 2
fi

# Move fastembed cache off /tmp so macOS reboots don't wipe ONNX weights.
# Honors any pre-existing FASTEMBED_CACHE_PATH the operator may have set.
export FASTEMBED_CACHE_PATH="${FASTEMBED_CACHE_PATH:-${HOME}/.cache/fastembed}"
mkdir -p "${FASTEMBED_CACHE_PATH}"

cd "${REPO_ROOT}"
exec uv run python scripts/demo_setup.py "$@"
