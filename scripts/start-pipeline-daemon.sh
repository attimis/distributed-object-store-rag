#!/usr/bin/env bash
# OneBucket pipeline daemon — launcher for the "local-daemon" execution mode.
#
# Thin shell wrapper around scripts/oneb_pipeline_daemon.py. Validates the
# environment before launching the daemon so failures surface as friendly
# errors instead of opaque Python tracebacks.
#
# Per ADR-013 (Dual-Mode Execution Strategy): this is the entry point for
# "local-daemon" execution mode. It does not replace the AWS Lambda chain;
# see the runbook for the Single-Active-Mode rule.
#
# Usage:
#   bash scripts/start-pipeline-daemon.sh
#   bash scripts/start-pipeline-daemon.sh --poll-interval 10
#
# Required env:
#   AWS_PROFILE              — AWS creds source (or AWS access-key vars)
#   ONEBUCKET_CHUNK_ROOT     — s3://<bucket>/chunks
#   ONEBUCKET_DATASET_ROOT   — s3://<bucket>/metadata

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# --- Validate uv binary. Without uv the daemon
#     can't find its dependencies; bail early with an install directive. ---
if ! command -v uv >/dev/null 2>&1; then
    echo "ERROR: 'uv' is not on PATH." >&2
    echo "Install it via:" >&2
    echo "  curl -LsSf https://astral.sh/uv/install.sh | sh" >&2
    echo "Then re-run this script." >&2
    exit 1
fi

# --- Validate AWS creds source ---
if [ -z "${AWS_PROFILE:-}" ] && [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
    echo "ERROR: neither AWS_PROFILE nor AWS_ACCESS_KEY_ID is set." >&2
    echo "Set AWS_PROFILE to your configured profile name, e.g.:" >&2
    echo "  export AWS_PROFILE=onebucket-demo" >&2
    exit 1
fi

# --- Validate OneBucket env vars ---
if [ -z "${ONEBUCKET_CHUNK_ROOT:-}" ]; then
    echo "ERROR: ONEBUCKET_CHUNK_ROOT is not set." >&2
    echo "Set it to the s3:// URI for the chunks prefix, e.g.:" >&2
    echo "  export ONEBUCKET_CHUNK_ROOT=s3://my-bucket/chunks" >&2
    exit 1
fi
if [ -z "${ONEBUCKET_DATASET_ROOT:-}" ]; then
    echo "ERROR: ONEBUCKET_DATASET_ROOT is not set." >&2
    echo "Set it to the s3:// URI for the LanceDB metadata, e.g.:" >&2
    echo "  export ONEBUCKET_DATASET_ROOT=s3://my-bucket/metadata" >&2
    exit 1
fi

# --- Validate the URIs are s3:// shaped (not file:// or local paths) ---
case "$ONEBUCKET_CHUNK_ROOT" in
    s3://*) ;;
    *)
        echo "ERROR: ONEBUCKET_CHUNK_ROOT must be an s3:// URI (got $ONEBUCKET_CHUNK_ROOT)" >&2
        exit 1
        ;;
esac
case "$ONEBUCKET_DATASET_ROOT" in
    s3://*) ;;
    *)
        echo "ERROR: ONEBUCKET_DATASET_ROOT must be an s3:// URI (got $ONEBUCKET_DATASET_ROOT)" >&2
        exit 1
        ;;
esac

# --- Sanity print before launch ---
echo "==> Starting OneBucket pipeline daemon"
echo "    AWS_PROFILE         = ${AWS_PROFILE:-<not set; using AWS_ACCESS_KEY_ID>}"
echo "    ONEBUCKET_CHUNK_ROOT  = $ONEBUCKET_CHUNK_ROOT"
echo "    ONEBUCKET_DATASET_ROOT= $ONEBUCKET_DATASET_ROOT"
echo "    poll-interval         = ${ONEBUCKET_DAEMON_POLL_INTERVAL:-5}s (env override available)"
echo ""
echo "Watch this terminal for [idle] / [processing <id>] / [health] log lines."
echo "Stop with Ctrl-C; the heartbeat file is cleaned up automatically."
echo ""

# --- Launch via exec so SIGTERM / Ctrl-C propagates cleanly to the Python
#     process. Without exec, the shell
#     intermediates signal delivery and the heartbeat may be orphaned on
#     forced termination. ---
exec uv run python "${REPO_ROOT}/scripts/oneb_pipeline_daemon.py" "$@"
