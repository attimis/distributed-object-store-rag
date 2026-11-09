#!/usr/bin/env bash
# Start the OneBucket event consumer (the `onebucket-events` execution mode).
#
# This wrapper validates the environment, fills in the global OneBucket
# hostnames for anything you did not set, and then runs
# scripts/oneb_event_consumer.py in the foreground. Run exactly one pipeline
# consumer per OneBucket at a time: this one OR scripts/start-pipeline-daemon.sh.
#
# Usage:
#   bash scripts/start-event-consumer.sh
#   bash scripts/start-event-consumer.sh --catchup-on-start   # also process raw/ objects uploaded while down
#
# Before the first run, sign in once:  uv run python scripts/onebucket-login.py
#
# Environment:
#   ONEBUCKET_CHUNK_ROOT     required  s3://<onebucket>/chunks
#   ONEBUCKET_DATASET_ROOT   required  s3://<onebucket>/metadata
#   AWS_PROFILE or AWS_ACCESS_KEY_ID   required  the S3 credentials for OneBucket
#   ONEBUCKET_EVENTS_URL     optional  default https://events.onebucket.io (global);
#                                      use https://events.<region>.onebucket.io to pin a
#                                      region: list at https://www.onebucket.io/support
#   AWS_ENDPOINT_URL         optional  default https://s3.onebucket.io (global S3 endpoint)
#   ONEBUCKET_TOKEN_FILE     optional  default ~/.config/onebucket/token.json
#   ONEBUCKET_EVENT_LOG_PATH optional  default <repo>/.runbook-state/events.jsonl

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if ! command -v uv >/dev/null 2>&1; then
    echo "ERROR: 'uv' is not on PATH. Install: curl -LsSf https://astral.sh/uv/install.sh | sh" >&2
    exit 1
fi

for var in ONEBUCKET_CHUNK_ROOT ONEBUCKET_DATASET_ROOT; do
    value="${!var:-}"
    if [ -z "$value" ]; then
        echo "ERROR: $var is not set (e.g. export $var=s3://my-onebucket/${var#ONEBUCKET_}" | tr '[:upper:]' '[:lower:]' >&2
        exit 1
    fi
    case "$value" in s3://*) ;; *) echo "ERROR: $var must be an s3:// URI (got $value)" >&2; exit 1 ;; esac
done

if [ -z "${AWS_PROFILE:-}" ] && [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
    echo "ERROR: neither AWS_PROFILE nor AWS_ACCESS_KEY_ID is set; the consumer needs S3 credentials for OneBucket." >&2
    exit 1
fi

# This mode is OneBucket-specific, so the global hostnames are the right
# default for anything left unset. Regional hostnames are listed on the
# support page; set the variables explicitly to pin one.
if [ -z "${ONEBUCKET_EVENTS_URL:-}" ]; then
    export ONEBUCKET_EVENTS_URL="https://events.onebucket.io"
    echo "[info] ONEBUCKET_EVENTS_URL not set; using the global endpoint $ONEBUCKET_EVENTS_URL"
fi
if [ -z "${AWS_ENDPOINT_URL:-}" ]; then
    export AWS_ENDPOINT_URL="https://s3.onebucket.io"
    echo "[info] AWS_ENDPOINT_URL not set; using the global S3 endpoint $AWS_ENDPOINT_URL"
fi
case "$ONEBUCKET_EVENTS_URL" in https://*) ;; *) echo "ERROR: ONEBUCKET_EVENTS_URL must be https:// (got $ONEBUCKET_EVENTS_URL)" >&2; exit 1 ;; esac

TOKEN_FILE="${ONEBUCKET_TOKEN_FILE:-$HOME/.config/onebucket/token.json}"
if [ ! -s "$TOKEN_FILE" ]; then
    echo "ERROR: no OneBucket token at $TOKEN_FILE." >&2
    echo "       Sign in once with:  uv run python scripts/onebucket-login.py" >&2
    exit 1
fi

echo "==> Starting OneBucket event consumer (onebucket-events mode)"
echo "    ONEBUCKET_EVENTS_URL     = $ONEBUCKET_EVENTS_URL"
echo "    AWS_ENDPOINT_URL         = $AWS_ENDPOINT_URL"
echo "    AWS_PROFILE              = ${AWS_PROFILE:-<not set; using AWS_ACCESS_KEY_ID>}"
echo "    ONEBUCKET_CHUNK_ROOT     = $ONEBUCKET_CHUNK_ROOT"
echo "    ONEBUCKET_DATASET_ROOT   = $ONEBUCKET_DATASET_ROOT"
echo "    token file               = $TOKEN_FILE"
echo "    event log                = ${ONEBUCKET_EVENT_LOG_PATH:-<repo>/.runbook-state/events.jsonl}"
echo
echo "Watch for [connected] / [batch] / [processing] / [chunked] / [embedded] lines."
echo "Catch-up scan from another terminal:  kill -USR1 \$(pgrep -f oneb_event_consumer.py)"
echo "Stop with Ctrl-C; the heartbeat file is removed on exit."
echo

# exec so Ctrl-C / SIGTERM reach the Python process directly.
exec uv run python "${REPO_ROOT}/scripts/oneb_event_consumer.py" "$@"
