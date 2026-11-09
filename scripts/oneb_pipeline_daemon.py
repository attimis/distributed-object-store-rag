#!/usr/bin/env python3
"""OneBucket pipeline daemon — the ``local-daemon`` execution mode.

Long-running process that polls ``s3://<bucket>/raw/`` for new files and
runs the chunker + embedder pipeline locally on each one. It is one of the
execution modes alongside the AWS Lambda chain.

This is the local-mode "third-party pipeline" architecturally: it watches
the bucket and reacts. Same pattern Unstructured / LlamaIndex / a user's
own service would inhabit. ADR-013 covers the dual-mode coexistence rules.

This polling loop is the alternative to the event consumer
(``scripts/oneb_event_consumer.py``, the ``onebucket-events`` mode): use it
against plain S3, or when you do not want to sign in to the OneBucket events
MCP server. Both share the same heartbeat contract and call the same pipeline
functions; only the trigger source differs, and the surrounding scaffolding
(heartbeat, processing loop, state model) is intentionally
trigger-source-agnostic.

Usage:
    bash scripts/start-pipeline-daemon.sh
    # or directly:
    uv run python scripts/oneb_pipeline_daemon.py \\
        --bucket onebucket-demo-... \\
        --poll-interval 5

Required env (validated by the shell wrapper):
    AWS_PROFILE              — AWS creds source
    ONEBUCKET_CHUNK_ROOT     — s3://<bucket>/chunks (informs the daemon
                                which bucket/prefix to watch)
    ONEBUCKET_DATASET_ROOT   — s3://<bucket>/metadata (for LanceDB writes)
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import logging
import os
import signal
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

# Ensure src/ is on path so the script works regardless of cwd.
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from onebucket_demo.constants import MAX_FILE_BYTES  # noqa: E402
from onebucket_demo.gates import check_type  # noqa: E402
from onebucket_demo.pipeline import (  # noqa: E402
    EmptySource,
    chunk_and_write,
    embed_and_commit,
)


HEARTBEAT_INTERVAL_SECONDS = 10.0
HEALTH_LOG_EVERY_N_POLLS = 10
DEFAULT_HEARTBEAT_PATH = "/tmp/oneb-pipeline-daemon.heartbeat"


# ---------------------------------------------------------------------------
# Logging helpers — explicit IDLE / PROCESSING / HEALTH state tags so the
# demo terminal makes pipeline activity legible: every poll cycle logs
# state; every Nth poll also emits [health] confirming S3 reachability,
# so a silently stuck daemon is distinguishable from an idle one.
# ---------------------------------------------------------------------------


def _ts() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(tag: str, msg: str) -> None:
    print(f"[{_ts()}] [{tag}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Daemon state — held in a single dataclass-shaped dict so heartbeat writes
# can serialize it directly. processed_set is the in-memory cache of which
# raw files we've already produced manifests for; reconstructed from S3 on
# startup so the daemon is fully stateless across restarts.
# ---------------------------------------------------------------------------


class _DaemonState:
    def __init__(self, *, bucket: str, poll_interval: float) -> None:
        self.bucket = bucket
        self.poll_interval = poll_interval
        self.started_at = _ts()
        self.pid = os.getpid()
        self.processed_set: set[str] = set()  # raw keys already manifested
        self.daemon_state: str = "idle"  # "idle" | "processing"
        self.current_raw_file_id: str | None = None
        self.processed_count: int = 0
        self.poll_counter: int = 0

    def heartbeat_dict(self) -> dict:
        return {
            "pid": self.pid,
            "started_at": self.started_at,
            "last_seen": _ts(),
            "mode": "local-daemon",
            "poll_interval_seconds": self.poll_interval,
            "bucket": f"s3://{self.bucket}",
            "daemon_state": self.daemon_state,
            "current_raw_file_id": self.current_raw_file_id,
            "processed_count": self.processed_count,
        }


# ---------------------------------------------------------------------------
# Heartbeat write. World-readable (0o666) so the MCP server can read it even
# if the daemon runs under a different UID/GID. Atomic via tempfile + rename
# so a reader never sees a half-written file.
# ---------------------------------------------------------------------------


def _write_heartbeat(path: str, state: _DaemonState) -> None:
    data = json.dumps(state.heartbeat_dict(), sort_keys=True) + "\n"
    parent = Path(path).parent
    parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", dir=parent, delete=False, prefix=".heartbeat-tmp-"
    ) as tmp:
        tmp.write(data)
        tmp_path = tmp.name
    os.replace(tmp_path, path)
    os.chmod(path, 0o666)


def _delete_heartbeat(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


# ---------------------------------------------------------------------------
# S3 scan — uses the boto3 paginator so buckets with >1000 objects still
# work (a single list_objects_v2 call silently truncates at 1000). Returns the set of all raw_keys
# under raw/ (plus the existing chunks/<hash>/MANIFEST.json keys for the
# processed-set reconstruction).
# ---------------------------------------------------------------------------


def _list_raw_keys(s3, bucket: str) -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix="raw/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Skip the "raw/" folder marker itself and any sub-prefix markers
            if key == "raw/" or key.endswith("/"):
                continue
            keys.append(key)
    return keys


def _processed_raw_keys(s3, bucket: str) -> set[str]:
    """Rebuild the processed set by scanning chunks/ for MANIFEST.json keys and
    extracting each one's ``raw_object_key`` field.

    Critical design note: the daemon's poll loop filters new files by comparing
    S3 raw/ keys (filenames like ``raw/aapl-10k-1778066541.txt``) against this
    set. So this set MUST contain raw_object_keys (filenames), not
    raw_file_ids (content hashes). Earlier versions of this function returned
    hashes which never matched the filename namespace, causing the daemon to
    reprocess every raw file on every startup.

    Costs one S3 GET per existing manifest at startup. Bounded by document
    count, not by poll frequency. For demo-scale corpora (<1000 docs) this
    is negligible.
    """
    paginator = s3.get_paginator("list_objects_v2")
    manifest_keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix="chunks/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/MANIFEST.json"):
                manifest_keys.append(key)

    seen: set[str] = set()
    for mkey in manifest_keys:
        try:
            body = s3.get_object(Bucket=bucket, Key=mkey)["Body"].read()
            data = json.loads(body.decode("utf-8"))
        except (ClientError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            _log("warning", f"could not read manifest {mkey}: {exc}; skipping")
            continue
        raw_object_key = data.get("raw_object_key")
        if isinstance(raw_object_key, str) and raw_object_key:
            seen.add(raw_object_key)
        else:
            _log(
                "warning",
                f"manifest {mkey} has missing or invalid raw_object_key; "
                "the corresponding raw file (if any) will be reprocessed",
            )
    return seen


# ---------------------------------------------------------------------------
# Dual-mode collision guard. If S3 already has a Lambda notification configured on raw/, both the Lambda
# chain and this daemon will fire on every upload. Log a WARNING; do not
# refuse to start (the user may be deliberately testing both).
# ---------------------------------------------------------------------------


def _check_for_lambda_collision(s3, bucket: str) -> None:
    try:
        config = s3.get_bucket_notification_configuration(Bucket=bucket)
    except ClientError as exc:
        _log("warning", f"could not read bucket notification config: {exc}")
        return
    lambda_configs = config.get("LambdaFunctionConfigurations", [])
    for entry in lambda_configs:
        filter_rules = (
            entry.get("Filter", {}).get("Key", {}).get("FilterRules", [])
        )
        for rule in filter_rules:
            if rule.get("Name", "").lower() == "prefix" and rule.get(
                "Value", ""
            ).startswith("raw"):
                _log(
                    "warning",
                    "An active Lambda notification is configured on the raw/ "
                    "prefix of this bucket. Running this daemon AND the Lambda "
                    "chain simultaneously will cause both to process every upload "
                    "(dual-processing). Per ADR-013, only one execution mode "
                    "should be active per bucket at a time. Either remove the S3 "
                    "notification or stop this daemon.",
                )
                return


# ---------------------------------------------------------------------------
# Processing — calls the shared pipeline functions. Same code path as the
# Lambda handlers (lambda_handler_chunker / lambda_handler_embedder). This
# function does NOT re-implement type/size gates
# — chunk_and_write calls them via the gates module already, but we mirror
# the Lambda handler's pre-checks for early-skip behavior parity.
# ---------------------------------------------------------------------------


def _download_to_tmp(s3, bucket: str, key: str) -> Path:
    local_name = Path(key).name
    local_path = Path(tempfile.gettempdir()) / local_name
    s3.download_file(bucket, key, str(local_path))
    return local_path


def _process_one(
    s3, bucket: str, raw_key: str, chunk_root: str, dataset_root: str
) -> str | None:
    """Run chunker + embedder for one raw file. Returns the raw_file_id on
    success; None if skipped/failed. Logs each phase."""
    # Pre-gates mirror the Lambda handler so behavior is parity-identical.
    pseudo_path = Path(raw_key)
    ok, reason = check_type(pseudo_path)
    if not ok:
        _log("skip", f"{raw_key}: {reason}")
        return None
    head = s3.head_object(Bucket=bucket, Key=raw_key)
    size_bytes = int(head.get("ContentLength", 0))
    if size_bytes > MAX_FILE_BYTES:
        _log(
            "skip",
            f"{raw_key}: file too large ({size_bytes} bytes > {MAX_FILE_BYTES})",
        )
        return None

    local_path = _download_to_tmp(s3, bucket, raw_key)
    source_uri = f"s3://{bucket}/{raw_key}"
    try:
        manifest = chunk_and_write(
            local_path,
            source_uri=source_uri,
            raw_object_key=raw_key,
            chunk_root=chunk_root,
        )
    except EmptySource:
        _log("skip", f"{raw_key}: empty source")
        return None

    raw_file_id = manifest.raw_file_id
    _log(
        "chunked",
        f"{raw_key} → raw_file_id={raw_file_id} chunks={manifest.n_chunks}",
    )

    manifest_uri = f"{chunk_root.rstrip('/')}/{raw_file_id}/MANIFEST.json"
    result = embed_and_commit(
        manifest_uri,
        dataset_root=dataset_root,
        chunk_root=chunk_root,
    )
    _log(
        "embedded",
        f"raw_file_id={raw_file_id} rows_committed={result.n_committed}",
    )
    return raw_file_id


# ---------------------------------------------------------------------------
# Main loop. Sequential processing, one file at a time. The heartbeat is
# rewritten on every iteration so it is always recent enough for the MCP
# server's 30-second staleness check.
# ---------------------------------------------------------------------------


def _main_loop(
    bucket: str,
    chunk_root: str,
    dataset_root: str,
    heartbeat_path: str,
    poll_interval: float,
) -> None:
    s3 = boto3.client("s3")

    # Startup checks
    _log("startup", f"watching s3://{bucket}/raw/ every {poll_interval}s")
    _check_for_lambda_collision(s3, bucket)

    state = _DaemonState(bucket=bucket, poll_interval=poll_interval)

    # Rebuild processed set from S3 — daemon is fully stateless across restarts.
    # The set contains raw_object_keys (filenames), reconstructed by reading
    # each existing MANIFEST.json and extracting its raw_object_key field.
    # This matches the namespace of the keys we'll see when listing raw/ in
    # the poll loop below, so the membership check is a direct string compare.
    state.processed_set = _processed_raw_keys(s3, bucket)
    state.processed_count = len(state.processed_set)
    _log(
        "startup",
        f"reconstructed processed set: {state.processed_count} raw files "
        f"already manifested in chunks/",
    )

    # SIGTERM / Ctrl-C handler — delete heartbeat and exit cleanly.
    def _shutdown(signum, frame):  # noqa: ARG001
        _log("shutdown", f"received signal {signum}; cleaning up heartbeat")
        _delete_heartbeat(heartbeat_path)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    while True:
        state.poll_counter += 1
        state.daemon_state = "idle"
        state.current_raw_file_id = None
        _write_heartbeat(heartbeat_path, state)

        # Health log every Nth cycle
        if state.poll_counter % HEALTH_LOG_EVERY_N_POLLS == 0:
            _log(
                "health",
                f"s3_reachable=ok bucket=s3://{bucket} "
                f"processed_count={state.processed_count} "
                f"poll_counter={state.poll_counter}",
            )

        # Find new raw files
        try:
            raw_keys = _list_raw_keys(s3, bucket)
        except ClientError as exc:
            _log("error", f"list_objects_v2 failed: {exc}")
            _log("idle", f"sleeping {poll_interval}s before retry")
            time.sleep(poll_interval)
            continue

        # Filter out raw keys we've already processed. `state.processed_set`
        # contains raw_object_keys (filenames) reconstructed from existing
        # manifests at startup (and incrementally appended after each
        # successful process in this loop). Same namespace as the keys
        # we just listed, so this is a direct string-membership check.
        new_keys = [k for k in raw_keys if k not in state.processed_set]
        if new_keys:
            _log("found", f"{len(new_keys)} new raw file(s)")

        for raw_key in new_keys:
            state.daemon_state = "processing"
            state.current_raw_file_id = None  # not known until chunk_and_write
            _write_heartbeat(heartbeat_path, state)
            _log("processing", f"{raw_key}")
            try:
                raw_file_id = _process_one(
                    s3, bucket, raw_key, chunk_root, dataset_root
                )
            except Exception as exc:  # noqa: BLE001 — log and continue
                _log("error", f"processing {raw_key} failed: {exc}")
                state.processed_set.add(raw_key)  # don't retry forever
                continue
            if raw_file_id is not None:
                state.processed_count += 1
                state.current_raw_file_id = raw_file_id
                _write_heartbeat(heartbeat_path, state)
            # Mark the raw key as seen (whether or not it produced a manifest)
            state.processed_set.add(raw_key)
            state.daemon_state = "idle"
            state.current_raw_file_id = None
            _write_heartbeat(heartbeat_path, state)

        if not new_keys:
            _log("idle", f"no new files; sleeping {poll_interval}s")
        time.sleep(poll_interval)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="oneb-pipeline-daemon")
    parser.add_argument(
        "--bucket",
        default=None,
        help="S3 bucket name. If omitted, derives from ONEBUCKET_CHUNK_ROOT.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=float(os.environ.get("ONEBUCKET_DAEMON_POLL_INTERVAL", "5")),
        help="Seconds between S3 polls (default 5).",
    )
    parser.add_argument(
        "--heartbeat-file",
        default=os.environ.get(
            "ONEBUCKET_DAEMON_HEARTBEAT_PATH", DEFAULT_HEARTBEAT_PATH
        ),
        help=(
            "Path to write the heartbeat JSON. "
            "Defaults to /tmp/oneb-pipeline-daemon.heartbeat (or "
            "$ONEBUCKET_DAEMON_HEARTBEAT_PATH if set)."
        ),
    )
    return parser.parse_args()


def _derive_bucket_from_env() -> str:
    chunk_root = os.environ.get("ONEBUCKET_CHUNK_ROOT", "")
    if not chunk_root.startswith("s3://"):
        raise RuntimeError(
            "Cannot derive bucket: ONEBUCKET_CHUNK_ROOT is not set to an "
            "s3:// URI. Set it (e.g., s3://my-bucket/chunks) or pass --bucket."
        )
    return urlparse(chunk_root).netloc


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()

    bucket = args.bucket or _derive_bucket_from_env()
    chunk_root = os.environ.get("ONEBUCKET_CHUNK_ROOT")
    dataset_root = os.environ.get("ONEBUCKET_DATASET_ROOT")
    if not chunk_root or not dataset_root:
        _log(
            "error",
            "ONEBUCKET_CHUNK_ROOT and ONEBUCKET_DATASET_ROOT must both be set. "
            "Run scripts/start-pipeline-daemon.sh to validate env.",
        )
        return 2

    try:
        _main_loop(
            bucket=bucket,
            chunk_root=chunk_root,
            dataset_root=dataset_root,
            heartbeat_path=args.heartbeat_file,
            poll_interval=args.poll_interval,
        )
    except KeyboardInterrupt:
        _delete_heartbeat(args.heartbeat_file)
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
