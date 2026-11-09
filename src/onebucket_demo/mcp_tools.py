"""The MCP tools beyond search and fetch: everything an agent needs to run
the demo from natural language alone.

Ingest and status
  * :func:`ingest_document`  fetch a document (an SEC ticker's latest 10-K,
    any URL, or a local path), convert HTML to text, upload it to ``raw/``.
    Whatever execution mode is active picks it up from there.
  * :func:`check_ingest_status`  where a document is in the pipeline
    (uploaded, chunking, embedding, ready), read from S3 and the index.
  * :func:`list_documents`  what is in the index, with chunk counts.

Telemetry (read from the event consumer's JSONL log)
  * :func:`get_replication_metrics`  per-peer write latency for one ingest.
  * :func:`get_chunk_read_metrics`  chunk fetch latency in a time window.

Deployment config (``variables.yml``)
  * :func:`get_runbook_config`, :func:`set_runbook_variable`,
    :func:`validate_runbook_config`.

Destructive, for demo recovery
  * :func:`delete_document`, :func:`purge_peer_cache`, :func:`rebuild_index`.
    Each is strict about its preconditions and refuses on any inconsistency
    rather than muddling through.

AWS credentials resolve through boto3's standard chain; the Claude Desktop
``env`` block sets ``AWS_PROFILE`` and ``AWS_ENDPOINT_URL`` so the server
talks to the right S3 endpoint with the right key.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import boto3
import html2text
import httpx
from botocore.exceptions import ClientError

from . import runbook_config, search
from .storage import TABLE_NAME, connect

logger = logging.getLogger(__name__)

# A document id is the first 16 hex characters of the SHA-256 of its text.
# Every tool that puts an id into an S3 key or a LanceDB predicate checks it
# against this pattern first.
_RAW_FILE_ID_RE = re.compile(r"[0-9a-f]{16}")


SEC_USER_AGENT = os.environ.get(
    "ONEBUCKET_SEC_USER_AGENT", "OneBucket-Demo onebucket@example.com"
)

# Execution modes. The MCP server never chunks or embeds anything itself: it
# uploads to raw/ and something else picks the file up. Which "something"
# is the execution mode: the AWS Lambda chain (``lambda``), the polling
# daemon (``local-daemon``) or the OneBucket event consumer
# (``onebucket-events``). All three do the same S3 PUT; the two local modes
# additionally check the consumer's heartbeat before accepting an upload,
# because an upload nobody is watching would silently never be indexed.
EXECUTION_MODE_LOCAL_DAEMON = "local-daemon"
EXECUTION_MODE_LAMBDA = "lambda"
EXECUTION_MODE_ONEBUCKET_EVENTS = "onebucket-events"
DEFAULT_EXECUTION_MODE = EXECUTION_MODE_LOCAL_DAEMON

# Past this many seconds without a heartbeat update the consumer is assumed dead.
DAEMON_HEARTBEAT_STALE_AFTER_SECONDS = 30

DEFAULT_DAEMON_HEARTBEAT_PATH = "/tmp/oneb-pipeline-daemon.heartbeat"


def _execution_mode() -> str:
    mode = os.environ.get("ONEBUCKET_EXECUTION_MODE", DEFAULT_EXECUTION_MODE)
    if mode not in (
        EXECUTION_MODE_LOCAL_DAEMON,
        EXECUTION_MODE_LAMBDA,
        EXECUTION_MODE_ONEBUCKET_EVENTS,
    ):
        raise RuntimeError(
            f"ONEBUCKET_EXECUTION_MODE={mode!r} is invalid. "
            f"Must be one of: {EXECUTION_MODE_LOCAL_DAEMON!r}, "
            f"{EXECUTION_MODE_LAMBDA!r}, "
            f"{EXECUTION_MODE_ONEBUCKET_EVENTS!r}."
        )
    return mode


def _daemon_heartbeat_path() -> str:
    return os.environ.get(
        "ONEBUCKET_DAEMON_HEARTBEAT_PATH", DEFAULT_DAEMON_HEARTBEAT_PATH
    )


def _read_heartbeat() -> dict | None:
    """Read and parse the daemon heartbeat file. Returns None on missing/invalid."""
    path = _daemon_heartbeat_path()
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def _daemon_heartbeat_fresh(now: _dt.datetime | None = None) -> tuple[bool, dict | None]:
    """Check whether the daemon heartbeat exists and is recent.

    Returns (is_fresh, heartbeat_dict_or_None). Caller can use the dict to
    inspect the daemon's reported bucket / state / current_raw_file_id.
    """
    hb = _read_heartbeat()
    if hb is None:
        return False, None
    last_seen_str = hb.get("last_seen")
    if not last_seen_str:
        return False, hb
    try:
        # ISO-8601 with trailing Z — interpret as UTC.
        last_seen = _dt.datetime.strptime(
            last_seen_str, "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=_dt.timezone.utc)
    except ValueError:
        return False, hb
    now = now or _dt.datetime.now(_dt.timezone.utc)
    age_seconds = (now - last_seen).total_seconds()
    return age_seconds <= DAEMON_HEARTBEAT_STALE_AFTER_SECONDS, hb


@dataclass(frozen=True)
class _BucketLayout:
    """Derive bucket name + S3 prefixes + DLQ name from the env vars the
    Claude Desktop config provides. We accept either ``s3://bucket/raw`` style
    full URIs or ``s3://bucket/chunks`` and infer siblings — same convention
    as the Lambda env vars.
    """

    bucket: str
    raw_prefix: str  # e.g. "raw"
    chunks_prefix: str  # e.g. "chunks"
    metadata_root: str  # e.g. "s3://bucket/metadata"
    region: str
    embedder_dlq_name: str  # e.g. "onebucket-embedder-dlq"


def _bucket_layout() -> _BucketLayout:
    chunk_root = os.environ.get("ONEBUCKET_CHUNK_ROOT", "")
    dataset_root = os.environ.get("ONEBUCKET_DATASET_ROOT", "")
    raw_root = os.environ.get(
        "ONEBUCKET_RAW_ROOT",
        # Default: replace "/chunks" with "/raw" in the chunk_root URI
        chunk_root.replace("/chunks", "/raw") if chunk_root else "",
    )
    if not raw_root.startswith("s3://"):
        raise RuntimeError(
            "ingest_document requires ONEBUCKET_RAW_ROOT or ONEBUCKET_CHUNK_ROOT "
            "set to an s3:// URI in the Claude Desktop config; got "
            f"raw_root={raw_root!r}"
        )
    parsed = urlparse(raw_root)
    bucket = parsed.netloc
    raw_prefix = parsed.path.lstrip("/")
    chunks_prefix = (
        urlparse(chunk_root).path.lstrip("/")
        if chunk_root.startswith("s3://")
        else "chunks"
    )
    region = os.environ.get("AWS_REGION") or os.environ.get(
        "AWS_DEFAULT_REGION", "us-east-1"
    )
    # Convention from scripts/aws_deploy.py: ${LAMBDA_PREFIX}-embedder-dlq
    lambda_prefix = os.environ.get("ONEBUCKET_LAMBDA_PREFIX", "onebucket")
    return _BucketLayout(
        bucket=bucket,
        raw_prefix=raw_prefix,
        chunks_prefix=chunks_prefix,
        metadata_root=dataset_root,
        region=region,
        embedder_dlq_name=f"{lambda_prefix}-embedder-dlq",
    )


# -----------------------------------------------------------------------
# ingest_document
# -----------------------------------------------------------------------


_TICKER_PATTERN = re.compile(r"^[A-Z]{1,5}$")


def _is_ticker(s: str) -> bool:
    return bool(_TICKER_PATTERN.fullmatch(s.strip().upper()))


def _is_url(s: str) -> bool:
    return s.startswith(("http://", "https://"))


def _looks_like_html(text: str) -> bool:
    head = text[:2000].lower()
    return "<html" in head or "<!doctype html" in head or "<body" in head


def _html_to_text(html: str) -> str:
    h = html2text.HTML2Text()
    h.ignore_links = True
    h.ignore_images = True
    h.body_width = 0
    text = h.handle(html)
    return re.sub(r"\n{3,}", "\n\n", text).strip() + "\n"


def _resolve_ticker_to_10k_url(ticker: str) -> tuple[str, str]:
    """Look up the ticker's CIK then return (most-recent-10-K URL, filing date)."""
    headers = {"User-Agent": SEC_USER_AGENT}
    with httpx.Client(timeout=30, headers=headers) as client:
        # Step 1: ticker → CIK
        r = client.get("https://www.sec.gov/files/company_tickers.json")
        r.raise_for_status()
        tickers = r.json()
        upper = ticker.upper()
        match = next(
            (c for c in tickers.values() if c["ticker"] == upper), None
        )
        if match is None:
            raise RuntimeError(f"ticker {ticker!r} not found in SEC EDGAR")
        cik = int(match["cik_str"])

        # Step 2: CIK → submissions JSON → most recent 10-K filing
        r = client.get(f"https://data.sec.gov/submissions/CIK{cik:010d}.json")
        r.raise_for_status()
        recent = r.json()["filings"]["recent"]
        for i, form in enumerate(recent["form"]):
            if form == "10-K":
                accession = recent["accessionNumber"][i].replace("-", "")
                primary = recent["primaryDocument"][i]
                date = recent["filingDate"][i]
                url = (
                    f"https://www.sec.gov/Archives/edgar/data/"
                    f"{cik}/{accession}/{primary}"
                )
                return url, date
        raise RuntimeError(
            f"no 10-K found for ticker {ticker!r} (cik={cik}) in recent filings"
        )


def _fetch_remote(url: str) -> bytes:
    headers = {"User-Agent": SEC_USER_AGENT}
    with httpx.Client(timeout=60, headers=headers, follow_redirects=True) as client:
        r = client.get(url)
        r.raise_for_status()
        return r.content


def _safe_filename(stem: str, extension: str = "txt") -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-").lower()[:60]
    return f"{cleaned or 'doc'}.{extension}"


def ingest_document(source: str) -> dict:
    """Ingest a document into the OneBucket pipeline by uploading to
    ``s3://<bucket>/raw/...``. What processes the upload depends on
    ``ONEBUCKET_EXECUTION_MODE``:

      * ``lambda``: an S3 notification fires the chunker Lambda, whose
        manifest fires the embedder Lambda.
      * ``local-daemon``: ``scripts/oneb_pipeline_daemon.py`` polls ``raw/``
        and runs both phases locally.
      * ``onebucket-events``: ``scripts/oneb_event_consumer.py`` learns of
        the upload from OneBucket's events server and runs both phases.

    All three return immediately; the agent polls ``check_ingest_status``.
    The two local modes first check the consumer's heartbeat and refuse to
    upload when nothing is listening, because an upload that is never
    processed is worse than a clear error.

    ``source`` is one of:
      * ticker symbol (``"AAPL"``): looks up most recent 10-K
      * full URL: fetched directly
      * local file path: read from disk

    Returns a dict ready for JSON serialization, including the
    ``raw_file_id`` (sha256[:16] of the uploaded text body — same hash the
    chunker computes, so callers can use it to scope future searches).
    """
    layout = _bucket_layout()

    # The two local modes need a live consumer process: check its heartbeat
    # (both consumers write the same file). Lambda mode is not checked here;
    # S3 notifications are AWS-managed and fire if the deploy wired them.
    mode = _execution_mode()
    daemon_currently_busy = False
    daemon_current_raw_file_id: str | None = None
    if mode in (EXECUTION_MODE_LOCAL_DAEMON, EXECUTION_MODE_ONEBUCKET_EVENTS):
        fresh, hb = _daemon_heartbeat_fresh()
        if not fresh:
            auth_state = (hb or {}).get("auth_state")
            if auth_state and auth_state != "ok":
                return {
                    "error": "DAEMON_AUTH_EXPIRED",
                    "message": (
                        f"The event consumer stopped because its OneBucket sign-in "
                        f"is no longer valid (auth_state={auth_state!r}). The upload "
                        "was NOT performed. Sign in again and restart it:\n"
                        "  uv run python scripts/onebucket-login.py\n"
                        "  bash scripts/start-event-consumer.sh\n"
                        "Then retry."
                    ),
                    "heartbeat_path": _daemon_heartbeat_path(),
                    "auth_state": auth_state,
                }
            return {
                "error": "DAEMON_NOT_RUNNING",
                "message": (
                    f"ONEBUCKET_EXECUTION_MODE={mode!r} but no recent daemon "
                    f"heartbeat found at {_daemon_heartbeat_path()!r}. "
                    "The upload was NOT performed. Start the consumer in a "
                    "separate terminal:\n"
                    "  bash scripts/start-pipeline-daemon.sh   # local-daemon mode\n"
                    "  bash scripts/start-event-consumer.sh    # onebucket-events mode\n"
                    "Then retry."
                ),
                "heartbeat_path": _daemon_heartbeat_path(),
                "heartbeat_present": hb is not None,
            }
        if (hb or {}).get("auth_state", "ok") != "ok":
            return {
                "error": "DAEMON_AUTH_EXPIRED",
                "message": (
                    "The event consumer's OneBucket sign-in has expired "
                    f"(auth_state={hb.get('auth_state')!r}); it will not process "
                    "uploads. The upload was NOT performed. Sign in again with "
                    "`uv run python scripts/onebucket-login.py` and restart the consumer."
                ),
                "heartbeat_path": _daemon_heartbeat_path(),
                "auth_state": hb.get("auth_state"),
            }
        # Refuse if the consumer watches a different bucket than the one this
        # server uploads to: the file would land and never be processed.
        daemon_bucket_uri = (hb or {}).get("bucket", "")
        configured_bucket_uri = f"s3://{layout.bucket}"
        if daemon_bucket_uri and daemon_bucket_uri != configured_bucket_uri:
            return {
                "error": "DAEMON_WATCHING_WRONG_BUCKET",
                "message": (
                    f"The local pipeline daemon is watching {daemon_bucket_uri!r}, "
                    f"but the MCP server is configured to upload to "
                    f"{configured_bucket_uri!r}. The upload was NOT performed. "
                    "Either restart the daemon with the correct bucket, or update "
                    "ONEBUCKET_CHUNK_ROOT and restart the MCP server."
                ),
                "daemon_watching": daemon_bucket_uri,
                "mcp_configured_for": configured_bucket_uri,
            }
        # Consumer is fresh and on the right bucket. Report whether it is busy
        # so the agent can tell the user the upload is queued behind another job.
        if (hb or {}).get("daemon_state") == "processing":
            daemon_currently_busy = True
            daemon_current_raw_file_id = (hb or {}).get("current_raw_file_id")

    s3 = boto3.client("s3", region_name=layout.region)

    source = source.strip()
    source_kind: str
    source_label: str

    if _is_ticker(source):
        url, filing_date = _resolve_ticker_to_10k_url(source)
        body_bytes = _fetch_remote(url)
        source_kind = "ticker"
        source_label = f"{source.upper()}-10k-{filing_date}"
        provenance_url = url
    elif _is_url(source):
        body_bytes = _fetch_remote(source)
        source_kind = "url"
        source_label = urlparse(source).path.rsplit("/", 1)[-1] or "remote-doc"
        provenance_url = source
    else:
        # Local path
        if not os.path.exists(source):
            raise RuntimeError(f"local path does not exist: {source}")
        with open(source, "rb") as fh:
            body_bytes = fh.read()
        source_kind = "path"
        source_label = os.path.basename(source)
        provenance_url = f"file://{os.path.abspath(source)}"

    # Convert HTML → text if needed; otherwise upload as-is (caller may have
    # already converted, or it might be plain markdown/txt).
    try:
        as_text = body_bytes.decode("utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        as_text = body_bytes.decode("latin-1", errors="replace")

    if _looks_like_html(as_text):
        text = _html_to_text(as_text)
    else:
        text = as_text

    body = text.encode("utf-8")
    raw_file_id = hashlib.sha256(body).hexdigest()[:16]
    filename = _safe_filename(source_label, "txt")
    raw_key = f"{layout.raw_prefix.rstrip('/')}/{filename}"

    s3.put_object(
        Bucket=layout.bucket,
        Key=raw_key,
        Body=body,
        ContentType="text/plain; charset=utf-8",
        Metadata={"source-kind": source_kind, "source-url": provenance_url[:1024]},
    )

    if mode == EXECUTION_MODE_LOCAL_DAEMON:
        status_msg = (
            "uploaded; daemon will process on next poll cycle "
            "(typical 5–10s for chunker, ~30s–4min for embedder depending on doc size)"
        )
    elif mode == EXECUTION_MODE_ONEBUCKET_EVENTS:
        status_msg = (
            "uploaded; the event consumer will pick up the OneBucket event on its "
            "next poll and dispatch (seconds to trigger; ~30s-4min total for chunker+embedder)"
        )
    else:  # EXECUTION_MODE_LAMBDA
        status_msg = (
            "uploaded; chunker Lambda will fire on the S3 event "
            "(typical 5–10s), embedder follows on manifest"
        )
    response: dict = {
        "raw_file_id": raw_file_id,
        "raw_key": raw_key,
        "raw_uri": f"s3://{layout.bucket}/{raw_key}",
        "source_kind": source_kind,
        "source_url": provenance_url,
        "size_bytes": len(body),
        "approx_tokens": len(body) // 4,
        "execution_mode": mode,
        "status": status_msg + ". Poll check_ingest_status with the returned "
        "raw_file_id to track progress.",
    }
    if mode in (EXECUTION_MODE_LOCAL_DAEMON, EXECUTION_MODE_ONEBUCKET_EVENTS):
        # Same shape for both local modes; both consumers write a compatible heartbeat.
        response["daemon_currently_busy"] = daemon_currently_busy
        if daemon_currently_busy and daemon_current_raw_file_id:
            response["daemon_current_raw_file_id"] = daemon_current_raw_file_id
    return response


# -----------------------------------------------------------------------
# check_ingest_status
# -----------------------------------------------------------------------


def check_ingest_status(raw_file_id: str) -> dict:
    """Poll the pipeline state for a single ``raw_file_id`` (the value
    returned by :func:`ingest_document`). Reports across S3, the embedder
    DLQ, and the LanceDB index. Cheap to call repeatedly.
    """
    import datetime as _dt

    if not raw_file_id or not _RAW_FILE_ID_RE.fullmatch(raw_file_id):
        raise RuntimeError(f"invalid raw_file_id: {raw_file_id!r}")

    layout = _bucket_layout()
    s3 = boto3.client("s3", region_name=layout.region)
    sqs = boto3.client("sqs", region_name=layout.region) if _execution_mode() == EXECUTION_MODE_LAMBDA else None

    chunks_prefix = f"{layout.chunks_prefix.rstrip('/')}/{raw_file_id}/"
    listed = s3.list_objects_v2(Bucket=layout.bucket, Prefix=chunks_prefix)
    chunk_count = 0
    manifest_present = False
    most_recent_modified: _dt.datetime | None = None
    for item in listed.get("Contents", []):
        key = item["Key"]
        if key.endswith("/MANIFEST.json"):
            manifest_present = True
        elif "/chunk_" in key:
            chunk_count += 1
        # Track the most recent S3 write under this prefix as a freshness signal:
        # a stalled ingest will have a stale LastModified; an actively-progressing
        # one (chunker writing chunks, or embedder commits triggering manifest
        # rewrites) will have something recent.
        modified = item.get("LastModified")
        if modified is not None and (
            most_recent_modified is None or modified > most_recent_modified
        ):
            most_recent_modified = modified

    # Embedder DLQ depth, account-wide (message bodies are not cheaply
    # queryable per document). Only lambda mode has a DLQ; in the local modes
    # the call would go to the S3 endpoint (AWS_ENDPOINT_URL applies to every
    # boto3 client) and fail, so it is skipped and reported as -1 (unknown).
    dlq_pending = -1
    if _execution_mode() == EXECUTION_MODE_LAMBDA:
        try:
            url = sqs.get_queue_url(QueueName=layout.embedder_dlq_name)["QueueUrl"]
            attrs = sqs.get_queue_attributes(
                QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"]
            )
            dlq_pending = int(attrs["Attributes"]["ApproximateNumberOfMessages"])
        except ClientError as e:
            logger.warning("DLQ check failed: %s", e)

    # LanceDB row count for this raw_file_id.
    #
    # checkout_latest() is essential here. By default, a Table object opened
    # via open_table() pins the manifest version at open time and never
    # auto-refreshes. In a long-running MCP server process, this means a
    # status check seconds after a writer commits will STILL return 0 rows
    # because the reader is pinned to the pre-commit manifest. We explicitly
    # advance to the latest manifest on every status read; this is a tiny S3
    # GET on the metadata pointer, so cost is negligible.
    rows_committed = 0
    try:
        db = connect(layout.metadata_root)
        table = db.open_table(TABLE_NAME)
        try:
            table.checkout_latest()
        except Exception as e:  # noqa: BLE001
            logger.debug("checkout_latest no-op or unavailable: %s", e)
        rows_committed = sum(
            1
            for r in table.search().where(
                f"raw_file_id = '{raw_file_id}'"
            ).limit(10000).to_list()
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("LanceDB count failed: %s", e)

    # Freshness signal — seconds since the most recent S3 write under this
    # raw_file_id's prefix. Useful for distinguishing "still progressing"
    # (chunks landed seconds ago) from "actually stalled" (no activity in
    # several minutes after embedder should have completed).
    last_activity_seconds_ago: int | None = None
    if most_recent_modified is not None:
        delta = _dt.datetime.now(_dt.timezone.utc) - most_recent_modified
        last_activity_seconds_ago = int(delta.total_seconds())

    if rows_committed > 0 and rows_committed >= chunk_count:
        phase = "ready"
    elif manifest_present and rows_committed == 0:
        phase = "embedding"
    elif chunk_count > 0 and not manifest_present:
        phase = "chunking"
    elif chunk_count == 0:
        phase = "uploading_or_chunker_pending"
    else:
        phase = "partial"  # chunks landed, manifest landed, but no rows yet

    return {
        "raw_file_id": raw_file_id,
        "chunk_count": chunk_count,
        "manifest_present": manifest_present,
        "rows_committed": rows_committed,
        # Account-wide DLQ depth. NOT a per-document failure count — a
        # non-zero value here may be left over from earlier debugging or
        # from other documents. Do NOT conclude this raw_file_id failed
        # based on dlq_pending alone; only a missing or stale row count
        # AFTER the expected wait time indicates a true failure.
        "dlq_pending": dlq_pending,
        "dlq_pending_note": (
            "Account-wide depth; not a failure signal for this raw_file_id "
            "specifically. Treat as informational unless rows_committed "
            "stays at 0 long after the expected ingest window AND "
            "last_activity_seconds_ago > 600."
        ),
        "last_activity_seconds_ago": last_activity_seconds_ago,
        "phase": phase,
        "ready": phase == "ready",
    }


# -----------------------------------------------------------------------
# Event-log metrics: get_replication_metrics, get_chunk_read_metrics
#
# Both read the JSONL event log the event consumer writes. One line per
# OneBucket event; every request shows up as one `gateway` record (what the
# client saw) plus one `backend` record per peer bucket the request reached,
# sharing `req_id`. Per-peer replication latency is therefore
# `backend.event_time - gateway.event_time` within a request.
# -----------------------------------------------------------------------


def _default_event_log_path() -> Path:
    """Match the consumer's resolution: $ONEBUCKET_EVENT_LOG_PATH or
    <repo>/.runbook-state/events.jsonl."""
    override = os.environ.get("ONEBUCKET_EVENT_LOG_PATH")
    if override:
        p = Path(override)
        if not p.is_absolute():
            p = Path(__file__).resolve().parent.parent.parent / p
        return p
    return Path(__file__).resolve().parent.parent.parent / ".runbook-state" / "events.jsonl"


def _parse_iso(s: str) -> _dt.datetime:
    """RFC3339 with any fractional precision, Z or offset."""
    s = s.strip().replace("Z", "+00:00")
    if "." in s:
        head, _, tail = s.partition(".")
        frac, sign, tz = tail.partition("+")
        if not sign:
            frac, sign, tz = tail.partition("-")
        frac = frac[:6].ljust(6, "0")
        s = f"{head}.{frac}{sign}{tz}" if sign else f"{head}.{frac}"
    return _dt.datetime.fromisoformat(s)


def _iter_event_log(log_path: Path):
    with log_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _stats(xs: list[int], *, extremes: bool = False) -> dict:
    if not xs:
        out = {"count": 0, "median_ms": None, "p95_ms": None}
        if extremes:
            out.update({"min_ms": None, "max_ms": None})
        return out
    s = sorted(xs)
    n = len(s)
    out = {"count": n, "median_ms": s[n // 2], "p95_ms": s[min(n - 1, int(n * 0.95))]}
    if extremes:
        out.update({"min_ms": s[0], "max_ms": s[-1]})
    return out


def _metadata_bucket() -> str:
    """The OneBucket the LanceDB index lives in (netloc of the dataset root)."""
    root = os.environ.get("ONEBUCKET_DATASET_ROOT", "")
    return urlparse(root).netloc if root.startswith("s3://") else ""


def get_replication_metrics(raw_file_id: str) -> dict:
    """Latencies observed by the event consumer while one document was
    ingested: end-to-end time, chunk write count, and per-peer acknowledgement
    latency for the writes the pipeline made.

    Only the most recent ingest of the document counts: the gateway
    ``s3:PutObject`` record whose ``dispatch_raw_file_id`` matches starts a
    new session and resets the accumulators.
    """
    if not raw_file_id or not _RAW_FILE_ID_RE.fullmatch(raw_file_id):
        raise RuntimeError(f"invalid raw_file_id: {raw_file_id!r}")

    log_path = _default_event_log_path()
    if not log_path.exists():
        return {"error": "EVENT_LOG_MISSING", "message": f"event log not found at {log_path}", "raw_file_id": raw_file_id}

    chunk_key_substr = f"chunks/{raw_file_id}/"
    meta_bucket = _metadata_bucket()

    first_received_at: str | None = None
    last_dispatch_done_at: str | None = None
    chunk_event_count = 0
    metadata_event_count = 0
    gateway_times: dict[str, _dt.datetime] = {}
    backend_acks: dict[str, list[tuple[str, int]]] = {}  # req_id -> [(endpoint, ack_ms)]
    meta_reqs: set[str] = set()

    for ev in _iter_event_log(log_path):
        key = ev.get("key") or ""
        layer = ev.get("layer")
        is_session_start = ev.get("dispatch_raw_file_id") == raw_file_id and layer == "gateway"
        relevant = is_session_start or chunk_key_substr in key or (
            meta_bucket and ev.get("bucket") == meta_bucket and first_received_at is not None
        )
        if not relevant:
            continue
        if is_session_start:
            first_received_at = ev.get("consumer_received_at")
            last_dispatch_done_at = ev.get("consumer_dispatch_done_at")
            chunk_event_count = 0
            metadata_event_count = 0
            gateway_times = {}
            backend_acks = {}
            meta_reqs = set()
        received = ev.get("consumer_received_at")
        done = ev.get("consumer_dispatch_done_at")
        if received and (first_received_at is None or received < first_received_at):
            first_received_at = received
        if done and (last_dispatch_done_at is None or done > last_dispatch_done_at):
            last_dispatch_done_at = done
        if ev.get("action") != "s3:PutObject":
            continue
        req_id = ev.get("req_id") or ""
        try:
            ev_time = _parse_iso(ev.get("event_time") or "")
        except (ValueError, TypeError):
            continue
        if layer == "gateway":
            gateway_times[req_id] = ev_time
            if "chunk_" in key:
                chunk_event_count += 1
            if meta_bucket and ev.get("bucket") == meta_bucket:
                metadata_event_count += 1
                meta_reqs.add(req_id)
        elif layer == "backend" and req_id in gateway_times:
            ack_ms = int((ev_time - gateway_times[req_id]).total_seconds() * 1000)
            if 0 <= ack_ms <= 60_000:
                backend_acks.setdefault(req_id, []).append((ev.get("endpoint") or "?", ack_ms))

    if first_received_at is None:
        return {"error": "NO_EVENTS_FOUND", "message": f"no events for raw_file_id={raw_file_id!r} in {log_path}", "raw_file_id": raw_file_id}

    per_peer: dict[str, list[int]] = {}
    quorum: list[int] = []
    for req_id, acks in backend_acks.items():
        for endpoint, ack_ms in acks:
            per_peer.setdefault(endpoint, []).append(ack_ms)
        if req_id in meta_reqs:
            quorum.append(max(a for _, a in acks))

    total_seconds: float | None = None
    if last_dispatch_done_at:
        try:
            total_seconds = round((_parse_iso(last_dispatch_done_at) - _parse_iso(first_received_at)).total_seconds(), 1)
        except (ValueError, TypeError):
            pass

    return {
        "raw_file_id": raw_file_id,
        "event_log_path": str(log_path),
        "ingest_started_at": first_received_at,
        "ingest_completed_at": last_dispatch_done_at,
        "total_elapsed_seconds": total_seconds,
        "chunk_event_count": chunk_event_count,
        "metadata_event_count": metadata_event_count,
        "per_peer_ack_latency": {peer: _stats(acks) for peer, acks in per_peer.items()},
        "metadata_quorum_ack_ms": _stats(quorum),
    }


def get_chunk_read_metrics(
    start_time: str,
    end_time: str | None = None,
    raw_file_id: str | None = None,
) -> dict:
    """Chunk fetch latencies in a time window, as recorded by OneBucket itself.

    Uses the ``gateway`` record of every ``s3:GetObject`` on a chunk file (the
    request as the client experienced it) for duration, and the ``backend``
    records sharing its ``req_id`` to name the peer that served it.
    """
    log_path = _default_event_log_path()
    if not log_path.exists():
        return {"error": "EVENT_LOG_MISSING", "message": f"event log not found at {log_path}"}
    try:
        start_dt = _parse_iso(start_time)
    except (ValueError, TypeError) as exc:
        return {"error": "INVALID_ARGUMENT", "message": f"start_time: {exc}"}
    if end_time:
        try:
            end_dt = _parse_iso(end_time)
        except (ValueError, TypeError) as exc:
            return {"error": "INVALID_ARGUMENT", "message": f"end_time: {exc}"}
    else:
        end_dt = _dt.datetime.now(_dt.timezone.utc)
    if raw_file_id is not None and not _RAW_FILE_ID_RE.fullmatch(raw_file_id):
        return {"error": "INVALID_ARGUMENT", "message": "raw_file_id must be 16 lowercase hex characters"}
    rfid_substr = f"chunks/{raw_file_id}/" if raw_file_id else None

    gateway_reads: dict[str, dict] = {}
    backend_by_req: dict[str, list[dict]] = {}
    for ev in _iter_event_log(log_path):
        if ev.get("action") != "s3:GetObject":
            continue
        key = ev.get("key") or ""
        if "chunks/" not in key or not key.endswith(".txt"):
            continue
        if rfid_substr and rfid_substr not in key:
            continue
        req_id = ev.get("req_id") or ev.get("event_id") or ""
        if ev.get("layer") == "backend":
            backend_by_req.setdefault(req_id, []).append(ev)
            continue
        received = ev.get("consumer_received_at")
        if not received:
            continue
        try:
            received_dt = _parse_iso(received)
        except (ValueError, TypeError):
            continue
        if received_dt < start_dt or received_dt > end_dt:
            continue
        dur = ev.get("duration_ms")
        if isinstance(dur, int) and dur >= 0:
            gateway_reads[req_id] = ev

    durations: list[int] = []
    per_policy: dict[str, list[int]] = {}
    per_first_responder: dict[str, int] = {}
    events_kept: list[dict] = []
    for req_id, ev in gateway_reads.items():
        dur = ev["duration_ms"]
        durations.append(dur)
        bucket = ev.get("bucket") or ""
        if bucket:
            per_policy.setdefault(bucket, []).append(dur)
        responder = None
        served = [b for b in backend_by_req.get(req_id, []) if b.get("status_code") in (200, 206)]
        if served:
            served.sort(key=lambda b: b.get("event_time") or "")
            responder = served[0].get("endpoint")
            per_first_responder[responder] = per_first_responder.get(responder, 0) + 1
        if len(events_kept) < 50:
            events_kept.append({
                "timestamp": ev.get("consumer_received_at"), "key": ev.get("key"), "bucket": bucket,
                "duration_ms": dur, "first_responder": responder, "status": ev.get("status_code"),
            })

    return {
        "window": {"start": start_dt.isoformat(), "end": end_dt.isoformat(), "raw_file_id": raw_file_id},
        "events_count": len(durations),
        "duration_ms": _stats(durations, extremes=True),
        "per_policy": {b: _stats(xs, extremes=True) for b, xs in per_policy.items()},
        "per_first_responder": per_first_responder,
        "events": events_kept,
    }


# -----------------------------------------------------------------------
# list_documents
# -----------------------------------------------------------------------


def list_documents() -> dict:
    """Enumerate distinct documents currently committed to the LanceDB
    global index. Returns one entry per ``raw_file_id`` with chunk count
    and a representative ``source_uri``. Useful for admin queries
    ("what's currently in the index?") — but for query routing the agent
    should prefer multi-hop discovery (a search on a topic to find
    relevant docs) rather than listing-and-name-matching.
    """
    layout = _bucket_layout()
    db = connect(layout.metadata_root)
    table = db.open_table(TABLE_NAME)
    # Same rationale as check_ingest_status: pin-to-open-time means a list_documents
    # call right after a fresh ingest can miss the new doc. Refresh to latest.
    try:
        table.checkout_latest()
    except Exception as e:  # noqa: BLE001
        logger.debug("checkout_latest no-op or unavailable: %s", e)
    rows = (
        table.search()
        .select(["raw_file_id", "source_uri"])
        .limit(100000)
        .to_list()
    )

    by_id: dict[str, dict] = {}
    for r in rows:
        rid = r.get("raw_file_id", "")
        if not rid:
            continue
        if rid not in by_id:
            by_id[rid] = {
                "raw_file_id": rid,
                "source_uri": r.get("source_uri", ""),
                "chunk_count": 0,
            }
        by_id[rid]["chunk_count"] += 1

    return {
        "documents": sorted(
            by_id.values(), key=lambda d: d["source_uri"]
        ),
        "total_documents": len(by_id),
        "total_chunks": sum(d["chunk_count"] for d in by_id.values()),
    }


# -----------------------------------------------------------------------------
# Runbook configuration tools — read / set / validate variables.yml
#
# The runbook + metrics.py + this MCP server all consult a single YAML at
# docs/demo_runbook/variables.yml. These tools let the agent introspect that
# config and (for operator_intent.* values) update it in place. Deployment
# facts (sections 1-6 of the YAML) mirror the OneBucket console; editing them
# here does NOT change the deployment. Change the console first, then mirror.
# Loader implementation: src/onebucket_demo/runbook_config.py.
# -----------------------------------------------------------------------------


def _to_plain(obj):
    """Convert ruamel CommentedMap / CommentedSeq to plain dict / list so the
    JSON response is clean (no YAML-internal types leak to the agent)."""
    if isinstance(obj, dict):
        return {k: _to_plain(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_plain(x) for x in obj]
    return obj


def get_runbook_config() -> dict:
    """Read variables.yml and return its full contents plus diagnostics
    (missing required values + validation errors). Used at runbook Phase 0 to
    tell the agent what needs operator input before the demo can proceed.
    """
    try:
        data = runbook_config.load()
    except FileNotFoundError as exc:
        return {
            "error": "RUNBOOK_CONFIG_NOT_FOUND",
            "message": str(exc),
            "path": str(runbook_config.resolve_path()),
        }
    return {
        "path": str(runbook_config.resolve_path()),
        "schema_version": data.get("schema_version"),
        "config": _to_plain(data),
        "missing_required": runbook_config.find_missing_required(data),
        "validation_errors": runbook_config.validate(data),
    }


def set_runbook_variable(path: str, value) -> dict:
    """Update a single variable in variables.yml at a dotted path (e.g.
    ``operator_intent.fresh_ticker_for_live_ingest``). Comments in the file
    are preserved.

    Intended for operator_intent.* edits — the deployment facts in sections
    1-6 should match the OneBucket console; changing them here without also
    updating the console produces a drifted config (the validator may still
    pass; runtime behavior will diverge).
    """
    try:
        data = runbook_config.set_variable(path, value)
    except FileNotFoundError as exc:
        return {"error": "RUNBOOK_CONFIG_NOT_FOUND", "message": str(exc)}
    except (KeyError, IndexError) as exc:
        return {
            "error": "PATH_NOT_FOUND",
            "message": (
                f"Could not navigate to {path!r}: {exc}. The path must "
                "reference an existing key; this tool does NOT auto-create "
                "nested structure."
            ),
        }
    except ValueError as exc:
        return {"error": "INVALID_PATH", "message": str(exc)}
    # Echo the actually-stored value (after type-coercion) so the agent sees
    # what's on disk, not what was sent. e.g. caller passed "10000000",
    # set_at_path coerced to 10000000 — the response shows int 10000000 so
    # the agent knows the stored type.
    try:
        stored = _to_plain(runbook_config.get_at_path(data, path))
    except (KeyError, IndexError, ValueError):
        stored = value
    return {
        "ok": True,
        "path": path,
        "new_value": stored,
        "input_was_coerced": stored != value,
        "validation_errors_after_write": runbook_config.validate(data),
    }


def validate_runbook_config() -> dict:
    """Re-read variables.yml and return the current validation errors (if
    any) plus any sentinel-flagged missing required values. Cheap to call;
    safe to invoke between any other tool calls when the agent wants a
    sanity check.
    """
    try:
        data = runbook_config.load()
    except FileNotFoundError as exc:
        return {"error": "RUNBOOK_CONFIG_NOT_FOUND", "message": str(exc)}
    errors = runbook_config.validate(data)
    missing = runbook_config.find_missing_required(data)
    return {
        "ok": not errors and not missing,
        "validation_errors": errors,
        "missing_required": missing,
    }


# -----------------------------------------------------------------------------
# Destructive tools: delete_document, purge_peer_cache, rebuild_index
#
# These are demo-recovery and cache-cold-warming tools. They modify state in
# ways the read-side tools can't undo. Each one is strict on preconditions —
# inconsistent state surfaces as an error pointing the operator at
# rebuild_index() rather than silently muddling through.
# -----------------------------------------------------------------------------


def _list_keys(s3, bucket: str, prefix: str) -> list[str]:
    """Paginated ListObjectsV2 returning a flat list of keys under prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith("/"):  # folder marker
                continue
            keys.append(k)
    return keys


_BATCH_DELETE_PARALLELISM = 10


def _batch_delete(
    s3, bucket: str, keys: list[str], max_workers: int = _BATCH_DELETE_PARALLELISM
) -> int:
    """Delete a list of S3 keys via parallel individual delete_object calls.

    S3 has no "delete a prefix" API, and the bulk DeleteObjects call is
    not part of the S3 surface OneBucket documents, so single-key deletes
    are fanned out across a small thread pool: about ten times faster than
    serial without depending on bulk delete.

    boto3 clients are thread-safe for concurrent calls on the same client.
    max_workers=10 keeps load on the endpoint modest (~10 in-flight at a
    time) while giving most of the speedup. Bump up for very large purges
    if rate-limiting headroom allows.

    Idempotent — a NoSuchKey response counts as success (already gone).
    Returns the number of keys successfully deleted (or confirmed absent).
    """
    if not keys:
        return 0

    success_count = [0]
    consecutive_gateway_failures = [0]
    circuit_broken = threading.Event()
    lock = threading.Lock()
    # If we see this many gateway failures in a row, stop. The endpoint is
    # sick; pounding it with more requests makes it worse and wastes the
    # operator's time. The partial-success count we return is fine — callers
    # already tolerate orphans (content-addressed re-writes overwrite anyway).
    CIRCUIT_BREAK_THRESHOLD = 20

    def _delete_one(key: str) -> None:
        if circuit_broken.is_set():
            return
        # Retry on transient gateway errors (a 502 mid-purge has been seen
        # when a backing peer flaps). botocore already retries four times;
        # add three application-level retries with a short backoff before
        # giving up on a single key. Log and move on rather than raise:
        # failing the whole purge over one stuck key is worse than leaving
        # an orphan to clean up later.
        last_exc: ClientError | None = None
        for attempt in range(3):
            try:
                s3.delete_object(Bucket=bucket, Key=key)
                with lock:
                    success_count[0] += 1
                    consecutive_gateway_failures[0] = 0
                return
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                if code in ("NoSuchKey", "404", "NotFound"):
                    with lock:
                        success_count[0] += 1  # already absent
                        consecutive_gateway_failures[0] = 0
                    return
                last_exc = exc
                time.sleep(0.5 * (attempt + 1))  # 0.5s, 1s, 1.5s
        # Three application retries exhausted — log and move on.
        with lock:
            consecutive_gateway_failures[0] += 1
            should_break = consecutive_gateway_failures[0] >= CIRCUIT_BREAK_THRESHOLD
        print(
            f"[batch_delete] WARN: giving up on key={key!r} after retries: "
            f"{last_exc}",
            file=sys.stderr,
            flush=True,
        )
        if should_break and not circuit_broken.is_set():
            circuit_broken.set()
            print(
                f"[batch_delete] CIRCUIT BREAKER: {CIRCUIT_BREAK_THRESHOLD} "
                "consecutive gateway failures — endpoint is sick. Stopping "
                "batch and returning partial count.",
                file=sys.stderr,
                flush=True,
            )

    # Cap workers at len(keys) to avoid wasted thread spin-up on small batches.
    workers = min(max_workers, len(keys))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        # list(...) forces consumption — propagates any non-NoSuchKey exception
        list(ex.map(_delete_one, keys))
    return success_count[0]


def delete_document(raw_file_id: str) -> dict:
    """Remove a document end-to-end: LanceDB rows → S3 chunks → S3 raw file.

    STRICT preconditions — refuses on any inconsistency between the three
    tiers. If the system is in a bad state (e.g., LanceDB has rows for a
    raw_file_id whose MANIFEST.json is missing in S3), this tool reports
    the inconsistency and tells you to run rebuild_index() rather than
    attempting partial recovery.

    Order of deletion is load-bearing: LanceDB first (so search stops
    surfacing chunks immediately, no 404 window on fetch), then chunks,
    then raw. If a step fails mid-way the tool propagates the error;
    re-running on the remaining state is safe.
    """
    if not raw_file_id or not isinstance(raw_file_id, str):
        return {
            "error": "INVALID_ARGUMENT",
            "message": "raw_file_id is required and must be a non-empty string.",
        }
    if not _RAW_FILE_ID_RE.fullmatch(raw_file_id):
        return {
            "error": "INVALID_ARGUMENT",
            "message": (
                "raw_file_id must be the 16-character lowercase hex content hash "
                "returned by ingest_document / list_documents."
            ),
        }

    layout = _bucket_layout()
    s3 = boto3.client("s3", region_name=layout.region)
    chunks_prefix = layout.chunks_prefix.rstrip("/")

    # 1. LanceDB must have rows for this raw_file_id
    db = connect(layout.metadata_root)
    if TABLE_NAME not in db.list_tables().tables:
        return {
            "error": "INDEX_NOT_INITIALIZED",
            "message": (
                f"LanceDB table {TABLE_NAME!r} doesn't exist at "
                f"{layout.metadata_root}. Run scripts/init_index.py first."
            ),
        }
    table = db.open_table(TABLE_NAME)
    matched = (
        table.search()
        .where(f"raw_file_id == '{raw_file_id}'")
        .limit(10**9)
        .to_arrow()
    )
    row_count = matched.num_rows
    if row_count == 0:
        return {
            "error": "NO_SUCH_DOCUMENT",
            "message": (
                f"No LanceDB rows match raw_file_id={raw_file_id!r}. Either "
                "the document was never indexed or it has already been "
                "deleted. Use list_documents() to see what's currently in the "
                "index."
            ),
        }

    # 2. MANIFEST.json must exist
    manifest_key = f"{chunks_prefix}/{raw_file_id}/MANIFEST.json"
    try:
        body = s3.get_object(Bucket=layout.bucket, Key=manifest_key)["Body"].read()
        manifest = json.loads(body.decode("utf-8"))
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return {
                "error": "MANIFEST_MISSING_DB_INCONSISTENT",
                "message": (
                    f"LanceDB has {row_count} rows for raw_file_id="
                    f"{raw_file_id!r} but s3://{layout.bucket}/{manifest_key} "
                    "doesn't exist. The index is out of sync with storage. "
                    "Run rebuild_index() to reconcile."
                ),
            }
        raise

    raw_object_key = manifest.get("raw_object_key")
    manifest_n_chunks = int(manifest.get("n_chunks", 0))
    if not raw_object_key:
        return {
            "error": "MANIFEST_MALFORMED",
            "message": (
                f"MANIFEST at s3://{layout.bucket}/{manifest_key} is missing "
                "raw_object_key. The chunker version that wrote this file is "
                "incompatible — run rebuild_index() to reconcile."
            ),
        }

    # 3. All chunk files referenced by MANIFEST must exist
    chunk_keys = [
        k
        for k in _list_keys(s3, layout.bucket, f"{chunks_prefix}/{raw_file_id}/")
        if not k.endswith("/MANIFEST.json")
    ]
    if len(chunk_keys) != manifest_n_chunks:
        return {
            "error": "CHUNKS_COUNT_MISMATCH_DB_INCONSISTENT",
            "message": (
                f"MANIFEST says n_chunks={manifest_n_chunks} but S3 has "
                f"{len(chunk_keys)} chunk file(s) under "
                f"chunks/{raw_file_id}/. The index is out of sync with "
                "storage. Run rebuild_index() to reconcile."
            ),
        }

    # 4. Raw file must exist
    try:
        s3.head_object(Bucket=layout.bucket, Key=raw_object_key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return {
                "error": "RAW_MISSING_DB_INCONSISTENT",
                "message": (
                    f"MANIFEST says raw_object_key={raw_object_key!r} but "
                    f"s3://{layout.bucket}/{raw_object_key} doesn't exist. "
                    "Run rebuild_index() to reconcile."
                ),
            }
        raise

    # Preconditions ok. Delete in order: LanceDB → chunks → raw.
    table.delete(f"raw_file_id == '{raw_file_id}'")
    search.reset_caches(layout.metadata_root)
    chunks_deleted = _batch_delete(
        s3, layout.bucket, chunk_keys + [manifest_key]
    )
    s3.delete_object(Bucket=layout.bucket, Key=raw_object_key)

    return {
        "raw_file_id": raw_file_id,
        "raw_object_key": raw_object_key,
        "lancedb_rows_deleted": row_count,
        "chunks_deleted": chunks_deleted,
        "raw_deleted": True,
    }


def purge_peer_cache(
    onebucket: str, peer_alias: str, key_pattern: str
) -> dict:
    """Delete objects directly from a backing peer, bypassing OneBucket.

    SAFETY: refuses if the peer's type != 'cache'. Sync peers hold canonical
    data; nuking them would corrupt the OneBucket's storage rather than
    purge a cache.

    Pattern semantics (S3-style prefix match, no middle-of-string globs):
      * "*"               → every key in the backing bucket
      * "prefix/*"        → all keys with that prefix
      * "exact/key.txt"   → exact-match single key

    Used to demonstrate cache-cold → cache-fill flows: purge the Wasabi cache
    of a doc's chunks, then re-read via demo-west to watch the fill happen.
    """
    if not onebucket or not peer_alias or not key_pattern:
        return {
            "error": "INVALID_ARGUMENT",
            "message": "onebucket, peer_alias, and key_pattern are all required.",
        }

    # 1. Load variables.yml to find the peer + backing-access creds
    try:
        config = runbook_config.load()
    except FileNotFoundError as exc:
        return {"error": "RUNBOOK_CONFIG_NOT_FOUND", "message": str(exc)}

    policies = config.get("policies", {}) or {}
    if onebucket not in policies:
        return {
            "error": "UNKNOWN_ONEBUCKET",
            "message": (
                f"OneBucket {onebucket!r} not in variables.yml. "
                f"Available: {list(policies.keys())}"
            ),
        }
    peers = policies[onebucket].get("peers", []) or []
    peer = next((p for p in peers if p.get("alias") == peer_alias), None)
    if peer is None:
        return {
            "error": "UNKNOWN_PEER",
            "message": (
                f"Peer {peer_alias!r} not in OneBucket {onebucket!r}. "
                f"Available: {[p.get('alias') for p in peers]}"
            ),
        }

    # 2. Safety: refuse on sync peer
    peer_type = peer.get("type")
    if peer_type != "cache":
        return {
            "error": "REFUSED_NOT_CACHE_PEER",
            "message": (
                f"Peer {peer_alias!r} on {onebucket!r} has type={peer_type!r}. "
                "purge_peer_cache only operates on cache peers — refusing to "
                "delete from a sync peer (that's the canonical data). Use "
                "delete_document for canonical removal."
            ),
        }

    # 3. Look up direct_access in endpoints
    endpoints = config.get("endpoints", {}) or {}
    endpoint = endpoints.get(peer_alias)
    if endpoint is None:
        return {
            "error": "ENDPOINT_NOT_DEFINED",
            "message": (
                f"Endpoint {peer_alias!r} not in variables.yml endpoints block."
            ),
        }
    direct = endpoint.get("direct_access")
    if direct is None:
        return {
            "error": "DIRECT_ACCESS_NOT_CONFIGURED",
            "message": (
                f"Endpoint {peer_alias!r} has no direct_access block in "
                "variables.yml. Add 'direct_access: {profile: ..., "
                "endpoint_url: ...}' to enable purge."
            ),
        }
    profile = direct.get("profile")
    endpoint_url = direct.get("endpoint_url")
    backing_bucket = peer.get("bucket")
    region = peer.get("region")
    if not backing_bucket:
        return {
            "error": "PEER_MISSING_BUCKET",
            "message": (
                f"Peer {peer_alias!r} on {onebucket!r} has no 'bucket' field "
                "in variables.yml."
            ),
        }

    # 4. Build the direct-access boto3 client
    session = boto3.Session(
        profile_name=profile, region_name=region
    )
    client_kwargs: dict = {}
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url
    s3 = session.client("s3", **client_kwargs)

    # 5. Resolve pattern → key list
    keys_to_delete: list[str] = []
    if key_pattern == "*":
        keys_to_delete = _list_keys(s3, backing_bucket, "")
    elif key_pattern.endswith("*"):
        prefix = key_pattern[:-1]
        keys_to_delete = _list_keys(s3, backing_bucket, prefix)
    else:
        # exact key
        try:
            s3.head_object(Bucket=backing_bucket, Key=key_pattern)
            keys_to_delete = [key_pattern]
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
                return {
                    "onebucket": onebucket,
                    "peer_alias": peer_alias,
                    "peer_type": peer_type,
                    "backing_bucket": backing_bucket,
                    "pattern": key_pattern,
                    "objects_deleted": 0,
                    "deleted_keys": [],
                    "message": (
                        f"Exact key {key_pattern!r} not in backing bucket; "
                        "nothing to delete."
                    ),
                }
            raise

    if not keys_to_delete:
        return {
            "onebucket": onebucket,
            "peer_alias": peer_alias,
            "peer_type": peer_type,
            "backing_bucket": backing_bucket,
            "pattern": key_pattern,
            "objects_deleted": 0,
            "deleted_keys": [],
            "message": "Pattern matched no keys; nothing to delete.",
        }

    # 6. Batch delete
    deleted = _batch_delete(s3, backing_bucket, keys_to_delete)

    # Truncate response if huge
    display_limit = 20
    deleted_keys_display = keys_to_delete[:display_limit]
    if len(keys_to_delete) > display_limit:
        deleted_keys_display = (
            keys_to_delete[:display_limit]
            + [f"... and {len(keys_to_delete) - display_limit} more"]
        )

    return {
        "onebucket": onebucket,
        "peer_alias": peer_alias,
        "peer_type": peer_type,
        "backing_bucket": backing_bucket,
        "pattern": key_pattern,
        "objects_deleted": deleted,
        "deleted_keys": deleted_keys_display,
    }


def rebuild_index(confirm: bool = False) -> dict:
    """Wipe + re-derive: delete LanceDB rows + every chunk in S3, then
    re-process each raw/ file via the existing pipeline (chunker + embedder).

    BLAST RADIUS: wipes ALL chunks and ALL LanceDB rows for the configured
    bucket. Requires confirm=true. Use after a delete_document failure
    reports CHUNKS_COUNT_MISMATCH / RAW_MISSING / MANIFEST_MISSING — or any
    time the index is suspected of being out of sync with storage.

    The raw/ files themselves are NOT touched — they're the source of truth.

    For a non-trivial corpus this can take several minutes (re-embedding is
    the slow step). Returns counts per file so the agent can show progress.
    """
    if not confirm:
        return {
            "error": "CONFIRM_REQUIRED",
            "message": (
                "rebuild_index wipes ALL chunks and ALL LanceDB rows for "
                "this bucket. Pass confirm=true to proceed. The raw/ files "
                "themselves are not touched."
            ),
        }

    layout = _bucket_layout()
    s3 = boto3.client("s3", region_name=layout.region)
    chunks_prefix = layout.chunks_prefix.rstrip("/")
    raw_prefix = layout.raw_prefix.rstrip("/")

    # 1. List all raw files
    raw_keys = _list_keys(s3, layout.bucket, f"{raw_prefix}/")
    if not raw_keys:
        return {
            "error": "NO_RAW_FILES",
            "message": (
                f"No raw files under s3://{layout.bucket}/{raw_prefix}/. "
                "Nothing to rebuild from."
            ),
        }

    # 2. Wipe LanceDB table by deleting all rows (preserve schema)
    db = connect(layout.metadata_root)
    if TABLE_NAME in db.list_tables().tables:
        table = db.open_table(TABLE_NAME)
        # Delete-all by always-true predicate
        table.delete("raw_file_id != ''")
        search.reset_caches(layout.metadata_root)
    # If the table doesn't exist, init_index.py should be run first;
    # rebuild assumes the table exists.

    # 3. Wipe all chunks in S3
    chunk_keys = _list_keys(s3, layout.bucket, f"{chunks_prefix}/")
    chunks_deleted_before = _batch_delete(s3, layout.bucket, chunk_keys)

    # 4. Re-process each raw file via the pipeline functions. Same code
    # path the daemon's _dispatch_chunker_and_embedder uses for live
    # ingests; reused here so behavior matches.
    from .gates import check_type
    from .pipeline import EmptySource, chunk_and_write, embed_and_commit
    import tempfile as _tempfile

    per_file: list[dict] = []
    chunk_root = f"s3://{layout.bucket}/{chunks_prefix}"
    for raw_key in raw_keys:
        ok, reason = check_type(Path(raw_key))
        if not ok:
            per_file.append(
                {"raw_key": raw_key, "skipped": reason}
            )
            continue
        local_path = Path(_tempfile.gettempdir()) / Path(raw_key).name
        try:
            s3.download_file(layout.bucket, raw_key, str(local_path))
            manifest = chunk_and_write(
                local_path,
                source_uri=f"s3://{layout.bucket}/{raw_key}",
                raw_object_key=raw_key,
                chunk_root=chunk_root,
            )
        except EmptySource:
            per_file.append({"raw_key": raw_key, "skipped": "empty source"})
            continue
        except Exception as exc:  # noqa: BLE001
            per_file.append(
                {"raw_key": raw_key, "error": f"chunk_and_write: {exc}"}
            )
            continue

        manifest_uri = f"{chunk_root}/{manifest.raw_file_id}/MANIFEST.json"
        try:
            result = embed_and_commit(
                manifest_uri,
                dataset_root=layout.metadata_root,
                chunk_root=chunk_root,
            )
        except Exception as exc:  # noqa: BLE001
            per_file.append(
                {
                    "raw_key": raw_key,
                    "raw_file_id": manifest.raw_file_id,
                    "chunks": manifest.n_chunks,
                    "error": f"embed_and_commit: {exc}",
                }
            )
            continue
        per_file.append(
            {
                "raw_key": raw_key,
                "raw_file_id": manifest.raw_file_id,
                "chunks": manifest.n_chunks,
                "lancedb_rows_committed": result.n_committed,
            }
        )

    return {
        "raw_files_found": len(raw_keys),
        "raw_files_processed": sum(1 for r in per_file if "raw_file_id" in r and "error" not in r),
        "raw_files_skipped": sum(1 for r in per_file if "skipped" in r),
        "raw_files_errored": sum(1 for r in per_file if "error" in r),
        "chunks_deleted_before_rebuild": chunks_deleted_before,
        "results": per_file,
    }
