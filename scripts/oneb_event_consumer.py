#!/usr/bin/env python3
"""OneBucket event consumer: the ``onebucket-events`` execution mode.

Polls OneBucket's ``events`` MCP server with ``tail_events`` and runs the
chunker + embedder pipeline on every successful ``PutObject`` under ``raw/``
in the watched OneBucket. It satisfies the same operational contract as the
polling daemon (``oneb_pipeline_daemon.py``): a heartbeat file the MCP server
reads, sequential dispatch through ``pipeline.chunk_and_write`` and
``pipeline.embed_and_commit``, and clean shutdown on SIGTERM/SIGINT. So the
MCP server never needs to know which of the two is running.

Why pull instead of push: the events server is a long-poll API (``tail_events``
blocks up to ``waitSeconds`` and always returns a cursor). A persisted cursor
makes restarts resume where they stopped, and there is no half-open socket to
babysit.

Lifecycle of one batch:
  1. ``tail_events(cursor, limit, waitSeconds)``.
  2. For every event: validate → dedup → classify → (dispatch) → append to the
     JSONL event log.
  3. Only when the whole batch is processed is the cursor written (atomically).
     A crash mid-batch replays the batch; the deduper and the pipeline's
     content-addressed, idempotent writes make the replay harmless.

Authentication: a one-time ``scripts/onebucket-login.py`` stores a refresh
token; this process refreshes the access token before it expires. When that
is no longer possible, the heartbeat records ``auth_state`` and the process
exits non-zero with instructions, instead of looping against the server.

Required env (validated by scripts/start-event-consumer.sh):
    ONEBUCKET_EVENTS_URL     events MCP server, e.g. https://events.onebucket.io
    ONEBUCKET_TOKEN_FILE     optional; default ~/.config/onebucket/token.json
    AWS_PROFILE (or keys)    the S3 credentials for the OneBucket S3 endpoint
    AWS_ENDPOINT_URL         the OneBucket S3 endpoint, e.g. https://s3.onebucket.io
    ONEBUCKET_CHUNK_ROOT     s3://<onebucket>/chunks
    ONEBUCKET_DATASET_ROOT   s3://<onebucket>/metadata
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import logging
import os
import random
import signal
import sys
import tempfile
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

import boto3
import httpx

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from onebucket_demo import onebucket_auth  # noqa: E402
from onebucket_demo.constants import MAX_FILE_BYTES  # noqa: E402
from onebucket_demo.event_consumer import (  # noqa: E402
    DISPATCH_CHUNKER,
    SKIP_INVALID,
    EventDeduper,
    InvalidEvent,
    StorageEvent,
    classify,
    is_invalid_cursor_error,
)
from onebucket_demo.gates import check_type  # noqa: E402
from onebucket_demo.pipeline import EmptySource, chunk_and_write, embed_and_commit  # noqa: E402

HEARTBEAT_INTERVAL_SECONDS = 10.0
DEFAULT_HEARTBEAT_PATH = "/tmp/oneb-pipeline-daemon.heartbeat"
DEFAULT_EVENT_LOG_PATH = _REPO_ROOT / ".runbook-state" / "events.jsonl"
DEFAULT_CURSOR_PATH = _REPO_ROOT / ".runbook-state" / "events.cursor"
DEFAULT_WAIT_SECONDS = 60
DEFAULT_BATCH_LIMIT = 100
BACKOFF_INITIAL_S = 1.0
BACKOFF_MAX_S = 60.0
MCP_PROTOCOL_VERSION = "2025-03-26"
# The global hostname routes to the nearest live region; set ONEBUCKET_EVENTS_URL
# to events.<region>.onebucket.io to pin one (list: https://www.onebucket.io/support).
DEFAULT_EVENTS_URL = "https://events.onebucket.io"
# A single event that keeps failing must not block the stream forever: after
# this many attempts it is logged as failed and the batch moves on. The
# polling daemon's catch-up scan (or --catchup-on-start) picks it up later.
MAX_DISPATCH_ATTEMPTS = 3

AUTH_OK = "ok"
AUTH_EXPIRED = "expired"
AUTH_REFRESH_FAILED = "refresh_failed"

log = logging.getLogger("oneb-event-consumer")


def _ts() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ts_ms() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _log(tag: str, msg: str) -> None:
    print(f"[{_ts()}] [{tag}] {onebucket_auth.redact(msg)}", flush=True)


def _resolve_repo_path(value: str | os.PathLike) -> Path:
    p = Path(value).expanduser()
    return p if p.is_absolute() else _REPO_ROOT / p


def _atomic_write(path: Path, data: str, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=str(path.parent))
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(data)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# --- JSONL event log ----------------------------------------------------------


class EventLog:
    """Append-only JSONL writer: one event per line, flushed on every write."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(path, "ab", buffering=0)
        _log("event_log", f"writing to {path}")

    def append(self, record: dict) -> None:
        try:
            line = json.dumps(record, separators=(",", ":")) + "\n"
            self._fh.write(line.encode("utf-8"))
        except (OSError, TypeError, ValueError) as exc:
            _log("warning", f"event log write failed: {type(exc).__name__}: {exc}")

    def close(self) -> None:
        try:
            self._fh.close()
        except OSError:
            pass


def event_to_record(event: StorageEvent, received_at: str) -> dict:
    """The JSONL record for one event. Dispatch fields are filled in later."""
    return {
        "consumer_received_at": received_at,
        "consumer_dispatch_done_at": None,
        "event_id": event.event_id,
        "req_id": event.req_id,
        "layer": event.layer,
        "endpoint": event.endpoint or None,
        "action": event.action,
        "bucket": event.bucket,
        "key": event.key,
        "status_code": event.status_code,
        "bytes_in": event.bytes_in,
        "bytes_out": event.bytes_out,
        "event_time": event.time,
        "duration_ms": event.duration_ms,
        "dispatch_outcome": None,
        "dispatch_raw_file_id": None,
        "dispatch_error": None,
        "dispatch_duration_ms": None,
    }


# --- Cursor -------------------------------------------------------------------


class CursorStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> str | None:
        try:
            value = self.path.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            return None
        except OSError as exc:
            _log("warning", f"cursor file unreadable ({exc}); starting from now")
            return None
        return value or None

    def save(self, cursor: str) -> None:
        _atomic_write(self.path, cursor + "\n")

    def reset(self) -> None:
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


# --- Heartbeat ----------------------------------------------------------------


class State:
    """Shared mutable state behind one lock: heartbeat thread, poller, signals."""

    def __init__(self, *, bucket: str, events_url: str) -> None:
        self.bucket = bucket
        self.events_url = events_url
        self.started_at = _ts()
        self.pid = os.getpid()
        self.daemon_state = "idle"
        self.current_raw_file_id: str | None = None
        self.processed_count = 0
        self.events_received = 0
        self.events_dispatched = 0
        self.events_skipped = 0
        self.connected = False
        self.auth_state = AUTH_OK
        self.last_event_at: str | None = None
        self.cursor_present = False
        self.lock = threading.Lock()

    def heartbeat_dict(self) -> dict:
        return {
            "pid": self.pid,
            "started_at": self.started_at,
            "last_seen": _ts(),
            "mode": "onebucket-events",
            "events_url": self.events_url,
            "bucket": f"s3://{self.bucket}",
            "daemon_state": self.daemon_state,
            "current_raw_file_id": self.current_raw_file_id,
            "processed_count": self.processed_count,
            "events_received": self.events_received,
            "events_dispatched": self.events_dispatched,
            "events_skipped": self.events_skipped,
            "connected": self.connected,
            "auth_state": self.auth_state,
            "last_event_at": self.last_event_at,
            "cursor_present": self.cursor_present,
        }


def write_heartbeat(path: str, state: State) -> None:
    with state.lock:
        data = json.dumps(state.heartbeat_dict(), sort_keys=True) + "\n"
    # World-readable on purpose: the MCP server may run under another user.
    _atomic_write(Path(path), data, mode=0o644)


def delete_heartbeat(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def _heartbeat_loop(state: State, path: str, stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            write_heartbeat(path, state)
        except OSError as exc:
            _log("warning", f"heartbeat write failed: {exc}")
        stop.wait(HEARTBEAT_INTERVAL_SECONDS)


# --- Pipeline dispatch --------------------------------------------------------


def dispatch_raw_object(s3, bucket: str, raw_key: str, chunk_root: str, dataset_root: str) -> str | None:
    """Chunk + embed one uploaded file. Returns the document id, or ``None``
    when the file was skipped by a gate (type, size, empty)."""
    ok, reason = check_type(Path(raw_key))
    if not ok:
        _log("skip", f"{raw_key}: {reason}")
        return None
    size_bytes = int(s3.head_object(Bucket=bucket, Key=raw_key).get("ContentLength", 0))
    if size_bytes > MAX_FILE_BYTES:
        _log("skip", f"{raw_key}: file too large ({size_bytes} bytes > {MAX_FILE_BYTES})")
        return None
    local_path = Path(tempfile.gettempdir()) / Path(raw_key).name
    s3.download_file(bucket, raw_key, str(local_path))
    try:
        manifest = chunk_and_write(
            local_path,
            source_uri=f"s3://{bucket}/{raw_key}",
            raw_object_key=raw_key,
            chunk_root=chunk_root,
        )
    except EmptySource:
        _log("skip", f"{raw_key}: empty source")
        return None
    finally:
        try:
            local_path.unlink()
        except OSError:
            pass
    _log("chunked", f"{raw_key} -> raw_file_id={manifest.raw_file_id} chunks={manifest.n_chunks}")
    manifest_uri = f"{chunk_root.rstrip('/')}/{manifest.raw_file_id}/MANIFEST.json"
    result = embed_and_commit(manifest_uri, dataset_root=dataset_root, chunk_root=chunk_root)
    _log("embedded", f"raw_file_id={manifest.raw_file_id} rows_committed={result.n_committed}")
    return manifest.raw_file_id


# --- MCP client (Streamable HTTP, JSON responses) -----------------------------


class SessionLost(RuntimeError):
    """The server no longer knows our session id; re-initialize."""


class EventsClient:
    """Minimal MCP client for the events server: initialize, then tools/call."""

    def __init__(
        self,
        events_url: str,
        tokens: onebucket_auth.TokenManager,
        wait_seconds: int,
        *,
        client: httpx.Client | None = None,
    ) -> None:
        self.url = onebucket_auth.check_url(events_url, "events URL").rstrip("/") + "/"
        self.tokens = tokens
        self.session_id: str | None = None
        self._next_id = 1
        # Read timeout slightly above the long-poll so a held request is not
        # mistaken for a dead connection, but a dead one is still detected.
        self._client = client or httpx.Client(
            timeout=httpx.Timeout(connect=10, read=wait_seconds + 30, write=10, pool=None)
        )

    def close(self) -> None:
        self._client.close()

    def _headers(self) -> dict:
        h = {
            "Authorization": f"Bearer {self.tokens.access_token()}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if self.session_id:
            h["mcp-session-id"] = self.session_id
        return h

    def _post(self, body: dict, *, retry_auth: bool = True) -> httpx.Response:
        response = self._client.post(self.url, headers=self._headers(), json=body)
        if response.status_code == 401 and retry_auth:
            self.tokens.force_refresh()
            return self._post(body, retry_auth=False)
        if response.status_code == 404 and self.session_id:
            raise SessionLost(response.text[:120])
        return response

    @staticmethod
    def _parse(response: httpx.Response, request_id: int | None = None) -> dict:
        """Return the JSON-RPC response for our request.

        A plain JSON body is the response. When events arrive while a
        ``tail_events`` long poll is waiting, the server switches the reply to
        a server-sent-event stream: one ``notifications/storage/event`` message
        per event, then the JSON-RPC response itself (which repeats those
        events and carries ``nextCursor``). The notifications are skipped and
        the message whose ``id`` matches the request (or, failing that, the
        last message with a ``result`` or ``error``) is returned.
        """
        if response.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"HTTP {response.status_code}", request=response.request, response=response
            )
        content_type = response.headers.get("content-type", "")
        if "text/event-stream" in content_type:
            messages = []
            for line in response.text.splitlines():
                if line.startswith("data:"):
                    try:
                        messages.append(json.loads(line[5:].strip()))
                    except json.JSONDecodeError:
                        continue
            replies = [m for m in messages if isinstance(m, dict) and ("result" in m or "error" in m)]
            if request_id is not None:
                matching = [m for m in replies if m.get("id") == request_id]
                if matching:
                    return matching[-1]
            return replies[-1] if replies else {}
        return response.json() if response.text.strip() else {}

    def initialize(self) -> dict:
        self.session_id = None
        rid = self._rpc_id()
        response = self._post(
            {
                "jsonrpc": "2.0",
                "id": rid,
                "method": "initialize",
                "params": {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "onebucket-demo-event-consumer", "version": "0.3.0"},
                },
            }
        )
        result = self._parse(response, rid).get("result", {})
        self.session_id = response.headers.get("mcp-session-id") or None
        self._client.post(
            self.url, headers=self._headers(), json={"jsonrpc": "2.0", "method": "notifications/initialized"}
        )
        return result

    def _rpc_id(self) -> int:
        self._next_id += 1
        return self._next_id

    def tail_events(self, cursor: str | None, limit: int, wait_seconds: int) -> tuple[list[dict], str | None, str | None]:
        """Returns ``(events, next_cursor, error_text)``; ``error_text`` is set
        when the server answered with a tool error (e.g. an invalid cursor)."""
        args: dict = {"limit": limit, "waitSeconds": wait_seconds}
        if cursor:
            args["cursor"] = cursor
        rid = self._rpc_id()
        response = self._post(
            {
                "jsonrpc": "2.0",
                "id": rid,
                "method": "tools/call",
                "params": {"name": "tail_events", "arguments": args},
            }
        )
        body = self._parse(response, rid)
        if "error" in body:
            raise RuntimeError(f"tail_events JSON-RPC error: {body['error']}")
        result = body.get("result", {})
        payload = result.get("structuredContent")
        if payload is None:
            content = result.get("content") or []
            text = content[0].get("text", "") if content else ""
            if result.get("isError"):
                return [], None, text or "tool error"
            try:
                payload = json.loads(text) if text else {}
            except json.JSONDecodeError:
                return [], None, f"unparseable tool result: {text[:120]}"
        if result.get("isError"):
            return [], None, json.dumps(payload)[:200]
        events = payload.get("events") or []
        return list(events), payload.get("nextCursor"), None


# --- Batch processing ---------------------------------------------------------


class Consumer:
    def __init__(
        self,
        *,
        state: State,
        s3,
        chunk_root: str,
        dataset_root: str,
        heartbeat_path: str,
        event_log: EventLog,
        deduper: EventDeduper,
    ) -> None:
        self.state = state
        self.s3 = s3
        self.chunk_root = chunk_root
        self.dataset_root = dataset_root
        self.heartbeat_path = heartbeat_path
        self.event_log = event_log
        self.deduper = deduper
        self._attempts: dict[str, int] = {}

    def process_batch(self, raw_events: list[dict]) -> bool:
        """Handle every event in order. Returns ``True`` when the cursor may
        advance past this batch, ``False`` when a dispatch failed and the
        batch must be replayed (the deduper skips what already succeeded)."""
        advance = True
        for raw in raw_events:
            received_at = _ts_ms()
            with self.state.lock:
                self.state.events_received += 1
            try:
                event = StorageEvent.from_dict(raw)
            except InvalidEvent as exc:
                _log("skip", f"invalid event ignored: {exc}")
                self.event_log.append(
                    {
                        "consumer_received_at": received_at,
                        "consumer_dispatch_done_at": _ts_ms(),
                        "event_id": str((raw or {}).get("eventId", ""))[:64] if isinstance(raw, dict) else "",
                        "dispatch_outcome": SKIP_INVALID,
                        "dispatch_error": str(exc)[:200],
                    }
                )
                with self.state.lock:
                    self.state.events_skipped += 1
                continue
            with self.state.lock:
                self.state.last_event_at = event.time
            if self.deduper.is_duplicate(event.event_id):
                continue
            record = event_to_record(event, received_at)
            outcome = classify(event, self.state.bucket)
            record["dispatch_outcome"] = outcome
            if outcome != DISPATCH_CHUNKER:
                self.deduper.record(event.event_id)
                with self.state.lock:
                    self.state.events_skipped += 1
                record["consumer_dispatch_done_at"] = _ts_ms()
                self.event_log.append(record)
                continue
            if not self._dispatch(event, record):
                advance = False
        return advance

    def _dispatch(self, event: StorageEvent, record: dict) -> bool:
        attempts = self._attempts.get(event.event_id, 0) + 1
        self._attempts[event.event_id] = attempts
        with self.state.lock:
            self.state.events_dispatched += 1
            self.state.daemon_state = "processing"
            self.state.current_raw_file_id = None
        write_heartbeat(self.heartbeat_path, self.state)
        _log("processing", f"{event.key} (eventId={event.event_id}, attempt {attempts})")
        started = time.monotonic()
        ok = True
        try:
            raw_file_id = dispatch_raw_object(
                self.s3, self.state.bucket, event.key, self.chunk_root, self.dataset_root
            )
            record["dispatch_raw_file_id"] = raw_file_id
            self.deduper.record(event.event_id)
            self._attempts.pop(event.event_id, None)
            with self.state.lock:
                if raw_file_id is not None:
                    self.state.processed_count += 1
                    self.state.current_raw_file_id = raw_file_id
        except Exception as exc:  # noqa: BLE001 - log, continue, decide on replay
            record["dispatch_error"] = f"{type(exc).__name__}: {exc}"[:300]
            _log("error", f"processing {event.key} failed: {type(exc).__name__}: {exc}")
            if attempts >= MAX_DISPATCH_ATTEMPTS:
                _log("error", f"{event.key}: giving up after {attempts} attempts; "
                             "run a catch-up scan (--catchup-on-start) once the cause is fixed")
                self.deduper.record(event.event_id)
                self._attempts.pop(event.event_id, None)
            else:
                ok = False
        record["dispatch_duration_ms"] = int((time.monotonic() - started) * 1000)
        record["consumer_dispatch_done_at"] = _ts_ms()
        self.event_log.append(record)
        with self.state.lock:
            self.state.daemon_state = "idle"
        write_heartbeat(self.heartbeat_path, self.state)
        return ok


# --- Catch-up scan ------------------------------------------------------------


def run_catchup(consumer: Consumer) -> None:
    """Dispatch every ``raw/`` object that has no MANIFEST yet. Used at startup
    (``--catchup-on-start``) or on SIGUSR1, e.g. after the consumer was down
    or after a cursor reset skipped an event window."""
    s3, bucket = consumer.s3, consumer.state.bucket
    _log("catchup", "scan started")
    paginator = s3.get_paginator("list_objects_v2")
    raw_keys = [
        o["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix="raw/")
        for o in page.get("Contents", [])
        if not o["Key"].endswith("/")
    ]
    manifested: set[str] = set()
    for page in paginator.paginate(Bucket=bucket, Prefix="chunks/"):
        for o in page.get("Contents", []):
            if o["Key"].endswith("/MANIFEST.json"):
                try:
                    body = s3.get_object(Bucket=bucket, Key=o["Key"])["Body"].read()
                    manifested.add(str(json.loads(body).get("raw_object_key", "")))
                except Exception as exc:  # noqa: BLE001
                    _log("warning", f"could not read {o['Key']}: {exc}")
    missing = [k for k in raw_keys if k not in manifested]
    _log("catchup", f"{len(raw_keys)} raw / {len(manifested)} manifested / {len(missing)} to dispatch")
    for raw_key in missing:
        with consumer.state.lock:
            consumer.state.daemon_state = "processing"
        write_heartbeat(consumer.heartbeat_path, consumer.state)
        try:
            raw_file_id = dispatch_raw_object(s3, bucket, raw_key, consumer.chunk_root, consumer.dataset_root)
            if raw_file_id is not None:
                with consumer.state.lock:
                    consumer.state.processed_count += 1
                    consumer.state.current_raw_file_id = raw_file_id
        except Exception as exc:  # noqa: BLE001
            _log("error", f"(catchup) {raw_key} failed: {type(exc).__name__}: {exc}")
        with consumer.state.lock:
            consumer.state.daemon_state = "idle"
        write_heartbeat(consumer.heartbeat_path, consumer.state)
    _log("catchup", "scan complete")


# --- Main ---------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="oneb-event-consumer", description="OneBucket events → pipeline")
    parser.add_argument("--bucket", default=None, help="OneBucket name to watch (default: from ONEBUCKET_CHUNK_ROOT)")
    parser.add_argument(
        "--heartbeat-file",
        default=os.environ.get("ONEBUCKET_DAEMON_HEARTBEAT_PATH", DEFAULT_HEARTBEAT_PATH),
    )
    parser.add_argument("--catchup-on-start", action="store_true", help="dispatch raw/ objects with no manifest before tailing")
    parser.add_argument(
        "--event-log",
        default=os.environ.get("ONEBUCKET_EVENT_LOG_PATH", str(DEFAULT_EVENT_LOG_PATH)),
        help="JSONL event log (default: <repo>/.runbook-state/events.jsonl)",
    )
    parser.add_argument(
        "--cursor-file",
        default=os.environ.get("ONEBUCKET_EVENT_CURSOR_PATH", str(DEFAULT_CURSOR_PATH)),
        help="where the tail cursor is persisted (default: <repo>/.runbook-state/events.cursor)",
    )
    parser.add_argument("--reset-cursor", action="store_true", help="ignore the stored cursor and start from now")
    parser.add_argument("--wait-seconds", type=int, default=int(os.environ.get("ONEBUCKET_EVENTS_WAIT_SECONDS", DEFAULT_WAIT_SECONDS)))
    parser.add_argument("--limit", type=int, default=DEFAULT_BATCH_LIMIT)
    return parser.parse_args()


def _bucket_from_env() -> str:
    chunk_root = os.environ.get("ONEBUCKET_CHUNK_ROOT", "")
    if not chunk_root.startswith("s3://"):
        raise RuntimeError("ONEBUCKET_CHUNK_ROOT must be an s3:// URI (or pass --bucket)")
    return urlparse(chunk_root).netloc


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(name)s] %(message)s")
    args = _parse_args()
    events_url = os.environ.get("ONEBUCKET_EVENTS_URL", "") or DEFAULT_EVENTS_URL
    chunk_root = os.environ.get("ONEBUCKET_CHUNK_ROOT", "")
    dataset_root = os.environ.get("ONEBUCKET_DATASET_ROOT", "")
    if not (events_url and chunk_root and dataset_root):
        _log("error", "ONEBUCKET_CHUNK_ROOT and ONEBUCKET_DATASET_ROOT must be set "
                      "(bash scripts/start-event-consumer.sh validates them)")
        return 2
    try:
        tokens = onebucket_auth.TokenManager(events_url)
        tokens.access_token()  # fail fast on a missing/unsafe token file
    except onebucket_auth.AuthError as exc:
        _log("error", f"{exc}")
        return 3

    bucket = args.bucket or _bucket_from_env()
    state = State(bucket=bucket, events_url=events_url)
    stop = threading.Event()
    catchup_requested = threading.Event()
    s3 = boto3.client("s3")
    event_log = EventLog(_resolve_repo_path(args.event_log))
    cursors = CursorStore(_resolve_repo_path(args.cursor_file))
    if args.reset_cursor:
        cursors.reset()
    consumer = Consumer(
        state=state, s3=s3, chunk_root=chunk_root, dataset_root=dataset_root,
        heartbeat_path=args.heartbeat_file, event_log=event_log, deduper=EventDeduper(),
    )

    signal_count = {"n": 0}

    def _shutdown(signum, frame):  # noqa: ARG001
        signal_count["n"] += 1
        if signal_count["n"] >= 2:
            _log("shutdown", f"signal {signum} received twice; forcing exit")
            delete_heartbeat(args.heartbeat_file)
            os._exit(130)
        _log("shutdown", f"signal {signum} received; finishing the current batch (Ctrl-C again to force)")
        stop.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGUSR1, lambda *_: catchup_requested.set())

    threading.Thread(target=_heartbeat_loop, args=(state, args.heartbeat_file, stop), daemon=True).start()
    _log("startup", f"tailing {events_url} for OneBucket={bucket!r}; wait={args.wait_seconds}s limit={args.limit}")

    if args.catchup_on_start:
        run_catchup(consumer)

    cursor = cursors.load()
    with state.lock:
        state.cursor_present = cursor is not None
    client = EventsClient(events_url, tokens, args.wait_seconds)
    backoff = BACKOFF_INITIAL_S
    exit_code = 0
    try:
        while not stop.is_set():
            try:
                if client.session_id is None:
                    info = client.initialize()
                    server = info.get("serverInfo", {})
                    _log("connected", f"{server.get('name', '?')} {server.get('version', '')} session established")
                    with state.lock:
                        state.connected = True
                        state.auth_state = AUTH_OK
                events, next_cursor, error_text = client.tail_events(cursor, args.limit, args.wait_seconds)
                backoff = BACKOFF_INITIAL_S
                if error_text is not None:
                    if is_invalid_cursor_error(error_text):
                        _log("warning", "stored cursor rejected by the server; resetting to now. Events that "
                                        "arrived while this consumer was down were NOT replayed: run "
                                        "`kill -USR1 <pid>` or restart with --catchup-on-start to reconcile.")
                        cursors.reset()
                        cursor = None
                        continue
                    raise RuntimeError(f"tail_events error: {error_text[:200]}")
                if events:
                    _log("batch", f"{len(events)} event(s)")
                if consumer.process_batch(events) and next_cursor:
                    cursor = next_cursor
                    cursors.save(cursor)
                    with state.lock:
                        state.cursor_present = True
                elif next_cursor and not events:
                    cursor = next_cursor
                    cursors.save(cursor)
                if catchup_requested.is_set():
                    catchup_requested.clear()
                    run_catchup(consumer)
            except onebucket_auth.AuthExpired as exc:
                with state.lock:
                    state.connected = False
                    state.auth_state = AUTH_REFRESH_FAILED if "refresh" in str(exc) else AUTH_EXPIRED
                write_heartbeat(args.heartbeat_file, state)
                _log("error", f"authentication failed: {exc}")
                _log("error", "sign in again with: uv run python scripts/onebucket-login.py  (then restart this consumer)")
                exit_code = 3
                break
            except SessionLost as exc:
                _log("reconnect", f"session no longer valid ({exc}); re-initializing")
                client.session_id = None
                with state.lock:
                    state.connected = False
            except (httpx.HTTPError, RuntimeError, OSError) as exc:
                with state.lock:
                    state.connected = False
                client.session_id = None
                sleep_for = backoff + random.uniform(0, backoff / 2)
                _log("disconnect", f"{type(exc).__name__}: {str(exc)[:200]}; retrying in {sleep_for:.1f}s")
                stop.wait(sleep_for)
                backoff = min(backoff * 2, BACKOFF_MAX_S)
    finally:
        client.close()
        _log("shutdown", "exiting; removing heartbeat")
        delete_heartbeat(args.heartbeat_file)
        event_log.close()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
