"""Pure logic for the OneBucket event consumer: parsing, validation, dedup and
routing. No network code lives here, so all of it is unit-testable; the
polling loop is in ``scripts/oneb_event_consumer.py``.

How OneBucket reports activity
------------------------------
Every request to a OneBucket produces several *events* that share one
``reqId``: one ``gateway`` event (the request as the client made it, tagged
with the caller's ``accessKey``) plus one ``backend`` event per peer bucket
the request reached (tagged with the peer's ``endpoint`` name). The events
MCP server exposes them through ``tail_events``, oldest first.

The consumer dispatches the pipeline on the **gateway** event of a successful
``s3:PutObject`` under ``raw/`` in the watched OneBucket. Using the gateway
event means each upload triggers exactly one pipeline run however many peers
the write fanned out to. Backend events are kept in the event log because
they carry the per-peer timing the demo's replication metrics are built from.
"""

from __future__ import annotations

import collections
from dataclasses import dataclass

# --- Filter constants ---------------------------------------------------------

ACTION_PUT_OBJECT = "s3:PutObject"
LAYER_GATEWAY = "gateway"
LAYER_BACKEND = "backend"
RAW_PREFIX = "raw/"
MANIFEST_SUFFIX = "/MANIFEST.json"
CHUNKS_PREFIX = "chunks/"
MAX_KEY_BYTES = 1024

# Default dedup ring buffer size. Bounded memory; collisions only matter under
# very high event rates AND a window of more than this many events between
# duplicate deliveries (a replayed batch after a crash, for example).
DEFAULT_DEDUP_CAPACITY = 10_000


class InvalidEvent(ValueError):
    """An event payload failed validation and must not reach the pipeline."""


# --- Event payload ------------------------------------------------------------


@dataclass(frozen=True)
class StorageEvent:
    """One storage event as returned by the ``events`` MCP server.

    Field names mirror the wire format (``eventId`` → ``event_id`` and so on).
    ``endpoint`` is set on backend events, ``access_key`` on gateway events;
    ``key`` is empty for bucket-level operations such as ``s3:ListObjectsV2``.
    """

    event_id: str
    req_id: str
    layer: str
    action: str
    bucket: str
    key: str
    status_code: int
    bytes_in: int
    bytes_out: int
    time: str
    duration_ms: int
    endpoint: str = ""
    access_key: str = ""

    @classmethod
    def from_dict(cls, event: dict) -> "StorageEvent":
        """Parse one event object. Unknown fields are ignored; the fields the
        consumer relies on are typed and bounded, and a key that could escape
        the bucket layout (parent segments, NUL bytes, URL schemes, absolute
        paths) raises :class:`InvalidEvent`.
        """
        if not isinstance(event, dict):
            raise InvalidEvent("event is not an object")
        try:
            key = str(event.get("key") or "")
            raw_bytes = event.get("bytes") or {}
            parsed = cls(
                event_id=_text(event.get("eventId"), "eventId", required=True),
                req_id=_text(event.get("reqId"), "reqId"),
                layer=_text(event.get("layer"), "layer", required=True),
                action=_text(event.get("action"), "action", required=True),
                bucket=_text(event.get("bucket"), "bucket"),
                key=key,
                status_code=int(event.get("statusCode", 0)),
                bytes_in=int(raw_bytes.get("in", 0) or 0),
                bytes_out=int(raw_bytes.get("out", 0) or 0),
                time=_text(event.get("time"), "time"),
                duration_ms=int(event.get("durationMs", 0) or 0),
                endpoint=_text(event.get("endpoint"), "endpoint"),
                access_key=_text(event.get("accessKey"), "accessKey"),
            )
        except (TypeError, ValueError, AttributeError) as exc:
            if isinstance(exc, InvalidEvent):
                raise
            raise InvalidEvent(f"malformed event: {type(exc).__name__}: {exc}") from exc
        validate_key(parsed.key)
        return parsed


def _text(value, name: str, *, required: bool = False, limit: int = 512) -> str:
    if value is None:
        if required:
            raise InvalidEvent(f"missing {name}")
        return ""
    if not isinstance(value, str):
        raise InvalidEvent(f"{name} must be a string")
    if "\x00" in value:
        raise InvalidEvent(f"{name} contains a NUL byte")
    if len(value) > limit:
        raise InvalidEvent(f"{name} longer than {limit} characters")
    return value


def validate_key(key: str) -> None:
    """Reject object keys that could point outside the expected layout."""
    if not key:
        return
    if len(key.encode("utf-8")) > MAX_KEY_BYTES:
        raise InvalidEvent("key longer than 1024 bytes")
    if "\x00" in key:
        raise InvalidEvent("key contains a NUL byte")
    if key.startswith("/") or "://" in key:
        raise InvalidEvent("key must be a plain relative object key")
    if ".." in key.replace("\\", "/").split("/"):
        raise InvalidEvent("key contains a parent-directory segment")


# --- Dedup --------------------------------------------------------------------


class EventDeduper:
    """Bounded ring buffer of eventIds already handled.

    Guards against a batch being replayed after a crash: the consumer only
    persists its cursor once a whole batch has been processed, so on restart
    the same events can arrive again. Capacity bounds both memory and the
    dedup window.
    """

    def __init__(self, capacity: int = DEFAULT_DEDUP_CAPACITY) -> None:
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        self._seen: collections.OrderedDict[str, None] = collections.OrderedDict()
        self._capacity = capacity

    def is_duplicate(self, event_id: str) -> bool:
        return event_id in self._seen

    def record(self, event_id: str) -> None:
        if event_id in self._seen:
            self._seen.move_to_end(event_id)
            return
        self._seen[event_id] = None
        while len(self._seen) > self._capacity:
            self._seen.popitem(last=False)

    def __len__(self) -> int:
        return len(self._seen)


# --- Dispatch routing ---------------------------------------------------------

# Outcome strings. They are written into the JSONL event log as
# ``dispatch_outcome`` so every filtered-out event is auditable; keep them
# stable, the demo's analytics reference them as literals.
DISPATCH_CHUNKER = "dispatch_chunker"
SKIP_NOT_PUT = "skip_not_put"
SKIP_NON_SUCCESS = "skip_non_success"
SKIP_WRONG_BUCKET = "skip_wrong_bucket"
SKIP_BACKEND_LAYER = "skip_backend_layer"  # per-peer echo of a request; telemetry only
SKIP_MANIFEST_WRITE = "skip_manifest_write"  # the pipeline's own commit marker
SKIP_CHUNK_WRITE = "skip_chunk_write"  # the pipeline's own chunk files
SKIP_OTHER = "skip_other"
SKIP_INVALID = "skip_invalid"  # failed validation; never reaches the pipeline

ALL_OUTCOMES = (
    DISPATCH_CHUNKER,
    SKIP_NOT_PUT,
    SKIP_NON_SUCCESS,
    SKIP_WRONG_BUCKET,
    SKIP_BACKEND_LAYER,
    SKIP_MANIFEST_WRITE,
    SKIP_CHUNK_WRITE,
    SKIP_OTHER,
    SKIP_INVALID,
)


def classify(event: StorageEvent, target_bucket: str) -> str:
    """Decide what to do with an event; returns one of the outcome constants.

    The checks run most-specific first so the logged reason explains *why*
    an event was ignored rather than just "wrong bucket".
    """
    if event.action != ACTION_PUT_OBJECT:
        return SKIP_NOT_PUT
    if event.status_code != 200:
        return SKIP_NON_SUCCESS
    if event.bucket != target_bucket:
        return SKIP_WRONG_BUCKET
    if event.layer != LAYER_GATEWAY:
        return SKIP_BACKEND_LAYER
    key = event.key
    if key.startswith(RAW_PREFIX) and len(key) > len(RAW_PREFIX):
        return DISPATCH_CHUNKER
    if key.startswith(CHUNKS_PREFIX) and key.endswith(MANIFEST_SUFFIX):
        return SKIP_MANIFEST_WRITE
    if key.startswith(CHUNKS_PREFIX):
        return SKIP_CHUNK_WRITE
    return SKIP_OTHER


def is_invalid_cursor_error(text: str) -> bool:
    """The events server reports a bad cursor as a tool error whose text
    starts with ``invalid cursor``; the consumer resets its cursor on it."""
    return text.strip().lower().startswith("invalid cursor")
