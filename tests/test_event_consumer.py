"""Tests for the event consumer's pure logic: parsing the events server's
payload, validating object keys, de-duplicating replays, and deciding which
events trigger the pipeline.

The sample payload in ``tests/fixtures/events_sample.json`` is the shape the
OneBucket events server returns: one gateway event plus one backend event per
peer, all sharing a ``reqId``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from onebucket_demo.event_consumer import (
    DISPATCH_CHUNKER,
    SKIP_BACKEND_LAYER,
    SKIP_CHUNK_WRITE,
    SKIP_MANIFEST_WRITE,
    SKIP_NON_SUCCESS,
    SKIP_NOT_PUT,
    SKIP_OTHER,
    SKIP_WRONG_BUCKET,
    EventDeduper,
    InvalidEvent,
    StorageEvent,
    classify,
    is_invalid_cursor_error,
    validate_key,
)

FIXTURE = Path(__file__).parent / "fixtures" / "events_sample.json"


@pytest.fixture
def sample() -> dict:
    return json.loads(FIXTURE.read_text())


def _event(**overrides) -> StorageEvent:
    base = dict(
        event_id="e1", req_id="r1", layer="gateway", action="s3:PutObject", bucket="demo-data",
        key="raw/doc.txt", status_code=200, bytes_in=10, bytes_out=0, time="2026-11-06T12:00:00Z",
        duration_ms=5,
    )
    base.update(overrides)
    return StorageEvent(**base)


# --- parsing ------------------------------------------------------------------


def test_fixture_parses_every_event(sample):
    events = [StorageEvent.from_dict(e) for e in sample["events"]]
    assert len(events) == sample["count"] == 5
    gateway_put = events[0]
    assert gateway_put.layer == "gateway" and gateway_put.access_key and not gateway_put.endpoint
    backend_put = events[1]
    assert backend_put.layer == "backend" and backend_put.endpoint == "EAST_S3" and not backend_put.access_key
    assert {e.req_id for e in events[:3]} == {"a1b2c3d4e5f60718"}, "one request, three layers"
    assert events[4].key == "", "bucket-level operations carry no key"
    assert events[0].bytes_in == 413204 and events[3].bytes_out == 2048


def test_unknown_fields_are_ignored():
    event = StorageEvent.from_dict(
        {"eventId": "x", "layer": "gateway", "action": "s3:PutObject", "bucket": "b", "key": "raw/a.txt",
         "statusCode": 200, "bytes": {"in": 1, "out": 0}, "time": "t", "durationMs": 1, "futureField": {"x": 1}}
    )
    assert event.event_id == "x" and event.key == "raw/a.txt"


@pytest.mark.parametrize(
    "bad",
    [
        "not a dict",
        {},
        {"eventId": "x"},
        {"eventId": "x", "layer": "gateway", "action": "s3:PutObject", "statusCode": "two hundred"},
        {"eventId": 123, "layer": "gateway", "action": "s3:PutObject"},
        {"eventId": "x", "layer": "gateway", "action": "s3:PutObject", "bucket": "b", "key": "raw/../../etc/passwd"},
        {"eventId": "x", "layer": "gateway", "action": "s3:PutObject", "bucket": "b", "key": "raw/a\x00.txt"},
        {"eventId": "x", "layer": "gateway", "action": "s3:PutObject", "bucket": "b", "key": "/etc/passwd"},
        {"eventId": "x", "layer": "gateway", "action": "s3:PutObject", "bucket": "b", "key": "file:///etc/passwd"},
        {"eventId": "x", "layer": "gateway", "action": "s3:PutObject", "bucket": "b", "key": "raw/" + "a" * 1100},
    ],
)
def test_malformed_or_hostile_events_are_rejected(bad):
    with pytest.raises(InvalidEvent):
        StorageEvent.from_dict(bad)


def test_validate_key_accepts_normal_keys():
    for key in ("raw/a.txt", "chunks/0123456789abcdef/chunk_0000.txt", "raw/sub dir/with spaces.md", ""):
        validate_key(key)


# --- classify -----------------------------------------------------------------


def test_only_the_gateway_put_under_raw_dispatches(sample):
    outcomes = [classify(StorageEvent.from_dict(e), "demo-data") for e in sample["events"]]
    assert outcomes == [DISPATCH_CHUNKER, SKIP_BACKEND_LAYER, SKIP_BACKEND_LAYER, SKIP_NOT_PUT, SKIP_NOT_PUT]


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({}, DISPATCH_CHUNKER),
        ({"action": "s3:GetObject"}, SKIP_NOT_PUT),
        ({"status_code": 403}, SKIP_NON_SUCCESS),
        ({"bucket": "someone-else"}, SKIP_WRONG_BUCKET),
        ({"layer": "backend", "endpoint": "EAST_S3"}, SKIP_BACKEND_LAYER),
        ({"key": "chunks/0123456789abcdef/MANIFEST.json"}, SKIP_MANIFEST_WRITE),
        ({"key": "chunks/0123456789abcdef/chunk_0003.txt"}, SKIP_CHUNK_WRITE),
        ({"key": "metadata/global_index.lance/data/x.lance"}, SKIP_OTHER),
        ({"key": "raw/"}, SKIP_OTHER),
        ({"key": ""}, SKIP_OTHER),
    ],
)
def test_classify_reasons(overrides, expected):
    assert classify(_event(**overrides), "demo-data") == expected


def test_classify_checks_bucket_before_layer():
    """A backend echo from another bucket is reported as the wrong bucket,
    which is the more useful reason when reading the log."""
    assert classify(_event(bucket="other", layer="backend"), "demo-data") == SKIP_WRONG_BUCKET


# --- dedup --------------------------------------------------------------------


def test_deduper_remembers_within_capacity():
    d = EventDeduper(capacity=3)
    for e in ("a", "b", "c"):
        assert not d.is_duplicate(e)
        d.record(e)
    assert d.is_duplicate("a") and len(d) == 3
    d.record("d")  # evicts "a", the oldest
    assert not d.is_duplicate("a") and d.is_duplicate("d") and len(d) == 3


def test_deduper_touch_keeps_recent_ids_alive():
    d = EventDeduper(capacity=2)
    d.record("a")
    d.record("b")
    d.record("a")  # touch: "a" is now the most recent
    d.record("c")  # evicts "b"
    assert d.is_duplicate("a") and not d.is_duplicate("b")


def test_deduper_rejects_zero_capacity():
    with pytest.raises(ValueError):
        EventDeduper(capacity=0)


# --- cursor errors ------------------------------------------------------------


def test_invalid_cursor_error_detection():
    assert is_invalid_cursor_error("invalid cursor: must be a nextCursor token from a previous tail_events call")
    assert is_invalid_cursor_error("  Invalid Cursor ")
    assert not is_invalid_cursor_error("rate limited")
