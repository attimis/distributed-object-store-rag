"""Tests for the two event-log metrics tools over a synthetic JSONL log in the
shape the event consumer writes (flat per-event records; one gateway record
plus one backend record per peer, sharing ``req_id``)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from onebucket_demo import mcp_tools

DOC = "0123456789abcdef"


def _rec(**kw) -> dict:
    base = {
        "consumer_received_at": "2026-11-06T12:00:00.000Z",
        "consumer_dispatch_done_at": "2026-11-06T12:00:00.000Z",
        "event_id": "e", "req_id": "r", "layer": "gateway", "endpoint": None,
        "action": "s3:PutObject", "bucket": "demo-data", "key": "", "status_code": 200,
        "bytes_in": 0, "bytes_out": 0, "event_time": "2026-11-06T12:00:00.000Z", "duration_ms": 10,
        "dispatch_outcome": "skip_other", "dispatch_raw_file_id": None, "dispatch_error": None,
        "dispatch_duration_ms": None,
    }
    base.update(kw)
    return base


def _write(path: Path, records: list[dict]) -> None:
    path.write_text("".join(json.dumps(r) + "\n" for r in records) + "not json\n")


@pytest.fixture
def log(tmp_path, monkeypatch) -> Path:
    p = tmp_path / "events.jsonl"
    monkeypatch.setenv("ONEBUCKET_EVENT_LOG_PATH", str(p))
    monkeypatch.setenv("ONEBUCKET_DATASET_ROOT", "s3://demo-meta/metadata")
    return p


def test_replication_metrics_join_backend_acks_by_request(log):
    records = [
        # An earlier ingest of the same document: must be ignored (session reset).
        _rec(req_id="old", key="raw/doc.txt", dispatch_outcome="dispatch_chunker", dispatch_raw_file_id=DOC,
             consumer_received_at="2026-11-06T11:00:00.000Z", consumer_dispatch_done_at="2026-11-06T11:00:09.000Z",
             event_time="2026-11-06T11:00:00.000Z"),
        _rec(req_id="old-c", key=f"chunks/{DOC}/chunk_0000.txt", event_time="2026-11-06T11:00:01.000Z"),
        # The latest ingest.
        _rec(req_id="g1", key="raw/doc.txt", dispatch_outcome="dispatch_chunker", dispatch_raw_file_id=DOC,
             consumer_received_at="2026-11-06T12:00:00.000Z", consumer_dispatch_done_at="2026-11-06T12:00:30.000Z"),
        _rec(req_id="c1", key=f"chunks/{DOC}/chunk_0000.txt", event_time="2026-11-06T12:00:01.000Z",
             consumer_received_at="2026-11-06T12:00:01.100Z", consumer_dispatch_done_at="2026-11-06T12:00:01.200Z"),
        _rec(req_id="c1", layer="backend", endpoint="EAST_S3", key=f"chunks/{DOC}/chunk_0000.txt",
             event_time="2026-11-06T12:00:01.250Z"),
        _rec(req_id="c1", layer="backend", endpoint="WEST_CACHE", key=f"chunks/{DOC}/chunk_0000.txt",
             event_time="2026-11-06T12:00:01.900Z"),
        _rec(req_id="c2", key=f"chunks/{DOC}/chunk_0001.txt", event_time="2026-11-06T12:00:02.000Z"),
        _rec(req_id="c2", layer="backend", endpoint="EAST_S3", key=f"chunks/{DOC}/chunk_0001.txt",
             event_time="2026-11-06T12:00:02.350Z"),
        _rec(req_id="m1", bucket="demo-meta", key="metadata/global_index.lance/data/a.lance",
             event_time="2026-11-06T12:00:20.000Z"),
        _rec(req_id="m1", layer="backend", endpoint="EAST_S3", bucket="demo-meta",
             key="metadata/global_index.lance/data/a.lance", event_time="2026-11-06T12:00:20.400Z"),
        _rec(req_id="m1", layer="backend", endpoint="WEST_CACHE", bucket="demo-meta",
             key="metadata/global_index.lance/data/a.lance", event_time="2026-11-06T12:00:21.100Z"),
        # Unrelated document and a GET: ignored.
        _rec(req_id="z", key="chunks/ffffffffffffffff/chunk_0000.txt"),
        _rec(req_id="y", action="s3:GetObject", key=f"chunks/{DOC}/chunk_0000.txt"),
    ]
    _write(log, records)
    m = mcp_tools.get_replication_metrics(DOC)
    assert "error" not in m
    assert m["ingest_started_at"] == "2026-11-06T12:00:00.000Z"
    assert m["ingest_completed_at"] == "2026-11-06T12:00:30.000Z"
    assert m["total_elapsed_seconds"] == 30.0
    assert m["chunk_event_count"] == 2
    assert m["metadata_event_count"] == 1
    assert m["per_peer_ack_latency"]["EAST_S3"] == {"count": 3, "median_ms": 350, "p95_ms": 400}
    assert m["per_peer_ack_latency"]["WEST_CACHE"]["count"] == 2
    assert m["metadata_quorum_ack_ms"] == {"count": 1, "median_ms": 1100, "p95_ms": 1100}


def test_replication_metrics_errors(log):
    assert mcp_tools.get_replication_metrics(DOC)["error"] == "EVENT_LOG_MISSING"
    _write(log, [_rec(req_id="g", key="raw/x.txt", dispatch_raw_file_id="fedcba9876543210")])
    assert mcp_tools.get_replication_metrics(DOC)["error"] == "NO_EVENTS_FOUND"
    with pytest.raises(RuntimeError):
        mcp_tools.get_replication_metrics("not-an-id")


def test_chunk_read_metrics_use_gateway_duration_and_first_backend(log):
    records = [
        _rec(req_id="r1", action="s3:GetObject", key=f"chunks/{DOC}/chunk_0000.txt", duration_ms=120,
             consumer_received_at="2026-11-06T12:05:00.000Z", status_code=206),
        _rec(req_id="r1", layer="backend", endpoint="WEST_CACHE", action="s3:GetObject",
             key=f"chunks/{DOC}/chunk_0000.txt", status_code=206, event_time="2026-11-06T12:05:00.030Z"),
        _rec(req_id="r1", layer="backend", endpoint="EAST_S3", action="s3:GetObject",
             key=f"chunks/{DOC}/chunk_0000.txt", status_code=206, event_time="2026-11-06T12:05:00.080Z"),
        _rec(req_id="r2", action="s3:GetObject", key=f"chunks/{DOC}/chunk_0001.txt", duration_ms=40,
             consumer_received_at="2026-11-06T12:05:01.000Z", bucket="demo-west"),
        _rec(req_id="r2", layer="backend", endpoint="WEST_CACHE", action="s3:GetObject",
             key=f"chunks/{DOC}/chunk_0001.txt", status_code=200, event_time="2026-11-06T12:05:01.010Z"),
        # Outside the window, a manifest read, and another document: all excluded.
        _rec(req_id="r3", action="s3:GetObject", key=f"chunks/{DOC}/chunk_0002.txt", duration_ms=999,
             consumer_received_at="2026-11-06T13:00:00.000Z"),
        _rec(req_id="r4", action="s3:GetObject", key=f"chunks/{DOC}/MANIFEST.json", duration_ms=5,
             consumer_received_at="2026-11-06T12:05:02.000Z"),
        _rec(req_id="r5", action="s3:GetObject", key="chunks/ffffffffffffffff/chunk_0000.txt", duration_ms=7,
             consumer_received_at="2026-11-06T12:05:02.000Z"),
    ]
    _write(log, records)
    m = mcp_tools.get_chunk_read_metrics("2026-11-06T12:05:00Z", "2026-11-06T12:10:00Z", raw_file_id=DOC)
    assert m["events_count"] == 2
    assert m["duration_ms"] == {"count": 2, "median_ms": 120, "p95_ms": 120, "min_ms": 40, "max_ms": 120}
    assert set(m["per_policy"]) == {"demo-data", "demo-west"}
    assert m["per_first_responder"] == {"WEST_CACHE": 2}
    assert m["events"][0]["first_responder"] == "WEST_CACHE" and m["events"][0]["status"] == 206

    unfiltered = mcp_tools.get_chunk_read_metrics("2026-11-06T12:05:00Z", "2026-11-06T12:10:00Z")
    assert unfiltered["events_count"] == 3


def test_chunk_read_metrics_argument_errors(log):
    assert mcp_tools.get_chunk_read_metrics("2026-11-06T12:05:00Z")["error"] == "EVENT_LOG_MISSING"
    _write(log, [])
    assert mcp_tools.get_chunk_read_metrics("yesterday")["error"] == "INVALID_ARGUMENT"
    assert mcp_tools.get_chunk_read_metrics("2026-11-06T12:05:00Z", raw_file_id="bad")["error"] == "INVALID_ARGUMENT"
    assert mcp_tools.get_chunk_read_metrics("2026-11-06T12:05:00Z")["events_count"] == 0
