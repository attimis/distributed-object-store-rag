"""Tests for the event consumer script: the MCP events client, batch
processing with cursor semantics, the JSONL log, cursor persistence and the
heartbeat. The pipeline dispatch is replaced by a fake so no S3 or model is
touched; the events server is an ``httpx.MockTransport``.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import time
from pathlib import Path

import httpx
import pytest

from onebucket_demo import onebucket_auth as auth
from onebucket_demo.event_consumer import DISPATCH_CHUNKER, SKIP_BACKEND_LAYER, SKIP_INVALID, EventDeduper

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "events_sample.json"
EVENTS = "https://events.onebucket.io"


@pytest.fixture(scope="module")
def consumer_mod():
    spec = importlib.util.spec_from_file_location(
        "oneb_event_consumer", REPO_ROOT / "scripts" / "oneb_event_consumer.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["oneb_event_consumer"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def sample() -> dict:
    return json.loads(FIXTURE.read_text())


class _StaticTokens:
    """TokenManager stand-in: hands out a fixed token, counts forced refreshes."""

    def __init__(self):
        self.refreshes = 0

    def access_token(self):
        return "at-test"

    def force_refresh(self):
        self.refreshes += 1
        return "at-test-2"


class FakeEventsServer:
    """Streamable-HTTP MCP events server in the shape OneBucket answers."""

    def __init__(self, batches: list[dict]):
        self.batches = list(batches)
        self.calls: list[dict] = []
        self.session = "sess-1"
        self.reject_first_auth = False
        self.drop_session_once = False

    def handler(self, request: httpx.Request) -> httpx.Response:
        if self.reject_first_auth:
            self.reject_first_auth = False
            return httpx.Response(401, text="Unauthorized")
        body = json.loads(request.content) if request.content else {}
        self.calls.append(body)
        method = body.get("method")
        if method == "initialize":
            return httpx.Response(
                200,
                headers={"mcp-session-id": self.session},
                json={"jsonrpc": "2.0", "id": body["id"], "result": {"serverInfo": {"name": "fake", "version": "0"}}},
            )
        if method == "notifications/initialized":
            return httpx.Response(202)
        if request.headers.get("mcp-session-id") != self.session:
            return httpx.Response(404, text="Invalid session ID")
        if self.drop_session_once:
            self.drop_session_once = False
            self.session = "sess-2"
            return httpx.Response(404, text="Invalid session ID")
        args = body["params"]["arguments"]
        if args.get("cursor") == "garbage":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": {
                "isError": True, "content": [{"type": "text", "text": "invalid cursor: must be a nextCursor token"}]}})
        batch = self.batches.pop(0) if self.batches else {"count": 0, "events": [], "nextCursor": "cur-empty"}
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": {
            "content": [{"type": "text", "text": json.dumps(batch)}], "structuredContent": batch}})


def _client_for(consumer_mod, server: FakeEventsServer, tokens=None):
    tokens = tokens or _StaticTokens()
    http = httpx.Client(transport=httpx.MockTransport(server.handler))
    return consumer_mod.EventsClient(EVENTS, tokens, wait_seconds=1, client=http), tokens


# --- EventsClient -------------------------------------------------------------


def test_client_initializes_and_tails(consumer_mod, sample):
    server = FakeEventsServer([sample])
    client, _ = _client_for(consumer_mod, server)
    info = client.initialize()
    assert info["serverInfo"]["name"] == "fake" and client.session_id == "sess-1"
    events, cursor, err = client.tail_events(None, 100, 1)
    assert err is None and len(events) == 5 and cursor == sample["nextCursor"]
    call = server.calls[-1]
    assert call["params"]["name"] == "tail_events"
    assert call["params"]["arguments"] == {"limit": 100, "waitSeconds": 1}
    events, cursor2, _ = client.tail_events(cursor, 100, 1)
    assert events == [] and cursor2 == "cur-empty"
    assert server.calls[-1]["params"]["arguments"]["cursor"] == sample["nextCursor"]


def test_client_reports_invalid_cursor_as_error_text(consumer_mod):
    client, _ = _client_for(consumer_mod, FakeEventsServer([]))
    client.initialize()
    events, cursor, err = client.tail_events("garbage", 10, 0)
    assert events == [] and cursor is None
    assert consumer_mod.is_invalid_cursor_error(err)


def test_client_refreshes_token_on_401_once(consumer_mod):
    server = FakeEventsServer([])
    server.reject_first_auth = True
    client, tokens = _client_for(consumer_mod, server)
    client.initialize()
    assert tokens.refreshes == 1


def test_client_raises_session_lost_on_404(consumer_mod):
    server = FakeEventsServer([])
    client, _ = _client_for(consumer_mod, server)
    client.initialize()
    server.drop_session_once = True
    with pytest.raises(consumer_mod.SessionLost):
        client.tail_events(None, 10, 0)
    client.initialize()  # recovers with the new session id
    assert client.session_id == "sess-2"


def test_client_refuses_untrusted_events_url(consumer_mod):
    with pytest.raises(auth.AuthConfigError):
        consumer_mod.EventsClient("https://evil.example.com", _StaticTokens(), 1, client=httpx.Client())


# --- Consumer.process_batch ---------------------------------------------------


class _Harness:
    def __init__(self, consumer_mod, tmp_path, *, fail_keys=(), fail_times=1):
        self.mod = consumer_mod
        self.state = consumer_mod.State(bucket="demo-data", events_url=EVENTS)
        self.log_path = tmp_path / "events.jsonl"
        self.heartbeat = str(tmp_path / "hb.json")
        self.dispatched: list[str] = []
        self.fail_keys = dict.fromkeys(fail_keys, fail_times)
        self.consumer = consumer_mod.Consumer(
            state=self.state, s3=object(), chunk_root="s3://demo-data/chunks",
            dataset_root="s3://demo-meta/metadata", heartbeat_path=self.heartbeat,
            event_log=consumer_mod.EventLog(self.log_path), deduper=EventDeduper(),
        )

    def fake_dispatch(self, s3, bucket, raw_key, chunk_root, dataset_root):  # noqa: ARG002
        if self.fail_keys.get(raw_key, 0) > 0:
            self.fail_keys[raw_key] -= 1
            raise RuntimeError("transient S3 error")
        self.dispatched.append(raw_key)
        return "0123456789abcdef"

    def records(self) -> list[dict]:
        return [json.loads(line) for line in self.log_path.read_text().splitlines() if line.strip()]


def test_batch_dispatches_gateway_put_once_and_logs_everything(consumer_mod, sample, tmp_path, monkeypatch):
    h = _Harness(consumer_mod, tmp_path)
    monkeypatch.setattr(consumer_mod, "dispatch_raw_object", h.fake_dispatch)
    assert h.consumer.process_batch(sample["events"]) is True
    assert h.dispatched == ["raw/example-10k-2026-01-31.txt"]
    records = h.records()
    assert [r["dispatch_outcome"] for r in records][:3] == [DISPATCH_CHUNKER, SKIP_BACKEND_LAYER, SKIP_BACKEND_LAYER]
    put = records[0]
    assert put["dispatch_raw_file_id"] == "0123456789abcdef" and put["req_id"] == "a1b2c3d4e5f60718"
    assert put["layer"] == "gateway" and records[1]["endpoint"] == "EAST_S3"
    assert put["consumer_dispatch_done_at"] and isinstance(put["dispatch_duration_ms"], int)
    assert h.state.events_received == 5 and h.state.events_dispatched == 1 and h.state.events_skipped == 4
    assert h.state.processed_count == 1 and h.state.daemon_state == "idle"
    assert json.loads(Path(h.heartbeat).read_text())["mode"] == "onebucket-events"


def test_replayed_batch_is_deduplicated(consumer_mod, sample, tmp_path, monkeypatch):
    h = _Harness(consumer_mod, tmp_path)
    monkeypatch.setattr(consumer_mod, "dispatch_raw_object", h.fake_dispatch)
    h.consumer.process_batch(sample["events"])
    h.consumer.process_batch(sample["events"])
    assert h.dispatched == ["raw/example-10k-2026-01-31.txt"]
    assert len(h.records()) == 5, "duplicates are not logged twice"


def test_transient_failure_holds_cursor_then_succeeds_on_replay(consumer_mod, sample, tmp_path, monkeypatch):
    h = _Harness(consumer_mod, tmp_path, fail_keys=["raw/example-10k-2026-01-31.txt"], fail_times=1)
    monkeypatch.setattr(consumer_mod, "dispatch_raw_object", h.fake_dispatch)
    assert h.consumer.process_batch(sample["events"]) is False, "cursor must not advance"
    assert h.records()[0]["dispatch_error"].startswith("RuntimeError")
    assert h.consumer.process_batch(sample["events"]) is True, "replay succeeds"
    assert h.dispatched == ["raw/example-10k-2026-01-31.txt"]


def test_persistent_failure_gives_up_after_max_attempts(consumer_mod, sample, tmp_path, monkeypatch):
    h = _Harness(consumer_mod, tmp_path, fail_keys=["raw/example-10k-2026-01-31.txt"], fail_times=99)
    monkeypatch.setattr(consumer_mod, "dispatch_raw_object", h.fake_dispatch)
    results = [h.consumer.process_batch(sample["events"]) for _ in range(consumer_mod.MAX_DISPATCH_ATTEMPTS)]
    assert results == [False] * (consumer_mod.MAX_DISPATCH_ATTEMPTS - 1) + [True]
    assert h.dispatched == []


def test_invalid_event_is_logged_and_skipped(consumer_mod, tmp_path, monkeypatch):
    h = _Harness(consumer_mod, tmp_path)
    monkeypatch.setattr(consumer_mod, "dispatch_raw_object", h.fake_dispatch)
    hostile = {"eventId": "evil", "layer": "gateway", "action": "s3:PutObject", "bucket": "demo-data",
               "key": "raw/../../etc/passwd", "statusCode": 200, "time": "t", "durationMs": 1}
    assert h.consumer.process_batch([hostile, "garbage"]) is True
    assert h.dispatched == []
    assert [r["dispatch_outcome"] for r in h.records()] == [SKIP_INVALID, SKIP_INVALID]


# --- cursor + log files -------------------------------------------------------


def test_cursor_store_round_trip_and_reset(consumer_mod, tmp_path):
    store = consumer_mod.CursorStore(tmp_path / "state" / "events.cursor")
    assert store.load() is None
    store.save("abc==")
    assert store.load() == "abc=="
    assert not list((tmp_path / "state").glob(".tmp-*")), "no temp files left behind"
    store.reset()
    assert store.load() is None


def test_event_log_lines_are_single_line_json(consumer_mod, tmp_path):
    log = consumer_mod.EventLog(tmp_path / "e.jsonl")
    log.append({"a": 1, "text": "two\nlines"})
    log.close()
    lines = (tmp_path / "e.jsonl").read_text().splitlines()
    assert len(lines) == 1 and json.loads(lines[0])["text"] == "two\nlines"


def test_heartbeat_contains_auth_state(consumer_mod, tmp_path):
    state = consumer_mod.State(bucket="demo-data", events_url=EVENTS)
    state.auth_state = consumer_mod.AUTH_EXPIRED
    consumer_mod.write_heartbeat(str(tmp_path / "hb"), state)
    hb = json.loads((tmp_path / "hb").read_text())
    assert hb["auth_state"] == "expired" and hb["connected"] is False and hb["bucket"] == "s3://demo-data"
    assert time.strptime(hb["last_seen"], "%Y-%m-%dT%H:%M:%SZ")


def test_repo_relative_paths_resolve_against_repo_root(consumer_mod):
    assert consumer_mod._resolve_repo_path("x/y.jsonl") == REPO_ROOT / "x" / "y.jsonl"
    assert consumer_mod._resolve_repo_path("/abs/y.jsonl") == Path("/abs/y.jsonl")


def test_client_reads_the_response_out_of_an_event_stream(consumer_mod, sample):
    """When events arrive during a long poll the server answers with a
    server-sent-event stream: a notification per event, then the JSON-RPC
    response with the events and nextCursor. The client must use the final
    response, not the first message."""
    batch = {"count": 1, "events": [sample["events"][0]], "nextCursor": "cur-after-stream"}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else {}
        if body.get("method") == "initialize":
            return httpx.Response(200, headers={"mcp-session-id": "s"}, json={"jsonrpc": "2.0", "id": body["id"], "result": {}})
        if body.get("method") == "notifications/initialized":
            return httpx.Response(202)
        notification = {"jsonrpc": "2.0", "method": "notifications/storage/event", "params": {"event": sample["events"][0]}}
        final = {"jsonrpc": "2.0", "id": body["id"], "result": {"structuredContent": batch,
                 "content": [{"type": "text", "text": json.dumps(batch)}]}}
        stream = f"event: message\ndata: {json.dumps(notification)}\n\nevent: message\ndata: {json.dumps(final)}\n\n"
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=stream)

    client = consumer_mod.EventsClient(EVENTS, _StaticTokens(), 1, client=httpx.Client(transport=httpx.MockTransport(handler)))
    client.initialize()
    events, cursor, err = client.tail_events("cur-before", 100, 20)
    assert err is None and cursor == "cur-after-stream"
    assert [e["eventId"] for e in events] == [sample["events"][0]["eventId"]]
