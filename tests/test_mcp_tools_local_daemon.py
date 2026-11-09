"""Tests for ``ingest_document``'s local-daemon execution-mode dispatch.

Coverage:
  * DAEMON_NOT_RUNNING returned when heartbeat is missing
  * DAEMON_NOT_RUNNING returned when heartbeat is stale (>30s old)
  * DAEMON_WATCHING_WRONG_BUCKET returned when bucket mismatches
  * daemon_currently_busy=True surfaces when daemon is processing another doc
  * Normal success path when heartbeat is fresh and idle
  * Lambda mode bypasses the heartbeat check entirely

These tests mock out S3 (moto) and the SEC EDGAR fetch path so they're fully
hermetic. The heartbeat file is a real on-disk file in a tmp dir.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import pytest
from moto import mock_aws

from onebucket_demo import mcp_tools


@pytest.fixture
def heartbeat_path(tmp_path, monkeypatch):
    """Point the daemon-heartbeat lookup at a per-test temp file."""
    path = tmp_path / "heartbeat.json"
    monkeypatch.setenv("ONEBUCKET_DAEMON_HEARTBEAT_PATH", str(path))
    return path


@pytest.fixture
def aws_local_daemon_env(monkeypatch, heartbeat_path):
    """Configure env so ingest_document operates in local-daemon mode against
    a moto-mocked S3 bucket."""
    monkeypatch.setenv("ONEBUCKET_EXECUTION_MODE", "local-daemon")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("ONEBUCKET_CHUNK_ROOT", "s3://onebucket-test-bucket/chunks")
    monkeypatch.setenv(
        "ONEBUCKET_DATASET_ROOT", "s3://onebucket-test-bucket/metadata"
    )
    with mock_aws():
        import boto3

        boto3.client("s3", region_name="us-east-1").create_bucket(
            Bucket="onebucket-test-bucket"
        )
        boto3.client("sqs", region_name="us-east-1").create_queue(
            QueueName="onebucket-embedder-dlq"
        )
        yield


def _write_heartbeat(path: Path, *, last_seen_seconds_ago: float = 0.0, **fields) -> None:
    """Write a heartbeat file with optional age + overridable fields."""
    now = _dt.datetime.now(_dt.timezone.utc)
    last_seen = now - _dt.timedelta(seconds=last_seen_seconds_ago)
    payload = {
        "pid": 12345,
        "started_at": (now - _dt.timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last_seen": last_seen.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "mode": "local-daemon",
        "poll_interval_seconds": 5,
        "bucket": "s3://onebucket-test-bucket",
        "daemon_state": "idle",
        "current_raw_file_id": None,
        "processed_count": 0,
    }
    payload.update(fields)
    path.write_text(json.dumps(payload))


# -----------------------------------------------------------------------
# Error envelopes
# -----------------------------------------------------------------------


def test_daemon_not_running_when_heartbeat_missing(aws_local_daemon_env, tmp_path):
    """A silent no-op is worse than a clear error: with no daemon running the
    upload would sit in raw/ forever. Missing heartbeat → DAEMON_NOT_RUNNING +
    actionable message."""
    result = mcp_tools.ingest_document("/tmp/nonexistent-for-test.txt")
    assert result.get("error") == "DAEMON_NOT_RUNNING"
    assert "start-pipeline-daemon.sh" in result["message"]
    assert result["heartbeat_present"] is False


def test_daemon_not_running_when_heartbeat_stale(aws_local_daemon_env, heartbeat_path):
    """Heartbeat older than DAEMON_HEARTBEAT_STALE_AFTER_SECONDS → DAEMON_NOT_RUNNING.
    The 30-second threshold is the named constant in mcp_tools."""
    _write_heartbeat(heartbeat_path, last_seen_seconds_ago=120.0)
    result = mcp_tools.ingest_document("/tmp/dummy.txt")
    assert result.get("error") == "DAEMON_NOT_RUNNING"
    # Heartbeat exists but is stale; the response acknowledges this so the
    # user can distinguish "never started" from "died".
    assert result["heartbeat_present"] is True


def test_daemon_watching_wrong_bucket(aws_local_daemon_env, heartbeat_path):
    """If the daemon is watching a different bucket
    than the MCP server is configured for, refuse the upload."""
    _write_heartbeat(heartbeat_path, bucket="s3://some-other-bucket")
    result = mcp_tools.ingest_document("/tmp/dummy.txt")
    assert result.get("error") == "DAEMON_WATCHING_WRONG_BUCKET"
    assert result["daemon_watching"] == "s3://some-other-bucket"
    assert result["mcp_configured_for"] == "s3://onebucket-test-bucket"


# -----------------------------------------------------------------------
# Queue-awareness
# -----------------------------------------------------------------------


def test_daemon_busy_flag_in_response(aws_local_daemon_env, heartbeat_path, tmp_path):
    """If the heartbeat shows the daemon mid-processing, the
    response includes daemon_currently_busy=True so the agent can communicate
    'queued behind another job' to the user."""
    _write_heartbeat(
        heartbeat_path,
        daemon_state="processing",
        current_raw_file_id="abc123def456",
    )
    # Provide a tiny local file to ingest so the upload path runs cleanly.
    src = tmp_path / "tiny.txt"
    src.write_text("hello world\n")

    result = mcp_tools.ingest_document(str(src))
    # Successful upload, busy state surfaced
    assert "error" not in result
    assert result["daemon_currently_busy"] is True
    assert result["daemon_current_raw_file_id"] == "abc123def456"
    assert result["execution_mode"] == "local-daemon"


def test_normal_success_with_fresh_idle_daemon(
    aws_local_daemon_env, heartbeat_path, tmp_path
):
    """Happy path: fresh heartbeat + idle state + matching bucket → upload proceeds."""
    _write_heartbeat(heartbeat_path)  # default: idle, current bucket, last_seen=now
    src = tmp_path / "tiny.txt"
    src.write_text("hello world\n")

    result = mcp_tools.ingest_document(str(src))
    assert "error" not in result
    assert result["execution_mode"] == "local-daemon"
    assert result["daemon_currently_busy"] is False
    assert len(result["raw_file_id"]) == 16
    assert result["raw_key"].startswith("raw/")
    assert "daemon will process" in result["status"]

    # Verify the object actually landed in moto S3
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    obj = s3.get_object(Bucket="onebucket-test-bucket", Key=result["raw_key"])
    body = obj["Body"].read().decode("utf-8")
    assert "hello world" in body


# -----------------------------------------------------------------------
# Lambda mode bypasses the heartbeat check entirely
# -----------------------------------------------------------------------


def test_lambda_mode_ignores_heartbeat(aws_local_daemon_env, monkeypatch, tmp_path):
    """When ONEBUCKET_EXECUTION_MODE=lambda, the heartbeat check is skipped
    entirely. S3 events fire AWS Lambda; the local daemon is irrelevant."""
    monkeypatch.setenv("ONEBUCKET_EXECUTION_MODE", "lambda")
    # No heartbeat written — would fail in local-daemon mode, but lambda mode
    # never checks.
    src = tmp_path / "tiny.txt"
    src.write_text("hello world\n")

    result = mcp_tools.ingest_document(str(src))
    assert "error" not in result
    assert result["execution_mode"] == "lambda"
    assert "Lambda will fire" in result["status"]
    # daemon-only fields should NOT be present
    assert "daemon_currently_busy" not in result


def test_invalid_execution_mode_raises(aws_local_daemon_env, monkeypatch):
    """Bogus mode value → clear configuration error."""
    monkeypatch.setenv("ONEBUCKET_EXECUTION_MODE", "potato")
    with pytest.raises(RuntimeError, match="ONEBUCKET_EXECUTION_MODE"):
        mcp_tools.ingest_document("/tmp/dummy.txt")


# -----------------------------------------------------------------------
# Heartbeat-fresh helper unit tests (no S3 required)
# -----------------------------------------------------------------------


def test_heartbeat_fresh_helper_returns_false_when_missing(heartbeat_path):
    # heartbeat_path fixture sets env var but doesn't create the file
    fresh, hb = mcp_tools._daemon_heartbeat_fresh()
    assert fresh is False
    assert hb is None


def test_heartbeat_fresh_helper_returns_true_when_recent(heartbeat_path):
    _write_heartbeat(heartbeat_path, last_seen_seconds_ago=2.0)
    fresh, hb = mcp_tools._daemon_heartbeat_fresh()
    assert fresh is True
    assert hb is not None
    assert hb["bucket"] == "s3://onebucket-test-bucket"


def test_heartbeat_fresh_helper_returns_false_when_stale(heartbeat_path):
    _write_heartbeat(heartbeat_path, last_seen_seconds_ago=120.0)
    fresh, hb = mcp_tools._daemon_heartbeat_fresh()
    assert fresh is False
    # Heartbeat dict still returned for diagnostic purposes
    assert hb is not None
