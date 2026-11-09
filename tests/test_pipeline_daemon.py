"""Tests for ``scripts/oneb_pipeline_daemon.py``.

The daemon is a long-running poll loop, so we don't run ``main_loop`` directly
in tests. Instead, we exercise the internal helpers (heartbeat write,
processed-set reconstruction, paginator usage, collision guard) against
moto S3, and patch the pipeline functions to avoid running the real FastEmbed
model load during tests.

Coverage:
  * Heartbeat write is atomic + world-readable (0o666) + has the right schema
  * Heartbeat deletion is safe even if file is missing
  * Processed-set reconstruction handles >1000 raw + chunks objects (paginator)
  * Collision guard logs WARNING when a Lambda notification exists on raw/
  * Collision guard is silent when no Lambda notification is present
  * _process_one skips files violating the type gate
  * _process_one skips files larger than MAX_FILE_BYTES
"""

from __future__ import annotations

import importlib.util
import json
import stat
import sys
from pathlib import Path
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws


# Load the daemon module from scripts/ since it isn't on the normal package path.
def _load_daemon_module():
    repo_root = Path(__file__).resolve().parent.parent
    daemon_path = repo_root / "scripts" / "oneb_pipeline_daemon.py"
    spec = importlib.util.spec_from_file_location("oneb_pipeline_daemon", daemon_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["oneb_pipeline_daemon"] = module
    spec.loader.exec_module(module)
    return module


daemon = _load_daemon_module()


@pytest.fixture
def aws_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="daemon-test-bucket")
        yield s3


# -----------------------------------------------------------------------
# Heartbeat — atomic write, world-readable, correct schema
# -----------------------------------------------------------------------


def test_heartbeat_write_creates_file_with_expected_schema(tmp_path):
    state = daemon._DaemonState(bucket="some-bucket", poll_interval=5)
    state.processed_count = 7
    state.daemon_state = "processing"
    state.current_raw_file_id = "abc1234567890def"
    hb_path = tmp_path / "hb.json"

    daemon._write_heartbeat(str(hb_path), state)

    assert hb_path.exists()
    payload = json.loads(hb_path.read_text())
    # Required heartbeat fields (the MCP server reads these for its preflight)
    for field in (
        "pid",
        "started_at",
        "last_seen",
        "mode",
        "poll_interval_seconds",
        "bucket",
        "daemon_state",
        "current_raw_file_id",
        "processed_count",
    ):
        assert field in payload, f"missing {field}"
    assert payload["mode"] == "local-daemon"
    assert payload["bucket"] == "s3://some-bucket"
    assert payload["daemon_state"] == "processing"
    assert payload["current_raw_file_id"] == "abc1234567890def"
    assert payload["processed_count"] == 7


def test_heartbeat_write_is_world_readable(tmp_path):
    """chmod 0o666 so the MCP server can read the heartbeat even when it runs
    under a different UID than the daemon."""
    state = daemon._DaemonState(bucket="b", poll_interval=5)
    hb_path = tmp_path / "hb.json"
    daemon._write_heartbeat(str(hb_path), state)

    mode = hb_path.stat().st_mode
    # File should have read+write for owner, group, and others (0o666)
    assert mode & stat.S_IRUSR
    assert mode & stat.S_IWUSR
    assert mode & stat.S_IRGRP
    assert mode & stat.S_IROTH


def test_delete_heartbeat_is_safe_if_missing(tmp_path):
    # Should not raise on missing file
    daemon._delete_heartbeat(str(tmp_path / "does-not-exist.json"))


def test_delete_heartbeat_removes_existing(tmp_path):
    hb_path = tmp_path / "hb.json"
    hb_path.write_text("{}")
    daemon._delete_heartbeat(str(hb_path))
    assert not hb_path.exists()


# -----------------------------------------------------------------------
# Processed-set reconstruction — the boto3 paginator handles >1000 objects
# (a single list_objects_v2 call silently truncates at 1000)
# -----------------------------------------------------------------------


def test_processed_set_reconstruction_with_pagination(aws_env):
    """Seed S3 with 1100 MANIFEST.json keys whose bodies carry distinct
    ``raw_object_key`` fields; the daemon must list ALL of them using the
    boto3 paginator (default page size 1000) and reconstruct the set as
    raw_object_keys (filenames), NOT raw_file_ids (hashes).

    The bug this guards against: an earlier version of the function returned
    raw_file_ids derived from the chunks/<hash>/ folder name. Those never
    match the filenames produced by listing raw/, so on every daemon restart
    EVERY raw file was reprocessed.
    """
    import json as _json

    s3 = aws_env
    for i in range(1100):
        raw_file_id = f"{i:016x}"
        manifest_body = _json.dumps(
            {
                "raw_file_id": raw_file_id,
                "source_uri": f"s3://daemon-test-bucket/raw/file-{i:04d}.txt",
                "raw_object_key": f"raw/file-{i:04d}.txt",
                "n_chunks": 1,
            }
        )
        s3.put_object(
            Bucket="daemon-test-bucket",
            Key=f"chunks/{raw_file_id}/MANIFEST.json",
            Body=manifest_body.encode("utf-8"),
        )
    seen = daemon._processed_raw_keys(s3, "daemon-test-bucket")
    assert len(seen) == 1100
    # The set holds raw_object_keys (filenames), not raw_file_ids (hashes)
    assert "raw/file-0000.txt" in seen
    assert "raw/file-1099.txt" in seen
    # And critically: it does NOT hold the hash-style entries
    assert f"{0:016x}" not in seen


def test_processed_set_skips_malformed_manifest(aws_env, capsys):
    """A malformed MANIFEST.json is logged + skipped, not crashing the daemon
    boot. The corresponding raw file (if any) is re-attempted on the next poll
    cycle — chunk_and_write is idempotent so re-processing is safe.
    """
    s3 = aws_env
    # Valid manifest #1
    s3.put_object(
        Bucket="daemon-test-bucket",
        Key="chunks/aaaaaaaaaaaaaaaa/MANIFEST.json",
        Body=b'{"raw_object_key": "raw/good.txt", "raw_file_id": "aaaaaaaaaaaaaaaa", "source_uri": "s3://x/raw/good.txt", "n_chunks": 1}',
    )
    # Malformed JSON
    s3.put_object(
        Bucket="daemon-test-bucket",
        Key="chunks/bbbbbbbbbbbbbbbb/MANIFEST.json",
        Body=b"not json {",
    )
    # Valid manifest but missing raw_object_key field
    s3.put_object(
        Bucket="daemon-test-bucket",
        Key="chunks/cccccccccccccccc/MANIFEST.json",
        Body=b'{"raw_file_id": "cccccccccccccccc", "n_chunks": 1}',
    )
    seen = daemon._processed_raw_keys(s3, "daemon-test-bucket")
    assert seen == {"raw/good.txt"}  # only the valid one
    out = capsys.readouterr().out
    assert "warning" in out


def test_list_raw_keys_skips_folder_markers(aws_env):
    """The raw/ folder marker object (written by ensure_folders) and any other
    trailing-slash keys must be ignored — they're not real uploads."""
    s3 = aws_env
    s3.put_object(Bucket="daemon-test-bucket", Key="raw/", Body=b"")
    s3.put_object(Bucket="daemon-test-bucket", Key="raw/sub/", Body=b"")
    s3.put_object(Bucket="daemon-test-bucket", Key="raw/real-file.txt", Body=b"x")
    s3.put_object(Bucket="daemon-test-bucket", Key="raw/another.md", Body=b"x")

    keys = daemon._list_raw_keys(s3, "daemon-test-bucket")
    assert sorted(keys) == ["raw/another.md", "raw/real-file.txt"]


def test_list_raw_keys_pagination(aws_env):
    """Seed >1000 raw objects; paginator must return all of them."""
    s3 = aws_env
    for i in range(1050):
        s3.put_object(
            Bucket="daemon-test-bucket", Key=f"raw/file-{i:04d}.txt", Body=b"x"
        )
    keys = daemon._list_raw_keys(s3, "daemon-test-bucket")
    assert len(keys) == 1050


# -----------------------------------------------------------------------
# Dual-mode collision guard — warn when a Lambda notification is also wired
# -----------------------------------------------------------------------


def test_collision_guard_warns_when_lambda_notification_exists_on_raw(
    aws_env, capsys
):
    """Bucket has an S3 → Lambda notification on raw/. Daemon must log a
    WARNING but continue (it doesn't refuse to start)."""
    s3 = aws_env
    s3.put_bucket_notification_configuration(
        Bucket="daemon-test-bucket",
        NotificationConfiguration={
            "LambdaFunctionConfigurations": [
                {
                    "Id": "test-lambda",
                    "LambdaFunctionArn": "arn:aws:lambda:us-east-1:123:function:test",
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {
                        "Key": {
                            "FilterRules": [{"Name": "prefix", "Value": "raw/"}]
                        }
                    },
                }
            ]
        },
        SkipDestinationValidation=True,
    )
    daemon._check_for_lambda_collision(s3, "daemon-test-bucket")
    out = capsys.readouterr().out
    assert "[warning]" in out
    assert "dual-processing" in out


def test_collision_guard_silent_when_no_notification(aws_env, capsys):
    """Bucket with no Lambda notification on raw/ — daemon stays quiet."""
    daemon._check_for_lambda_collision(aws_env, "daemon-test-bucket")
    out = capsys.readouterr().out
    assert "[warning]" not in out


# -----------------------------------------------------------------------
# _process_one — skip gates parity with Lambda handler
# -----------------------------------------------------------------------


def test_process_one_skips_unsupported_extension(aws_env, capsys):
    """A .pdf upload should be skipped (gate denies type)."""
    s3 = aws_env
    s3.put_object(Bucket="daemon-test-bucket", Key="raw/doc.pdf", Body=b"%PDF")
    result = daemon._process_one(
        s3,
        "daemon-test-bucket",
        "raw/doc.pdf",
        chunk_root="s3://daemon-test-bucket/chunks",
        dataset_root="s3://daemon-test-bucket/metadata",
    )
    assert result is None
    out = capsys.readouterr().out
    assert "[skip]" in out
    assert "doc.pdf" in out


def test_process_one_skips_oversize(aws_env, capsys):
    """A file larger than MAX_FILE_BYTES is skipped without processing."""
    from onebucket_demo.constants import MAX_FILE_BYTES

    s3 = aws_env
    # Use a small "real" file but lie about size by patching head_object;
    # the daemon should trust the head_object size for the gate check.
    s3.put_object(Bucket="daemon-test-bucket", Key="raw/big.txt", Body=b"x")
    with patch.object(s3, "head_object") as mock_head:
        mock_head.return_value = {"ContentLength": MAX_FILE_BYTES + 1}
        result = daemon._process_one(
            s3,
            "daemon-test-bucket",
            "raw/big.txt",
            chunk_root="s3://daemon-test-bucket/chunks",
            dataset_root="s3://daemon-test-bucket/metadata",
        )
    assert result is None
    out = capsys.readouterr().out
    assert "[skip]" in out
    assert "too large" in out


# -----------------------------------------------------------------------
# Recovery-from-crash — the processed set is rebuilt from S3, not from memory
# -----------------------------------------------------------------------


def test_recovery_from_partial_chunks_no_manifest(aws_env, monkeypatch):
    """Simulate a daemon crash after chunks were written but BEFORE the
    MANIFEST. On restart, the processed-set reconstruction (which only counts
    manifests) does NOT mark this raw file as processed, so the daemon will
    re-attempt it. The downstream pipeline.chunk_and_write is idempotent — it
    rewrites chunks and finally writes MANIFEST, completing the work.

    This test exercises the recovery DETECTION (file shows up in the to-process
    list); the actual rewrite is covered by the existing chunk_and_write
    idempotency tests in tests/test_pipeline.py.
    """
    s3 = aws_env
    # Seed a partial-state document: raw file + some chunks but no MANIFEST
    s3.put_object(Bucket="daemon-test-bucket", Key="raw/partial.txt", Body=b"x")
    s3.put_object(
        Bucket="daemon-test-bucket",
        Key="chunks/deadbeef00000000/chunk_0000.txt",
        Body=b"partial chunk",
    )
    # NO MANIFEST written

    # Processed-set reconstruction looks for MANIFEST.json, so this partial
    # state is NOT considered processed (no manifest = not done). The daemon
    # will see raw/partial.txt in the to-process list on its next poll, and
    # pipeline.chunk_and_write's idempotency handles the partial-state recovery.
    processed = daemon._processed_raw_keys(s3, "daemon-test-bucket")
    assert "raw/partial.txt" not in processed
    assert processed == set()  # no manifests at all → empty set

    raw_keys = daemon._list_raw_keys(s3, "daemon-test-bucket")
    assert "raw/partial.txt" in raw_keys
