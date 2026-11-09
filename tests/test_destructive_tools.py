"""Tests for the destructive demo tools: delete_document, purge_peer_cache,
rebuild_index.

These tools change state the read-side tools cannot undo, so each one is
strict about its preconditions. The tests here lock down the refusal paths
(bad ids, missing confirmation, sync peers) against a synthetic runbook
config, so nothing depends on any particular deployment's variables file.
The happy paths run under moto in ``test_mcp_tools.py``.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from onebucket_demo import mcp_tools

SYNTHETIC_CONFIG = """
schema_version: 1
endpoints:
  EAST_S3:
    url: "https://s3.example.com:443"
    provider: "aws"
    use_region_toggle: true
  WEST_CACHE:
    url: "https://s3.example.net:443"
    provider: "other"
    use_region_toggle: false
policies:
  demo-data:
    quorum: 1
    peers:
      - alias: EAST_S3
        type: sync
        required: true
        bucket: "example-east-bucket"
        region: "us-east-1"
      - alias: WEST_CACHE
        type: cache
        required: false
        bucket: "example-west-bucket"
        region: "us-west-1"
  demo-meta:
    quorum: 2
    peers:
      - alias: EAST_S3
        type: sync
        required: true
        bucket: "example-east-meta"
        region: "us-east-1"
      - alias: WEST_CACHE
        type: sync
        required: true
        bucket: "example-west-meta"
        region: "us-west-1"
"""


@pytest.fixture
def synthetic_config(tmp_path: Path, monkeypatch) -> Path:
    p = tmp_path / "variables.yml"
    p.write_text(textwrap.dedent(SYNTHETIC_CONFIG).lstrip())
    monkeypatch.setenv("ONEBUCKET_RUNBOOK_CONFIG", str(p))
    return p


@pytest.fixture
def minimal_env(monkeypatch):
    """Just enough environment for ``_bucket_layout()`` to resolve."""
    monkeypatch.setenv("ONEBUCKET_CHUNK_ROOT", "s3://example-bucket/chunks")
    monkeypatch.setenv("ONEBUCKET_DATASET_ROOT", "s3://example-bucket/metadata")
    monkeypatch.setenv("ONEBUCKET_EXECUTION_MODE", "lambda")


class TestDeleteDocument:
    @pytest.mark.parametrize("bad", ["", None, 12345, "not-a-hash", "ABCDEF0123456789", "0" * 15])
    def test_rejects_anything_but_a_16_hex_id(self, minimal_env, bad) -> None:
        result = mcp_tools.delete_document(bad)  # type: ignore[arg-type]
        assert result["error"] == "INVALID_ARGUMENT"


class TestRebuildIndex:
    @pytest.mark.parametrize("confirm", [None, False, 0])
    def test_refuses_without_explicit_confirmation(self, minimal_env, confirm) -> None:
        kwargs = {} if confirm is None else {"confirm": confirm}
        result = mcp_tools.rebuild_index(**kwargs)  # type: ignore[arg-type]
        assert result["error"] == "CONFIRM_REQUIRED"
        assert "confirm=true" in result["message"]


class TestPurgePeerCache:
    def test_empty_args_invalid_argument(self, synthetic_config) -> None:
        assert mcp_tools.purge_peer_cache("", "x", "*")["error"] == "INVALID_ARGUMENT"
        assert mcp_tools.purge_peer_cache("x", "", "*")["error"] == "INVALID_ARGUMENT"
        assert mcp_tools.purge_peer_cache("x", "y", "")["error"] == "INVALID_ARGUMENT"

    def test_unknown_onebucket(self, synthetic_config) -> None:
        result = mcp_tools.purge_peer_cache("nonexistent-policy", "EAST_S3", "*")
        assert result["error"] == "UNKNOWN_ONEBUCKET"

    def test_unknown_peer_within_known_onebucket(self, synthetic_config) -> None:
        result = mcp_tools.purge_peer_cache("demo-data", "FAKE_PEER", "*")
        assert result["error"] == "UNKNOWN_PEER"

    def test_refuses_on_sync_peer(self, synthetic_config) -> None:
        result = mcp_tools.purge_peer_cache("demo-data", "EAST_S3", "*")
        assert result["error"] == "REFUSED_NOT_CACHE_PEER"
        assert "sync" in result["message"].lower()

    def test_refuses_on_every_sync_peer_of_a_strict_policy(self, synthetic_config) -> None:
        for peer in ("EAST_S3", "WEST_CACHE"):
            result = mcp_tools.purge_peer_cache("demo-meta", peer, "*")
            assert result["error"] == "REFUSED_NOT_CACHE_PEER", peer

    def test_cache_peer_without_direct_access_is_refused(self, synthetic_config) -> None:
        result = mcp_tools.purge_peer_cache("demo-data", "WEST_CACHE", "*")
        assert result["error"] == "DIRECT_ACCESS_NOT_CONFIGURED"
        assert "direct_access" in result["message"]

    def test_endpoint_referenced_but_not_in_endpoints_block(self, tmp_path: Path, monkeypatch) -> None:
        p = tmp_path / "variables.yml"
        p.write_text(
            textwrap.dedent(
                """
                schema_version: 1
                endpoints: {}
                policies:
                  test-policy:
                    quorum: 1
                    peers:
                      - alias: DANGLING_REF
                        type: cache
                        required: false
                        bucket: "test-cache-bucket"
                        region: "us-east-1"
                """
            ).lstrip()
        )
        monkeypatch.setenv("ONEBUCKET_RUNBOOK_CONFIG", str(p))
        result = mcp_tools.purge_peer_cache("test-policy", "DANGLING_REF", "*")
        assert result["error"] == "ENDPOINT_NOT_DEFINED"
