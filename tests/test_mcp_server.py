"""Tests for the MCP server wrapper: tool schema, startup canary, the cache
warmer switch and chunk-reference resolution.

The stdio transport itself is exercised by ``tests/test_mcp_transport.py``.
"""

import json
import threading

import pytest

from onebucket_demo import mcp_server
from onebucket_demo.errors import CanaryMismatchError
from onebucket_demo.init_index import main as init_index
from onebucket_demo.mcp_server import (
    DEFAULT_DATASET_ROOT,
    SERVER_NAME,
    TOOL_NAME,
    _TOOL_DEFINITION,
    _resolve_chunk_uri,
    build_server,
)
from onebucket_demo.storage import sidecar_path


def test_search_tool_schema():
    assert _TOOL_DEFINITION.name == TOOL_NAME == "search_global_index"
    schema = _TOOL_DEFINITION.inputSchema
    assert schema["type"] == "object"
    assert schema["properties"]["query"]["type"] == "string"
    assert schema["properties"]["limit"]["type"] == "integer"
    assert schema["properties"]["limit"]["default"] == 3
    assert schema["properties"]["limit"]["minimum"] == 1
    assert schema["properties"]["limit"]["maximum"] == 30
    assert schema["required"] == ["query"]
    # scope is how an agent narrows a follow-up search to known documents.
    assert schema["properties"]["scope"]["type"] == "array"


def test_server_registers_all_thirteen_tools(tmp_path):
    dataset_root = tmp_path / "metadata"
    init_index(str(dataset_root))
    build_server(dataset_root)
    names = {
        mcp_server.TOOL_NAME,
        mcp_server.FETCH_TOOL_NAME,
        mcp_server.INGEST_TOOL_NAME,
        mcp_server.STATUS_TOOL_NAME,
        mcp_server.METRICS_TOOL_NAME,
        mcp_server.READ_METRICS_TOOL_NAME,
        mcp_server.LIST_TOOL_NAME,
        mcp_server.CONFIG_GET_TOOL_NAME,
        mcp_server.CONFIG_SET_TOOL_NAME,
        mcp_server.CONFIG_VALIDATE_TOOL_NAME,
        mcp_server.DELETE_DOC_TOOL_NAME,
        mcp_server.PURGE_CACHE_TOOL_NAME,
        mcp_server.REBUILD_INDEX_TOOL_NAME,
    }
    assert len(names) == 13


def test_build_server_passes_canary(tmp_path):
    dataset_root = tmp_path / "metadata"
    init_index(str(dataset_root))
    server = build_server(dataset_root)
    assert server.name == SERVER_NAME


def test_build_server_refuses_on_canary_mismatch(tmp_path):
    """The server refuses to start when the index was written by different
    model versions than the ones it would query with."""
    dataset_root = tmp_path / "metadata"
    init_index(str(dataset_root))
    sp = sidecar_path(dataset_root)
    meta = json.loads(sp.read_text())
    meta["sparse_canary_hash"] = "0" * 64
    sp.write_text(json.dumps(meta))

    with pytest.raises(CanaryMismatchError):
        build_server(dataset_root)


def test_cache_warmer_is_off_under_test(tmp_path):
    """conftest sets ONEBUCKET_DISABLE_CACHE_WARMER; no thread may start."""
    dataset_root = tmp_path / "metadata"
    init_index(str(dataset_root))
    before = {t.name for t in threading.enumerate()}
    build_server(dataset_root)
    assert not mcp_server._warmer_started.is_set()
    assert "lancedb-cache-warmer" not in {t.name for t in threading.enumerate()} - before


def test_cache_warmer_starts_once_when_enabled(tmp_path, monkeypatch):
    monkeypatch.delenv("ONEBUCKET_DISABLE_CACHE_WARMER", raising=False)
    monkeypatch.setattr(mcp_server, "WARMER_INTERVAL_S", 3600.0)
    dataset_root = tmp_path / "metadata"
    init_index(str(dataset_root))
    build_server(dataset_root)
    build_server(dataset_root)
    assert mcp_server._warmer_started.is_set()
    warmers = [t for t in threading.enumerate() if t.name == "lancedb-cache-warmer"]
    assert len(warmers) == 1 and warmers[0].daemon


def test_default_dataset_root_constant():
    assert DEFAULT_DATASET_ROOT == "data/metadata"


@pytest.mark.parametrize(
    "uri, root, expected",
    [
        ("s3://b/chunks/abc/chunk_0000.txt", "s3://b/chunks", "s3://b/chunks/abc/chunk_0000.txt"),
        ("file:///tmp/x/chunks/abc/chunk_0000.txt", "/tmp/x/chunks", "file:///tmp/x/chunks/abc/chunk_0000.txt"),
        ("chunks/abc/chunk_0000.txt", "s3://b/chunks", "s3://b/chunks/abc/chunk_0000.txt"),
        ("chunks/abc/chunk_0000.txt", "s3://b/chunks/", "s3://b/chunks/abc/chunk_0000.txt"),
        ("abc/chunk_0000.txt", "s3://b/chunks", "s3://b/chunks/abc/chunk_0000.txt"),
        ("chunks/abc/chunk_0000.txt", "s3://b/data", "s3://b/data/chunks/abc/chunk_0000.txt"),
    ],
)
def test_resolve_chunk_uri_joins_relative_references(uri, root, expected):
    assert _resolve_chunk_uri(uri, root) == expected


@pytest.mark.parametrize(
    "bad",
    [
        "../etc/passwd",
        "chunks/../../secret",
        "chunks/abc/..\\..\\x",
        "/etc/passwd",
        "http://example.com/x",
        "gs://bucket/key",
        "chunks/abc/chunk\x00.txt",
        "",
    ],
)
def test_resolve_chunk_uri_rejects_escapes(bad):
    with pytest.raises(ValueError):
        _resolve_chunk_uri(bad, "s3://b/chunks")
