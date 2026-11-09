"""Subprocess + stdio MCP transport-conformance test.

This test is the **only place** that exercises the actual MCP stdio boundary.
The in-process e2e test (test_e2e.py) calls the contract layer directly and
cannot detect protocol-framing bugs, schema-registration bugs, or async
transport bugs in the MCP wrapper. This subprocess test does.

What it verifies:
  - Spawning ``scripts/run_mcp.py`` as a subprocess with a seeded dataset
    succeeds (canary check passes).
  - MCP handshake completes over stdio.
  - ``list_tools`` returns the ``search_global_index`` tool with the exact
    documented input schema.
  - ``call_tool("search_global_index", {...})`` returns a JSON-serialized
    response that matches the documented response shape — two arrays, each
    entry has only ``chunk_uri / source_uri / score``, and ``chunk_text`` is
    **absent as a key** at every level (search returns pointers only; chunk
    text is fetched in a separate step).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from onebucket_demo.init_index import main as init_index
from onebucket_demo.mcp_server import TOOL_NAME
from onebucket_demo.writer import ingest

REPO_ROOT = Path(__file__).parent.parent


@pytest.fixture
def seeded_dataset(tmp_path):
    dataset_root = tmp_path / "metadata"
    chunk_root = tmp_path / "chunks"
    init_index(str(dataset_root))
    samples_dir = REPO_ROOT / "data" / "samples"
    for f in sorted(samples_dir.iterdir()):
        if f.suffix in {".txt", ".md"}:
            ingest(f, dataset_root=dataset_root, chunk_root=chunk_root)
    return dataset_root


def _server_params(dataset_root: Path) -> StdioServerParameters:
    env = dict(os.environ)
    env["ONEBUCKET_DATASET_ROOT"] = str(dataset_root)
    return StdioServerParameters(
        command=sys.executable,
        args=[str(REPO_ROOT / "scripts" / "run_mcp.py")],
        env=env,
        cwd=str(REPO_ROOT),
    )


async def test_mcp_subprocess_lists_search_tool(seeded_dataset):
    async with stdio_client(_server_params(seeded_dataset)) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools_result = await session.list_tools()
            tool_names = [t.name for t in tools_result.tools]
            assert TOOL_NAME in tool_names

            tool = next(t for t in tools_result.tools if t.name == TOOL_NAME)
            schema = tool.inputSchema
            assert schema["type"] == "object"
            assert "query" in schema["properties"]
            assert "limit" in schema["properties"]
            assert schema["properties"]["query"]["type"] == "string"
            assert schema["properties"]["limit"]["type"] == "integer"
            assert schema["required"] == ["query"]


async def test_mcp_subprocess_call_tool_returns_documented_shape(seeded_dataset):
    """Pull the actual stdio response shape and verify it against the
    documented response shape."""
    async with stdio_client(_server_params(seeded_dataset)) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            call_result = await session.call_tool(
                TOOL_NAME, arguments={"query": "quarterly revenue", "limit": 3}
            )
            # The handler returns a single TextContent with JSON payload
            assert call_result.content
            text_blocks = [c for c in call_result.content if hasattr(c, "text")]
            assert text_blocks, "no text content in tool response"
            payload = json.loads(text_blocks[0].text)

            # Documented response shape — two arrays
            assert "semantic_matches" in payload
            assert "keyword_matches" in payload
            assert isinstance(payload["semantic_matches"], list)
            assert isinstance(payload["keyword_matches"], list)


async def test_mcp_subprocess_response_has_no_chunk_text_key(seeded_dataset):
    """`chunk_text` is absent as a KEY anywhere in the
    response, asserted at the actual transport boundary (not the in-process
    function) so protocol-layer bugs cannot mask the contract violation.
    """
    async with stdio_client(_server_params(seeded_dataset)) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            call_result = await session.call_tool(
                TOOL_NAME, arguments={"query": "revenue", "limit": 3}
            )
            text_blocks = [c for c in call_result.content if hasattr(c, "text")]
            payload = json.loads(text_blocks[0].text)

            # Top-level
            assert "chunk_text" not in payload
            # Inside each entry of each array
            expected_keys = {
                "chunk_uri",
                "source_uri",
                "score",
                # Added for multi-hop scoping + citation
                "raw_file_id",
                "chunk_index",
            }
            for arr_name in ("semantic_matches", "keyword_matches"):
                for entry in payload.get(arr_name, []):
                    assert set(entry.keys()) == expected_keys
                    assert "chunk_text" not in entry


async def test_mcp_subprocess_entry_keys_match_documented_shape(seeded_dataset):
    async with stdio_client(_server_params(seeded_dataset)) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            call_result = await session.call_tool(
                TOOL_NAME, arguments={"query": "revenue", "limit": 3}
            )
            text_blocks = [c for c in call_result.content if hasattr(c, "text")]
            payload = json.loads(text_blocks[0].text)
            for arr_name in ("semantic_matches", "keyword_matches"):
                for entry in payload[arr_name]:
                    assert isinstance(entry["chunk_uri"], str)
                    assert isinstance(entry["source_uri"], str)
                    assert isinstance(entry["score"], float)
