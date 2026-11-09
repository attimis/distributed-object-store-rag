#!/usr/bin/env python3
"""Run the OneBucket Demo MCP server over stdio.

Usage:
    uv run python scripts/run_mcp.py

Reads ONEBUCKET_DATASET_ROOT from environment (default: data/metadata).
Refuses to serve if the dataset's canary hash does not match the runtime
canary (writer/reader version drift detection: an index embedded with one
model version must not be searched with another).
"""

from __future__ import annotations

import asyncio

from onebucket_demo.mcp_server import run

if __name__ == "__main__":
    asyncio.run(run())
