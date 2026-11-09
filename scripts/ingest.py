#!/usr/bin/env python3
"""CLI entry point for ingestion.

Usage:
    uv run python scripts/ingest.py <file_path> [dataset_root] [chunk_root]

Defaults: dataset_root=data/metadata, chunk_root=data/chunks
"""

from __future__ import annotations

import sys

from onebucket_demo.writer import ingest


def main() -> int:
    args = sys.argv[1:]
    if not args:
        print(
            f"usage: {sys.argv[0]} <file_path> [dataset_root] [chunk_root]",
            file=sys.stderr,
        )
        return 2
    path = args[0]
    dataset_root = args[1] if len(args) >= 2 else "data/metadata"
    chunk_root = args[2] if len(args) >= 3 else "data/chunks"
    result = ingest(path, dataset_root=dataset_root, chunk_root=chunk_root)
    print(result)
    return 0 if result.accepted else 1


if __name__ == "__main__":
    sys.exit(main())
