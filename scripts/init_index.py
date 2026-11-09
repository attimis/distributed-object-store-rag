#!/usr/bin/env python3
"""CLI entry point for out-of-band index initialization (ADR-003).

Usage:
    uv run python scripts/init_index.py [dataset_root]

Default dataset_root: data/metadata
"""

from __future__ import annotations

import sys

from onebucket_demo.init_index import DEFAULT_DATASET_ROOT, main

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) > 1:
        print(f"usage: {sys.argv[0]} [dataset_root]", file=sys.stderr)
        sys.exit(2)
    sys.exit(main(args[0] if args else DEFAULT_DATASET_ROOT))
