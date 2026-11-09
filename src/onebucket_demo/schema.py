"""Lance dataset schema per ADR-004 §4 (pointers-only)."""

from __future__ import annotations

import pyarrow as pa

from .constants import DENSE_DIM

SCHEMA_VERSION = "1"


def get_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.string()),
            pa.field("raw_file_id", pa.string()),
            pa.field("chunk_index", pa.int32()),
            pa.field("chunk_uri", pa.string()),
            pa.field("source_uri", pa.string()),
            pa.field("vector", pa.list_(pa.float32(), DENSE_DIM)),
            pa.field(
                "sparse_vector",
                pa.struct(
                    [
                        pa.field("indices", pa.list_(pa.int32())),
                        pa.field("values", pa.list_(pa.float32())),
                    ]
                ),
            ),
            pa.field("created_at", pa.timestamp("us", tz="UTC")),
        ]
    )
