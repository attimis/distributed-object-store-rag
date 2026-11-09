# ADR-004: Pointers-Only Index Schema and Storage Layout

**Status:** Accepted
**Date:** 2026-05-05

## Context

The index stores one row per chunk. The obvious design puts the chunk text in that row so a search returns the text directly. The demo deliberately does not do that: the text lives in object storage, and the index stores only pointers to it. Three reasons:

- The Lance dataset stays small, which keeps replication of `metadata/` cheap and fast.
- Reading a chunk's text becomes an ordinary object read, served by whichever peer bucket is nearest to the reader. That is the property the demo wants to show off, and `get_chunk_read_metrics` measures it.
- There is exactly one copy of every piece of text, so deleting a document means deleting objects, not rewriting rows.

This record fixes the schema and the object layout so writers and readers in every execution mode agree on them.

## Decision

### Object layout

Everything lives in one bucket under three prefixes:

| Key | Contents |
|---|---|
| `raw/<filename>` | The source document as uploaded. |
| `chunks/<raw_file_id>/chunk_NNNN.txt` | One object per chunk, four-digit zero-padded index. |
| `chunks/<raw_file_id>/MANIFEST.json` | Written last by the chunker; the embedder's trigger and input (ADR-012). |
| `metadata/global_index.lance/` | The LanceDB dataset. |
| `metadata/_init_metadata.json` | The initialization sidecar (ADR-003). |

`raw_file_id` is the first 16 hex characters of the SHA-256 of the document text (ADR-009), so re-uploading the same content lands on the same keys and the same rows.

### Schema

`schema.py` defines the table (schema version 1):

| Column | Type | Meaning |
|---|---|---|
| `id` | string | `<raw_file_id>-<NNNN>`; the `merge_insert` key. |
| `raw_file_id` | string | Document identity; the unit of search scoping and deletion. |
| `chunk_index` | int32 | Position of the chunk in the document. |
| `chunk_uri` | string | Full URI of the chunk object. |
| `source_uri` | string | Full URI of the source document under `raw/`. |
| `vector` | fixed-size list of 384 float32 | Dense embedding (ADR-005). |
| `sparse_vector` | struct of `indices` (list of int32) and `values` (list of float32) | Sparse embedding (ADR-005). |
| `created_at` | timestamp, UTC | When the row was embedded. |

There is no text column. Search returns pointers; the agent calls `fetch_chunk` for the text it wants.

### URI discipline

Writers store the URI exactly as they wrote it: `s3://<bucket>/chunks/...` in the cloud modes, `file:///...` in the local filesystem demo. Readers pass such URIs straight to `storage.read_chunk`, which picks the backend by scheme. The MCP server also accepts a relative `chunks/<id>/chunk_NNNN.txt` reference and joins it onto its configured chunk root, and it refuses anything that could escape that root (parent-directory segments, other URL schemes, absolute paths). Because the bucket name in a stored URI is the OneBucket's name rather than a physical peer, a reader in any region resolves the same URI through its own endpoint.

## Consequences

What this buys:

- A small index, cheap replication, one copy of every chunk.
- Search responses are small and have the same shape in every mode.
- Deleting a document is a prefix delete plus a row delete keyed on `raw_file_id`.

What it costs:

- Every result the agent reads costs one extra object read. The demo turns that cost into a feature by measuring where the read was served from.
- The schema version is recorded in the sidecar; a schema change means rebuilding the index (`rebuild_index`) rather than migrating rows in place.

What it rules out:

- Returning text from search, and any design that would let the index drift from the objects it points at.

## Related

- [ADR-001: Single-Writer LanceDB Index on Object Storage](ADR-001-lance-metadata-sync.md)
- [ADR-003: Index Initialization Is a Separate, One-Time Step](ADR-003-async-index-initialization.md)
- [ADR-005: Hybrid Search with Explicit Sparse Vectors](ADR-005-hybrid-search-implementation.md)
- [ADR-009: Chunking Strategy](ADR-009-chunking-strategy.md)
- [ADR-012: Chained Lambdas with the Manifest as Commit Point](ADR-012-chained-lambda-architecture.md)
- [src/onebucket_demo/schema.py](../../../src/onebucket_demo/schema.py)
- [src/onebucket_demo/storage.py](../../../src/onebucket_demo/storage.py)
- [src/onebucket_demo/mcp_server.py](../../../src/onebucket_demo/mcp_server.py)
