# ADR-005: Hybrid Search with Explicit Sparse Vectors

**Status:** Accepted
**Date:** 2026-05-05

## Context

Dense embeddings are good at meaning and poor at exact terms: a query for a ticker symbol, a part number or a rare proper noun often misses the chunk that contains it verbatim. Hybrid retrieval pairs a dense search with a keyword-style search and lets something combine the two.

LanceDB offers two ways to get the keyword half:

1. **A full-text (BM25) index** built into LanceDB. It is a real inverted index, but it has to be built and rebuilt as a batch step. ADR-003 forbids index building in the writers, so a full-text index would always lag behind the latest uploads until a batch job ran.
2. **Explicit sparse vectors**: a learned sparse model (SPLADE) turns each chunk into a vocabulary-weighted vector that is stored as data in a column and appended just like the dense vector. Nothing to rebuild.

LanceDB has no native query over a struct-typed sparse column, so option 2 means doing the sparse scoring ourselves.

## Decision

Store explicit sparse vectors and do the sparse search in Python.

- **Models.** Dense: `BAAI/bge-small-en-v1.5` (384 dimensions). Sparse: `prithivida/Splade_PP_en_v1`. Both run through fastembed, and both have a 512-token context, which is why chunks are budgeted to 512 tokens (ADR-009).
- **Storage.** `sparse_vector` is a struct of `indices` (int32 list) and `values` (float32 list), written in the same `merge_insert` as the dense `vector` (ADR-004).
- **Dense search.** A LanceDB vector search over `vector`, with an optional `where raw_file_id IN (...)` scope filter applied at scan time. Score is `1 / (1 + L2 distance)`; the vectors are unit length, so this ranks the same as cosine similarity.
- **Sparse search.** A full scan of the pointer columns and `sparse_vector`, cached in memory per `(dataset_root, table version)`, then a dot product between the query's sparse vector and every row's, with the scope applied as a set filter. At demo scale the in-memory product takes tens of milliseconds; the cache is dropped as soon as the table version changes.
- **Two lists, no fusion.** `search_global_index` returns `semantic_matches` and `keyword_matches` as separate ranked lists, each entry carrying `chunk_uri`, `source_uri`, `raw_file_id`, `chunk_index` and `score`. The scores are on different scales on purpose. The consuming model reads both lists and decides what to fetch; there is no reciprocal-rank fusion, no alpha weight and no re-ranker to tune.
- **Canary.** `embed_sparse.compute_canary_hash` digests the model names, the fastembed version and the sparse tokenizer. `init_index` records it in the sidecar; the MCP server and the embedder compare it at startup and refuse to run on a mismatch, because an index written with one model version and queried with another returns silently wrong results.

## Consequences

What this buys:

- Keyword search is as fresh as the last commit. There is no secondary index to rebuild or keep in sync over object storage.
- The agent sees why a chunk matched (meaning or terms), which is more useful to it than a single fused score.
- The writer/reader contract is enforced, not assumed.

What it costs:

- Sparse search is O(N) per query in Python. The per-version row cache makes it fast at thousands of chunks; it would not survive hundreds of thousands (ADR-010).
- Rows are slightly larger because of the sparse column.
- The canary catches model-name, library and tokenizer changes but not a silent weights swap under the same name, which is why the Lambda images pin the model files.

What it rules out:

- Server-side hybrid ranking and any tuning of fusion weights inside this repo.

## Related

- [ADR-003: Index Initialization Is a Separate, One-Time Step](ADR-003-async-index-initialization.md)
- [ADR-004: Pointers-Only Index Schema and Storage Layout](ADR-004-metadata-schema-locality.md)
- [ADR-009: Chunking Strategy](ADR-009-chunking-strategy.md)
- [ADR-010: Keep LanceDB as the Index Engine](ADR-010-database-engine-pivot.md)
- [src/onebucket_demo/search.py](../../../src/onebucket_demo/search.py)
- [src/onebucket_demo/embed_dense.py](../../../src/onebucket_demo/embed_dense.py)
- [src/onebucket_demo/embed_sparse.py](../../../src/onebucket_demo/embed_sparse.py)
- [docs/operations/mcp-tools-guide.md](../../operations/mcp-tools-guide.md)
