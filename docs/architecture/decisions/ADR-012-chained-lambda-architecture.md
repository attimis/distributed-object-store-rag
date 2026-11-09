# ADR-012: Chained Lambdas with the Manifest as Commit Point

**Status:** Accepted
**Date:** 2026-05-05

## Context

The first version of the cloud pipeline was a single Lambda: one invocation read the upload, chunked it, embedded every chunk and committed to the index. It works for one file at a time and breaks down as soon as several files arrive together:

1. **Concurrency.** Several uploads mean several invocations, each committing to the same dataset with no per-file ownership boundary. The workaround, reserved concurrency of one, serialises everything.
2. **Cold-start tax.** Loading the sparse model dominates invocation latency, and a monolith pays it even when chunking is the only work that needs doing.
3. **Retry granularity.** Chunking is deterministic and cheap; embedding is expensive. A monolith retries both when either fails.
4. **Stage boundaries.** A pipeline whose stages are separate, event-bounded units can have each stage triggered, observed or replaced independently. The long-running modes (ADR-013) depend on exactly this split.

Three designs were compared: per-chunk fan-out (one embedder invocation per chunk), a queue between the stages, and per-file batching triggered by a manifest object.

## Decision

The `lambda` execution mode is **two chained Lambda functions connected by S3 object-created notifications**, with per-file batching:

```
raw/<file>
   |  S3 notification, prefix raw/
   v
Chunker Lambda  (pipeline.chunk_and_write)
   - run the gates (ADR-006)
   - chunk, compute raw_file_id (ADR-009)
   - write chunks/<raw_file_id>/chunk_NNNN.txt for every chunk
   - write chunks/<raw_file_id>/MANIFEST.json LAST
   |  S3 notification, prefix chunks/ AND suffix MANIFEST.json
   v
Embedder Lambda  (pipeline.embed_and_commit)
   - check the canary against the sidecar (ADR-005)
   - read the manifest and every chunk it names
   - embed dense + sparse in bounded batches
   - one merge_insert for the whole document
   v
metadata/global_index.lance
```

### The manifest is the commit point

The chunker writes `MANIFEST.json` only after every chunk object has been written. Because an S3 put is atomic, the embedder either sees a complete manifest or nothing. If the chunker dies halfway, no manifest exists, the embedder's filter never matches and nothing downstream fires. On retry the chunker, being deterministic, rewrites identical chunk objects and converges on a manifest write.

The manifest carries `raw_file_id`, `source_uri`, `n_chunks`, `raw_object_key`, `created_at` and a mandatory `schema_version`; an embedder that meets a version it does not know refuses the manifest instead of guessing. The embedder also checks that the manifest's `raw_file_id` matches the directory it was found in, a cheap defence against a manifest copied to the wrong place.

### Trigger filters

The embedder's notification needs both filters: `prefix=chunks/` scopes it to the chunk namespace and `suffix=MANIFEST.json` makes it fire once per document, never per chunk. Either filter alone is wrong (a silent stall, or one invocation per chunk), so the notification configuration shipped with the deployment carries both.

### Who does what

| Responsibility | Chunker | Embedder |
|---|---|---|
| Gates | Yes, before any chunk is written | No; if chunks exist, gates passed |
| Content addressing | Computes `raw_file_id` | Reads it from the manifest |
| Model load and canary check | No; the image has no model | Yes, on every cold start |
| Manifest | Writes it last | Reads it |
| Embedding and index commit | No | Yes, one `merge_insert` per document |

### Error semantics

Gate failures in the chunker return success with `skipped: true`, since a retry could never pass. Everything else in either function re-raises so the Lambda runtime retries and, if the failure persists, parks the event on that function's dead-letter queue.

### Concurrency

Several documents in flight mean several embedder invocations committing concurrently. Each owns exactly one document's rows, and LanceDB's optimistic commit retry serialises the commits. That is how ADR-001's single-writer intent is kept in this mode: one logical writer per document, one pipeline per bucket.

## Consequences

What this buys:

- The sparse model loads once per document, not once per chunk, and embedding runs as batched inference rather than one call per chunk.
- `2 x documents` invocations instead of `1 + total chunks`.
- Cheap, safe retries: idempotent chunk writes and a manifest that cannot be observed half-written.
- Every stage of the pipeline is visible in the bucket itself: `raw/`, then `chunks/<id>/`, then the manifest, then the index commit.
- The same two functions serve the long-running modes (ADR-013), because the stage boundary is already explicit.

What it costs:

- Two functions to deploy, log and watch instead of one.
- The filter pair is a correctness dependency that lives in configuration.
- Chunks from a chunker that died before writing its manifest are inert but take up space. Stale-chunk garbage collection is out of scope for the demo; `rebuild_index` wipes and re-derives everything if a bucket gets into a confusing state.
- The manifest's name and location are load-bearing; renaming either breaks the chain silently, so tests guard them.

What it rules out:

- Per-chunk fan-out (roughly fifty times the compute per document, and heavy commit contention) and a queue between the stages (infrastructure the demo does not need at its volume).

## Related

- [ADR-001: Single-Writer LanceDB Index on Object Storage](ADR-001-lance-metadata-sync.md)
- [ADR-003: Index Initialization Is a Separate, One-Time Step](ADR-003-async-index-initialization.md)
- [ADR-005: Hybrid Search with Explicit Sparse Vectors](ADR-005-hybrid-search-implementation.md)
- [ADR-006: Input Gates: File Type and File Size](ADR-006-demo-constraint-enforcement.md)
- [ADR-013: Execution Modes](ADR-013-execution-modes.md)
- [src/onebucket_demo/pipeline.py](../../../src/onebucket_demo/pipeline.py)
- [src/onebucket_demo/manifest.py](../../../src/onebucket_demo/manifest.py)
- [src/onebucket_demo/lambda_handler_chunker.py](../../../src/onebucket_demo/lambda_handler_chunker.py)
- [src/onebucket_demo/lambda_handler_embedder.py](../../../src/onebucket_demo/lambda_handler_embedder.py)
- [docs/operations/aws-deployment-runbook.md](../../operations/aws-deployment-runbook.md)
