# ADR-002: Ingestion Compute: Lambda and Long-Running Processes

**Status:** Accepted
**Date:** 2026-05-05

## Context

When a document lands under `raw/` in the bucket, something has to chunk it, embed every chunk twice (dense and sparse) and commit the rows to the index. The question was where that compute runs.

Two options were on the table:

- **AWS Lambda**, triggered by S3 object-created notifications. Nothing to keep running, and it scales to zero. The costs are hard ceilings: a 15-minute invocation limit, a memory ceiling (3008 MB on a default account quota), a container-image size that is paid on every cold start, and embedding models that load on every cold start.
- **A long-running process** that watches the bucket and reacts, the way a third-party ingestion service would. No execution ceilings, and it runs on a laptop with nothing deployed, but someone has to keep it alive and it processes one document at a time.

A heavier design (an SQS queue feeding container workers) was considered and rejected: it adds a VPC, a cluster and a queue to a demo whose point is the storage layer, not the compute layer.

## Decision

Keep the pipeline small enough to fit inside Lambda, and ship both compute options.

**Fit inside Lambda.** The demo accepts only plain-text and Markdown files of at most 10 MB (ADR-006). Embedding runs in bounded batches (16 chunks by default, `ONEBUCKET_EMBED_BATCH_SIZE` to override) so peak memory stays under the Lambda ceiling even for a long document. The models are baked into the embedder image, and the chunker image carries no embedding or database stack at all: `pipeline.py` imports fastembed, LanceDB and pyarrow lazily inside `embed_and_commit`, which keeps the chunker image around 200 MB and the embedder image about ten times larger. A test fails if either Lambda handler grows logic of its own.

**Ship both.** The two pipeline phases, `pipeline.chunk_and_write` and `pipeline.embed_and_commit`, are the only implementation. The `lambda` execution mode wraps them in two chained Lambda functions (ADR-012). The `local-daemon` and `onebucket-events` modes run them inside a long-running process on the operator's machine (ADR-013). Which one is active is a per-bucket choice, not a code change.

## Consequences

What this buys:

- A newcomer can run the whole loop on a laptop against a bucket with nothing deployed, and switch to Lambda later without touching the pipeline.
- The Lambda path has no servers and costs nothing while idle.
- A bug fixed in `pipeline.py` is fixed in every mode.

What it costs:

- Large documents are out: a file over 10 MB is rejected at the gate rather than processed slowly.
- Two paths to test and document. The local path adds its own tests next to the Lambda tests that run against a mocked S3.
- Embedder cold starts are dominated by model load; the chained design (ADR-012) amortises that per document, not per chunk.

What it rules out:

- Arbitrarily large inputs, PDF parsing and any multi-stage fan-out that needs a queue. Those belong in a production pipeline, not in this demo.

## Related

- [ADR-006: Input Gates: File Type and File Size](ADR-006-demo-constraint-enforcement.md)
- [ADR-012: Chained Lambdas with the Manifest as Commit Point](ADR-012-chained-lambda-architecture.md)
- [ADR-013: Execution Modes](ADR-013-execution-modes.md)
- [src/onebucket_demo/pipeline.py](../../../src/onebucket_demo/pipeline.py)
- [src/onebucket_demo/lambda_handler_chunker.py](../../../src/onebucket_demo/lambda_handler_chunker.py)
- [src/onebucket_demo/lambda_handler_embedder.py](../../../src/onebucket_demo/lambda_handler_embedder.py)
- [docs/operations/execution-modes.md](../../operations/execution-modes.md)
