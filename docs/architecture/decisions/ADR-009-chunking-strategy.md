# ADR-009: Chunking Strategy

**Status:** Accepted
**Date:** 2026-05-05

## Context

Every document the pipeline ingests is split into chunks before embedding, and every downstream property depends on how that split is done: retrieval quality, the number of embedding calls, and whether a chunk fits the embedding models at all. Both models in use (dense `bge-small`, sparse SPLADE; ADR-005) have a 512-token context window, so a chunk that exceeds it is silently truncated at embedding time.

"Semantic chunks" is an ambiguous phrase. It can mean the colloquial RAG sense, chunks that respect paragraph and sentence boundaries instead of cutting mid-sentence, or the literal sense, boundaries chosen by embedding similarity between adjacent sentences. These are very different systems with very different costs, and the ambiguity had to be settled before the chunker was written.

The off-the-shelf options (the sentence splitters that ship with RAG frameworks, LangChain's recursive character splitter) all do roughly the right thing but pull in tens of megabytes of transitive dependencies for about eighty lines of logic. The chunker ships inside a Lambda container image whose size is paid on every cold start, so dependency weight is a design input, not an afterthought.

## Decision

The pipeline uses a hand-rolled **recursive cascade splitter with token-budgeted packing and suffix overlap**, implemented in `chunking.py`. No chunking library is used.

### Cascade

The splitter tries boundaries at progressively finer granularity. A finer level is used only for a piece that is still over budget; pieces already within budget pass through untouched.

| Level | Boundary | Why |
|---|---|---|
| 1 | Paragraph breaks (`\n\n`) | The largest semantic unit the source offers. |
| 2 | Line breaks (`\n`) | Lists, headings and code-like blocks that are not blank-line separated. |
| 3 | Sentence boundaries, regex `[.!?][ \t\n]+` | Keeps sentences whole when a paragraph is still too long. |
| 4 | Word boundaries (whitespace) | Last resort, reached only for pathological input such as a single 600-token sentence. The splitter logs a warning with a hash of the piece when it gets here. |

### Packing

In-budget pieces are concatenated into chunks. A chunk closes when appending the next piece would exceed the target, and the next chunk starts with the tail of the one just closed.

| Constant | Value | Notes |
|---|---|---|
| `CHUNK_TOKEN_TARGET` | 512 | Matches the context window of both embedding models. |
| `CHUNK_TOKEN_OVERLAP` | 50 | Tokens of suffix carried into the next chunk for boundary continuity. |

Both constants live in `constants.py` and are imported, never duplicated. Token counting uses the `bge-small` tokenizer, loaded once and reused, as the single budgeting authority; the SPLADE tokenizer is not consulted during packing because the two windows are equal.

### Identity

`raw_file_id` is the first 16 hex characters of the SHA-256 of the full document text. The same text always yields the same id and, because the splitter is deterministic, the same chunks. This is what makes retries and re-uploads idempotent (ADR-012).

### What "semantic" means here

The colloquial reading is the one in scope: chunks preserve paragraph and sentence units. Embedding-similarity chunking is out of scope and would need a new decision.

### Deferred

- Markdown-heading-aware splitting (split on `#` headings before the paragraph cascade for `.md` input). Paragraph splits are adequate for the demo corpus; the cascade is the natural place to add a level later.
- Adaptive overlap, and budgeting against more than one tokenizer.

## Consequences

What this buys:

- Zero chunking dependencies, so the chunker image stays small (ADR-002).
- Deterministic, testable boundaries: the same input always produces the same chunks, which makes regression tests exact and content addressing possible.
- One source of truth for the token budget, shared by chunking, embedding and tests.
- A small, auditable module that anyone can read end to end.

What it costs:

- The project owns the chunker, including its edge cases. Every edge-case fix must come with a regression test that names the case; no silent fixes.
- The sentence regex is a heuristic: it mishandles abbreviations ("Dr. Smith"), decimals and ellipses, and it does not know non-Western punctuation. Acceptable for an English demo corpus.
- Long structured Markdown chunks on paragraphs, not headings.

What it rules out:

- Similarity-driven chunking and framework splitters, in this repo.

## Alternatives considered

- **Fixed-token sliding window.** Simplest possible, but it cuts mid-sentence and mid-word and ignores document structure. Rejected because retrieval quality is the point.
- **Embedding-similarity chunking.** Boundaries follow topic shifts, but it needs five to ten times more embedding calls at ingest, is non-deterministic across threshold tuning, and is hard to test. Rejected as disproportionate for the demo.
- **Framework splitters.** Functionally equivalent to what was hand-rolled, at the cost of tens of megabytes of dependencies in the Lambda image and coupling to a framework's release cadence for trivial logic. Rejected on weight.
- **Markdown-heading-aware splitting.** Deferred, not rejected (see above).

## Related

- [ADR-002: Ingestion Compute: Lambda and Long-Running Processes](ADR-002-lambda-compute-strategy.md)
- [ADR-005: Hybrid Search with Explicit Sparse Vectors](ADR-005-hybrid-search-implementation.md)
- [ADR-012: Chained Lambdas with the Manifest as Commit Point](ADR-012-chained-lambda-architecture.md)
- [src/onebucket_demo/chunking.py](../../../src/onebucket_demo/chunking.py)
- [src/onebucket_demo/constants.py](../../../src/onebucket_demo/constants.py)
