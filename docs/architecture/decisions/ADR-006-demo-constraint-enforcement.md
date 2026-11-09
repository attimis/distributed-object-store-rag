# ADR-006: Input Gates: File Type and File Size

**Status:** Accepted
**Date:** 2026-05-05

## Context

The demo ingests whatever a user uploads. Without bounds, one oversized or unsupported file can exhaust the embedder's memory, run the chunker past its time limit, or quietly fill the index with garbage (a PDF treated as text produces exactly that). The failure would land in the middle of a demo, so the limits must be deterministic, checked first and explained clearly.

## Decision

Two gates run before any chunking or embedding work, in every execution mode.

**File type.** Only `.txt` and `.md` are accepted (`ALLOWED_EXTENSIONS` in `constants.py`). Everything else is rejected. PDF is named explicitly in the rejection message because it is the file users reach for first: "PDF support is not included in this demo; convert to text first." Real PDF ingestion needs text extraction, encoding detection and handling of embedded objects, which is a parsing layer this demo does not carry.

**File size.** At most 10 MB (`MAX_FILE_BYTES`). Larger files are rejected with the measured size and the limit in the message.

**Semantics.** `gates.py` returns `(ok, reason)` instead of raising, so every entry point (the chunker Lambda, both local processes and the local ingest) handles a rejection the same way: log the reason, report success to whatever triggered it, and never retry something that cannot pass. In the Lambda mode that means the chunker returns `skipped: true` and the event is consumed rather than parked on the dead-letter queue.

## Consequences

What this buys:

- Failure modes are bounded and predictable, and the message tells the user what to do.
- The 10 MB bound is what lets the pipeline fit inside Lambda limits (ADR-002).
- No ambiguity about PDF "sort of working": it does not, and the message says so.

What it costs:

- Users with PDF-heavy material must convert first. Converting a public filing to text and uploading it is part of the documented demo flow.

What it rules out:

- Any other format (`.docx`, `.html`, PDF) and larger files, in this demo.

## Related

- [ADR-002: Ingestion Compute: Lambda and Long-Running Processes](ADR-002-lambda-compute-strategy.md)
- [ADR-012: Chained Lambdas with the Manifest as Commit Point](ADR-012-chained-lambda-architecture.md)
- [src/onebucket_demo/gates.py](../../../src/onebucket_demo/gates.py)
- [src/onebucket_demo/constants.py](../../../src/onebucket_demo/constants.py)
- [src/onebucket_demo/lambda_handler_chunker.py](../../../src/onebucket_demo/lambda_handler_chunker.py)
