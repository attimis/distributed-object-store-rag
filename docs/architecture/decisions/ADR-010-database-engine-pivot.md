# ADR-010: Keep LanceDB as the Index Engine

**Status:** Accepted
**Date:** 2026-05-05

## Context

Once hybrid search was built it became clear that LanceDB has no native indexed query over the struct-typed sparse column. Sparse vectors can be stored without storing text (the pointers-only contract of ADR-004 holds), but querying them is a Python-side dot product over a full-table scan, O(N) per query. At demo scale (thousands of chunks) that is tens of milliseconds and perfectly usable; at hundreds of thousands of chunks it would not be. Upstream issues tracking native sparse-vector search had been dormant for over a year.

A pivot to a different engine with native sparse search was proposed. The candidates were embedded engines backed by a log-structured (LSM-tree) storage layer, which rewrite their files in place during compaction.

## Decision

Keep LanceDB. The pivot is rejected.

The demo's load-bearing property is that the index is a set of immutable fragment files plus a small manifest, which a file-level replication policy can copy between buckets without any coordinator, snapshot or export step (ADR-001). An engine that compacts files in place cannot be replicated that way: a copy taken mid-compaction is inconsistent. Trading that property for faster sparse search at a scale the demo never reaches would be solving the wrong problem.

The Python-side sparse search stays as built (ADR-005), with its per-version row cache.

Any future engine change must satisfy all of:

1. A storage format that is file-level replicable by an external policy without a coordinator; append-only preferred, in-place compaction disqualifying.
2. Native indexed sparse-vector query at scale.
3. Embedded operation, with no separate server lifecycle for the MCP server to manage.
4. A bounded embedder image: roughly 2 GB including the baked-in models.
5. Production-grade maturity.

No engine meeting all five had been identified. If none exists when scale demands it, the alternative to evaluate first is a sparse-only side index stored next to the Lance dataset and replicated by the same policy.

## Consequences

What this buys:

- The replication story the whole demo exists to show stays intact.
- No migration, no new dependency, no second storage format.

What it costs:

- Sparse search remains linear in corpus size. This is a hard limit around the hundred-thousand-chunk mark and is documented as such.

What it rules out:

- Server-based or LSM-backed vector databases, for this project.

## Related

- [ADR-001: Single-Writer LanceDB Index on Object Storage](ADR-001-lance-metadata-sync.md)
- [ADR-003: Index Initialization Is a Separate, One-Time Step](ADR-003-async-index-initialization.md)
- [ADR-004: Pointers-Only Index Schema and Storage Layout](ADR-004-metadata-schema-locality.md)
- [ADR-005: Hybrid Search with Explicit Sparse Vectors](ADR-005-hybrid-search-implementation.md)
- [src/onebucket_demo/search.py](../../../src/onebucket_demo/search.py)
