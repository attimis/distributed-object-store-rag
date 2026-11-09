# ADR-003: Index Initialization Is a Separate, One-Time Step

**Status:** Accepted
**Date:** 2026-05-05

## Context

An early sketch of the writer created the LanceDB table and built a vector index inline, on every upload:

    table.add(rows)
    table.create_index(vector_column_name="vector", index_type="IVF_PQ")

Building an IVF_PQ index means running k-means over the whole dataset. Doing that for every document makes each upload slower than the last, burns compute in proportion to the corpus and eventually times out the writer. A second, quieter problem is creation itself: if every writer is allowed to create the table when it finds none, two writers starting at the same time race to create the same dataset.

## Decision

Index creation is decoupled from ingestion and done out of band, once, by an operator.

- `init_index.py` creates the **empty** `global_index` table with the schema from `schema.py`, and writes a sidecar file `_init_metadata.json` next to it recording the fastembed version, both model names, the sparse-model canary hash (ADR-005), the schema version and the initialization time.
- Writers (`pipeline.embed_and_commit`, whichever mode calls it) only ever `merge_insert` into a table that already exists. A missing table is reported as a setup error, never papered over.
- `init_index` is idempotent: run against an existing dataset it does nothing, except that a missing sidecar is recreated so the canary check has something to read.
- **No index is built.** Neither an IVF vector index nor a full-text index. At demo scale (thousands of chunks) a flat scan of the dense column is fast, and sparse search is done in Python (ADR-005). Adding an approximate-nearest-neighbour index later would happen at this same out-of-band point, not in the writers.

Running `init_index` is step zero of every deployment, local or cloud, and the operations docs say so prominently.

## Consequences

What this buys:

- Writes are cheap and bounded: one `merge_insert` per document.
- "The index does not exist" is an explicit, early error instead of a race.
- The sidecar gives the writer/reader model-version contract a home (ADR-005).

What it costs:

- One manual step before the first upload. Forgetting it is the most common setup mistake, which is why the writers fail loudly.
- Query cost grows linearly with corpus size because there is no vector index. That is acceptable for a demo and would be the first thing to revisit at scale (ADR-010).

What it rules out:

- Writers that create or re-index the dataset as a side effect.

## Related

- [ADR-001: Single-Writer LanceDB Index on Object Storage](ADR-001-lance-metadata-sync.md)
- [ADR-005: Hybrid Search with Explicit Sparse Vectors](ADR-005-hybrid-search-implementation.md)
- [ADR-010: Keep LanceDB as the Index Engine](ADR-010-database-engine-pivot.md)
- [src/onebucket_demo/init_index.py](../../../src/onebucket_demo/init_index.py)
- [src/onebucket_demo/schema.py](../../../src/onebucket_demo/schema.py)
- [src/onebucket_demo/storage.py](../../../src/onebucket_demo/storage.py)
