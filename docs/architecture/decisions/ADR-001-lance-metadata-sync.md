# ADR-001: Single-Writer LanceDB Index on Object Storage

**Status:** Accepted
**Date:** 2026-05-05

## Context

The demo keeps its whole search index in LanceDB. LanceDB is an embedded database: there is no server process, and a dataset is a directory of immutable data fragments plus a small versioned manifest that says which fragments make up the current version. That directory can live on a local disk or directly in an S3 bucket, and any process that can read the bucket can open the dataset and query it.

What LanceDB does not provide is coordination between independent writer processes. Each commit is an optimistic append to the version chain: a writer that discovers a newer version than the one it started from retries. That is fine for the occasional overlap, but two separately deployed pipelines appending to the same dataset at the same time produce retries, wasted embedding work and, at worst, conflicting versions. The index therefore needs exactly one logical writer at any time.

At the same time the demo exists to show reads from anywhere: a reader in another region, or on a laptop, should query a nearby copy of the index without any application code knowing where that copy came from.

## Decision

The index is one LanceDB table, `global_index`, stored under the `metadata/` prefix of the bucket (`<dataset_root>/global_index.lance`, where `dataset_root` is `s3://<bucket>/metadata` or a local directory). Three rules keep it single-writer and portable:

1. **One active pipeline per bucket.** Exactly one execution mode (ADR-013) runs against a bucket at a time. Within that mode, each document is committed as one `merge_insert` keyed on the row `id`, so a retry rewrites the same rows instead of duplicating them. Overlapping commits from the Lambda mode's parallel invocations are resolved by LanceDB's own optimistic retry.
2. **Creation is separate from writing.** The table is created once, out of band, by `init_index` (ADR-003). Writers never create it, so there is no race to create the dataset.
3. **Replication belongs to the storage layer.** The application writes to one URI and reads from one URI. `storage.py` chooses the backend by URI scheme (`s3://` or a local path), and the boto3 client honours `AWS_ENDPOINT_URL`, so the identical calls go to AWS S3 or to a OneBucket S3 endpoint (`https://s3.onebucket.io`, or a regional endpoint such as `https://s3.us-ashburn-1.onebucket.io`; the region list is at https://www.onebucket.io/support). When the bucket is a OneBucket, writes to `metadata/` fan out to its peer buckets and a reader in another region is served by the nearest peer. No application-level copying, snapshotting or ETL exists anywhere in the repo.

Readers refresh the table handle to the latest committed version before every query, so a newly replicated commit becomes visible without restarting the MCP server.

## Consequences

What this buys:

- Local, AWS S3 and OneBucket deployments run the same code path; only environment variables differ.
- The dataset is a set of immutable files plus a manifest, which is exactly the kind of data a file-level replication policy can copy safely. This is the property ADR-010 protects when it rejects other database engines.
- There is no index server to run, scale or keep in sync.

What it costs:

- Readers see the index with whatever staleness the replication policy allows. The demo measures this (`get_replication_metrics`) rather than hiding it.
- The single-active-mode rule is a discipline, not a lock. The local daemon warns at startup if a Lambda notification is configured on the same bucket, but nothing stops an operator from running two pipelines.
- Under the Lambda mode, several documents arriving at once mean several embedder invocations committing concurrently; LanceDB's optimistic retry handles this at demo cadence but would thrash at high volume.

What it rules out:

- Multiple independent writer pipelines on one bucket.
- Database engines that need a server process or that rewrite files in place during compaction, since those cannot be replicated as plain files (ADR-010).

## Related

- [ADR-003: Index Initialization Is a Separate, One-Time Step](ADR-003-async-index-initialization.md)
- [ADR-004: Pointers-Only Index Schema and Storage Layout](ADR-004-metadata-schema-locality.md)
- [ADR-010: Keep LanceDB as the Index Engine](ADR-010-database-engine-pivot.md)
- [ADR-012: Chained Lambdas with the Manifest as Commit Point](ADR-012-chained-lambda-architecture.md)
- [ADR-013: Execution Modes](ADR-013-execution-modes.md)
- [src/onebucket_demo/storage.py](../../../src/onebucket_demo/storage.py)
- [src/onebucket_demo/pipeline.py](../../../src/onebucket_demo/pipeline.py)
- [src/onebucket_demo/search.py](../../../src/onebucket_demo/search.py)
- [docs/operations/onebucket-setup.md](../../operations/onebucket-setup.md)
