# ADR-013: Execution Modes

**Status:** Accepted
**Date:** 2026-05-12

## Context

The cloud pipeline (ADR-012) couples two things that are separable: the trigger (an S3 notification) and the place the work runs (AWS Lambda). Three needs pulled them apart:

1. **Run on a laptop.** A newcomer should be able to run the whole loop against a bucket with nothing deployed to AWS. Lambda must be a choice, not a prerequisite.
2. **Bring your own credentials.** Using your own bucket and using your own Lambda deployment should be independent decisions.
3. **React to OneBucket's own events.** When the bucket is a OneBucket, the storage layer already emits an event for every request it serves. The pipeline should be able to run off those events, and the chunking and embedding code must not change when the trigger source does.

A tempting shortcut was to chunk and embed synchronously inside the MCP server's `ingest_document` tool. It was rejected: a real ingestion pipeline is a service that watches storage and reacts, not a function the uploader calls, and the demo should model that. A separate process also makes the pipeline visible, which is a good demo moment: the file is processed in one terminal while the agent waits in another.

## Decision

Three execution modes share one pipeline. `pipeline.chunk_and_write` and `pipeline.embed_and_commit` are the only implementation of chunking, embedding and index writes; each mode is a thin trigger around them. The MCP server reads the active mode from `ONEBUCKET_EXECUTION_MODE`.

| Mode | Trigger | Where the pipeline runs |
|---|---|---|
| `lambda` | S3 object-created notifications: chunker on `raw/`, embedder on `chunks/*/MANIFEST.json` | Two Lambda functions in the operator's AWS account (ADR-012) |
| `local-daemon` (default) | `scripts/oneb_pipeline_daemon.py` polls `raw/` on an interval (5 s by default) | The daemon process on the operator's machine |
| `onebucket-events` | `scripts/oneb_event_consumer.py` long-polls OneBucket's events MCP server with `tail_events` and dispatches on the `gateway` record of each successful `s3:PutObject` under `raw/` | The consumer process on the operator's machine |

### The two local modes

Both are long-running processes with the same operational contract: sequential dispatch through the two pipeline functions, clean shutdown on SIGTERM and SIGINT, and a heartbeat file (`/tmp/oneb-pipeline-daemon.heartbeat` by default, `ONEBUCKET_DAEMON_HEARTBEAT_PATH` to override) rewritten on every cycle with the mode name and the current state. Before accepting an upload in either local mode, the MCP server's `ingest_document` reads that file and refuses if the heartbeat is missing or older than 30 seconds, so a user never uploads into a pipeline that is not running. The MCP server does not care which of the two processes wrote it.

The polling daemon works against any S3-compatible bucket and needs only S3 credentials. It is the right choice for plain S3, and for anyone who does not want to sign in to OneBucket's events service. At startup it also checks the bucket's notification configuration and warns if a Lambda is already subscribed to `raw/`.

The event consumer is for OneBucket-backed deployments. Signing in is a one-time OAuth 2.1 authorization-code flow with PKCE (`scripts/onebucket-login.py`) that stores a refresh token; the consumer refreshes its access token before it expires and exits with instructions when it no longer can, rather than looping against the server. `tail_events` blocks for up to a wait period and always returns a cursor; the consumer persists the cursor only after a whole batch is processed, so a crash replays the batch, and the deduplicator plus the pipeline's idempotent, content-addressed writes make the replay harmless. Every event, including the per-peer `backend` records, is appended to `.runbook-state/events.jsonl`; that log is what the replication and chunk-read metrics tools read (ADR-007).

### Single active mode

Exactly one mode runs against a bucket at a time. Two triggers on the same `raw/` prefix would both process every upload, race on the manifest and waste compute. The rule is enforced by visibility rather than by a lock: the daemon's startup warning is a best-effort signal, and it does not refuse to start because an operator may be deliberately comparing modes.

Switching modes is operational: stop the running process (or leave the Lambda idle), start the other, and set `ONEBUCKET_EXECUTION_MODE` in the MCP server's environment so its status messages match. Storage is the same in every mode (`raw/`, `chunks/`, `metadata/`), and every reader is agnostic to which mode produced what it reads.

## Consequences

What this buys:

- The full loop runs on a laptop with one script and a configured MCP server; no AWS deployment required.
- No duplicated pipeline logic. Each mode diverges only in its trigger code.
- The event-driven mode turns the storage layer's own records into demo telemetry without adding instrumentation to the pipeline.
- Zero Lambda cost during local demos; polling costs are on the order of cents per day at the default interval.

What it costs:

- The heartbeat lives on a filesystem, so the MCP server and the local process must share a machine. Cross-host setups are out of scope.
- The local processes are single-threaded: uploads queue behind the document being processed.
- Polling adds up to one interval of detection lag; the event mode removes it but needs a OneBucket and a sign-in.
- A long-running process can outlive its cloud credentials; the daemon's periodic health log distinguishes idle from unauthenticated.
- Three paths to test and document.

What it rules out:

- Synchronous ingestion inside the MCP server, and running more than one trigger against a bucket.

## Related

- [ADR-001: Single-Writer LanceDB Index on Object Storage](ADR-001-lance-metadata-sync.md)
- [ADR-002: Ingestion Compute: Lambda and Long-Running Processes](ADR-002-lambda-compute-strategy.md)
- [ADR-007: Telemetry Storage and Agent Access](ADR-007-telemetry-storage.md)
- [ADR-012: Chained Lambdas with the Manifest as Commit Point](ADR-012-chained-lambda-architecture.md)
- [src/onebucket_demo/pipeline.py](../../../src/onebucket_demo/pipeline.py)
- [src/onebucket_demo/mcp_tools.py](../../../src/onebucket_demo/mcp_tools.py)
- [src/onebucket_demo/event_consumer.py](../../../src/onebucket_demo/event_consumer.py)
- [docs/operations/execution-modes.md](../../operations/execution-modes.md)
- [docs/operations/runbooks/local-daemon.md](../../operations/runbooks/local-daemon.md)
- [docs/operations/runbooks/event-consumer.md](../../operations/runbooks/event-consumer.md)
