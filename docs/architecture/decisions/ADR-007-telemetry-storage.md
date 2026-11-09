# ADR-007: Telemetry Storage and Agent Access

**Status:** Superseded by ADR-013 (the event log of the `onebucket-events` mode)
**Date:** 2026-05-05

## Context

The demo wants the agent to report on the architecture it is using: how long an upload took to land on every peer, where a chunk read was served from, how many objects moved. The original proposal was to have every component append its own JSON-lines telemetry files to a `telemetry/` prefix in the bucket, let the storage layer replicate that prefix alongside `metadata/`, and give the agent an MCP tool that ran SQL over those files with an embedded analytical engine.

## Decision

That design was never built, and it is superseded. OneBucket already records every request it serves: for each request there is one `gateway` event (the request as the client made it) and one `backend` event per peer bucket the request reached, all sharing a `reqId`. The `onebucket-events` execution mode (ADR-013) tails those events through OneBucket's events MCP server and appends every one to a local JSON-lines log at `.runbook-state/events.jsonl`, together with what the pipeline did about it. Two MCP tools read that log: `get_replication_metrics` (per-document end-to-end time and per-peer acknowledgement latency for the writes) and `get_chunk_read_metrics` (chunk fetch latency in a time window and which peer served each read). No telemetry prefix exists in the bucket, no SQL engine is bundled, and the pipeline writes no telemetry of its own.

## Consequences

The measurements come from the storage layer that performed the work rather than from timers in the application, which is both more trustworthy and far less code. The cost is that the metrics exist only in the `onebucket-events` mode and only on the machine running the consumer; in the `lambda` and `local-daemon` modes the two tools report that the log is missing.

## Related

- [ADR-008: Distributed Tracing via Trace-Id Propagation](ADR-008-distributed-tracing.md)
- [ADR-013: Execution Modes](ADR-013-execution-modes.md)
- [src/onebucket_demo/event_consumer.py](../../../src/onebucket_demo/event_consumer.py)
- [src/onebucket_demo/mcp_tools.py](../../../src/onebucket_demo/mcp_tools.py)
- [docs/operations/runbooks/event-consumer.md](../../operations/runbooks/event-consumer.md)
