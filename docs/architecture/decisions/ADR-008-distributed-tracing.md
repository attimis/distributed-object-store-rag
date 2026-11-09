# ADR-008: Distributed Tracing via Trace-Id Propagation

**Status:** Deprecated
**Date:** 2026-05-05

## Context

A request in this system crosses several layers: the writer, the storage gateway, the peer buckets it replicates to, and a reader in some other region. The conventional way to reconstruct one request's timeline across layers is to stamp it with a trace identifier at the origin and have every layer carry that identifier into its own logs, so the logs can be joined later.

The proposal here was to attach the identifier to every storage request, provisionally as a query-string parameter on the object URI, and to rely on the storage gateway to parse it, forward it through its own replication and cache-fill operations, and surface it in its telemetry. The S3 protocol reserves query strings for protocol operations (presigned signatures, multipart upload ids), so this would have required a gateway feature that parses and strips a custom parameter before forwarding. The fallbacks were a custom request header, or embedding the identifier in the object key, which pollutes the keyspace and breaks content addressing.

## Decision

The proposal is deprecated and nothing of it was built. There is no trace identifier anywhere in the code.

The need it addressed is covered differently. OneBucket already correlates its own work per request: every request produces one `gateway` event and one `backend` event per peer, and all of them share a `reqId` assigned by OneBucket, not by the client. The event consumer stores those events in a local log and the metrics tools join on `reqId` (ADR-007). So the join key the proposal wanted to inject from the outside is already generated on the inside, at the only layer whose timing the demo needs to see.

## Consequences

- No application code carries, logs or forwards a trace identifier, and no storage URI is modified for observability.
- Correlation is limited to what the storage layer reports: a timeline spans the gateway and the peers, not the agent's own reasoning or the MCP server's internals. For the demo's question ("where did this read come from, and how long did the write take to reach each peer?") that is the right scope.
- If end-to-end tracing across the application is ever wanted, the right mechanism is a request header the gateway forwards, not a query string or a key suffix. That would be a new decision.

## Related

- [ADR-007: Telemetry Storage and Agent Access](ADR-007-telemetry-storage.md)
- [ADR-013: Execution Modes](ADR-013-execution-modes.md)
- [src/onebucket_demo/event_consumer.py](../../../src/onebucket_demo/event_consumer.py)
