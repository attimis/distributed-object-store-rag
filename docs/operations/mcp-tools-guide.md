# MCP tools reference

The server in `src/onebucket_demo/mcp_server.py` registers thirteen tools.
Each handler calls one Python function and serialises the result; the
descriptions the model sees are in the same file and are deliberately
detailed, because they are the only documentation the model gets. This page
is the human version: what each tool does, what it returns, and how they
are meant to be combined.

## The workflow the tools are designed for

```
ingest_document("AAPL")                 -> raw_file_id, upload done
check_ingest_status(raw_file_id)        -> phase: ... -> ready
search_global_index(q, limit=20)        -> discovery: which documents matter?  (read raw_file_id on the hits)
search_global_index(q, scope=[id, ...]) -> ONE focused search across those documents
fetch_chunk(chunk_uri) x N              -> the text to cite
```

Two things the model gets wrong without being told: it should run **one**
scoped search across all relevant documents (the ranking picks the best
evidence wherever it lives), not one search per document; and it should
fetch chunk text before answering, because search returns pointers only.

## Read tools

### `search_global_index(query, limit=3, scope=None)`

Hybrid search over the whole index. Returns two ranked lists,
`semantic_matches` (dense vectors, score `1/(1+L2)` in (0, 1]) and
`keyword_matches` (SPLADE sparse dot product, score in [0, inf)). Each hit
has `chunk_uri`, `source_uri`, `raw_file_id`, `chunk_index`, `score`. The
lists may overlap or differ; reading both is the point. A `_timing_ms`
block reports time spent inside the tool (query embedding, each scan),
which is the honest number to quote for storage-side latency.

- `limit`: 1 to 30 per list. 10 to 20 for a discovery search, 3 to 5 for a
  focused one.
- `scope`: a list of `raw_file_id` values to restrict to. Empty or omitted
  means the whole corpus.
- Nothing found: `{"message": "No relevant documents found ...", "_timing_ms": {...}}`.
- Index unreachable: `{"error": "CONNECTION_FAILED", "message": "..."}`.

### `fetch_chunk(chunk_uri)`

The text of one chunk. Accepts the `chunk_uri` from a hit (`s3://...` or
`file://...`) or a relative `chunks/<id>/chunk_NNNN.txt`, which is joined
onto `ONEBUCKET_CHUNK_ROOT`. Anything that could escape the chunk root
(parent segments, other URL schemes, absolute paths) is refused.

### `list_documents()`

Every `raw_file_id` in the index with a representative `source_uri` and its
`chunk_count`, plus totals. For "what is in the index?"; for routing a
question, a discovery search is more reliable than matching filenames.

### `check_ingest_status(raw_file_id)`

Where a document is: `chunk_count`, `manifest_present`, `rows_committed`,
`last_activity_seconds_ago`, and a `phase` of
`uploading_or_chunker_pending`, `chunking`, `embedding`, `partial` or
`ready` (`ready: true` when searchable). `dlq_pending` is the account-wide
dead-letter depth in `lambda` mode; it is not a per-document failure signal
and is `-1` when there is no queue to read.

Timing expectations: a small document is ready in under a minute; a 10-K
(100 to 200 chunks) takes a few minutes, most of it embedding. A document is
likely stuck only when `phase` is `embedding`, `rows_committed` is 0 and
`last_activity_seconds_ago` exceeds 600.

## Ingest

### `ingest_document(source)`

`source` is an SEC ticker (`"AAPL"`: the latest 10-K is fetched from EDGAR
and converted from HTML), any `http(s)://` URL, or an absolute local path
on the machine running the server. The text is uploaded to
`raw/<name>.txt`; what happens next depends on `ONEBUCKET_EXECUTION_MODE`.

Returns `raw_file_id` (the first 16 hex characters of the SHA-256 of the
text, the same id the chunker will assign), `raw_key`, `raw_uri`,
`size_bytes`, `source_kind`, `source_url`, `execution_mode`, a `status`
sentence, and in the local modes `daemon_currently_busy`.

Refusals, all before any upload: `DAEMON_NOT_RUNNING` (no fresh heartbeat
in a local mode), `DAEMON_AUTH_EXPIRED` (the event consumer's OneBucket
sign-in lapsed), `DAEMON_WATCHING_WRONG_BUCKET` (the consumer watches a
different bucket than this server uploads to).

## Telemetry (`onebucket-events` mode)

Both read the consumer's JSONL event log (`.runbook-state/events.jsonl` or
`ONEBUCKET_EVENT_LOG_PATH`). OneBucket records every request as one
`gateway` event plus one `backend` event per peer, sharing `req_id`.

### `get_replication_metrics(raw_file_id)`

For the most recent ingest of one document: `ingest_started_at`,
`ingest_completed_at`, `total_elapsed_seconds`, `chunk_event_count`,
`metadata_event_count`, `per_peer_ack_latency` (per endpoint: count,
median and p95 of backend-minus-gateway time) and `metadata_quorum_ack_ms`
(for writes to the metadata OneBucket: the slowest peer per request).
Errors: `EVENT_LOG_MISSING`, `NO_EVENTS_FOUND`.

### `get_chunk_read_metrics(start_time, end_time=None, raw_file_id=None)`

Chunk fetches in a window (RFC3339 timestamps; `end_time` defaults to now):
`events_count`, `duration_ms` (median, p95, min, max of the gateway
duration), `per_policy` (per OneBucket), `per_first_responder` (which peer
served each read first), and up to 50 events.

## Demo configuration

`get_runbook_config()`, `set_runbook_variable(path, value)` and
`validate_runbook_config()` read and write `docs/demo_runbook/variables.yml`
(copied from `variables.example.yml`; path overridable with
`ONEBUCKET_RUNBOOK_CONFIG`). `set_runbook_variable` takes a dotted path
such as `operator_intent.fresh_ticker_for_live_ingest`, preserves the
file's comments, coerces a string to the field's existing numeric or
boolean type, and never creates new keys. Intended for the
`operator_intent.*` section; sections 1 to 6 mirror the OneBucket console
and editing them here changes nothing there. `get_runbook_config` reports
`missing_required` (placeholders still unfilled) and `validation_errors`.

## Destructive tools

These change state the read tools cannot undo. Each is strict about its
preconditions and refuses on any inconsistency rather than muddling
through; the error names the recovery (`rebuild_index`).

### `delete_document(raw_file_id)`

Removes a document end to end: index rows first (so search stops surfacing
it immediately), then chunk files and manifest, then the raw object. The id
must be the 16-hex document id. Refuses with `NO_SUCH_DOCUMENT`,
`MANIFEST_MISSING_DB_INCONSISTENT`, `CHUNKS_COUNT_MISMATCH_DB_INCONSISTENT`
or `RAW_MISSING_DB_INCONSISTENT` when storage and index disagree.

### `purge_peer_cache(onebucket, peer_alias, key_pattern)`

Deletes objects directly on a backing peer, bypassing OneBucket, to
demonstrate a cold cache filling up again on read. Refuses unless the peer
is `type: cache` in `variables.yml` (`REFUSED_NOT_CACHE_PEER`), and needs
that endpoint's `direct_access` profile. `key_pattern` is `*` (everything),
`prefix*`, or an exact key.

### `rebuild_index(confirm=false)`

Deletes every index row and every chunk, then re-chunks and re-embeds every
`raw/` object. `raw/` is never touched. Requires `confirm=true`; the
model should ask before passing it. Minutes for a real corpus.

## Environment variables the server reads

| Variable | Purpose | Default |
|---|---|---|
| `ONEBUCKET_DATASET_ROOT` | where the LanceDB index lives (`s3://...` or a path) | `data/metadata` |
| `ONEBUCKET_CHUNK_ROOT` | where chunks live; also derives the bucket and the `raw/` prefix | `data/chunks` |
| `ONEBUCKET_RAW_ROOT` | override the upload prefix | chunk root with `/chunks` replaced by `/raw` |
| `ONEBUCKET_EXECUTION_MODE` | `local-daemon`, `onebucket-events` or `lambda` | `local-daemon` |
| `AWS_PROFILE`, `AWS_ENDPOINT_URL`, `AWS_REGION` | S3 credentials and endpoint for boto3 | boto3 defaults; region `us-east-1` |
| `ONEBUCKET_DAEMON_HEARTBEAT_PATH` | the consumers' heartbeat file | `/tmp/oneb-pipeline-daemon.heartbeat` |
| `ONEBUCKET_EVENT_LOG_PATH` | the consumer's JSONL log | `<repo>/.runbook-state/events.jsonl` |
| `ONEBUCKET_RUNBOOK_CONFIG` | the demo config file | `<repo>/docs/demo_runbook/variables.yml` |
| `ONEBUCKET_LAMBDA_PREFIX` | names the embedder DLQ in `lambda` mode | `onebucket` |
| `ONEBUCKET_EMBED_BATCH_SIZE` | chunks per embedding batch | `16` |
| `ONEBUCKET_DISABLE_CACHE_WARMER` | set to `1` to skip pre-warming new documents | unset |
| `ONEBUCKET_SEC_USER_AGENT` | the User-Agent EDGAR requires | a generic demo string |

## Anti-patterns worth telling the model about

- Searching per document with `scope=[one id]` in a loop. One search with
  all ids is faster and ranks better.
- Answering from `chunk_uri` values without fetching them.
- Concluding a document failed from `dlq_pending` alone.
- Calling `rebuild_index` as a first resort; `delete_document` on the
  offending id is usually enough.
