"""The MCP server: tool definitions and the stdio transport.

Every tool handler does exactly one thing: call the matching Python function
(in ``search.py`` or ``mcp_tools.py``) and JSON-serialise its return value.
No logic lives at the transport boundary, so what a test sees in-process is
what Claude Desktop sees over MCP.

The tool descriptions are long on purpose. They are the only documentation
the model gets, and they teach it the intended workflow: a broad discovery
search, one focused search scoped to the documents that matter, then
``fetch_chunk`` for the text it will cite.

At startup the server checks the index's canary hash against the models it
would query with and refuses to serve on a mismatch (see ``embed_sparse``).
"""

from __future__ import annotations

import asyncio
import json
import os
import threading as _threading
import time as _time
from pathlib import Path

from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from . import storage
from .mcp_tools import (
    check_ingest_status,
    delete_document,
    get_chunk_read_metrics,
    get_replication_metrics,
    get_runbook_config,
    ingest_document,
    list_documents,
    purge_peer_cache,
    rebuild_index,
    set_runbook_variable,
    validate_runbook_config,
)
from .search import assert_canary, search_global_index

SERVER_NAME = "onebucket-demo-mcp"
SERVER_VERSION = "0.2.0"
DEFAULT_DATASET_ROOT = "data/metadata"
DEFAULT_CHUNK_ROOT = "data/chunks"
TOOL_NAME = "search_global_index"
FETCH_TOOL_NAME = "fetch_chunk"
INGEST_TOOL_NAME = "ingest_document"
STATUS_TOOL_NAME = "check_ingest_status"
METRICS_TOOL_NAME = "get_replication_metrics"
READ_METRICS_TOOL_NAME = "get_chunk_read_metrics"
LIST_TOOL_NAME = "list_documents"
CONFIG_GET_TOOL_NAME = "get_runbook_config"
CONFIG_SET_TOOL_NAME = "set_runbook_variable"
CONFIG_VALIDATE_TOOL_NAME = "validate_runbook_config"
DELETE_DOC_TOOL_NAME = "delete_document"
PURGE_CACHE_TOOL_NAME = "purge_peer_cache"
REBUILD_INDEX_TOOL_NAME = "rebuild_index"
ENV_DATASET_ROOT = "ONEBUCKET_DATASET_ROOT"
ENV_CHUNK_ROOT = "ONEBUCKET_CHUNK_ROOT"

_TOOL_DEFINITION = Tool(
    name=TOOL_NAME,
    description=(
        "Hybrid (dense + sparse) search across the global RAG index. "
        "Returns two ranked arrays: `semantic_matches` (cosine-similar by "
        "BGE-small-en-v1.5 vectors) and `keyword_matches` (SPLADE sparse "
        "dot-product). Each hit exposes `chunk_uri`, `source_uri`, "
        "`raw_file_id`, `chunk_index`, and `score`. The two arrays may "
        "overlap or differ; reading both is the recommended hybrid pattern. "
        "\n\n"
        "TYPICAL AGENTIC FLOW (multi-hop):\n"
        "  1. DISCOVERY HOP: search broadly with `limit=20` and no `scope`. "
        "Inspect the returned `raw_file_id` values to identify which "
        "documents are relevant to the user's question.\n"
        "  2. FOCUSED HOP: ONE search with `scope=[<id1>, <id2>, ...]` "
        "covering ALL relevant docs at once — NOT one search per doc. "
        "LanceDB returns the globally top-N most relevant chunks across the "
        "scope; doing N separate searches is wasteful AND less accurate "
        "because it forces a fixed budget per doc instead of letting the "
        "ranking pick the strongest evidence wherever it lives.\n"
        "  3. RETRIEVAL: call `fetch_chunk` for the top hits (the URIs alone "
        "are not enough — you need the chunk text to synthesize).\n"
        "  4. SYNTHESIS: ground the answer in the fetched chunks; cite by "
        "`source_uri` + `chunk_index`.\n"
        "\n"
        "If you already know the document(s) of interest (e.g., the user "
        "just asked you to ingest AAPL's 10-K and you have its raw_file_id), "
        "skip the discovery hop and go straight to focused search.\n"
        "\n"
        "EFFICIENCY ANTI-PATTERN to avoid: do NOT iterate searches per-doc "
        "with scope=[single_id] when the user's question spans multiple "
        "docs. ONE search with scope=[id1, id2, id3, ...] and limit=10-15 "
        "gives you the best evidence across the whole set in one round-trip. "
        "Only do per-doc searches when the user has explicitly asked for "
        "a per-document breakdown AND you've already identified that one "
        "doc's chunks are systematically being crowded out of a multi-doc "
        "result list."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": (
                    "Natural language query. Discovery hops do well with "
                    "broad topical phrasing (e.g., 'oil exploration risk'); "
                    "focused hops do well with the user's literal question."
                ),
            },
            "limit": {
                "type": "integer",
                "description": (
                    "Max hits per array (1–30). Default 3. Use 10–20 for "
                    "discovery hops (recall over precision); 3–5 for focused "
                    "follow-up searches."
                ),
                "default": 3,
                "minimum": 1,
                "maximum": 30,
            },
            "scope": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Optional list of `raw_file_id` values (16-char hex) to "
                    "restrict the search to. Pass an empty list / omit "
                    "entirely to search the whole corpus. Build this list "
                    "from a discovery-hop's results, or from a previous "
                    "ingest_document call's `raw_file_id` return value."
                ),
            },
        },
        "required": ["query"],
    },
)


_FETCH_TOOL_DEFINITION = Tool(
    name=FETCH_TOOL_NAME,
    description=(
        "Retrieve the plain-text contents of a single chunk by its URI. Pass "
        "a `chunk_uri` value returned by `search_global_index`. Handles both "
        "full `s3://` URIs and relative `chunks/...` paths (resolved against "
        "the configured chunk root). Returns the chunk text — typically a "
        "few KB of natural language. Call this for each of the top search "
        "hits you want to use as context, then synthesize an answer "
        "citing them by `source_uri` and `chunk_index`."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "chunk_uri": {
                "type": "string",
                "description": (
                    "A `chunk_uri` value from a `search_global_index` hit. "
                    "Full `s3://` URI or relative `chunks/<hash>/chunk_NNNN.txt`."
                ),
            },
        },
        "required": ["chunk_uri"],
    },
)


_INGEST_TOOL_DEFINITION = Tool(
    name=INGEST_TOOL_NAME,
    description=(
        "Add a document to the OneBucket RAG index. CALL THIS DIRECTLY when "
        "the user asks you to ingest content — it handles fetching, HTML→"
        "text conversion, and S3 upload internally. You do NOT need to ask "
        "the user for a URL when they give you a ticker; the tool resolves "
        "the ticker → CIK → most-recent-10-K via SEC EDGAR's submissions "
        "API for you.\n"
        "\n"
        "Three valid `source` formats:\n"
        "  1. SEC ticker symbol like 'AAPL', 'MSFT', 'GOOG' — pulls the "
        "most recent 10-K filing from EDGAR. This is the typical path.\n"
        "  2. Full URL like 'https://www.sec.gov/Archives/edgar/...' or any "
        "other public HTML/text URL — fetched directly.\n"
        "  3. Absolute local file path on the MCP server's host — read from "
        "disk; useful when the user has a doc already prepared.\n"
        "\n"
        "WHEN THE USER ASKS 'ingest AAPL's 10-K' OR 'pull the latest "
        "Microsoft annual report': call ingest_document(source='AAPL') or "
        "ingest_document(source='MSFT'). Do NOT ask for a URL first. Do NOT "
        "ask for confirmation. The tool was designed for this; the user has "
        "already authorized it by configuring this MCP server.\n"
        "\n"
        "Returns immediately with `raw_file_id` (the document's content "
        "hash — same value used everywhere downstream as the document's "
        "primary key, including the `scope` parameter of search_global_index). "
        "Total ingest latency: a few seconds for upload + chunker run, plus "
        "~30s–4min for embedding depending on document size. After calling "
        "this, poll `check_ingest_status(raw_file_id)` until `phase == 'ready'` "
        "before running searches against the new document."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "source": {
                "type": "string",
                "description": (
                    "Most common: a ticker symbol like 'AAPL'. Also accepts "
                    "a full URL ('https://...') or an absolute local file "
                    "path. The tool autodetects which kind it received."
                ),
                "examples": ["AAPL", "MSFT", "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/aapl-20240928.htm", "/tmp/onebucket-e2e/doc.txt"],
            },
        },
        "required": ["source"],
    },
)


_STATUS_TOOL_DEFINITION = Tool(
    name=STATUS_TOOL_NAME,
    description=(
        "Report the current pipeline state for a previously-uploaded "
        "document, identified by its `raw_file_id` (returned from "
        "ingest_document). Cheap to call; safe to poll every ~30 seconds "
        "during a long ingest.\n"
        "\n"
        "Returns:\n"
        "  • `chunk_count` — chunk files landed in S3 for this doc.\n"
        "  • `manifest_present` — chunker has finished writing.\n"
        "  • `rows_committed` — rows in LanceDB matching this raw_file_id; "
        "this is the load-bearing signal for searchability.\n"
        "  • `dlq_pending` — ACCOUNT-WIDE DLQ depth (NOT per-doc!). May "
        "include stale messages from prior debugging sessions. DO NOT "
        "conclude THIS document failed based on dlq_pending alone.\n"
        "  • `last_activity_seconds_ago` — seconds since the most recent "
        "S3 write under this doc's prefix. Use to distinguish 'still "
        "progressing' (small number, the embedder is mid-flight) from "
        "'actually stalled' (>600s with no progress).\n"
        "  • `phase` ∈ {'uploading_or_chunker_pending', 'chunking', "
        "'embedding', 'ready', 'partial'}. `ready: true` when fully "
        "searchable.\n"
        "\n"
        "TIMING EXPECTATIONS — important so you don't conclude 'stalled' "
        "prematurely:\n"
        "  • Tiny docs (~5-20 chunks): chunker ~5s, embedder ~30-60s.\n"
        "  • 10-K-sized docs (100-200 chunks): chunker ~10s, embedder "
        "~3-6 min (we batch-embed 16 chunks at a time on a 3 GB Lambda).\n"
        "  • Large 10-Ks (300+ chunks): up to 8 min for embedder.\n"
        "  • CONCURRENT INGESTS — when 3+ docs ingest at the same time, "
        "LanceDB optimistic-concurrency-control retries can EXTEND any "
        "individual doc's wall-clock by 2-3x as embedder Lambdas race on "
        "the manifest commit. With 5 concurrent ingests, allow up to 12-15 "
        "minutes per doc before suspecting failure.\n"
        "\n"
        "TRUE FAILURE SIGNALS:\n"
        "  • `phase == 'embedding'` AND `last_activity_seconds_ago > 600` "
        "AND `rows_committed == 0` — likely failed (check CloudWatch).\n"
        "  • `chunk_count > 0` AND `manifest_present == False` AND "
        "`last_activity_seconds_ago > 300` — chunker may have failed "
        "between writing chunks and writing the manifest.\n"
        "Do NOT conclude failure from dlq_pending alone."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "raw_file_id": {
                "type": "string",
                "description": (
                    "16-char hex content hash returned by ingest_document."
                ),
            },
        },
        "required": ["raw_file_id"],
    },
)


_METRICS_TOOL_DEFINITION = Tool(
    name=METRICS_TOOL_NAME,
    description=(
        "Replication latencies observed while one document was ingested, "
        "from the event consumer's log (onebucket-events mode). OneBucket "
        "reports every request as one gateway event plus one backend event "
        "per peer; the per-peer numbers here are backend-minus-gateway "
        "timestamps within each request.\n"
        "\n"
        "Returns:\n"
        "  • `ingest_started_at` / `ingest_completed_at` — first/last "
        "consumer-observed timestamps for this doc.\n"
        "  • `total_elapsed_seconds` — wall-clock from upload to fully "
        "embedded.\n"
        "  • `chunk_event_count` / `metadata_event_count` — writes under "
        "`chunks/<id>/` and to the metadata OneBucket.\n"
        "  • `per_peer_ack_latency` — per endpoint alias: "
        "{count, median_ms, p95_ms}.\n"
        "  • `metadata_quorum_ack_ms` — per metadata write, the slowest "
        "peer's ack; the median is the strict-sync cost of the index tier.\n"
        "\n"
        "If the event log is missing returns `error: EVENT_LOG_MISSING`. "
        "If no events match returns `error: NO_EVENTS_FOUND` — usually "
        "means the doc was ingested before the consumer's event log "
        "started, OR the raw_file_id is wrong."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "raw_file_id": {
                "type": "string",
                "description": "16-char hex content hash.",
            },
        },
        "required": ["raw_file_id"],
    },
)


_READ_METRICS_TOOL_DEFINITION = Tool(
    name=READ_METRICS_TOOL_NAME,
    description=(
        "Chunk fetch latencies from the event consumer's log, filtered by "
        "time window. OneBucket's own per-request duration_ms is far more "
        "honest than the agent's wall-clock around MCP calls (which "
        "includes Claude Desktop overhead and the MCP round-trip), so quote "
        "these numbers when describing retrieval speed.\n"
        "\n"
        "Args:\n"
        "  start_time — ISO timestamp (e.g. '2026-05-28T19:42:00Z'). "
        "Filters events with consumer_received_at >= start_time.\n"
        "  end_time   — optional; defaults to now (UTC).\n"
        "  raw_file_id — optional; restrict to chunks/<id>/*.\n"
        "\n"
        "Returns: events_count, duration_ms stats (median/p95/min/max), "
        "per_policy breakdown (median ms per bucket name), and "
        "per_first_responder (count of events served by each peer "
        "alias as the first 200 response).\n"
        "\n"
        "Typical usage: record the current UTC timestamp before a batch "
        "of fetches; afterwards call this tool with that timestamp and "
        "'now' to get the storage-side median fetch latency for them."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "start_time": {
                "type": "string",
                "description": "ISO timestamp UTC, lower bound on consumer_received_at.",
            },
            "end_time": {
                "type": "string",
                "description": "ISO timestamp UTC, upper bound. Defaults to now.",
            },
            "raw_file_id": {
                "type": "string",
                "description": "Optional 16-char hex hash to filter to a specific doc.",
            },
        },
        "required": ["start_time"],
    },
)


_LIST_TOOL_DEFINITION = Tool(
    name=LIST_TOOL_NAME,
    description=(
        "Enumerate all documents currently committed to the global index. "
        "Returns one entry per distinct `raw_file_id`, with `chunk_count` "
        "and a representative `source_uri`. Useful for admin queries "
        "('what's in the index?') and for sanity-checking after a bulk "
        "ingest.\n"
        "\n"
        "Note for query routing: prefer multi-hop discovery via "
        "search_global_index over listing-and-name-matching here. Searching "
        "for a topic ('oil exploration') reliably surfaces the relevant "
        "raw_file_ids; matching on source_uri filenames does not."
    ),
    inputSchema={"type": "object", "properties": {}},
)


_CONFIG_GET_TOOL_DEFINITION = Tool(
    name=CONFIG_GET_TOOL_NAME,
    description=(
        "Read the runbook's variables.yml (the single source of truth for "
        "this deployment's policy names, peer aliases, pipeline routing, "
        "pricing assumptions, and workload defaults). Returns the full "
        "parsed config, the resolved file path, any 'TODO' sentinel values "
        "the operator still needs to fill in (`missing_required`), and "
        "consistency errors from the validator (`validation_errors`).\n"
        "\n"
        "Call this at the start of a demo session (runbook Phase 0) to "
        "decide what to ask the operator. If `missing_required` is empty "
        "and `validation_errors` is empty, the deployment is ready to run."
    ),
    inputSchema={"type": "object", "properties": {}},
)


_CONFIG_SET_TOOL_DEFINITION = Tool(
    name=CONFIG_SET_TOOL_NAME,
    description=(
        "Update a single variable in variables.yml via dotted path "
        "(e.g. 'operator_intent.fresh_ticker_for_live_ingest' → 'GOOG', "
        "or 'operator_intent.pricing.aws_egress_usd_per_gb' → 0.095). "
        "Comments in the YAML are preserved. The write is atomic.\n"
        "\n"
        "INTENDED USE: edit operator_intent.* values during demo setup. "
        "Deployment facts (sections 1-6: onebucket, endpoints, policies, "
        "pipeline, daemon, classifier_outcomes) mirror the OneBucket "
        "console; editing them here does NOT change the deployment and "
        "runtime behavior will diverge from the YAML. Change the console "
        "first, then mirror the change here.\n"
        "\n"
        "Returns ok=true with the actually-stored value (after any "
        "type coercion — e.g. passing '10000000' on an int field stores "
        "and returns 10000000), an `input_was_coerced` flag, and any "
        "validation errors that appeared after the write so the agent can "
        "warn the operator if the change made the config inconsistent. "
        "Returns an error envelope (no write performed) if the path "
        "doesn't exist or is malformed.\n"
        "\n"
        "Type-coercion rules: if the existing value is int/float/bool and "
        "you pass a string parseable to that type, we coerce — guards "
        "against accidentally rewriting an int field to a quoted string "
        "in the YAML. Anything else passes through; the validator will "
        "flag obvious mismatches."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": (
                    "Dotted path into the YAML. Examples: "
                    "'operator_intent.fresh_ticker_for_live_ingest', "
                    "'operator_intent.workload.y1_read_fraction', "
                    "'operator_intent.pricing.aws_egress_usd_per_gb'. "
                    "List indices via brackets: 'policies.demo-data.peers[0].required'."
                ),
            },
            "value": {
                "description": (
                    "New value. Strings, numbers, booleans, lists, and "
                    "null all OK — must match what makes sense for that "
                    "field (the validator will flag obvious mismatches)."
                ),
            },
        },
        "required": ["path", "value"],
    },
)


_CONFIG_VALIDATE_TOOL_DEFINITION = Tool(
    name=CONFIG_VALIDATE_TOOL_NAME,
    description=(
        "Re-read variables.yml and report current consistency errors plus "
        "any 'TODO'-sentinel values still needing operator input. Cheap "
        "to call; safe between any other tool calls when the agent wants "
        "a quick sanity check before proceeding with a demo phase."
    ),
    inputSchema={"type": "object", "properties": {}},
)


_DELETE_DOC_TOOL_DEFINITION = Tool(
    name=DELETE_DOC_TOOL_NAME,
    description=(
        "Remove a document end-to-end from the OneBucket RAG index: deletes "
        "LanceDB rows for the given raw_file_id, then deletes all chunk "
        "files + MANIFEST.json from S3, then deletes the raw source file. "
        "Order is load-bearing — LanceDB first so search stops surfacing "
        "the chunks immediately, no window where the UI returns URIs that "
        "404 on fetch.\n"
        "\n"
        "STRICT: refuses on any inconsistency between LanceDB and S3 (e.g., "
        "manifest missing, chunk count mismatch, raw file gone). Inconsistency "
        "errors include a pointer to rebuild_index() — running this tool will "
        "NOT silently muddle through partial state. If a previous deletion "
        "failed mid-way and you want to clean up, call rebuild_index instead.\n"
        "\n"
        "Use list_documents() to discover raw_file_ids currently in the index."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "raw_file_id": {
                "type": "string",
                "description": (
                    "The 16-char hex content hash that identifies the "
                    "document (same value list_documents and ingest_document "
                    "return). Required."
                ),
            },
        },
        "required": ["raw_file_id"],
    },
)


_PURGE_CACHE_TOOL_DEFINITION = Tool(
    name=PURGE_CACHE_TOOL_NAME,
    description=(
        "Delete objects directly from a backing peer, bypassing OneBucket. "
        "Used to demonstrate cache-cold to cache-fill flows: purge the cache "
        "peer's copy of a document's chunks, then re-read through the "
        "OneBucket that uses it as a cache and watch the copies reappear.\n"
        "\n"
        "SAFETY: refuses if the named peer has type != 'cache'. Sync peers "
        "hold canonical data; deleting from them would corrupt the OneBucket "
        "instead of clearing a cache. Use delete_document for canonical "
        "removal.\n"
        "\n"
        "Pattern semantics (S3 prefix-style, no middle-of-string globs):\n"
        "  * '*'                              → every key in the backing bucket\n"
        "  * 'chunks/abc123/*'                → all chunks under that prefix\n"
        "  * 'chunks/abc123/chunk_0000.txt'   → exact-match single key"
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "onebucket": {
                "type": "string",
                "description": (
                    "OneBucket policy name from variables.yml policies block "
                    "(e.g., 'demo-data')."
                ),
            },
            "peer_alias": {
                "type": "string",
                "description": (
                    "Peer alias within that policy (e.g., 'WEST_WASABI'). "
                    "Must reference a peer with type='cache' or the call "
                    "refuses."
                ),
            },
            "key_pattern": {
                "type": "string",
                "description": (
                    "What to delete. Use '*' for full cache purge, "
                    "'<prefix>*' for prefix match, or an exact key for "
                    "single-object removal."
                ),
            },
        },
        "required": ["onebucket", "peer_alias", "key_pattern"],
    },
)


_REBUILD_INDEX_TOOL_DEFINITION = Tool(
    name=REBUILD_INDEX_TOOL_NAME,
    description=(
        "Wipe + re-derive the index from scratch: deletes ALL LanceDB rows "
        "and ALL chunk files for the configured bucket, then re-runs the "
        "chunker + embedder pipeline on every raw/ file currently in S3. "
        "The raw/ files themselves are NOT touched — they're the source of "
        "truth.\n"
        "\n"
        "BLAST RADIUS is large. Use after delete_document reports an "
        "INCONSISTENT error (manifest/chunks/raw out of sync), or any time "
        "the index is suspected of being in a bad state. For a non-trivial "
        "corpus this can take several minutes (re-embedding is the slow "
        "step).\n"
        "\n"
        "Requires confirm=true. The agent should ask the operator before "
        "passing confirm=true."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "confirm": {
                "type": "boolean",
                "description": (
                    "Must be true. Acknowledges that this wipes ALL chunks "
                    "and ALL LanceDB rows for the bucket. Defaults to false; "
                    "false returns an error rather than running the rebuild."
                ),
            },
        },
        "required": [],
    },
)


def _resolve_dataset_root(dataset_root: str | Path | None) -> str:
    if dataset_root is not None:
        return str(dataset_root)
    return os.environ.get(ENV_DATASET_ROOT, DEFAULT_DATASET_ROOT)


# Background cache warmer. When a document is ingested while the server is
# running, the first search that touches its rows pays a cold-cache cost
# (extra S3 reads for the new column data). A background thread polls the
# index for new document ids and runs one scoped search per new document so
# the first user-visible search after an ingest is already warm. It is a
# demo nicety, not a correctness feature; ONEBUCKET_DISABLE_CACHE_WARMER=1
# turns it off.
_warmer_seen_ids: set[str] = set()
_warmer_lock = _threading.Lock()
_warmer_started = _threading.Event()
WARMER_INTERVAL_S = 30.0


def _cache_warmer_loop(dataset_root: str) -> None:
    """Long-lived thread: poll LanceDB every WARMER_INTERVAL_S; for any
    raw_file_id we haven't seen yet, run a small scope-filtered search to
    pull its column data into the cached Table's working set.

    The first iteration runs one throwaway search immediately so the
    embedding models are loaded and the table and sparse-row caches are
    populated; without it the first real search pays about a second of
    model start-up plus the sparse-row load.
    """
    from .search import _get_table

    # Throwaway warmup BEFORE entering the polling loop. Best-effort; if
    # the table or embedder isn't ready yet, just skip and let the loop
    # retry on its normal schedule.
    try:
        search_global_index("warmup", limit=1, dataset_root=dataset_root)
    except Exception:  # noqa: BLE001
        pass

    while True:
        try:
            table = _get_table(dataset_root)
            arrow = (
                table.search()
                .select(["raw_file_id"])
                .limit(10**9)
                .to_arrow()
            )
            current_ids = {x for x in arrow.column("raw_file_id").to_pylist() if x}
            with _warmer_lock:
                new_ids = current_ids - _warmer_seen_ids
                _warmer_seen_ids.update(current_ids)
            for rid in sorted(new_ids):
                try:
                    # limit=1 + scope = the cheapest scan that still touches
                    # this doc's dense + sparse column data.
                    search_global_index(
                        "warm",
                        limit=1,
                        dataset_root=dataset_root,
                        scope=[rid],
                    )
                except Exception:  # noqa: BLE001 — best-effort
                    pass
        except Exception:  # noqa: BLE001 — best-effort; never crash the thread
            pass
        _time.sleep(WARMER_INTERVAL_S)


ENV_DISABLE_CACHE_WARMER = "ONEBUCKET_DISABLE_CACHE_WARMER"


def _start_cache_warmer_thread(dataset_root: str) -> None:
    """Spawn the cache-warmer thread once per MCP-server process.

    Idempotent across repeated ``build_server`` calls. Setting
    ``ONEBUCKET_DISABLE_CACHE_WARMER=1`` skips the thread entirely; the test
    suite sets it so unit tests never leave a background thread running.
    """
    if os.environ.get(ENV_DISABLE_CACHE_WARMER, "").strip() not in ("", "0", "false"):
        return
    if _warmer_started.is_set():
        return
    _warmer_started.set()
    t = _threading.Thread(
        target=_cache_warmer_loop,
        args=(dataset_root,),
        daemon=True,
        name="lancedb-cache-warmer",
    )
    t.start()


def _resolve_chunk_root(chunk_root: str | Path | None) -> str:
    if chunk_root is not None:
        return str(chunk_root)
    return os.environ.get(ENV_CHUNK_ROOT, DEFAULT_CHUNK_ROOT)


def _resolve_chunk_uri(uri: str, chunk_root: str) -> str:
    """Turn a chunk reference from a search hit into a URI ``storage.read_chunk``
    accepts.

    Full ``s3://`` and ``file://`` URIs (what the pipeline writes into the
    index) pass through. A relative ``chunks/<id>/chunk_NNNN.txt`` reference
    is joined onto the configured chunk root. Anything that could escape the
    chunk root is refused: parent-directory segments, NUL bytes, other URL
    schemes, and absolute filesystem paths.
    """
    if not isinstance(uri, str) or not uri or "\x00" in uri:
        raise ValueError("chunk_uri must be a non-empty string without NUL bytes")
    if ".." in uri.replace("\\", "/").split("/"):
        raise ValueError("chunk_uri must not contain parent-directory segments")
    if uri.startswith(("s3://", "file://")):
        return uri
    if "://" in uri or uri.startswith("/"):
        raise ValueError(
            "chunk_uri must be an s3:// or file:// URI from a search hit, or a "
            "chunks/<id>/chunk_NNNN.txt path relative to the chunk root"
        )
    root = chunk_root.rstrip("/")
    # A relative reference that already starts with "chunks/" would otherwise
    # land at chunks/chunks/<id>/... under a root that ends in /chunks.
    rel = uri
    if rel.startswith("chunks/") and root.endswith("/chunks"):
        rel = rel[len("chunks/"):]
    return f"{root}/{rel}"


def build_server(
    dataset_root: str | Path | None = None,
    chunk_root: str | Path | None = None,
) -> Server:
    """Build the MCP server and run the startup canary check.

    Raises CanaryMismatchError if the dataset's recorded canary does not
    match the runtime canary — server refuses to serve in that case.
    """
    root = _resolve_dataset_root(dataset_root)
    chunks = _resolve_chunk_root(chunk_root)
    assert_canary(root)
    _start_cache_warmer_thread(root)

    server: Server = Server(SERVER_NAME, version=SERVER_VERSION)

    @server.list_tools()
    async def _list_tools() -> list[Tool]:
        return [
            _TOOL_DEFINITION,
            _FETCH_TOOL_DEFINITION,
            _INGEST_TOOL_DEFINITION,
            _STATUS_TOOL_DEFINITION,
            _METRICS_TOOL_DEFINITION,
            _READ_METRICS_TOOL_DEFINITION,
            _LIST_TOOL_DEFINITION,
            _CONFIG_GET_TOOL_DEFINITION,
            _CONFIG_SET_TOOL_DEFINITION,
            _CONFIG_VALIDATE_TOOL_DEFINITION,
            _DELETE_DOC_TOOL_DEFINITION,
            _PURGE_CACHE_TOOL_DEFINITION,
            _REBUILD_INDEX_TOOL_DEFINITION,
        ]

    @server.call_tool()
    async def _call_tool(name: str, arguments: dict) -> list[TextContent]:
        if name == TOOL_NAME:
            scope = arguments.get("scope")
            result = search_global_index(
                arguments["query"],
                limit=arguments.get("limit", 3),
                dataset_root=root,
                scope=scope if scope else None,
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        if name == FETCH_TOOL_NAME:
            resolved = _resolve_chunk_uri(arguments["chunk_uri"], chunks)
            text = storage.read_chunk(resolved)
            return [TextContent(type="text", text=text)]
        if name == INGEST_TOOL_NAME:
            result = ingest_document(arguments["source"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        if name == STATUS_TOOL_NAME:
            result = check_ingest_status(arguments["raw_file_id"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        if name == METRICS_TOOL_NAME:
            result = get_replication_metrics(arguments["raw_file_id"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        if name == READ_METRICS_TOOL_NAME:
            result = get_chunk_read_metrics(
                start_time=arguments["start_time"],
                end_time=arguments.get("end_time"),
                raw_file_id=arguments.get("raw_file_id"),
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        if name == LIST_TOOL_NAME:
            result = list_documents()
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        if name == CONFIG_GET_TOOL_NAME:
            result = get_runbook_config()
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        if name == CONFIG_SET_TOOL_NAME:
            result = set_runbook_variable(
                arguments["path"], arguments["value"]
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        if name == CONFIG_VALIDATE_TOOL_NAME:
            result = validate_runbook_config()
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        if name == DELETE_DOC_TOOL_NAME:
            result = delete_document(arguments["raw_file_id"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        if name == PURGE_CACHE_TOOL_NAME:
            result = purge_peer_cache(
                arguments["onebucket"],
                arguments["peer_alias"],
                arguments["key_pattern"],
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        if name == REBUILD_INDEX_TOOL_NAME:
            result = rebuild_index(confirm=arguments.get("confirm", False))
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        raise ValueError(f"Unknown tool: {name}")

    return server


async def run(
    dataset_root: str | Path | None = None,
    chunk_root: str | Path | None = None,
) -> None:
    server = build_server(dataset_root, chunk_root)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name=SERVER_NAME,
                server_version=SERVER_VERSION,
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


# Module entry point: `python -m onebucket_demo.mcp_server` (and the equivalent
# Claude Desktop config that invokes the same command) needs an actual call to
# launch the asyncio loop. Without this block the module imports cleanly,
# defines `run`, and exits — Claude Desktop logs "transport closed unexpectedly"
# because the child process is gone before it can respond to `initialize`.
if __name__ == "__main__":
    asyncio.run(run())
