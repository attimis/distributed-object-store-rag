# OneBucket RAG Tiering Demo: Runbook

**What this demo shows, in one sentence:** you ask your documents a question
in plain language, you get a real sourced answer, and underneath, the
storage layer moved across to the second site *only* the bytes that answer
actually needed, leaving the rest of the corpus where it was.

**Format:** a 30-second cold open (Act 0) and four acts (Act 1 in two
halves, a short interlude 2.5, then Acts 3 and 4), about 20 minutes, **one**
check-in with the operator (after Act 2). Story first (a real question and a
real answer), then the numbers, then a visual to close. A new filing is set
ingesting in the cold open and processes in the background through Acts 1
and 2, so there is no idle polling; by Act 3 it has joined the answer on its
own.

**Audience:** mixed, some non-technical. Acts 1 and 2 are built for them:
watch RAG work, then watch the data move. Act 3's numbers are deliberately
quick. Act 4 is the takeaway image.

**Driving it:** an AI agent runs the tool calls; a human operator narrates.
The agent works act by act and pauses only at the Act 2 check-in, not at
every step. Engineering depth (metric SQL, evidence ledger, architecture
internals, TCO methodology, known issues) lives in
`onebucket_engineering_reference.md`; keep it out of the live demo. If a
technical audience member asks a hard question, answer from the reference;
do not detour the demo into it.

**Everything deployment-specific comes from `variables.yml`** (copied from
`variables.example.yml` and filled in; see Prerequisites). The fresh ticker,
the demo question, the seed corpus, the policy names and the pricing
assumptions are all read from it at the start of the session. This runbook
never hard-codes them; where it needs an example it says so.

**The one structural requirement.** Keep the metadata (the LanceDB index)
in its own OneBucket that replicates eagerly to every peer, separate from
the data OneBucket whose other peers fill lazily on read. Placement is per
OneBucket, so one OneBucket cannot replicate the index eagerly and the
chunks lazily at the same time. Everything else (names, providers, regions,
extra peers) is your choice.

**Tool names** are written `server:tool`. `onebucket-demo:search_global_index`
is this repo's MCP server; `data.onebucket.io:get` is the OneBucket data
connector (tools `list`, `head`, `get`, `put`, `locate` and others);
`events.onebucket.io:tail_events` is the OneBucket events connector
(`list_events`, `get_event`, `tail_events`). The demo itself only needs the
first two; the consumer uses the third.

---

## Prerequisites

Everything below is done once, off-stage, before the pre-demo checklist.
`demo_setup_runbook.md` walks through the setup script; this list is what
has to exist.

1. **A OneBucket account** at <https://app.onebucket.io>. The console has
   three tabs: **Credentials** (the access key your S3 clients use),
   **Endpoints** (one per backing storage provider) and **Policies** (your
   OneBuckets, each fanning out to peer buckets). Peer modes are `sync`,
   `async` and `cache`.
2. **Backing buckets on the providers you have.** Use any providers and
   any endpoint aliases; the example config registers an AWS S3 bucket and a
   Wasabi bucket aliased `EAST_S3` and `WEST_WASABI`. **A single provider
   works:** with one sync peer and no cache peers the whole pipeline and
   every tool still run; the cache-fill segments (Acts 2 and 2.5 and the Act 3 cost
   numbers) and the cross-region segment (Addendum A) then show nothing to
   measure, so they are marked as needing a second provider.
3. **Your OneBuckets, named however you like.** Set them up as described in
   [`onebucket-setup.md`, "What the pipeline needs from your policies"](../operations/onebucket-setup.md#what-the-pipeline-needs-from-your-policies).
   In short: your data OneBucket (example name `demo-data`) holds `raw/` and
   `chunks/`; your metadata OneBucket (example name `demo-meta`) holds the
   LanceDB index; the optional mirror (example name `demo-west`) is used only
   by Addendum A. The runbook reads the actual names from `variables.yml`
   (`pipeline.raw_writes_to_policy`, `pipeline.chunks_writes_to_policy`,
   `pipeline.metadata_writes_to_policy`).
4. **`docs/demo_runbook/variables.yml`**: copy `variables.example.yml` to
   that name (it is gitignored) and replace every `<placeholder>`. Loading
   fails closed while any placeholder remains, so nothing in this runbook
   runs against a half-filled config. Section 7 (`operator_intent`) is where
   you set the seed corpus, the fresh ticker and the question.
5. **Endpoints.** The global hostnames `https://s3.onebucket.io`,
   `https://events.onebucket.io` and `https://data.onebucket.io` route to
   the nearest live region and are the right default. To pin a region use
   `<service>.<region>.onebucket.io` (for example
   `https://s3.us-ashburn-1.onebucket.io`); the current region list is at
   <https://www.onebucket.io/support>. Keep `variables.yml` section 1 and
   the Claude Desktop config below identical.
6. **An AWS CLI profile** holding the OneBucket access key
   (`aws configure --profile <name>`), named in `variables.yml`
   `daemon.aws_profile`.
7. **One OneBucket sign-in for the event consumer:**
   `uv run python scripts/onebucket-login.py`. It opens the browser for
   OAuth and stores the token at `~/.config/onebucket/token.json`. Nothing
   is pasted into a terminal or a chat.
8. **Claude Desktop** with three MCP servers:
   - This repo's server. The key in `claude_desktop_config.json` is yours
     to choose; this runbook and the start prompt use `onebucket-demo`.
     Every environment variable must be in the `env` block (a shell
     `export` does not reach Claude Desktop's process):

     ```json
     {
       "mcpServers": {
         "onebucket-demo": {
           "command": "uv",
           "args": [
             "--directory", "/path/to/distributed-object-store-rag",
             "run", "python", "-m", "onebucket_demo.mcp_server"
           ],
           "env": {
             "AWS_PROFILE": "<aws-cli-profile-with-your-onebucket-access-key>",
             "AWS_ENDPOINT_URL": "https://s3.onebucket.io",
             "ONEBUCKET_DATASET_ROOT": "s3://demo-meta/metadata",
             "ONEBUCKET_CHUNK_ROOT": "s3://demo-data/chunks",
             "ONEBUCKET_EXECUTION_MODE": "onebucket-events",
             "ONEBUCKET_EVENTS_URL": "https://events.onebucket.io",
             "ONEBUCKET_TOKEN_FILE": "~/.config/onebucket/token.json",
             "ONEBUCKET_EVENT_LOG_PATH": ".runbook-state/events.jsonl"
           }
         }
       }
     }
     ```

     `AWS_PROFILE`, `AWS_ENDPOINT_URL`, `ONEBUCKET_DATASET_ROOT`,
     `ONEBUCKET_CHUNK_ROOT` and `ONEBUCKET_EXECUTION_MODE=onebucket-events`
     are required. The last three are optional and shown at their defaults
     (`ONEBUCKET_EVENT_LOG_PATH` is relative to the repository root).
   - The OneBucket **`data.onebucket.io`** connector (object tools: `list`,
     `head`, `get`, `put`, `locate` and others).
   - The OneBucket **`events.onebucket.io`** connector (`list_events`,
     `get_event`, `tail_events`); not called during the demo, useful for
     the checklist and for answering questions afterwards.

   Both connectors are added in Claude Desktop by their URL and sign in
   through the browser. Quit and relaunch Claude Desktop after editing the
   config; it reads MCP config at startup only.
9. **The setup script has run green:** `bash scripts/demo-setup.sh` (seed
   corpus ingested, consumer connected, verification passed). It prints the
   start prompt; the same prompt is in `preflight-prompt.md`.

---

## Scaling posture: do not anchor on the small numbers

Everything in this demo runs **single-threaded, from the operator's laptop,
against a real but small deployment**. The numbers you see (KB of chunks,
ms of search, seconds of ingest) are honest measurements of that exact
setup, but they are not the architecture's ceiling. They are a point
estimate of one CPU doing one query at a time.

**Two anchoring rules for the agent and the operator:**

1. **Express latency and volume as rates wherever possible.** "About 74 KB/s
   single-threaded ingest" tells a more useful story than "5 s for 400 KB".
   "About 0.35 queries/s on one CPU" is more honest than "2.8 s per search".
   Rates scale; absolutes mislead.
2. **Reserve absolute numbers for the cost projection in Act 3.** That is
   the one place where the audience needs concrete dollars per year.
   Everywhere else, frame the cost or latency as a rate that the audience
   can mentally multiply by their own concurrency and scale.

When in doubt, name the constraint that produced the number ("single laptop
process, no parallelism, no GPU, no batching") and the audience will scale
it themselves.

---

## RAG stack: what the demo covers, what it does not

A production RAG system has roughly these layers:

| Layer | Industry standard | This demo's choice |
|---|---|---|
| LLM generation | Latest frontier model | Same: the model running in Claude Desktop |
| Re-ranking | Cross-encoder rerank on top-k | Skipped, out of scope |
| Hybrid retrieval | Dense + sparse, RRF or weighted blend | Yes: BGE-small (dense) + SPLADE (sparse) |
| Vector index | HNSW / IVF, GPU-served at production rates | LanceDB, single laptop process |
| Embedding compute | GPU servers, large batches, embedding cache | fastembed on CPU, small batches |
| Chunking | Semantic + overlapping windows, hierarchical | Same approach; single process |
| **Object storage tiering** | **Often eager or metadata-only** | **OneBucket lazy tier: the layer this demo is about** |
| Source storage | S3 / Wasabi / GCS and so on | Two providers, two regions |

The demo is about the **storage tiering** layer in bold. Everything else is
well-trodden ground; the demo uses industry-standard choices so the audience
can focus on what is actually novel. If a technical audience member asks
about chunk-size tuning, reranker choice or embedding model selection,
briefly acknowledge the choice and redirect to the storage architecture.

---

## How the cache peer behaves

One description, used everywhere in this runbook.

A policy with a `sync` peer and a `cache` peer serves a read from the cache
peer when it already holds a copy, and from the sync peer otherwise. When
the sync peer serves the read, a copy lands on the cache peer, so the next
read of that object is local. **Cache peers hold copies of what has been
read or prefetched.** The cache peer's backing bucket therefore fills up as
the demo asks questions: a direct listing of it (see
`verify_onebucket_tiering.py`) shows exactly the chunks that have been read
and nothing else. Writes go to the sync peer only; nothing ever lands on a
cache peer at ingest time.

Two consequences for the demo:

- **"Warm" means "already on the cache peer".** A warm read is served
  locally; a cold read goes to the sync peer and pays the cross-site
  latency while the copy is placed.
- **`data.onebucket.io:locate` (and the per-object detail of `list`) reports
  which backing buckets hold an object.** That is the placement signal.
  It can lag the read that warmed the object by a while, so the agent
  trusts the read latency it measured on stage and does not poll placement
  mid-act (see Act 2).

---

## Visual rendering: inline only, deferred where possible

The operator is screen-recording inside a **fixed Claude Desktop chat
window**: no second monitor, no side panel, no separate browser tab. Three
rules apply to every visual the agent produces:

1. **Render inline in the chat only.** Never instruct the operator to open a
   separate window, browser tab or side panel mid-demo.
2. **Defer large visuals to the end.** The chunk-grid plus cost-bars
   composite belongs at **Act 4**, not in the middle. Acts 1 to 3
   communicate through Summary Cards and prose, not artifacts.
3. **Operator may screenshot.** The operator will grab inline visuals for a
   design team to overlay post-recording. Make them screenshot-friendly:
   clean layout, single viewport-sized artifact, no scrolling required.

**No mid-demo visuals at all.** The only artifact in this demo is the **Act 4
composite** (master metrics table, chunk grid, cost bars). Acts 0 to 3
communicate through Summary Cards and prose. If you think an Act needs a
visual mid-demo, it does not; describe it in words.

---

## Verbosity discipline: bold the load-bearing words

Demo audiences cannot read prose at LLM speed. Anything that matters must
be visually scannable. Four rules:

1. **Bold the metric and the punchline in every Summary Card**, not the
   supporting prose. Example:
   > Cold read: **470 ms**. Warm read: **90 ms**. **5x speedup, zero
   > infrastructure work.**
2. **Do not restate metrics across acts** unless explicitly comparing to the
   prior act's value. Each Summary Card shows only the metrics that act
   introduced or changed.
3. **Cap "what just happened underneath" at 2 sentences.** If you need 3,
   you are explaining the architecture instead of the moment. Save that for
   Act 4.
4. **Hard cap: at most 6 metric rows per Summary Card.** Earlier recordings
   shipped cards with 10 to 12 rows, unreadable on screen. If a card has
   more than 6 candidate metrics, drop the least load-bearing rows. Keep
   only the metrics that answer "what is the headline number from THIS
   act".

The audience reads the **bolded fragments**. The unbolded prose is backstop
for anyone who pauses the recording.

---

## Deferred metrics: collect throughout, emit once at Act 4

**The agent does not render metric tables inside each Act.** All
quantitative measurements (fetch wall-clocks, bytes crossed, cumulative
cache state and so on) are **collected as the demo runs** and emitted as a
**single master metrics table** at Act 4.

Why: earlier recordings paid a heavy verbosity tax for interleaved metrics,
and placement reporting lags the reads that warmed the objects, so probing
it mid-act either showed stale state or required the agent to loop with
retries. Both kill the recording. By Act 4 the deployment has had 15 or
more minutes to settle and the measurements are real. Defer once, emit
once, no retries.

### What the agent collects per Act (silently, into working memory)

For each Act, the agent **notes timestamps and numbers** but does NOT
display them.

**"Silently" means: do not emit any preamble in user-visible chat that
names or restates the numbers being collected.** No "noted: search latency
639 ms" lines. No "for the Act 4 table I am capturing ..." lines. The
audience must see ONLY:

- The tool calls firing (unavoidable; Claude Desktop renders them as compact
  "Used a tool" labels)
- The Summary Card (5 to 6 lines)
- The architectural callout (one bolded sentence)

Then move on. Hold the numbers in your conversation context; they stay in
your reasoning and reach the chat only in Act 4's master table.

Wall-clock recipe per `data.onebucket.io:get` call:

1. Note the system time just before issuing the tool call.
2. Note the system time as soon as the result returns.
3. Subtract; that is the wall-clock for that call.
4. Classify the read: **warm** (the chunk was already in the cumulative
   warm set at the start of this Act) or **cold** (first read of this
   chunk through the policy).
5. Accumulate into per-Act buckets keyed by classification.

Also record the **UTC timestamp at the start of each Act**. After an Act's
reads are done, `onebucket-demo:get_chunk_read_metrics(start_time=<act
start>, end_time=<now>)` returns the storage layer's own `duration_ms` for
those reads and `per_first_responder` (which peer served each one). That
is the honest per-read number for Act 4 and it confirms the warm/cold
classification. If it returns `events_count: 0`, the consumer did not see
those reads; use the wall-clocks and say so in the Act 4 table note.

What to track per Act:

- **Pre-demo checklist**: start timestamp, chunks pre-warmed (URIs)
- **Act 0**: start timestamp, ticker, `raw_file_id`, `size_bytes`
- **Act 1A**: start timestamp, chunks retrieved (URIs), per-call wall-clocks
  (all cold: first reads, the cache peer starts filling here)
- **Act 1B**: start timestamp, chunks retrieved (URIs), per-call
  wall-clocks, classified warm (already read in the checklist or Act 1A)
  or cold (new)
- **Act 2 Beat A**: warm-chunk wall-clock and cold-chunk wall-clock (one
  each, the on-stage measurement)
- **Act 2.5**: start timestamp, chunks retrieved, per-call wall-clocks,
  classified warm or cold
- **Act 3**: start timestamp, chunks retrieved, per-call wall-clocks (most
  of the fresh ticker's chunks will be cold). Do NOT collect ingest timings,
  per-peer acks or row counts.
- **Addendum** (if run): per-call wall-clocks through the addendum policy

**Search latency is intentionally not tracked.** Local embedding plus a
LanceDB scan on a laptop CPU is not part of the architecture story. If it
is mentioned at all (in inline narration), label it **"local embed
calculation"** so the audience knows that is edge cost, not the storage
layer.

### What Acts 0 to 3 emit instead: the Summary Card

A compact block per Act:

```markdown
### 🎬 Act N: <title>

**Goal:** <one line>

**Result:**
- <bullet 1>
- <bullet 2>
- <bullet 3>

**Architectural callout:** **<one bolded sentence, the moment's punchline>**
```

That is it. No metric tables. Goal, three bullets and the callout are the
whole card. Use real observations from this run; if something is
unavailable write `n/a` rather than inventing it.

### What Act 4 emits

A single composite, inline:

1. **Master metrics table**: columns are Acts, rows are measurements
2. **Chunk-warming grid**: per-document tile rows showing final cache state
3. **1 PB cost-projection bars**: architectures A, B and C
4. **One-sentence closing line**

See Act 4 for the full spec.

### Where each metric lives (do not guess)

| Metric type | Where to get it |
|---|---|
| Search latency (DB only) | `onebucket-demo:search_global_index` returns `_timing_ms.total_internal`: the LanceDB plus embedder cost inside the tool, not wall-clock. Not part of the architecture story; if quoted, label it "local embed calculation". If `_timing_ms` is missing, the MCP server is running stale code: quit Claude Desktop fully and relaunch. Do not invent the number. |
| Chunk-fetch latency, on stage | Wall-clock around each `data.onebucket.io:get` call. It includes some Claude Desktop and MCP overhead; say so if the absolute number is questioned. The cold-vs-warm delta is the architectural signal. |
| Chunk-fetch latency, storage-side | `onebucket-demo:get_chunk_read_metrics(start_time, end_time, raw_file_id)`: the gateway `duration_ms` of each chunk read in the window, plus `per_first_responder`. |
| LLM thinking time / orchestration overhead | The difference between Claude Desktop's wall-clock for a turn and the sum of the tool calls' internal latencies. Surface it separately if asked: "storage X ms, model plus orchestration Y s on top". |
| Raw file size | `onebucket-demo:ingest_document` returns `size_bytes`; for a document ingested earlier, `data.onebucket.io:head` on its `raw/` key. |
| Chunk totals (count, bytes) | `onebucket-demo:list_documents` returns `raw_file_id`, `source_uri` and `chunk_count` per document plus `total_documents` and `total_chunks`; bytes via `data.onebucket.io:list` on `chunks/<raw_file_id>/` and a sum. |
| Ingest end-to-end latency (upload to searchable) | `onebucket-demo:get_replication_metrics(raw_file_id)` returns `total_elapsed_seconds`. |
| Per-peer ack latencies (median, p95) | `get_replication_metrics` returns `per_peer_ack_latency[<alias>]`, computed from the gateway and backend events that share a `req_id`. |
| Strict-sync metadata quorum ack | `get_replication_metrics` returns `metadata_quorum_ack_ms`. |
| Chunk count for a document (from events) | `get_replication_metrics` returns `chunk_event_count`. |
| Cache state (what is warm on which peer) | `data.onebucket.io:list` on `chunks/<raw_file_id>/` (per-object placement), or `locate` on one key. |

If a metric is not available from any of these in the time you have, write
`n/a` rather than fabricating a number.

### Why search takes 2 to 3 s on this rig (for audience questions)

`_timing_ms.total_internal` from `search_global_index` typically breaks
down to:

| Component | Typical ms | What it is | Production amortisation |
|---|---|---|---|
| `embed_query` | 800 to 1100 | fastembed cold-load plus encode of the query into dense and sparse vectors, on CPU, single thread | GPU-backed embedding server, batched, under 50 ms per query |
| `dense_search` | 500 to 800 | LanceDB opens the table (manifest read from object storage), scans dense vectors flat, ranks top-k; re-opens the table every call | Cached connection and table, HNSW index, under 50 ms |
| `sparse_search` | 1000 to 1500 | Same access pattern, but SPLADE sparse vectors are larger and there is no sparse index | Sparse index plus cached column groups |

If the audience asks why search is slow, name the constraints honestly:
cold encoder on a CPU, no GPU, a flat index scanned from object storage on
every call, no in-memory cache. Production deployments run this layer in
single-digit milliseconds, but the storage tiering architecture beneath it
is identical. The demo is not selling search speed.

---

## Pre-demo checklist (about 2 min, operator plus agent)

This is the section the start prompt points at ("start at the pre-demo
checklist"). It has one name in this runbook; there is no separate
"phase" before it.

1. **Pipeline driver running.** `bash scripts/demo-setup.sh` started the
   event consumer (`daemon.mode: onebucket-events` in `variables.yml`), or
   the operator ran `bash scripts/start-event-consumer.sh` in a terminal.
   One process drives the pipeline and writes the event log. The operator
   confirms `connected: true` and `auth_state: "ok"` in
   `/tmp/oneb-pipeline-daemon.heartbeat`.
2. **Config check.** Agent calls `onebucket-demo:get_runbook_config` and
   confirms `missing_required` and `validation_errors` are both empty.
   Read and remember from the result: the policy names
   (`pipeline.raw_writes_to_policy`, `pipeline.chunks_writes_to_policy`,
   `pipeline.metadata_writes_to_policy`), the cache peer alias of the
   chunks policy, `operator_intent.fresh_ticker_for_live_ingest`,
   `operator_intent.multi_hop_demo_question`,
   `operator_intent.document_id_pattern` and the pricing and workload
   blocks. Every later step uses these values; this runbook calls the
   chunks policy "the policy" and its cache peer "the cache peer".
3. **Index check.** Agent calls `onebucket-demo:list_documents` and confirms:
   - The seed corpus is present (every ticker in
     `operator_intent.seed_corpus_tickers` has a document whose
     `source_uri` matches `document_id_pattern`, by default
     `<ticker>-10k-`). The question in `variables.yml` is written to span
     more than one of them.
   - The fresh ticker is **not** yet indexed. Act 0 ingests it live and
     the Act 3 re-ask depends on it being absent at the start. If it is
     already indexed from a previous run, the operator removes it
     off-stage (`onebucket-demo:delete_document`) or changes the ticker in
     `variables.yml` before the demo begins.
4. **Permission pre-warm: call every tool now, once.** To prevent permission
   dialogs from interrupting the live demo, the agent calls each tool it
   will need later with safe read-only arguments. The operator clicks
   **"Always allow"** on each prompt as it appears; from then on there are
   no interruptions through Act 4. Run as one parallel batch where
   possible:

   - `onebucket-demo:search_global_index` with `query="warmup probe", limit=1`
   - `onebucket-demo:fetch_chunk` with one `chunk_uri` from the search above
   - `onebucket-demo:check_ingest_status` with any existing `raw_file_id`
   - `onebucket-demo:get_replication_metrics` with the same `raw_file_id`
     (read-only event-log scan)
   - `onebucket-demo:get_chunk_read_metrics` with a `start_time` five
     minutes ago (UTC ISO format)
   - `data.onebucket.io:list` on the policy, prefix `chunks/<any raw_file_id>/`
   - `data.onebucket.io:locate` on one chunk key
   - `data.onebucket.io:get` on one chunk key through the policy (this read
     also warms that chunk; that is fine)
   - **Visualisation widget**: one trivial inline visualisation (a tiny KPI
     card or a single bar) so the widget tool's permission prompt fires
     now. Act 4 uses this mechanism heavily.

   **Do NOT pre-call `ingest_document`**: that is the on-stage Act 0 moment
   and the operator grants it then. Do not call the destructive tools
   (`delete_document`, `rebuild_index`, `purge_peer_cache`) either; they
   are not used in the demo proper. If a prompt for one appears, refuse it.

5. **Pre-warm the cache peer for Act 2's warm comparison.** The weakest
   moment of earlier recordings was Act 2 Beat A: the agent ran retries
   trying to catch a cache fill that had not completed yet. The cure is to
   warm a few chunks **here, off-stage, before Act 0 starts**, so by the
   time Act 2 fires its warm read several minutes later the bytes are on
   the cache peer.

   Agent: run `search_global_index` with the configured question and
   `limit=5`, take the document that dominates the results, and read
   **5 of its chunks** through the policy with `data.onebucket.io:get`
   (the top hits are the natural choice). Record the chunk URIs as the
   **known-warm set** that Act 2 Beat A reuses.

   This is fire-and-forget: the agent does not verify the fill completed
   before proceeding. It just gives the bytes a head start.

That is the whole checklist. No phase-by-phase gating before Act 0.

### The readiness banner: the one deliberate stop before the demo

After all the checks and pre-warms pass, the agent delivers a short
audience-facing intro and then **stops, waiting for the operator** to say
"proceed" / "begin" / "go". This is the natural pause where the operator
turns to the audience, sets context and launches the live portion.

**Agent output at this stop.** The operator is recording the screen, so the
intro needs to **dominate the viewport**: push the checklist chatter
off-screen and present a clean, screenshot-worthy running order. Emit the
following verbatim (including the banner padding; it is load-bearing for
the recording):

```
═══════════════════════════════════════════════════════════════════════

                  ✅  PREFLIGHT COMPLETE  ✅

           All systems checked. Permissions granted.
           Index inspected. Consumer connected.
           Cache state confirmed.

           Ready to begin the demo.

═══════════════════════════════════════════════════════════════════════



                  📋  WHAT YOU'RE ABOUT TO SEE  📋

           A real RAG system answering a real question
           over real SEC filings. Underneath, a storage
           architecture that moves only the bytes the
           question actually needed.

           Total runtime: about 20 minutes.
           One operator check-in (after Act 2).
           Everything else runs end-to-end.

═══════════════════════════════════════════════════════════════════════



                  🎬  THE RUNNING ORDER  🎬


   Act 0   Drop a new SEC filing into the pipeline, LIVE.
           Chunking and embedding run in the background
           while we work. We come back to it in Act 3.

   Act 1A  Single-shot RAG. A real question, a real cited
           answer, the way most RAG systems ship today.

   Act 1B  Same question, MULTI-HOP reasoning. The system
           decides where to look, not us. The cache warms
           on every read.

   Act 2   What moved underneath: exactly which bytes
           crossed to the second site, and the cold-vs-warm
           latency delta, measured on stage.
           ⏸  Operator check-in here.

   Act 2.5 Hit-rate amplification. A different but related
           question reuses the chunks we just warmed.

   Act 3   Re-ask the opening question. The Act 0 filing is
           NOW part of the answer: the index replicated
           strict-sync while the chunks tiered lazily.
           Then: the cost numbers, projected to 1 PB.

   Act 4   The closing visual: chunk grid plus cost bars in
           one inline artifact. The whole architecture on
           one page.


═══════════════════════════════════════════════════════════════════════



              Type  "proceed"  when you're ready
              and I'll start Act 0.



═══════════════════════════════════════════════════════════════════════
```

The triple-banner format is intentional: markdown collapses consecutive
blank lines, but these character-art separators force visible vertical
space the renderer cannot compress, so the running order fills the screen
and pushes the checklist output out of frame.

**Operator:** when ready (deck up, audience attention focused, mic on),
type `proceed`. The agent then runs straight through Act 0 to Act 4 with
only the Act 2 mandatory check-in in between.

---

## Act 0: put a document into the pipeline (about 30 s)

The first on-stage action, and a quiet one. It starts a clock that runs
silently under Acts 1 and 2 so nobody waits on it later.

**Agent:** `onebucket-demo:ingest_document source=<fresh ticker from
variables.yml>`. It returns `raw_file_id` and `size_bytes` immediately; note
them, then move on. Do not poll, do not wait.

**Operator, one line** (name the company the ticker belongs to):

> "Before we ask anything, I'm adding a new filing to the corpus. It will
> upload, chunk and embed in the background while we work. We'll come back
> to it."

Then go straight into Act 1. The ingest finishes on its own during the next
ten minutes.

**Render the Act 0 Summary Card:**

```markdown
### 🎬 Act 0: fresh filing into the pipeline, live

**Goal:** Drop a new SEC filing into the corpus on stage so its chunks flow
through the background pipeline during Acts 1 and 2.

**Result:**
- Ticker uploaded; raw_file_id captured.
- The event consumer picked up the PutObject event from the events stream.
- Chunk, embed and index running in the background; no polling, no waiting.

**Architectural callout:** **The ingest pipeline runs in parallel with the
demo. We don't wait, we work.**
```

**Silently collect** (for the Act 4 master table): timestamp, ticker,
`raw_file_id`, `size_bytes`, PUT wall-clock.

**In production this looks like:** an ingestion fleet, GPU-backed encoders,
large batches, ingest throughput in the GB/s range. Use this line in
narration if the audience asks; do not surface it in the card.

---

## Act 1A: single-shot RAG (about 2 min)

**The moment:** the operator asks the question that Act 1B will answer
again, and the system answers it the way most RAG ships today: one search,
read the top hits, synthesise. No multi-hop. This sets up the contrast Act
1B reveals.

**Operator:** read the question from `variables.yml`
(`operator_intent.multi_hop_demo_question`) out loud.

**Agent, single-shot RAG:**

1. `onebucket-demo:search_global_index` with the question, `limit=6`, no
   `scope`. Treat this as a one-and-done lookup.
2. For each top passage, read the chunk **through the policy** with
   `data.onebucket.io:get bucket=<policy> key=chunks/<id>/chunk_NNNN.txt`
   and `onebucket-demo:fetch_chunk` for the body. These are the corpus's
   first reads of those chunks; the cache peer starts filling here, but do
   not mention that yet.
3. Synthesise a short answer (2 to 3 sentences per company is enough at
   this stage; Act 1B gives the full version).

**The line:** "This is the simple version. One search, read what it found,
answer. Just RAG."

**Render the Act 1A Summary Card:**

```markdown
### 🎬 Act 1A: single-shot RAG

**Goal:** Answer the question with one search and the top hits, the way
most RAG ships today.

**Result:**
- <N> chunks retrieved from <the documents that dominated>.
- Answer synthesised; <which company's coverage was strong, which was
  shallow and why, based on what the chunks actually contained>.
- No reasoning about where to look. One shot.

**Architectural callout:** **One search gets you an answer. It can miss the
strongest evidence.**
```

**Silently collect** (for the Act 4 master table): chunk URIs retrieved,
per-call wall-clocks (all cold).

---

## Act 1B: multi-hop with cache warming (about 6 min)

**The moment:** same question, evolved flow: multi-hop reasoning, and every
chunk read warms the cache peer. The audience now sees what changes when
the system is allowed to decide where to look.

**Operator:** the question is the **same one as Act 1A**. Do not re-ask it
out loud; the audience just heard it. Frame the evolution instead:

> "Same question. But this time we let the system do the work the user
> *would* have had to do manually. Watch: instead of one search, the agent
> makes a discovery search across the whole corpus, looks at which
> documents dominated, then makes a focused search scoped to just those
> documents. That's **multi-hop retrieval**: the system figures out *where*
> to look, not just *what* to look for. And every chunk it reads gets a
> copy at the second site for next time."

The question is company-agnostic on purpose: it spans more than one filing,
which makes Act 2's picture richer and sets up the Act 3 re-ask.

**Agent, run the RAG flow and narrate the multi-hop pattern as you go.**
The whole point is that the *user* asked one plain-language question; the
*system* made more than one search call to answer it.

1. **Discovery hop, broad search.** `onebucket-demo:search_global_index`
   with the full question, `limit=10`, no `scope`. Say:

   > "First hop: we don't know which filings have the answer yet, so we
   > cast a wide net across the whole corpus."

   Show the top results briefly (which tickers dominated). Do not read
   chunks yet.

2. **Focused hop, scoped to the top filers.** A second `search_global_index`
   with the same question and `scope=[<raw_file_id>, ...]` set to the 2 or
   3 documents from hop 1, `limit=10`. Say:

   > "Second hop: now that we know which documents are relevant, we drill
   > in. Same question, scoped to just those filers. This is the system
   > deciding *where* to look, not the user re-typing."

3. **Retrieve the top passages, each through the policy.** For each of the
   top 6 or so passages (about 3 per top company), call
   `data.onebucket.io:get bucket=<policy> key=chunks/<id>/chunk_NNNN.txt`
   and `onebucket-demo:fetch_chunk`. The `get` is the read that warms the
   cache peer; `fetch_chunk` returns the body for synthesis. Do not narrate
   this split out loud; it is a chat-tool artifact.

4. **Synthesise a genuine, cited answer**: 3 to 5 sentences per company,
   each grounded in specific language from the chunks, citing the filing.
   This is the deliverable of Act 1.

**What the audience takes away:** "I asked my documents a plain-language
question and got a real, sourced answer, and the system did multi-hop
reasoning to find it, not me." No infrastructure talk yet; Act 1 is pure
RAG.

**Before rendering the Summary Card: do the multi-hop value analysis.**

This is the demo's strongest punchline and the agent **must** surface it
explicitly, not bury it in a bullet. Compare the chunks Act 1B retrieved
against the chunks Act 1A retrieved. Identify chunks Act 1B surfaced that
Act 1A missed (different chunk indices for the same documents) and ask:
*what is in those chunks that the single shot did not find?*

Render the comparison **prominently above the Summary Card**:

```markdown
> 🎯 **Why multi-hop changed the answer**
>
> Act 1A (single shot) retrieved <list chunk indices>; it landed on
> <characterise what those chunks actually contained>.
>
> Act 1B (multi-hop, focused) retrieved <list chunk indices>: different
> chunks for at least <N> of the documents, surfacing <what Act 1B found
> that the single shot did not>.
>
> Same question. Different retrieval strategy. **The single-shot answer
> would have shipped without <the most relevant passage it missed>.**
> That's what multi-hop reasoning buys you: the system probes the corpus
> before committing to a narrow retrieval scope.
```

Be specific. Do not say "multi-hop found more chunks"; say *what was in the
chunks it found that the single shot missed*. The audience needs to see a
quality improvement, not a quantity improvement. If the two retrievals
happen to coincide, say so plainly and move on; do not manufacture a
difference.

**Render the Act 1B Summary Card** (keep the callout above it):

```markdown
### 🎬 Act 1B: multi-hop reasoning, cache warming

**Goal:** Same question, evolved retrieval: let the system decide *where*
to look, not just *what* to look for.

**Result:**
- Hop 1 discovery surfaced <documents>; the rest of the corpus filtered out.
- Hop 2 focused on <documents>; retrieved <N> chunks, including <what the
  single shot missed>.
- Every chunk read through <policy>; each first read placed a copy on the
  cache peer.

**Architectural callout:** **Multi-hop isn't about more chunks, it's about
better ones.**
```

**Silently collect** (for the Act 4 master table): chunk URIs retrieved,
per-call wall-clocks classified warm (already read in the checklist or Act
1A) or cold, scope `raw_file_id`s.

**In production this looks like:** the multi-hop pattern stays, but each
hop's hybrid retrieval runs on co-located GPUs answering thousands of
queries per second, often with a cross-encoder reranker on top. Cache
warming happens asynchronously in the data path, not blocking the user. The
same lazy-tier shape (*only what was read crosses*) applies whether you
answer one query per second from a laptop or ten thousand from a fleet.

---

## Act 2: what moved underneath (about 5 min)

*Needs a second provider (a cache peer on your data OneBucket); on a single-provider setup there is no cache fill to show.*

**The reveal:** "While that answer came together, the storage layer moved
data between sites. Here is exactly what, and what it didn't. And here is
the latency difference, measured on stage."

**Agent, two beats, in order:**

**Beat A: show the latency delta (cold vs warm), live.**

The chunks the checklist pre-warmed several minutes ago are warm by now.
Read one warm and one cold and report the **wall-clock around each `get`
call**; that is the measurement.

1. **Warm chunk:** one URI from the checklist's known-warm set.
   `data.onebucket.io:get bucket=<policy> key=<that chunk>`; record the
   wall-clock.
2. **Cold chunk:** any chunk of a document nobody has queried in this
   session (one of the seed filings the question did not touch). Same
   `get` through the same policy; record the wall-clock.
3. **Trust the wall-clock.** Do NOT probe `locate` here; placement reporting
   lags and a mid-act poll is what broke earlier recordings. The read
   latency is the measurement.
4. **Report what you measured.** Present the contrast plainly, bold the
   numbers:

   > "**Warm: <warm_ms> ms.** Cold: **<cold_ms> ms**. Same policy, same
   > client, same network. **<ratio>x speedup**, and the only difference
   > is whether the cache peer had been asked for that byte range before."

   Earlier runs on this setup saw roughly 90 ms warm and 470 ms cold, about
   5x. **If today's numbers differ from that range, say so**: for example,
   "that's less of a gap than this setup usually shows; the pre-warmed
   chunk may not have finished propagating". Never substitute an expected
   number for a measured one, and never narrate the runbook's retry rules
   or fallbacks to the audience.

**Beat B: count what crossed, in aggregate. No visual.**

1. `data.onebucket.io:list bucket=<policy> prefix=chunks/<id>/` for one of
   the documents Act 1 queried. From the per-object placement, count the
   chunks that are on the cache peer versus the document's total
   (`chunk_count` from `list_documents`). Repeat for the other queried
   document if time allows.
2. **Narrate the counts in plain text. Do NOT render a visual here.** The
   chunk grid is deferred to Act 4. Say the numbers:

   > "<k> chunks now warm on the second site. **<total minus k> chunks of
   > <document> never moved.** The filing we added in Act 0, still
   > embedding in the background, has crossed zero bytes. You'll see the
   > full grid at the end."

   That is the whole beat. No tool calls except the `list`. No artifact.
   If the listing has not caught up with Act 1's reads yet, quote the
   count it shows and say the listing lags; do not loop.

**The line:** "Only the passages the question actually needed crossed to
the second site. The rest of the document, most of it, never moved. The
cache fills itself, shaped by what people actually ask. And the cost of
asking again is a several-times speedup, with zero infrastructure work on
our part."

**Render the Act 2 Summary Card:**

```markdown
### 🎬 Act 2: what moved underneath

**Goal:** Show, at the byte level, exactly which chunks crossed to the
second site and which didn't, plus the latency cost of each path.

**Result:**
- <k> chunks (the ones Act 1 actually read) now on the cache peer.
- <percentage> of the corpus never moved.
- Measured on stage: **<cold_ms> ms cold / <warm_ms> ms warm, <ratio>x**.

**Architectural callout:** **Data moves on read, not on write, and "what
was read" is a tiny fraction of "what's available".**
```

**Silently collect** (for the Act 4 master table): warm-chunk wall-clock,
cold-chunk wall-clock, speedup ratio, cumulative warm count, percentage of
the demo corpus moved, bytes that crossed.

**In production this looks like:** the warm/cold gap stays; it is a property
of the network topology, not the load. At scale you also get **hit-rate
amplification**: across millions of queries the same hot chunks get cached
once and served warm forever after; cold reads only happen on truly new
passages. The cross-site egress bill is bounded by your *query
distribution*, not your *corpus size*.

**CHECK-IN with the operator**

> "That's the core idea: lazy tiering, data moves on read, not on write.
> Ready for the numbers, or any questions first?"

This is the demo's only mandatory stop.

---

## Act 2.5: hit-rate amplification (about 2 min)

*Needs a second provider (a cache peer on your data OneBucket); on a single-provider setup there is no cache fill to show.*

**After the check-in, before the cost numbers, prove the cache earns its
keep on the next question.** Act 2 showed the cache filling on one
question. Act 2.5 shows what happens when a *different but related*
question reuses some of the same chunks: warm hits, not cold reads.

**Agent:**

1. Derive a **related but narrower question** from the configured one: same
   topic area, a more specific angle, likely to land on some of the same
   passages (for a risk-factor question, narrow to one category of risk the
   Act 1 chunks mentioned). Say it out loud.
2. Run the multi-hop pattern through the policy, as in Act 1B. Record the
   per-chunk read wall-clock and classify each chunk as warm (already in
   the cumulative warm set) or cold (new).
3. Sort the retrieved chunks into the two buckets.
4. Present the contrast:
   > "Same policy, same network, same client. The cache earned its keep:
   > those chunks served at warm latency on a question that wasn't even
   > asked yet when we did the first warming."

**Render the Act 2.5 Summary Card:**

```markdown
### 🎬 Act 2.5: hit-rate amplification

**Goal:** Show the cache earning its keep on a *different* question that
happens to share chunks with Act 1's question.

**Result:**
- A related but narrower question retrieved <N> chunks.
- **<k> of <N> were warm**, already on the cache peer from Act 1's reads.
- The other <N minus k> were new cold fills<, including chunks from the
  freshly ingested filing if it is already searchable>.

**Architectural callout:** **The cache hit rate compounds with question
volume. Yesterday's read serves today's different question.**
```

**Silently collect** (for the Act 4 master table): warm-hit count, cold-miss
count, chunk URIs retrieved, cumulative warm count after Act 2.5.

For narration, not for the card: the second question reused <k> of the <N>
chunks the first question warmed; no cross-site fetch for those bytes, just
a local read off the cache peer. The other <N minus k> chunks were new and
cold-filled the cache on their first read. The more the audience asks, the
more questions land entirely on already-warm bytes.

**In production this looks like:** at production query volumes the
hot-chunk hit rate typically reaches **70 to 90 % within the first hour of
traffic** for a well-bounded corpus. Cross-site egress is bounded by the
*new* questions that surface *new* passages, not by the total query count.
That is why the lazy-tier cost curve flattens: once the popular passages
are warm, they stay warm.

---

## Act 3: the corpus caught up, and the numbers (about 6 min)

Three beats. The first is the payoff of Act 0; the second is the numbers,
kept deliberately fast; the third is one line.

**Beat 1: re-ask the opening question (about 3 min).**

The agent re-runs the *exact same question* from Act 1: an optional single
silent `onebucket-demo:check_ingest_status` on the Act 0 `raw_file_id` to
confirm `phase: "ready"` (by this point it almost always is; do not make it
an on-stage poll), then `search_global_index`, then read and `fetch_chunk`
the top passages exactly as in Act 1B, and synthesise the answer.

This time the fresh ticker is in the results. It was not indexed when Act 1
ran; it uploaded, chunked, embedded, and its index entries replicated to
both sites while the audience watched Acts 1 and 2. The new answer cites
it alongside the filings from Act 1.

**Patience policy: the single-laptop embedder may still be in flight.** A
200-chunk 10-K embedding on one CPU can take **8 to 15 minutes**,
especially if other documents are ingesting concurrently. The
`check_ingest_status` phases are `uploading_or_chunker_pending`,
`chunking`, `embedding`, `ready` and `partial`. Do not treat the fresh
ticker as stalled just because `last_activity_seconds_ago` crossed 600;
**the genuine timeout for Act 3 is `last_activity_seconds_ago > 900` AND
`rows_committed == 0`**. Below that, it is still progressing.

**If the fresh ticker is genuinely not ready by Act 3:**

1. Do not fake it. Say plainly: "The **local embed calculation** is still
   finishing; it runs on the operator's laptop CPU for demo purposes. The
   storage architecture this demo is about is already done with its part;
   it's the embedding pipeline that takes the time, and that runs on GPUs
   at production rates. The architecture story holds either way."
2. Deliver Beat 2 (the cost tables) as planned; it does not depend on the
   fresh ticker.
3. Skip the "re-asked question now cites the fresh ticker" narrative and
   pivot directly to the architecture punchline.

The audience watching the cost reveal cares more about the cost story than
the live-ingest punchline. Do not let an embedder delay derail Act 3.

**The line:** "Same question, word for word. We didn't reload anything or
re-point the system at a new file. A filing that wasn't in the system
fifteen minutes ago is already part of the answer, because the search
index, the part that has to be consistent everywhere, replicates to both
sites on every write in tens of milliseconds. The index updates eagerly;
only the bulky document payload tiers lazily. One system, two replication
behaviours, each matched to what the data needs."

**Beat 2: what crossed, and what it costs (about 3 min).**

This is the reveal. Two tables. The first uses **today's measured numbers**
on one filing; the second extrapolates them to a 1 PB corpus. Show both;
do not paraphrase them.

**Anchor numbers: fill these in from today's run before showing Table A.**

- **Raw file (R)**: bytes of the queried 10-K's raw object (`size_bytes`
  from Act 0 for the fresh filing, or `data.onebucket.io:head` on the raw
  key of the Act 1 document)
- **Chunks total (T_total)**: sum of the chunk sizes from the Act 2 `list`;
  T_total / R is the chunking overhead
- **Metadata per ingest (M)**: bytes of the LanceDB fragments and
  manifests written during an ingest (`bytes_in` of the metadata PUT events
  for the Act 0 ingest). If the event log cannot supply it, say so and use
  the example value from one earlier run, about 611 KB, labelled as such.
- **Metadata ack latencies**: not surfaced in the demo. Strict-sync
  metadata replication is sub-second; the architecture point ("index
  eager, chunks lazy") does not need a specific number.
- **Cold read latency**: measured live in Act 2 Beat A
- **Warm read latency**: measured live in Act 2 Beat A
- **Read fraction (f)**: chunks served from the cache peer during the demo
  divided by the total chunks of that filing (about 10 % per multi-hop
  question and about 3 % per single-hop in earlier runs; use today's)

### The three architectures, named honestly

Before showing the tables, the agent **must explain what each column
represents** in industry terms; Act 3's punchline is incoherent without
this. Keep it short:

**A. Eager replication (the safe and expensive default).** Copy every chunk
to every region at ingest time: S3 cross-region replication, a globally
replicated table for the index, or both. Reads are always local. **You pay
full storage twice and full egress on every byte at ingest, whether anyone
reads it or not.**

**B. VectorDB-stores-chunks (the "modern RAG" default).** Use a vector
database to hold both the embeddings AND the chunk text inline. The index
is replicated (managed by the vector DB), so reads are local. **But chunks
now live in attached storage on the DB nodes, which is 5 to 10 times the
price of object storage.** Plus you pay vector-DB compute hours whether
queries flow or not. This is what most production RAG looks like today.

**C. Lazy-tier (OneBucket, this demo).** The index strict-syncs across
sites (the bit that has to be consistent everywhere). Chunks live on cheap
object storage and tier lazily on read. **You pay storage once on the
primary, plus the small fraction that gets cached at the second site after
queries warm it.**

**Table A: per document (one 10-K from today's run):**

| Metric | A. Eager replication | B. VectorDB-stores-chunks | C. Lazy-tier (OneBucket) |
|---|---|---|---|
| Bytes crossing on ingest | T_total + M | T_total + M (into the vector DB) | M only |
| Replication latency added to ingest | per-chunk sync x chunks (about 150 ms x N) | full document indexed into the DB at ingest (seconds per document) | none on chunks; one ack on metadata |
| Cold chunk fetch from 2nd site | warm latency (always local) | warm latency (always in the DB) | cold latency (first read of each chunk) |
| Warm chunk fetch from 2nd site | warm latency | warm latency | warm latency |
| Bytes crossing per query | 0 | 0 | 0 (after warm) |
| Storage tier for chunks | object storage x 2 regions | **attached DB storage (5 to 10 x object storage price)** | object storage x 1 region + lazy cache |
| Pay for idle vector-DB compute? | no | **yes (DB nodes 24/7)** | no |

**The line for Table A:** "Eager replication and the VectorDB approach both
give you local-latency reads everywhere. The cost is the bill: **double
object storage in column A, attached DB storage at 5 to 10 times the price
plus DB compute in column B**. Lazy-tier stores once, plus a small warm
cache that grows on demand."

**Table B: extrapolated to a 1 PB corpus, annualised.**

State the assumptions out loud before showing the table; they are how the
audience challenges the numbers. The pricing and workload values come from
`variables.yml` (`operator_intent.pricing`, `operator_intent.workload`);
the figures below use the example file's defaults.

- Raw corpus: **1 PB**
- Chunks with about 15 % overhead: **1.15 PB**
- Metadata: **about 10 TB** (scales with vector count)
- Read fraction over year one: **30 %** of chunks, the mid-range of a
  researched **20 to 40 %** band for domain-expert and enterprise-knowledge
  RAG. The agent **must cite the research live** when presenting this
  number:
  - **Primary source**: Jin et al., RAGCache (Peking University,
    arXiv:2404.12457): the top 3 % of documents serve 60 % of retrieval
    requests on MMLU, a 20x concentration versus uniform.
  - **Supporting**: Ruparel et al., 2025, multi-tenant RAG simulation:
    74.26 % LFU hit rate at Zipfian s=1, so about 25 to 30 % of the corpus
    is the active set. A publicly described financial-services deployment
    shows the same shape in production.
  - **Adjacent**: Cloudflare and Akamai CDN data show the same power law on
    web-cache hit rates.
  - Full synthesis: [`research/findings_summary.md`](research/findings_summary.md)
    and the underlying [`research/read_fraction_grounding.md`](research/read_fraction_grounding.md).

  **Show the range, not the point estimate** in narration:
  > "Industry data puts this at **20 to 40 % of corpus chunks in year one**
  > for enterprise RAG. We used **30 %** for the math. Even at the 40 %
  > upper bound, lazy-tier is still about 10x cheaper than eager."

- Per-document information density (compounds with corpus skew; use it in
  Q&A when challenged): within a single SEC 10-K, only **10 to 15 % of
  chunks** ever contribute to a retrieved answer (TextRank/LexRank sentence
  centrality; FinanceBench measured at scale on 361 10-Ks). Most of the
  corpus *and* most of each document is dead weight from a retrieval
  perspective.
- Egress pricing: **$0.09/GB** (`aws_egress_usd_per_gb`, same across architectures)
- Object storage on the 2nd site (eager / lazy): **$5.99/TB-month** (`wasabi_storage_usd_per_tb_month`)
- VectorDB attached storage: **about $100/TB-month** (managed vector-DB
  effective rate, about 17x object storage)
- VectorDB compute (idle baseline): **about $2 K/month per 100 TB** of
  index served, about $240 K/yr at 1 PB scale
- Query volume: **10 M queries/year** (`y1_query_count`)

| Cost line | A. Eager replication | B. VectorDB-stores-chunks | C. Lazy-tier (OneBucket) |
|---|---|---|---|
| Chunk bytes stored on 2nd site | 1.15 PB object storage | 1.15 PB attached DB storage | about 345 TB (30 %) object storage |
| **Storage cost on 2nd site / yr** | **about $1.0 M** | **about $1.4 M** (1.15 PB x about $100/TB-month) | **about $25 K** |
| **VectorDB compute / yr** | $0 | **about $240 K** (idle baseline at 1 PB scale) | $0 |
| Chunk bytes crossing on ingest | 1.15 PB | 1.15 PB (into the DB) | 0 |
| Metadata bytes crossing on ingest | 10 TB | 10 TB | 10 TB |
| Ingest egress (one-time seed) | about $104 K | about $104 K | about $900 |
| Read egress / yr (10 M x about 7.5 KB) | $0 (always local) | $0 (always in the DB) | $0 (warming amortises) |
| Read egress, 30 % of chunks propagating | $0 | $0 | **about $31 K** (one-time over year one) |
| **Year-one total on 2nd site** | **about $1.10 M** | **about $1.74 M** | **about $57 K** |
| **Year-two+ steady state / yr** | **about $1.0 M / yr** | **about $1.64 M / yr** | **about $25 K / yr** |

**The closing line for Beat 2:**

> "**Three architectures, three bills.** Eager replication: about a million
> dollars a year for a redundant copy on object storage. VectorDB stores
> everything, the way most RAG ships today: about **$1.6 million a year**,
> because the chunks live on attached DB storage and you pay for idle
> compute. Lazy-tier, what OneBucket does, runs **about $25 K a year steady
> state**, because most of a corpus is never queried, and the parts that
> *are* queried serve at warm latency after the first hit."

**Then ground the cost-story assumption** (the audience's inevitable
challenge; pre-empt it with three named sources):

> "These numbers lean on a research-backed assumption: real RAG corpora are
> **heavily skewed**. Three sources, all measured at scale.
>
> **One: Peking University's RAGCache research** (Jin et al., 2024) mapped
> exact document access patterns on standard benchmarks like MMLU. They
> found that **the top 3 % of documents served 60 % of all retrieval
> requests**, a concentration **twenty times higher than uniform**.
>
> **Two: the ARC framework** (Lin et al., 2025) demonstrated a **79.8 %
> cache hit rate using less than 0.015 % of the storage footprint**,
> because human users ask the same semantic question in dozens of
> syntactic variations and a well-tuned cache traps all of them.
>
> **Three: Cloudflare and Akamai built their CDN businesses on this exact
> power law.** It's the same Pareto distribution. We're just moving it from
> web traffic to retrieval traffic.
>
> The **30 % year-one read fraction** we used is the industry mid-range for
> enterprise RAG; published numbers span 20 to 40 %. **At the high end,
> 40 %, lazy-tier is still about 10x cheaper than eager replication.** The
> architecture isn't betting on something we hope is true; it's betting on
> something the retrieval literature has been measuring for a decade."

(If the audience asks where lazy-tier *breaks down*, the agent should be
ready with the counter-evidence: legal contract review, forensic discovery
and regulatory compliance crawls; see the Act 4 counter-evidence section.
Honesty about limits builds trust.)

**Beat 3: the pipeline you didn't wait for.** One line, tying back to Act 0:

> "And the fresh filing went from upload to fully searchable **in real
> time, while we were doing other things.** Nobody waited."

(Do NOT quote a specific number of minutes, seconds or rows. The embedding
and chunking pipeline runs on the operator's laptop and its speed is
irrelevant to the architecture story. "Real time" is the only descriptor
needed.)

**Render the Act 3 Summary Card:**

```markdown
### 🎬 Act 3: same question, new answer

**Goal:** Re-ask Act 1's question verbatim; show that the Act 0 filing is
now part of the answer, ingested live during the demo.

**Result:**
- The fresh ticker now appears in the top results.
- Its index entries replicated strict-sync to both sites during Acts 1 and 2.
- Its chunks had NOT moved to the second site before this read; they
  tiered lazily on the first read that needed them.

**Architectural callout:** **The index replicates eagerly because it must
be consistent. The chunks tier lazily because they don't. One
architecture, two replication behaviours.**
```

**Silently collect** (for the Act 4 master table): the fresh ticker's
positions in the re-asked search; the chunk URIs retrieved and their
wall-clocks.

(Do NOT collect or display `total_elapsed_seconds`, per-peer ack medians,
rows committed or embedding throughput. None of those are part of the
architecture story.)

**In production this looks like:** the cost numbers in Table B are already
projected to PB scale; that is the part of the story that generalises
directly. What also scales: ingestion throughput (GB/s across an ingestion
fleet), query rate (10,000+ queries/s on a GPU-backed retrieval cluster)
and the asymmetric replication shape (strict-sync metadata, lazy chunks)
holds across all of it. The demo's small seed corpus and one-laptop
process is a fixture; the architecture is what scales.

---

## Act 4: the takeaway visual (about 3 min)

The Act 4 composite has several components, rendered in this order, all
inline in the chat flow. Each maps to a rendering mechanism; get the
mechanism right or the recording breaks.

### Rendering mechanism: read this FIRST before generating anything

Claude Desktop has two visual rendering paths that look superficially
similar but produce very different recording outcomes:

✅ **Inline visualisation**: widgets that render **directly in the chat
flow** alongside text (horizontal cost-projection bars, KPI cards, a
chunk-warming tile grid). The audience sees them without leaving the chat
scroll. This is the right mechanism.

❌ **HTML side panel**: an HTML artifact that **pops out into a side panel**
in a separate frame. Triggered by creating or editing `.html` files, or by
certain artifact types. **This breaks the recording flow**: the side panel
covers the chat area and shows as a separate framed window on the
recording.

**Rules:**

1. `cache_warming_visual.html` in this folder is the **layout reference**
   for the chunk grid (the start prompt names it). Read it if you need the
   layout; **never edit it and never open it as an artifact**. Rebuild the
   grid as an inline widget instead.
2. **Never use HTML-artifact tools** that render in a side panel.
3. **Use the inline visualisation mechanism**: the one that produced an
   inline widget during the checklist pre-warm. Use the same one here.

### Component 1: KPI strip (4 cards)

Render inline as a 4-card composite. Each card: label, big number, small
caveat. Cards:

- **Corpus egress avoided**: `<%>` (`<warm chunks>` of `<total chunks>`)
- **Cumulative cache hit rate**: `<%>` across all reads in this demo
  (caption: "production: 70 to 90 % within 1 hour of traffic")
- **Fresh filing crossed**: `<%>` (`<k>` of `<fresh total>`; before Act 3
  it was 0 %, "ingested live, never queried")
- **Warm vs cold read**: `<warm ms>` vs `<cold ms>`, `<ratio>x`

### Component 2: chunk-warming map

Per queried document: a tile grid where each tile is one chunk, lit tiles
are warm (on the cache peer), empty tiles are cold (sync peer only). One
row per document; label with the document name and a count summary
(`<k> of <total> warm · <%> crossed`). Source: `data.onebucket.io:list` on
each `chunks/<raw_file_id>/` prefix, placement per object.

### Component 3: 1 PB annual cost bar chart (three bars)

Horizontal bars with width proportional to the dollar amount so the
audience **sees** the spread:

- **A. Eager replication (object storage x 2)**: about $1.0 M/yr
- **B. VectorDB-stores-chunks**: about $1.64 M/yr
- **C. Lazy-tier (OneBucket, this demo)**: about $25 K/yr

### Component 4: SEC 10-K retrieval heat map (research-anchored)

Add this **only if the demo's corpus is SEC 10-Ks** (the example config).
An inline widget showing the section-level retrieval distribution from the
FinanceBench analysis. This is the strongest "why does this matter" visual:
it shows exactly which sections of a 10-K drive RAG load.

| SEC 10-K section | Retrieval share | Tier recommendation |
|---|---|---|
| **Item 7 (MD&A)** | **40 to 45 %** | Hot (pinned) |
| **Item 1A (Risk Factors)** | **25 to 30 %** | Hot (pinned) |
| **Item 8 (Financial Statements)** | **15 to 20 %** | Hot |
| Item 1 (Business) | 5 to 10 % | Warm |
| Item 9A and exhibits | under 5 % | Cold / archive |

Narration when this appears (adapt the first sentence to the configured
question's section):
> "Today's question was a <section> question, and analysis of the
> FinanceBench benchmark across 361 SEC filings shows that section drives
> **<share>** of all retrievals against 10-K corpora. Item 7, Item 1A and
> Item 8 together drive **80 to 95 %** of all queries. The rest of the
> filing, procedural controls, exhibits, accounting boilerplate, is **cold**
> by design. The architecture matches what the data actually does."

### Component 5: counter-evidence (audience trust)

Add this **only when the audience asks "when does this break?"**; otherwise
keep it as backstop. Two clear scenarios:

| Workload | Why lazy-tier breaks | Architecture instead |
|---|---|---|
| **Legal contract review / forensic discovery** | Every clause is potentially load-bearing; cross-references form a dependency graph | Full corpus in the hot tier; clause-level segmenters |
| **Regulatory compliance crawls** (SR 11-7, pharma submission review) | The metric is 100 % recall, not precision; the long tail is the *target* | Disable lazy tiering or bound it by the compliance framework |

Narration if asked:
> "Two cases break this. Legal contract review: every clause is
> load-bearing, so the long tail isn't dead weight, it's the deliverable.
> And regulatory compliance crawls: the goal is **100 % recall**, the
> opposite of what lazy tiering is optimised for. For everything else, the
> bulk of enterprise RAG, the math holds."

### Component 6: closing line

> **"You pay to move what you read. Everything else stays where it is.
> That's the architecture."**

### Master metrics table: goes BELOW the visual composite

After the visual components above (which carry the architecture story),
append the master metrics table as **plain inline markdown**, not a widget.
The table is dense and reference-grade; widget rendering would clip it.

**Search latency is NOT in this table.** That is local fastembed encoding
plus a LanceDB scan on the operator's laptop CPU. If asked: "local embed
calculation" on a single CPU; production runs on GPU servers at
single-digit ms. Keep it out of the architecture story.

**How fetch latencies get sourced.** On stage, the agent wall-clocks each
`data.onebucket.io:get` call itself. For the table, prefer the storage
layer's own numbers: one `onebucket-demo:get_chunk_read_metrics` call per
Act with that Act's start and end timestamps returns `duration_ms` stats
and `per_first_responder` (the peer that served each read, which is the
warm/cold classification). Use the wall-clocks only if the window returns
no events, and say so in a note under the table.

**Classifying reads per Act:**

- **Act 1A**: first reads of every chunk through the policy; all **cold**
  (served by the sync peer, copies placed on the cache peer).
- **Act 1B**: chunks already read in the checklist pre-warm or Act 1A are
  **warm**; new chunks are **cold**.
- **Act 2 Beat A**: one warm and one cold by construction.
- **Act 2.5**: mix; classify each chunk against the cumulative warm set at
  the start of the Act.
- **Act 3**: same pattern as 1B; most of the fresh ticker's chunks will be
  cold (first reads), overlaps with earlier Acts warm.

```markdown
| Measurement | Act 1A | Act 1B | Act 2 | Act 2.5 | Act 3 |
|---|---|---|---|---|---|
| Timestamp (UTC) | <hh:mm:ss> | <hh:mm:ss> | <hh:mm:ss> | <hh:mm:ss> | <hh:mm:ss> |
| **Warm fetches (cache hit)** | 0 | <N> @ ~<ms> | 1 @ ~<warm_ms> | <N> @ ~<ms> | <N> @ ~<ms> |
| **Cold fetches (cache miss, served from source)** | <N> @ ~<ms> | <N> @ ~<ms> | 1 @ ~<cold_ms> | <N> @ ~<ms> | <N> @ ~<ms> |
| **Cache hit rate (this act)** | 0 % | <%> | 50 % | <%> | <%> |
| **Bytes crossed to the cache peer (this act)** | <KB> | <KB> | <KB> | <KB> | <KB> |
| **Cumulative chunks warm** | <pre-warm> + <N> | <total> | <total> | <total> + <new> | <total> |
| **Cumulative hit rate (pre-warm + Acts 1A to here)** | <%> | <%> | <%> | <%> | <%> |
```

Cell format: `6 @ ~150 ms` means six fetches at about 150 ms average. Use
`n/a` when a classification does not apply.

**Required:** every Act column's read rows are filled with either a
count-at-latency or an explicit `n/a`. No empty cells.

### About the cache hit rate rows

This is the demo's strongest architectural-scaling signal. **Cache hit
rate** is the standard caching and CDN term for "what fraction of requests
were served from cache without going to source". In RAG it proxies the
**hot-chunk concentration**: real corpora are heavily skewed (a few chunks
carry the most semantically dense content and get retrieved repeatedly
across different questions), so the hit rate climbs fast with question
volume. This is the load-bearing reason the lazy-tier cost numbers hold up
at scale.

**How to compute it per Act:**

- For each chunk read THIS Act, check whether it was already in the
  cumulative warm set as of the START of the Act. (The cumulative warm set
  after the checklist is the 5 pre-warmed chunks; it grows after each Act.)
- **This-act hit rate** = (chunks already warm) / (chunks read this Act).
- **Cumulative hit rate** = (chunks already warm across all Acts so far) /
  (all reads across all Acts so far, pre-warm included).

**Expected trajectory** on a small seed corpus with three questions:

- Act 1A: 0 % (first reads, apart from any overlap with the pre-warm set)
- Act 1B: 20 to 60 % (same question; overlap with Act 1A's reads)
- Act 2: 50 % by construction (1 warm, 1 cold)
- Act 2.5: 15 to 35 % (typical overlap on a related question)
- Act 3: 30 to 60 % (same question as 1B, plus the fresh ticker's cold chunks)

**Production framing** (use in narration after revealing the row):

> "At scale this curve climbs fast. Public CDN data and RAG production case
> studies put steady-state hit rates at **70 to 90 % within the first hour
> of traffic** on a bounded corpus, because semantic density is heavily
> skewed and the same passages answer many different questions. **The cost
> savings come from this curve, not from the demo's handful of chunks.**"

Then a **KPI strip** below the table, bolded numbers only:

- **Total chunks warmed across the demo:** `<sum>` of `<total_chunks>` from
  `list_documents` (= `<%>` of the corpus)
- **Total bytes that crossed to the 2nd site:** `<KB>` (versus the corpus
  total from summing the `list` sizes, if collected; otherwise omit the
  comparison)
- **Fresh filing now in answers:** ingested live during the demo; index
  replicated strict-sync; chunks moved only when read
- **Steady-state cost at 1 PB:** **Eager about $1.0 M/yr · VectorDB about
  $1.64 M/yr · Lazy-tier about $25 K/yr**

Then the **chunk-warming grid as a plain markdown table**, one row per
queried document, listing total chunks versus warm chunk indices:

```markdown
| Document | Chunks total | Warm on the cache peer | Warm chunk indices |
|---|---|---|---|
| <doc 1> | <chunk_count> | <k> | <indices> |
| <doc 2> | <chunk_count> | <k> | <indices> |
| <fresh filing> | <chunk_count> | <k> | <indices, or n/a if never read> |
```

Then the **1 PB cost projection** as inline markdown bars (three lines,
proportional to the dollar amounts):

```
A. Eager replication      ████████████████████░░░░░░░░░░░  ~$1.0 M/yr
B. VectorDB-stores-chunks ████████████████████████████████  ~$1.64 M/yr
C. Lazy-tier (OneBucket)  █░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  ~$25 K/yr
```

Then the **closing line, bolded:**

> **You pay to move what you read. Everything else stays where it is.
> That's the architecture.**

---

## Addendum (optional, operator-triggered)

**Not in the running order.** Acts 0 to 4 stand alone. These segments are
optional follow-ons; skip them when the cost story alone is the punchline.

### Addendum A: the bidirectional read (optional: requires the mirror OneBucket and a second provider)

Requires the optional mirror OneBucket (example name `demo-west` in `variables.example.yml`): `WEST_WASABI` sync and
required, `EAST_S3` cache. It is the mirror image of your data OneBucket and tells
the cross-region story from the other side: a reader on the West side
whose first read of a chunk is served from the East source and leaves a
copy on its own side.

**Operator trigger:**

> The deployment also has the `demo-west` policy, the mirror image of
> `demo-data`. Read the Act 1 chunks through `demo-west` and show what a
> query that originates on the other side pays on its first read and on
> its second.

**What the agent does:**

1. `data.onebucket.io:list bucket=demo-west prefix=chunks/<Act 1 raw_file_id>/`
   and confirm from the response which peers the policy spans. Do not
   assume from the prompt; verify from the deployment.
2. Read 3 of the Act 1 chunks through `demo-west` with
   `data.onebucket.io:get`, wall-clocking each. These are first reads on
   that side: expect cold latency.
3. Read the same 3 again. Expect warm latency; the copies are now on the
   West side.
4. Say the two numbers and the ratio. Do not poll `locate` between the
   reads.

**Render the Addendum A Summary Card:**

```markdown
### 🔁 Addendum A: the same chunks, read from the other side

**Goal:** Show the architecture is symmetric: a policy whose home is the
other provider tiers lazily in the other direction.

**Result:**
- `demo-west` spans <peers>; its sync peer is the West bucket.
- First reads: **<cold_ms> ms** (served from the East source, copy placed
  West). Second reads: **<warm_ms> ms**.
- Nothing was pre-copied; the West side holds exactly the 3 chunks it read.

**Architectural callout:** **There's no special direction. Whichever side
asks first pays once; everyone after that reads locally.**
```

Append a `demo-west` column to the master metrics table with the
timestamp, the 3 cold and 3 warm fetches, and bytes that crossed.

### Addendum B: spinning up a new geography (about 3 min)

For audiences that care about geographic expansion or data-sovereignty
patterns.

**Setup (operator, briefly on stage):**

1. In the OneBucket console, create a **new policy** (name it after the
   region, for example `demo-<region>`) with:
   - Sequence 1: a bucket in the new region as `sync` (the origin; empty at
     creation time)
   - Sequence 2: `EAST_S3` as `cache`
   - Sequence 3: `WEST_WASABI` as `cache`
   - Do NOT add this policy to the metadata tier; index replication to the
     new region is not part of this story.

   This is the **inverted topology** versus Acts 0 to 4: the new region as
   origin, East and West as caches. The narrative payoff: "the architecture
   is symmetric; what we showed going to the second site also works into a
   third."

2. List the new policy's cache peer aliases in `variables.yml`
   `operator_intent.addendum_cache_peers` so `demo-setup.sh --purge-cache`
   can clean them between dry runs.

3. Trigger the addendum by pasting:

   > I just created a new policy `<name>` where the new region is the sync
   > origin and the East and West buckets are cache peers. Run the same
   > multi-hop question through `<name>` and show what happens when queries
   > originate there. Probe the policy yourself via `data.onebucket.io:list`
   > to confirm the peers before reading.

**What the agent does:**

1. **Probe the topology**: `data.onebucket.io:list bucket=<name>
   prefix=chunks/<any existing raw_file_id>/` and confirm the peers from
   the response.
2. **Run the same multi-hop question** as Act 1B (discovery hop, then
   focused hop). It retrieves the same chunks.
3. **Read each top chunk through the new policy** with
   `data.onebucket.io:get`, wall-clocking each. Each read tries the new
   origin (empty, miss), falls through to a cache peer that has the chunk,
   serves from there, and places a copy at the origin.
4. **Do not poll placement.** Capture the cross-region wall-clock on the
   read itself; that is the measurement.

**If the audience asks "isn't this just adding the region as a cache peer
to the existing policies?"**, say plainly:

> "Inverting the topology proves the architecture is **symmetric**. Adding
> the new region as a cache peer to the existing policies serves its
> queries from data that's already on East or West. Making it the sync
> origin of its own policy means the data tier can lazy-fill *into* it:
> the same dynamics we showed in Acts 2 and 2.5, just across a longer
> distance. **There's no special mode for a new region; the system doesn't
> know or care about geography.**"

**Render the Addendum B Summary Card:**

```markdown
### 🌍 Addendum B: inverted topology, queries from a new region

**Goal:** Re-run the Act 1 question through a new policy where the new
region is the sync origin and East/West are cache peers.

**Result:**
- Topology confirmed via `data.onebucket.io:list`: <peers>.
- <N> chunks retrieved; reads through `<name>` fell through to a cache
  peer (the origin was empty), then placed copies at the origin.
- Cross-region cold-fill latency: **<ms>** (RTT plus the serve from the
  cache peer). Subsequent reads at the origin stay warm.

**Architectural callout:** **The system doesn't know it's crossing an
ocean. Same lazy-tier dynamics, inverted geography. Spin up a new region
with no architecture change.**
```

Append a column for the new policy to the master metrics table
(timestamp, chunks retrieved, average fetch wall-clock, bytes that crossed
to the new region, cumulative chunks warm there).

**KPI strip for the closing (four at most, bolded):**

- **Cross-region cold read: <ms>** (origin miss, cache-peer hit)
- **Warm read once the origin fills: <ms>**, the same as local
- **New region online in about 30 seconds**: the policy creation; the
  bytes follow lazily
- **1 PB cache in the new region, steady state: about $25 K/yr**, the same
  shape as the second-site story, because the architecture is the same

---

## If something needs depth

`onebucket_engineering_reference.md` holds the metric definitions and SQL,
the evidence ledger, the architecture internals, the full cost and TCO
methodology, the `verify_onebucket_tiering.py` ground-truth probe and the
known-issue list. The live demo stays in Act 0 through Act 4; the reference
is where hard questions get answered afterwards.
