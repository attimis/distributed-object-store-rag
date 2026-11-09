# OneBucket Demo Script

A live-presentation walkthrough: the prompts to type, what to point at, what to say, and what to skip when you are short on time. The tool reference behind it is [operations/mcp-tools-guide.md](operations/mcp-tools-guide.md); the execution modes are explained in [operations/execution-modes.md](operations/execution-modes.md).

**Total runtime:** about 12 minutes with narration, or 7 minutes if you pre-load the indexed content.

---

## The story you are telling

In one paragraph:

> OneBucket presents one S3-compatible bucket that fans every write out to several backing buckets, in different regions or on different providers, without any application-level ETL. To show what that buys you, this demo builds a hybrid RAG pipeline on nothing but plain S3 semantics: drop a document in, a pipeline chunks it, embeds it and indexes it, and a Claude agent answers grounded questions against it. The index is LanceDB on S3, which is append-only data files plus an atomic manifest, exactly the shape OneBucket replicates cleanly. Point the same code at a OneBucket instead of a plain bucket and nothing in the application changes; you gain multi-region replication for free.

What you are showing:

1. A real document goes in with nothing more than an S3 PUT.
2. A pipeline consumer chunks and embeds it asynchronously, with no orchestration code.
3. Claude Desktop, talking to an MCP server pointed at the S3-resident index, answers grounded questions about that document with citations.
4. The entire index is plain S3 objects shaped to replicate cleanly.

What you are not showing (mention only if asked): retrieval-quality tuning, multi-tenant access control, document formats beyond text and markdown.

### Which pipeline is running

The pipeline consumer is whichever execution mode the MCP server is configured for (`ONEBUCKET_EXECUTION_MODE`):

| Mode | Consumer | What the audience can watch |
|---|---|---|
| `local-daemon` (default) | `bash scripts/start-pipeline-daemon.sh` polling `raw/` | the daemon's terminal |
| `onebucket-events` | `bash scripts/start-event-consumer.sh`, after a one-time `uv run python scripts/onebucket-login.py` sign-in | the consumer's terminal, with per-peer replication telemetry |

Everything runs on the laptop. The same two pipeline functions can run on AWS Lambda or any cloud compute that exposes them behind an API; [operations/execution-modes.md](operations/execution-modes.md) covers that, and the demo does not need it.

### Connectors in Claude Desktop

Three MCP servers may be present, and the prompts say "the onebucket-demo tools" to make sure Claude uses the right one:

- `onebucket-demo`: this repo's server (search, fetch, ingest, status, delete, rebuild, metrics, config). The demo uses this one.
- `data.onebucket.io`: the OneBucket data connector, which reads and writes objects through OneBucket directly.
- `events.onebucket.io`: the OneBucket events connector, which the `onebucket-events` consumer also subscribes to.

---

## Pre-demo checklist (30 minutes before)

```bash
# 1. Your OneBucket credentials work through the OneBucket endpoint
export AWS_PROFILE=<your-onebucket-profile>
export AWS_ENDPOINT_URL=https://s3.onebucket.io
aws s3 ls s3://<your-bucket>/ | head   # must list raw/ chunks/ metadata/ without an error

# 2. The pipeline consumer is up
#    local-daemon:      bash scripts/start-pipeline-daemon.sh   (separate terminal)
#    onebucket-events:  bash scripts/start-event-consumer.sh    (separate terminal)

# 3. The index is live and has the demo content pre-loaded
uv run python -c "
import lancedb
t = lancedb.connect('s3://<your-bucket>/metadata').open_table('global_index')
print(f'rows: {t.count_rows()}')"
# expect 130+ rows if the AAPL 10-K is pre-loaded

# 4. Claude Desktop has been fully restarted (Cmd-Q, then reopen) since the last
#    MCP config change or code change in this repo
```

**Pre-loading content** (do this before the demo if the audience will not wait 4 to 5 minutes for a full 10-K to embed): in Claude Desktop send

> Ingest the latest 10-K for AAPL using the onebucket-demo tools. Tell me when it is ready.

Wait until Claude reports `phase: "ready"` and note the `raw_file_id` it returns; you will reference it in Act 2.

---

## Windows to have open

- **Window 1 (primary):** Claude Desktop, full screen, so the audience sees the chat and the tool-call expanders.
- **Window 2 (the pipeline):** the consumer's terminal next to Window 1. The audience sees `[idle]`, then `[found] 1 new raw file(s)`, `[processing] raw/...` and the chunk and embed progress in real time as Claude triggers uploads. In `lambda` mode, the AWS console open on CloudWatch, `/aws/lambda/onebucket-chunker` and `-embedder` log streams.
- **Window 3:** a terminal for `aws s3 ls s3://<your-bucket>/ --recursive` when you want to show that this is just a bucket.

---

## Act 0: the pipeline-as-a-service prelude (15 seconds)

### What to say

> "Before we start: the pipeline that is going to chunk and embed the file I upload is running right here." (point at Window 2) "It is an independent process, the same shape any third-party document pipeline would have. It watches the bucket and reacts to new files. Right now it is idle. Watch what happens when I tell Claude to ingest a 10-K."


This act anchors the architectural story: the pipeline is not magic inside the agent, it is a separate piece of software, and the storage layer is what connects them.

---

## Act 1: the value proposition and the architecture (90 seconds)

### What to say

> "What you are seeing is a hybrid retrieval-augmented generation pipeline running entirely against S3. There is no managed vector database. Just a bucket, the same one you would dump CSVs and PDFs into, plus a small pipeline. The index is LanceDB, but it lives in S3 as immutable data files plus an atomic manifest. That detail is the load-bearing one: OneBucket replicates exactly this storage shape across regions, so the whole RAG stack replicates for free."

### What to do

In Window 3:

```bash
aws s3 ls s3://<your-bucket>/ --recursive | head -20
```

The audience sees `raw/`, `chunks/`, `metadata/global_index.lance/data/...lance`. Your line:

> "These are all plain S3 objects. `raw/` is what we dropped in; `chunks/` is what the chunker wrote; `metadata/global_index.lance/` is the searchable index. Put a OneBucket underneath this and every one of these objects lands in every peer region."

---

## Act 2: the full agentic loop in one prompt (5 to 6 minutes with a live ingest, 30 seconds if pre-loaded)

This is the centrepiece. The audience watches Claude chain five tools from one natural-language ask.

### Setup

Window 1, new conversation.

### Prompt (verbatim)

> Use the onebucket-demo tools to find Apple's 10-K and tell me three primary risks the company calls out for its business. Cite specific chunk indices.

### Variant if you pre-loaded AAPL

> Use the onebucket-demo tools to find Apple's 10-K; it should already be in the index. Tell me three primary risks the company calls out for its business, citing specific chunk indices.

### What Claude does, and what to narrate

1. **`search_global_index` (discovery hop).** Claude does a broad search for documents about Apple.

   Say: "Claude is doing a discovery search to find which document is Apple's. It does not know the filename; it lets the embeddings find the right document."

2. **If pre-loaded:** Claude goes straight to a scoped search (step 4). **If not:** Claude calls `ingest_document("AAPL")`.

   Say, if ingesting: "Claude is calling our ingest tool. Behind the scenes that is hitting SEC EDGAR, fetching the latest 10-K, converting it to text and uploading it to the bucket. The pipeline consumer picks it up from there."

3. **If ingesting:** Claude polls `check_ingest_status` every 30 seconds or so.

   Say: "This is genuinely waiting on the chunker and then the embedder, about four minutes for a full 10-K because the embedder works in batches of 16 chunks to keep memory bounded. We could go faster with more memory or parallel embedding; for a demo this is fine."

   Meanwhile, point at Window 2. The audience sees the consumer working through the file.

4. **`search_global_index` with `scope=[<aapl_id>]`**, a focused search.

   Say: "Now that Apple is indexed, Claude scopes the next search to that one document. This is the multi-hop pattern: discover the relevant documents first, then narrow the search to them. Without scoping, the answer would bleed across every document in the index."

5. **`fetch_chunk`, three to five calls**, pulling the actual chunk text.

   Say: "Search returns chunk URIs and scores, not the text. Claude has to call fetch_chunk to read the content it will synthesise from. This is the agentic part: Claude decides which chunks are worth reading."

6. **The answer:** three risks with chunk citations.

   Say: "And here is the answer, grounded in real chunks of the 10-K with citations. Every chunk index is a real S3 object. Auditable, traceable, no hallucination layer."

### Expected duration

- Pre-loaded: 30 to 45 seconds.
- Live ingest: 5 to 6 minutes including the polling. Use the time for architecture talk and early questions.

---

## Act 3: multi-hop discovery and bulk ingest (8 to 15 minutes including the embedding wait)

The most impressive move, and the moment audiences realise the flow is genuinely agentic rather than scripted. The pattern:

1. Ask about a topic the index does not cover yet.
2. Claude searches and reports honestly that there is no relevant content.
3. It offers to ingest representative documents.
4. You approve; Claude picks five and ingests them.
5. It polls for readiness.
6. It runs one focused query across the new set with `scope=[id1, id2, ...]`.
7. It synthesises a cross-document answer.

Skip this if your slot is under 10 minutes. It is worth the time otherwise; nothing scripted lands the agentic story as well.

### Prerequisites

Same conversation as Act 2, so the first search has something (irrelevant) to return. If you skipped Act 2, Claude says "the index is empty" instead of "the index has Apple but no oil companies".

### Prompt 1 (a topic the index does not cover)

> What are oil companies mentioning in their business risks for the future?

Claude calls `search_global_index("oil companies business risks")`, gets hits that are all from Apple with low scores, and tells you plainly that the index has no oil-company content. It offers options: ingest some oil-company 10-Ks, answer from general knowledge without citations, or take a specific URL.

Say: "This is the right behaviour. The agent is calibrated: it will not fabricate citations. The honest 'I have no evidence here, do you want me to go get some?' is exactly what a grounded RAG agent should do."

### Prompt 2 (the bulk ingest)

> Let's ingest more oil company 10-Ks. Pick 5 of the most representative, then run my query against those.

What Claude does:

1. **Picks five representative tickers** across subsectors, for example XOM and CVX (integrated majors), COP and OXY (independents) and MPC (refining). The selection comes from the model's own knowledge and is usually sensible.

2. **Five `ingest_document` calls in succession.** All five uploads land in the bucket within seconds.


   Say: "The consumer has five files queued and works through them one after another. The same two functions could run on Lambda or any cloud compute with an API; only the trigger changes."

3. **Polls `check_ingest_status` for each `raw_file_id`.** Early polls show `phase: "embedding"` with `rows_committed: 0`; later polls flip documents to `ready` one by one.


   Watch for Claude declaring a document "stalled" too early. The status response includes `last_activity_seconds_ago`, so Claude can tell "still progressing" from "idle for more than ten minutes". If it gives up before that, redirect: "check the status once more before giving up; the pipeline is still working on it."

4. **One scoped search with `scope=[xom_id, cvx_id, cop_id, oxy_id, mpc_id]`.** Claude should not run five separate per-document searches.

   Say: "One search across all five companies with the scope parameter. The ranker returns the globally top-N chunks across the whole scope, so we get the strongest evidence wherever it lives rather than a fixed quota per company."

5. **`fetch_chunk`, five to eight calls.**

6. **A cross-company answer**, usually clustered by theme (energy transition, commodity prices, geopolitics, regulation, decommissioning) with per-company citations.

### Expected wall clock

- Best case: 6 to 7 minutes including narration.
- Typical: 10 to 12 minutes.
- Worst case, one document unlucky on retries: 15 minutes.

Use the wait for Act 4 material and questions.

### If one document truly fails

If after 12 minutes one ingest has not completed and the consumer's log (or CloudWatch) shows real errors rather than slow progress:

> "One document is taking too long; we will proceed with the four that are ready. Notice Claude still gives a useful answer. The architecture degrades gracefully; nothing blocks on a single failed ingest."

Then have Claude run the scoped search with whatever subset is ready.

### Why this Act is the centrepiece

Three agentic behaviours land at once: calibrated honesty (Claude refuses to fabricate when the index lacks the answer), tool composition (ingest, poll, scoped search and fetch from one prompt) and cross-document synthesis (one search over many documents, one clustered answer). The architecture made all three easy, and OneBucket-replicable storage is what lets you put that architecture anywhere.

---

## Act 4: the architecture punchline (60 seconds)

In Window 3:

```bash
aws s3 ls s3://<your-bucket>/metadata/global_index.lance/ --recursive
```

Point at the output:

> "This is the entire vector index. Every file is append-only: the `_versions/...manifest` files are the atomic commit pointers, the `data/...lance` files are immutable columnar data. A search reads the latest manifest, then the data files it points at. Writes never modify an existing file. That is exactly the access pattern OneBucket replicates: append, then commit by publishing a new manifest. Put a OneBucket under this bucket and the whole RAG index is replicated across regions without changing a line of application code."

If you are already running against a OneBucket (`onebucket-events` mode), this is the moment to show the consumer's per-peer acknowledgement times, or to ask Claude for `get_replication_metrics` on the document you just ingested.

---

## Closing (30 seconds)

> "Three things to take away. One: Claude orchestrated all of this, search, fetch, ingest, polling, from a single natural-language ask; we exposed the right tools and let the model chain them. Two: the entire RAG stack is plain S3 plus a stateless pipeline, no managed vector database, no per-tenant cluster. Three: the storage format is OneBucket-replicable, which is the whole point. The current S3 layout already is the replicated layout."

> "Questions?"

---

## Scenarios you can run as regression checks

If you want to verify the demo still works without narrating it, these are the prompts and the behaviour to expect:

1. **Single-document ingest.** "Ingest the latest 10-K for AAPL and tell me its three main risks." Expect `ingest_document`, `check_ingest_status` until `ready`, a scoped `search_global_index`, several `fetch_chunk` calls, a cited answer.
2. **Scoped search on pre-loaded content.** Same ask with "it is already indexed". Expect no ingest call and a scoped search.
3. **Multi-hop discovery and bulk ingest.** Act 3 above. Expect an honest "no evidence" first, then five ingests, polling, one scoped search.
4. **Corpus inspection.** "What is in the index? One sentence per document." Expect `list_documents` plus a few searches and fetches, no ingest.
5. **Delete and re-ingest.** "Delete the Apple 10-K from the index, then confirm it is gone." Expect `list_documents`, `delete_document(raw_file_id)`, `list_documents` again with the document absent.

---

## Q&A: common questions and how to answer

### "How long does ingestion take?"

> "A typical 10-K is around 700 KB and 140 chunks; end to end is about five minutes, most of it in the embedder, which works in 16-chunk batches to stay under a 3 GB memory ceiling. With more memory or per-batch profiling that comes down to about a minute. A one-page document takes seconds."

### "What about retrieval quality?"

> "Honest answer: this is where I would look first if you wanted to scrutinise something. Hybrid dense plus sparse gets you into the right neighbourhood, but it has known failure modes; the sparse channel can be fooled by topical density rather than relevance. The standard fixes are reciprocal rank fusion across the two channels and a cross-encoder re-rank of the top 20. The infrastructure is settled; the quality knob is the next iteration."

### "Why LanceDB and not Qdrant, Pinecone or pgvector?"

> "LanceDB keeps its index as immutable data files plus a `_versions` manifest in object storage, which is the shape OneBucket replicates. Engines that keep state in a local key-value store cannot be replicated without intrusive integration. It was a load-bearing choice; [ADR-010](architecture/decisions/ADR-010-database-engine-pivot.md) explains it."

### "What happens if two writers commit at the same time?"

> "LanceDB uses optimistic concurrency control on the manifest. If two embedders commit at once, one wins, the other reads the new manifest and retries. We test this with concurrent uploads of two different files and assert both end up in the index."

### "How does this work with PDFs or Word documents?"

> "Today the chunker accepts plain text and markdown. The `ingest_document` tool converts SEC HTML to text before upload, and the same conversion step is where PDF and Office support would go. It is a small addition to the chunker; nothing in the demo needs it yet."

### "What does it cost?"

> "At demo scale, essentially nothing: pennies a month for S3 and for the two container images in ECR. At production scale it stays cheap because the pipeline is per-invocation and S3 storage is cheap. The bulk of the cost is embedding compute, which scales with document volume."

### "Can I delete a document?"

> "Yes. `delete_document(raw_file_id)` removes it end to end: it deletes the index rows first, so search stops returning the chunks immediately, then the chunk files and manifest, then the raw source. It is strict on purpose: if the index and the bucket disagree about a document, it refuses rather than guessing, and points you at `rebuild_index`, which wipes the index and chunks and reprocesses everything still under `raw/`."

### "Can I see this replicate across regions right now?"

> "If the bucket is a OneBucket, yes: the `onebucket-events` consumer reports per-peer acknowledgement latency for every write, and `get_replication_metrics` summarises it per document. Against a plain S3 bucket the storage shape is the same; the replication is what OneBucket adds underneath."

---

## Failure recovery during the demo

### Claude does not see the onebucket-demo tools

- Claude Desktop has not fully restarted since the last config change. Cmd-Q and reopen.
- Still missing: look at `~/Library/Logs/Claude/mcp-server-onebucket-demo.log` for a traceback and see the troubleshooting part of [operations/aws-deployment-runbook.md](operations/aws-deployment-runbook.md).

### A tool call returns an error envelope

- Most likely `AWS_PROFILE` is missing from the server's `env` block in `claude_desktop_config.json`; the shell export does not carry over.
- Next most likely: a canary mismatch on the LanceDB sidecar after a model or library upgrade. Re-run `scripts/init_index.py` against the dataset root.
- `DAEMON_NOT_RUNNING`: you are in `local-daemon` or `onebucket-events` mode and the consumer is not up. Start it in a separate terminal and retry; the upload was not performed.
- `DAEMON_AUTH_EXPIRED`: the `onebucket-events` consumer's sign-in lapsed. `uv run python scripts/onebucket-login.py`, restart the consumer, retry.

### A document sits in `phase: "embedding"` for more than ten minutes

- Check the consumer's terminal for an `[error]` line. If it has genuinely failed, pivot to pre-loaded content.
- Plan B prompt: "What is already in the index? Name the documents and give a one-sentence summary of each." This uses only `list_documents`, `search_global_index` and `fetch_chunk`, with no ingest dependency.

### "Transport closed" in Claude Desktop

- The MCP server crashed at startup. Triage in a terminal:

  ```bash
  AWS_PROFILE=onebucket-demo ONEBUCKET_DATASET_ROOT=s3://<your-bucket>/metadata \
    ONEBUCKET_CHUNK_ROOT=s3://<your-bucket>/chunks \
    uv run python -m onebucket_demo.mcp_server
  ```

  It should sit waiting on stdin (good) or print a Python traceback (that is your error message).

### "Thirty seconds left and the embedder has not finished"

- Pivot: "The embedding is going to take a few more minutes. Let me show you what is already indexed." Run Act 4 with `aws s3 ls`.
- The point of the demo is the architecture and the agentic flow, not the wall clock. Skip the wait gracefully.

---

## Timing summary

| Section | Pre-loaded | Live ingest |
|---|---|---|
| Pre-demo prep | 5 min (before the slot) | 30 min (before the slot) |
| Act 1: value proposition | 90 s | 90 s |
| Act 2: agentic RAG | 45 s | 5 to 6 min |
| Act 3: multi-hop (optional) | 2 to 3 min | 8 to 15 min |
| Act 4: architecture punchline | 60 s | 60 s |
| Closing | 30 s | 30 s |
| **Total without Act 3** | **4 min** | **9 min** |
| Q&A | as long as you like | as long as you like |

For a 5-minute slot: pre-load and skip Act 3. For a 10-minute slot: pre-load and do all four acts. For a 15-minute slot: ingest live so the audience sees the pipeline work, and do all four acts.

---

## See also

- [operations/mcp-tools-guide.md](operations/mcp-tools-guide.md): the full tool reference for questions that go deep on the API.
- [operations/execution-modes.md](operations/execution-modes.md): choosing and configuring the pipeline consumer.
- [operations/aws-deployment-runbook.md](operations/aws-deployment-runbook.md): the optional AWS Lambda example, for running the same pipeline off the laptop.
- [operations/onebucket-setup.md](operations/onebucket-setup.md): provisioning a OneBucket to run the demo against replicated storage.
- [architecture/decisions/ADR-013-execution-modes.md](architecture/decisions/ADR-013-execution-modes.md): why the three execution modes share one pipeline.
