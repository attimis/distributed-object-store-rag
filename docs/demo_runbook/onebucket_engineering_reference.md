# OneBucket RAG Tiering: Engineering Reference

**What this is.** The depth behind the demo. The demo script
(`onebucket_rag_demo_runbook.md`) is what you run in front of an audience:
four acts, about 20 minutes, story first. *This* document is what you read
to understand the system, validate a claim, or answer a hard question from a
technical audience member. Nothing here goes on a demo slide unless someone
asks for it.

**Companion files in this folder:**
- `onebucket_rag_demo_runbook.md`: the lean four-act demo script
- `variables.example.yml`: the config schema; copied to `variables.yml` (gitignored) and filled in per deployment
- `metrics.py`: schema-profiling-first metrics over the event log (`uv sync --extra demo` first; it needs DuckDB)
- `verify_onebucket_tiering.py`: plain-S3 ground-truth probe of the backing buckets

**Naming used below.** Name your OneBuckets however you like and use the
providers you have. This document uses the example names from
`variables.example.yml`: your data OneBucket `demo-data`, your metadata
OneBucket `demo-meta`, the optional mirror `demo-west`; endpoint aliases
`EAST_S3` (AWS S3) and `WEST_WASABI` (Wasabi). `metrics.py` and the MCP
tools read the actual names from `variables.yml`
(`pipeline.raw_writes_to_policy`, `chunks_writes_to_policy`,
`metadata_writes_to_policy`). For how to set the policies up, see
[`onebucket-setup.md`, "What the pipeline needs from your policies"](../operations/onebucket-setup.md#what-the-pipeline-needs-from-your-policies).

**The one structural requirement.** Keep the metadata (the LanceDB index)
in its own OneBucket that replicates eagerly to every peer, separate from
the data OneBucket whose other peers fill lazily on read. Placement is per
OneBucket, so one OneBucket cannot replicate the index eagerly and the
chunks lazily at the same time.

MCP tool names are written `server:tool`:
`onebucket-demo:list_documents` is this repo's server,
`data.onebucket.io:locate` is the OneBucket data connector,
`events.onebucket.io:tail_events` is the OneBucket events connector.

---

## 1. Status legend and what counts as proof

- **[PROVEN]**: directly observed in a session; will behave as described.
- **[VERIFY]**: expected from the architecture or tool docs but not yet observed
  end-to-end; confirm on a dry run and record actuals.
- **[EXTRAPOLATED]**: a unit measurement scaled by stated assumptions
  (the PB-scale TCO numbers). Honest projection, not measurement.

**The two-surfaces principle.** The per-object placement reported by
`data.onebucket.io:locate` (and the per-object detail in `list`) and the
event log the consumer writes are *two independent surfaces*. Something can
be proven on one and unobserved on the other; do not collapse them. The
metrics design (sections 4 and 5) depends on keeping them separate: M-COST
reads placement; M-HEADLINE, M-PIPELINE and M-SCALE read the event log.

---

## 2. Architecture

The example deployment: two required OneBuckets and one optional mirror,
fanning out to peer buckets on two backing providers (yours may differ;
a single provider also works, with nothing to measure in the cache-fill
and cross-region numbers):

| Policy | Peer 1 | Peer 2 | Quorum | Holds |
|---|---|---|---|---|
| `demo-data` | `EAST_S3` (AWS, sync, required) | `WEST_WASABI` (Wasabi, cache) | 1 | `raw/` + `chunks/` |
| `demo-west` (optional) | `WEST_WASABI` (Wasabi, sync, required) | `EAST_S3` (AWS, cache) | 1 | the mirror image, for the cross-region read story |
| `demo-meta` | `EAST_S3` (AWS, sync, required) | `WEST_WASABI` (Wasabi, sync, required) | 2 | LanceDB global index |

Key facts:

- **Raw files and chunks live in the same backing bucket per provider**,
  distinguished only by key prefix (`raw/` vs `chunks/`). Not separate stores.
- **`demo-meta` is the always-in-sync control**: every peer is `sync` and
  required, quorum = peer count (2 here); the vector index is on both peers after every write.
- **Peer modes** (console wording in parentheses): `sync` (Synchronous), the
  write lands on this peer before the request returns and counts toward the
  quorum; `async` (Asynchronous), acknowledged first and copied in the
  background; `cache` (Cache), holds copies of what has been read or
  prefetched through the policy so repeat reads are served locally, while
  the sync peer stays authoritative.
- The validated demo path uses `demo-data` and `demo-meta` only. `demo-west`
  is optional and adds the bidirectional story (reads that originate on the
  West side, with `EAST_S3` as its cache peer).
- Peer aliases are endpoint names and are reused across policies: `WEST_WASABI`
  is `type: cache` under `demo-data` but `type: sync` under `demo-meta`. The
  same alias means different things in different policies.
- The per-object placement signal is `data.onebucket.io:locate` (or the
  per-object detail of `list`): `[EAST_S3]` means source only (cold);
  `[EAST_S3, WEST_WASABI]` means the cache peer also holds it (warm). The
  list of peers a *policy* spans is not the per-object signal.

All canonical values (policy names, peer aliases, backing buckets, endpoint
URLs, daemon mode) live in `variables.yml`. Read them at session start with
`onebucket-demo:get_runbook_config`.

---

## 3. Cache-on-read mechanics

A read through a policy is answered by the first peer that has the object:
the cache peer if it already holds a copy, otherwise the sync peer. When the
sync peer serves the read, a copy is placed on the cache peer, so the next
read of the same object through that policy is served locally. That is what
"warming" a chunk means, and it is why the cache peer's backing bucket fills
up over a demo: `verify_onebucket_tiering.py` lists the cache bucket
directly and shows exactly the chunks that have been read.

Writes never go to a cache peer. The pipeline writes originals through
`demo-data`; they land on `EAST_S3`. Everything on `WEST_WASABI` under
`demo-data` is derived data that reads put there, which is why
`demo-setup.sh --purge-cache` (and `onebucket-demo:purge_peer_cache`) may
delete it to reset a demo.

**[VERIFY] on your deployment:** the lag between the sync-peer read and the
cache copy becoming visible to `locate`. The runbook tells the agent to
trust read latency rather than poll placement mid-act.

**Practical note for driving a demo:** `data.onebucket.io:get` returns the
object through the policy (and warms the cache peer), while
`onebucket-demo:fetch_chunk` reads through the configured chunk root (the
same policy) and returns the text the model synthesises from. In a chat-
driven demo the agent may make both calls per chunk; in a production RAG
app one read does both. The split is a chat-client artifact, not an
architecture property.

---

## 4. The event log (`events.jsonl`)

Every request to a OneBucket produces several events that share one
`reqId`: one `gateway` event (the request as the client made it, tagged
with the caller's `accessKey`) plus one `backend` event per peer bucket the
request reached (tagged with the peer's `endpoint` alias). Wire fields:
`eventId`, `reqId`, `layer`, `endpoint` (backend) or `accessKey` (gateway),
`action`, `bucket`, `key`, `statusCode`, `bytes{in,out}`, `time`,
`durationMs`. The events connector exposes them through
`events.onebucket.io:tail_events` (oldest first), `list_events` and
`get_event`.

The event consumer (`scripts/oneb_event_consumer.py`, the `onebucket-events`
mode in `variables.yml` `daemon.mode`) polls `tail_events`, dispatches the
chunker on the gateway event of every successful `s3:PutObject` under `raw/`
in the watched OneBucket, and appends one JSONL line per non-duplicate
event to `<repo>/.runbook-state/events.jsonl` (override with
`ONEBUCKET_EVENT_LOG_PATH`). Field names in the file are snake_case
(`event_id`, `req_id`, `layer`, `endpoint`, `action`, `bucket`, `key`,
`status_code`, `bytes_in`, `bytes_out`, `event_time`, `duration_ms`) plus
the consumer's own `consumer_received_at`, `consumer_dispatch_done_at`,
`dispatch_outcome`, `dispatch_raw_file_id`, `dispatch_error` and
`dispatch_duration_ms`. Filtered events are kept, so the file is the full
stream; duplicates are dropped so they do not corrupt timing math.

`dispatch_outcome` vocabulary (also listed in `variables.yml`
`classifier_outcomes`): `dispatch_chunker` (the only one that triggers
work), `skip_not_put`, `skip_non_success`, `skip_wrong_bucket`,
`skip_backend_layer`, `skip_manifest_write`, `skip_chunk_write`,
`skip_other`, `skip_invalid`.

DuckDB view for the metric queries (what `metrics.py` builds):

```python
import duckdb
con = duckdb.connect()
con.sql("""
  CREATE VIEW events AS
  SELECT * FROM read_json_auto(
    '.runbook-state/events.jsonl',
    format='newline_delimited', ignore_errors=true)
""")
```

`ignore_errors=true` lets DuckDB run against the live file while the
consumer writes; it skips a partial trailing line. "Rerun the query and the
numbers grow" is a legitimate live-demo move.

Schema facts:
- `bytes_in` on a gateway `s3:PutObject` event is the object size (upload body).
- The join `backend.req_id = gateway.req_id` is the basis of every per-peer
  number: per-peer acknowledgement lag is
  `backend.event_time - gateway.event_time` within one request.
- One backend event per peer: a write to `demo-meta` produces two backend
  events (both sync peers); a read through `demo-data` produces a backend
  event for each peer the request touched.

### 4a. What the event log does NOT capture

Read-triggered cache fills are not reported as `s3:PutObject` events, even
though they physically write to the cache peer. The log for a cold read
contains the `s3:GetObject` gateway event and its backend events, and no
PUT against the cache bucket.

**Implication:** M-COST (the egress / replication-bytes story) cannot be
computed from the event log. It is computed from per-object placement
(`locate` / `list`), see section 5.4. The log remains the right source for
M-HEADLINE (metadata sync writes, which do emit PUTs), M-PIPELINE (ingest
dispatch) and M-SCALE (read latency).

---

## 5. Metrics

`metrics.py` runs a schema-profile pass first (the fields actually present,
action / bucket / outcome distributions; it says so when a field it relies
on is missing instead of printing a confident wrong number), then
M-HEADLINE, M-PIPELINE, M-SCALE and an event-volume sanity table. M-COST is
a separate mode fed a per-object placement listing. Policy and peer names
come from `variables.yml`.

```
uv sync --extra demo
uv run python docs/demo_runbook/metrics.py
uv run python docs/demo_runbook/metrics.py --events PATH --config PATH
uv run python docs/demo_runbook/metrics.py --m-cost LOCATIONS.json --raw-size BYTES [--cache-peer ALIAS]
```

The MCP server exposes two of these to the agent:
`onebucket-demo:get_replication_metrics(raw_file_id)` (M-HEADLINE and
M-PIPELINE for one document: `total_elapsed_seconds`, `chunk_event_count`,
`metadata_event_count`, `per_peer_ack_latency[<alias>]` with count / median
/ p95, `metadata_quorum_ack_ms`) and
`onebucket-demo:get_chunk_read_metrics(start_time, end_time, raw_file_id)`
(M-SCALE for a time window: `duration_ms` stats, `per_policy`,
`per_first_responder`).

### 5.1 M-HEADLINE: per-peer ack lag on metadata strict-sync writes

**Filter:** gateway PUTs against the metadata policy only, the strict-sync
(2 of 2) OneBucket where the LanceDB writes land. Every such request has two
backend events, so peer-ack lag is well defined. `demo-data` has quorum 1,
so the metric would be meaningless there.

```sql
WITH gw AS (
  SELECT req_id, key, bytes_in, event_time
  FROM events
  WHERE layer = 'gateway' AND action = 's3:PutObject'
    AND bucket = 'demo-meta' AND status_code = 200
)
SELECT b.endpoint AS peer, count(*) AS puts,
       round(quantile_cont(epoch_ms(b.event_time::timestamp) - epoch_ms(gw.event_time::timestamp), 0.5)) AS p50_ms,
       round(quantile_cont(epoch_ms(b.event_time::timestamp) - epoch_ms(gw.event_time::timestamp), 0.95)) AS p95_ms
FROM gw JOIN events b ON b.req_id = gw.req_id AND b.layer = 'backend'
GROUP BY peer ORDER BY peer;
```

Example values from one run (two sessions, 3 and 6 metadata PUTs): the AWS
peer acked at p50 36 to 43 ms, the Wasabi peer at p50 147 to 176 ms; a
600 KB Lance fragment took about 240 ms on AWS and 550 ms on Wasabi. Both
peers return 200 on every metadata PUT, which is the 2 of 2 strict sync
made visible. The Wasabi peer runs 3 to 4 times the AWS peer at p50
(geographically further from the gateway). The lag includes any cross-host
clock skew, so label it "ack lag as observed by OneBucket", not "pure
network latency".

### 5.2 M-PIPELINE: chunker and embedder latency

- **Chunker latency** = `event_time(MANIFEST PUT) - event_time(raw PUT)`.
- **Embedder latency** = `event_time(first metadata PUT after the manifest) - event_time(MANIFEST PUT)`.

All three are gateway events. `metrics.py` matches the manifest to the raw
PUT through `dispatch_raw_file_id` (the id the consumer computed when it
dispatched the chunker) and prints one row per ingested document.

Example values from one run (single laptop, one ingest at a time):

| Doc | raw size | chunks | chunker | embedder | total raw to searchable |
|---|---:|---:|---:|---:|---:|
| 10-K, ~410 KB | 413 KB | 223 | 15.8 s | 141.5 s | 157.3 s |
| 10-K, ~1.6 MB | 1.57 MB | 876 | 54.2 s | 162.2 s | 216.4 s |

The embedder scales sub-linearly (about 4 times the chunk count ran only
about 15 % longer; the batch-embed step is well parallelised). The chunker
scales roughly linearly with raw size. The tool description's "large 10-Ks:
up to 8 min for embedder" envelope is conservative.

Note: a `check_ingest_status` poll may report `ready` slightly before the
first metadata PUT lands in the log; `rows_committed` increments as the
embedder writes, marginally ahead of the event. Both are honest signals of
different boundaries.

### 5.3 M-SCALE: chunk read latency by OneBucket

The gateway `duration_ms` of every successful `s3:GetObject` on a chunk
file, grouped by the OneBucket the read went through. This is the storage
layer's own number for the request, independent of Claude Desktop and MCP
overhead.

```sql
SELECT bucket, count(*) AS n,
       round(quantile_cont(duration_ms, 0.5)) AS p50,
       round(quantile_cont(duration_ms, 0.95)) AS p95,
       max(duration_ms) AS mx
FROM events
WHERE layer = 'gateway' AND action = 's3:GetObject'
  AND status_code IN (200, 206) AND key LIKE 'chunks/%.txt'
GROUP BY bucket ORDER BY bucket;
```

`get_chunk_read_metrics` adds `per_first_responder`: for each read, the
backend event with status 200 and the earliest `event_time` names the peer
that served it, which is how a read is classified as a cache hit or a
source read after the fact.

Example values from one run: a warm read (served by the cache peer) at
about 56 to 90 ms p50; a cold read (cache miss, served by the sync peer
while the cache fills) at about 330 to 490 ms. The ratio, 4 to 9 times, is
the architectural signal; the absolutes depend on where the operator sits
relative to the backing buckets.

### 5.4 M-COST: egress avoided by lazy tiering

**Per-document metric.** R = size of the parent raw file. T = sum of sizes of
that document's chunks whose placement includes the cache peer. Then
`T/R` = transferred fraction; `1 - T/R` = egress avoided.

Inputs: `R` from `onebucket-demo:ingest_document` (`size_bytes`) or a
`data.onebucket.io:head` on the raw key; the per-object placement from
`data.onebucket.io:list` (or one `locate` per chunk) under
`chunks/<raw_file_id>/`, saved as JSON and passed to
`metrics.py --m-cost LOCATIONS.json --raw-size R`. The listing is expected
as a list (or an `objects` array) of records with `size` and `locations`.

Outcome-based ("data placement now") rather than operation-based ("events
fired"), which section 4a forces anyway, since read-fills do not emit PUTs.

Example values from one run (one 404 KB 10-K with 211 chunks):

| Read pattern | Chunks warm | T | T/R | egress avoided |
|---|---:|---:|---:|---:|
| single-chunk probe (2 reads) | 2 / 211 | 2,485 B | 0.62 % | 99.4 % |
| 1 multi-hop question (5 searches, 18 reads) | 18 / 211 | 41,689 B | 10.31 % | 89.7 % |
| 2 multi-hop questions (cumulative) | 32 / 211 | 71,684 B | 17.73 % | 82.3 % |
| freshly ingested, never read (a 1.57 MB filing) | 0 / 876 | 0 B | 0.00 % | 100.0 % |

The progression (0.62 % to 10.31 % to 17.73 %) is the key finding: **T/R is
workload-cumulative. It rises monotonically as more questions are asked and
is bounded well below 100 % because most of a corpus is never queried.**

### 5.5 Event-log sanity checks

```sql
SELECT date_trunc('minute', event_time::timestamp) AS minute,
       count(*) AS events,
       count_if(dispatch_outcome = 'dispatch_chunker') AS dispatched,
       count_if(dispatch_outcome LIKE 'skip_%') AS skipped
FROM events GROUP BY 1 ORDER BY 1 DESC LIMIT 30;

SELECT bucket, layer, action, count(*) FROM events GROUP BY 1, 2, 3 ORDER BY 4 DESC;
```

`dispatch_chunker` count should equal the number of valid raw uploads;
everything else is correctly skipped.

---

## 6. Architecture comparison and TCO

Framing assumption: **multi-site is a requirement** (DR, data residency,
regional read latency). Single-site RAG is out of scope. The labelling
below is the runbook's (Act 3, Tables A and B).

### 6.0 The three architectures

- **A. Eager replication.** Copy every chunk to every region at ingest time
  (S3 cross-region replication, a globally replicated table for the index,
  or both). Reads are always local. You pay full storage twice and full
  egress on every byte at ingest, whether anyone reads it or not.
- **B. VectorDB-stores-chunks.** A vector database holds both the
  embeddings and the chunk text inline; the DB replicates the index, so
  reads are local. Chunks now live in attached storage on the DB nodes,
  5 to 10 times the price of object storage, plus DB compute hours whether
  queries flow or not. This is what most production RAG looks like today.
- **C. Lazy-tier (OneBucket, this demo).** The index strict-syncs across
  sites (the part that must be consistent everywhere). Chunks live on
  object storage and tier lazily on read. You pay storage once on the
  primary plus the fraction that gets cached at the second site after
  queries warm it.

The novel choice in C: treating chunks and metadata as *separate
replication policies* inside one storage abstraction. Neither alternative
separates the two.

### 6.1 Table A: per-document comparison [PROVEN at unit for column C]

Anchor values per 10-K-sized document, example values from one run: 404 KB
raw, 211 chunks, about 461 KB total chunks, about 611 KB metadata per
ingest. Column C numbers were measured; A and B are typical ranges from
documented behaviour, label them "expected range" in a deck.

| Per-document metric | A. Eager replication | B. VectorDB-stores-chunks | C. Lazy-tier (OneBucket) |
|---|---:|---:|---:|
| Bytes crossing on ingest | T_total + M (~1.07 MB) | T_total + M (into the DB) | M only (~611 KB) |
| Replication latency added to ingest | per-chunk sync, ~150 ms x 211 = +32 s | full document indexed into the DB at ingest, seconds | none on chunks; ~0.5 s ack on metadata |
| Cold chunk fetch from 2nd site | ~warm latency (always local) | ~warm latency (always in DB) | cold latency on the first read of each chunk |
| Warm chunk fetch from 2nd site | ~warm latency | ~warm latency | ~warm latency (measured 56 to 90 ms) |
| Bytes crossing per query | 0 | 0 | 0 after warm |
| Storage tier for chunks | object storage x 2 regions | attached DB storage (5 to 10 x object price) | object storage x 1 region + lazy cache |
| Pay for idle vector-DB compute? | no | yes (DB nodes 24/7) | no |

### 6.2 Table B: 1 PB extrapolated annual cost [EXTRAPOLATED]

Assumptions (from `variables.yml` `operator_intent.pricing` and
`operator_intent.workload`; state them to the audience): raw 1 PB; chunks
1.15 PB (15 % overlap); metadata about 10 TB; year-one read fraction 30 %
(researched band 20 to 40 %, see `research/findings_summary.md`); egress
$0.09/GB; object storage on the second site $5.99/TB-month; vector-DB
attached storage about $100/TB-month; vector-DB idle compute about $240 K/yr
at 1 PB scale; 10 M queries/year.

| Annual cost line | A. Eager replication | B. VectorDB-stores-chunks | C. Lazy-tier (OneBucket) |
|---|---:|---:|---:|
| Chunk bytes stored on 2nd site | 1.15 PB object storage | 1.15 PB attached DB storage | ~345 TB (30 %) object storage |
| Storage cost on 2nd site / yr | ~$1.0 M | ~$1.4 M | ~$25 K |
| VectorDB compute / yr | $0 | ~$240 K | $0 |
| Ingest egress (one-time seed) | ~$104 K | ~$104 K | ~$900 |
| Read egress, 30 % of chunks propagating | $0 | $0 | ~$31 K (one-time over year one) |
| **Year-one total on 2nd site** | **~$1.10 M** | **~$1.74 M** | **~$57 K** |
| **Year-two+ steady state / yr** | **~$1.0 M / yr** | **~$1.64 M / yr** | **~$25 K / yr** |

If eager replication is implemented on Wasabi-priced storage rather than
S3-priced storage, column A's storage drops to about $83 K/yr, still over
3 times lazy-tier. Present both pricing scenarios for honesty.

### 6.3 Honest reading

- **Year one: lazy-tier is about 5 % of eager's cost.** Most of the gap is
  storage; eager pays to store 1.15 PB that mostly never gets touched.
- **Year two onward: lazy-tier is about 2.5 % of eager.** Egress is behind
  you once the warm set stabilises; storage is the recurring cost.
- **Column B's bill is compute plus premium storage.** It buys local reads
  and a single vendor, at the price of paying for DB nodes whether queries
  flow or not.
- **Where eager wins: day-one DR.** If the primary dies the instant the
  corpus is ingested, eager has a complete second copy; lazy-tier has only
  the warm set (about 30 % at the end of year one). Ask the audience: "total
  loss of primary on day one, or graceful degradation while you rebuild?"
  Most enterprise RAG is the latter.
- **Robustness:** even at a 100 % read fraction (worst case), lazy-tier's
  storage cost asymptotically approaches eager's, but it got there by reads
  paying their own way, never by paying upfront for bytes that might never
  be queried.

### 6.4 Dashboard layout (deck slide)

```
                          A. Eager     B. VectorDB   C. Lazy-tier (OneBucket)
chunk bytes crossing      100 %        100 %         ~10 to 18 % (read-driven)
warm-read latency (2nd)   ~90 ms       ~90 ms        ~56 to 90 ms (measured)
chunk storage tier        object x 2   attached DB   object x 1 + cache
metadata sync model       async        DB-managed    strict 2 of 2
annual cost @ 1 PB (Y2+)  ~$1.0 M      ~$1.64 M      ~$25 K
```

---

## 7. Independent ground-truth probe

`locate` is OneBucket describing itself. To prove the abstraction is
truthful, `verify_onebucket_tiering.py` lists each backing bucket of a
policy over plain S3 with boto3, using the `direct_access` profile of each
endpoint in `variables.yml`, and compares: every sync peer must hold the
full set under the prefix, and a cache peer holds the subset that has been
read. Pointed at `demo-meta` with `--prefix metadata/` it is the
always-in-sync control (both sides identical).

```
uv run python docs/demo_runbook/verify_onebucket_tiering.py --raw-file-id <id>
uv run python docs/demo_runbook/verify_onebucket_tiering.py --policy demo-meta --prefix metadata/
```

The operator runs it locally; credentials come from the AWS CLI profiles
and are never pasted into chat. Endpoints without a `direct_access` block
are reported as skipped.

---

## 8. Index-state preflight and orphan chunks

Before a demo, confirm what is indexed: `onebucket-demo:list_documents`
(one entry per `raw_file_id` with `source_uri` and `chunk_count`). If the
demo question needs a document that is not present, ingest it or pivot the
question.

There may be **orphan chunk folders** in the bucket: `chunks/<raw_file_id>/`
prefixes whose id is not in `list_documents` (partial-ingest leftovers whose
LanceDB rows were never committed). Harmless, but they inflate any
corpus-bytes calculation. Clean-up is `onebucket-demo:rebuild_index` (wipes
and re-derives everything from `raw/`) or `delete_document` for one id; the
demo agent should not delete objects piecemeal through the data connector.

---

## 9. Known issues

Tracked here and kept out of the live demo.

### 9a. Placement reporting lags the read

The placement reported by `list` / `locate` can lag the read that warmed a
chunk by seconds to minutes. Ground truth for "is the metadata tier in
sync" is the M-HEADLINE PUT events (two backend acks per write), and for
"is the cache filling" it is the direct listing from
`verify_onebucket_tiering.py` or the warm-read latency itself. The runbook
tells the agent not to poll placement mid-act for this reason.

### 9b. Chunk-size variance on real 10-Ks

The chunker can emit very large chunks from documents containing big
tabular schedules (one 1.57 MB filing produced a single 221 KB chunk among
875 normal 1 to 3 KB chunks). It breaks nothing but skews per-chunk size
distributions; relevant for any per-chunk metric.

---

## 10. Portability: `variables.yml` and the config tools

All deployment-specific values live in `variables.yml` (schema and
comments in `variables.example.yml`; loading fails closed while any
`<placeholder>` remains). Three MCP tools on `onebucket-demo` manage it:

- `get_runbook_config`: returns the parsed config, `missing_required`,
  `validation_errors`. Call at session start.
- `set_runbook_variable`: edit one value by dotted path (intended for
  `operator_intent.*`; editing deployment facts here does not change the
  OneBucket console).
- `validate_runbook_config`: cheap re-check between phases.

`daemon.mode` selects the pipeline driver: `onebucket-events` (the event
consumer polling `events.onebucket.io`), `local-daemon` (the polling
daemon) or `lambda` (the AWS chain). See `variables.example.yml` for the
full schema and the operational notes (the `use_region_toggle` rule, the
quorum / required coupling).
