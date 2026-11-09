# Event consumer runbook (`onebucket-events` mode)

The event consumer (`scripts/oneb_event_consumer.py`) is the pipeline's
trigger when storage is a OneBucket: it long-polls OneBucket's **events**
MCP server, and for every successful `PutObject` under `raw/` in the
watched OneBucket it runs the chunker and the embedder, the same two
functions every other mode calls. Along the way it records every event it
sees in a JSONL log, which is where the demo's replication numbers come
from.

## How it works, in one minute

- **Auth.** OneBucket's MCP servers use OAuth 2.1. `scripts/onebucket-login.py`
  registers a public client, opens your browser once, and stores an access
  token and a refresh token at `~/.config/onebucket/token.json` (readable by
  you only). Access tokens are short-lived; the consumer refreshes them
  before they expire and never prints one.
- **Polling.** `tail_events(cursor, limit, waitSeconds)` returns events
  that arrived after the cursor, oldest first, holding the request up to
  `waitSeconds` when there are none. The consumer persists the cursor in
  `.runbook-state/events.cursor` after each fully processed batch, so a
  restart resumes where it stopped.
  When events arrive while a poll is waiting, the server answers with a
  server-sent-event stream instead of plain JSON: one
  `notifications/storage/event` message per event, then the tool result
  (which repeats them and carries the next cursor). The consumer reads the
  result and ignores the notifications.
- **Which events count.** A request to a OneBucket produces one `gateway`
  event (what the client did) plus one `backend` event per peer bucket
  (sharing a `reqId`). The consumer dispatches on the gateway `PutObject`
  under `raw/` only, so an upload that fanned out to three peers still runs
  the pipeline once. Backend events are kept in the log for telemetry.
- **Replay safety.** If the process dies mid-batch, the batch is delivered
  again; a ring buffer of seen event ids plus the pipeline's
  content-addressed, idempotent writes make the replay harmless. A document
  whose dispatch keeps failing is given up after three attempts and logged;
  a catch-up scan picks it up later.

## Prerequisites

1. A OneBucket to write to and the index initialised at the metadata
   OneBucket ([onebucket-setup.md](../onebucket-setup.md)).
2. An AWS CLI profile holding your OneBucket **S3** access key.
3. A one-time sign-in to the events server:

   ```bash
   uv run python scripts/onebucket-login.py
   ```

   It prints the sign-in URL (and opens it), waits for the callback on
   `127.0.0.1`, and reports where it stored the token. Use
   `--events-url https://events.<region>.onebucket.io` to sign in to a
   regional cluster; the token is bound to that one hostname.

## Starting

```bash
export AWS_PROFILE=my-onebucket-profile
export ONEBUCKET_CHUNK_ROOT=s3://demo-data/chunks
export ONEBUCKET_DATASET_ROOT=s3://demo-meta/metadata
# optional, to pin a region (default: the global hostnames)
# export AWS_ENDPOINT_URL=https://s3.us-ashburn-1.onebucket.io
# export ONEBUCKET_EVENTS_URL=https://events.us-ashburn-1.onebucket.io
bash scripts/start-event-consumer.sh
```

The wrapper validates the variables, fills in the global hostnames for
anything unset (and says so), checks the token file exists, and `exec`s the
consumer so Ctrl-C reaches it directly. Expect:

```
==> Starting OneBucket event consumer (onebucket-events mode)
    ONEBUCKET_EVENTS_URL     = https://events.onebucket.io
    ...
[2026-11-06T12:00:00Z] [event_log] writing to /path/to/repo/.runbook-state/events.jsonl
[2026-11-06T12:00:00Z] [startup] tailing https://events.onebucket.io for OneBucket='demo-data'; wait=60s limit=100
[2026-11-06T12:00:01Z] [connected] onebucket-synapse v0.10.11 session established
```

Options: `--catchup-on-start` (dispatch every `raw/` object that has no
manifest yet before tailing), `--reset-cursor` (ignore the stored cursor
and start from now), `--wait-seconds N` (long-poll length, 0 to 360).

### Smoke test

```bash
echo "hello $(date)" > /tmp/smoke.txt
aws s3 cp /tmp/smoke.txt s3://demo-data/raw/smoke.txt --endpoint-url https://s3.onebucket.io --profile my-onebucket-profile
```

Within one poll cycle the consumer logs `[batch] N event(s)`,
`[processing] raw/smoke.txt`, `[chunked] ... chunks=1` and
`[embedded] ... rows_committed=1`. The last line of the event log is the
gateway `PutObject` with `dispatch_outcome: "dispatch_chunker"`:

```bash
tail -1 .runbook-state/events.jsonl | python3 -m json.tool
```

## Stopping

Ctrl-C once: the current batch finishes, the heartbeat file is removed,
the process exits. Ctrl-C twice: immediate exit. From elsewhere:
`kill -TERM $(pgrep -f oneb_event_consumer.py)`.

## Health

The heartbeat file (`/tmp/oneb-pipeline-daemon.heartbeat`, or
`ONEBUCKET_DAEMON_HEARTBEAT_PATH`) is rewritten every ten seconds and after
every dispatch. The MCP server reads it before accepting an upload.

```bash
python3 -m json.tool /tmp/oneb-pipeline-daemon.heartbeat
```

| Field | Meaning |
|---|---|
| `mode` | `onebucket-events` |
| `connected` | an MCP session with the events server is open |
| `auth_state` | `ok`, or `expired` / `refresh_failed` when the sign-in is no longer valid (the consumer has exited) |
| `last_seen` | when the file was last written; the MCP server treats older than 30 s as dead |
| `daemon_state`, `current_raw_file_id` | `processing` while a document is being chunked and embedded |
| `events_received` / `events_dispatched` / `events_skipped` | counters since start |
| `cursor_present` | whether a persisted cursor exists |

## The event log

One JSON object per line in `.runbook-state/events.jsonl` (or
`ONEBUCKET_EVENT_LOG_PATH`). Fields: `consumer_received_at`,
`consumer_dispatch_done_at`, `event_id`, `req_id`, `layer`
(`gateway`/`backend`), `endpoint` (backend only), `action`, `bucket`,
`key`, `status_code`, `bytes_in`, `bytes_out`, `event_time`, `duration_ms`,
`dispatch_outcome`, `dispatch_raw_file_id`, `dispatch_error`,
`dispatch_duration_ms`. `dispatch_outcome` is one of `dispatch_chunker`,
`skip_not_put`, `skip_non_success`, `skip_wrong_bucket`,
`skip_backend_layer`, `skip_manifest_write`, `skip_chunk_write`,
`skip_other`, `skip_invalid`.

Two MCP tools read it: `get_replication_metrics(raw_file_id)` (per-peer
acknowledgement latency for one ingest, from backend-minus-gateway times
within each request) and `get_chunk_read_metrics(start_time, end_time,
raw_file_id)` (chunk fetch latency in a window). `docs/demo_runbook/metrics.py`
prints the same numbers from the shell (`uv sync --extra demo` first).

## Troubleshooting

**`auth_state` is `expired` or `refresh_failed`, process exited with code 3.**
The stored sign-in is no longer valid (token revoked, refresh rejected, or
the events URL changed since the login). Sign in again and restart:

```bash
uv run python scripts/onebucket-login.py
bash scripts/start-event-consumer.sh
```

The MCP server answers `DAEMON_AUTH_EXPIRED` to `ingest_document` while the
heartbeat shows this state, so an upload is never accepted into a dead
pipeline.

**`token file ... is readable by group/others`.** The consumer refuses a
token file that other users could read. `chmod 600` it, or re-run the login,
which recreates it correctly.

**`stored cursor rejected by the server; resetting to now`.** The cursor
file held a token the server no longer accepts (for example after a long
outage). Tailing restarts from now, which means uploads made while the
consumer was down were **not** replayed. Reconcile with a catch-up scan:
`kill -USR1 $(pgrep -f oneb_event_consumer.py)`, or restart with
`--catchup-on-start`.

**`[disconnect]` lines with growing retry delays.** The events server or
the network is unreachable; the consumer backs off exponentially (with
jitter) up to a minute and reconnects when it can. `connected` is `false`
meanwhile, and `ingest_document` still accepts uploads: they will be
processed once the stream resumes (or by a catch-up scan).

**Events arrive but `dispatch_outcome` is `skip_wrong_bucket`.** The
consumer watches the OneBucket named in `ONEBUCKET_CHUNK_ROOT`. Uploads to
another OneBucket are logged and ignored.

**`[error] processing raw/... failed`.** The chunker or embedder raised;
the error is in the log line and in `dispatch_error`. Usual causes:
`AWS_ENDPOINT_URL` not pointing at OneBucket (boto3 asks AWS for a bucket
that does not exist there), the index missing (`init_index.py` not run
against the metadata root), the S3 key in `AWS_PROFILE` lacking write
permission on the OneBucket. After three failed attempts the consumer moves
on; fix the cause and run a catch-up scan.

**Only one consumer at a time.** The polling daemon and the event consumer
write the same heartbeat file and would both process every upload. Stop one
before starting the other.

## Resetting state

```bash
pkill -f oneb_event_consumer.py
rm -f .runbook-state/events.cursor          # start tailing from now
rm -f .runbook-state/events.jsonl           # discard the telemetry log
rm -f /tmp/oneb-pipeline-daemon.heartbeat   # only if a crash left it behind
```

The token file is independent of all of this; delete
`~/.config/onebucket/token.json` only to force a fresh sign-in.
