# Polling daemon runbook (`local-daemon` mode)

The polling daemon (`scripts/oneb_pipeline_daemon.py`) is the simplest
trigger: a long-running process on your machine lists `raw/` every few
seconds and runs the chunker and the embedder on anything new. It works
against any S3-compatible bucket, needs nothing but an S3 key, and is the
default mode. It is the right first step with a OneBucket before you sign
in to the events server, and the only local option for a plain AWS or
Wasabi bucket.

## Starting

```bash
export AWS_PROFILE=my-onebucket-profile
export AWS_ENDPOINT_URL=https://s3.onebucket.io        # omit for plain AWS S3
export ONEBUCKET_CHUNK_ROOT=s3://my-data-onebucket/chunks
export ONEBUCKET_DATASET_ROOT=s3://my-meta-onebucket/metadata
bash scripts/start-pipeline-daemon.sh                   # add --poll-interval 10 to poll less often
```

The wrapper validates the variables and `exec`s the daemon so Ctrl-C reaches
it directly. Expect:

```
==> Starting OneBucket pipeline daemon
    ...
[2026-11-06T12:00:00Z] [startup] watching s3://my-data-onebucket/raw/ every 5.0s
[2026-11-06T12:00:01Z] [startup] reconstructed processed set: 0 raw files already manifested in chunks/
[2026-11-06T12:00:01Z] [idle] no new files; sleeping 5.0s
```

Leave it running in that terminal.

### What "already processed" means

The daemon keeps no state of its own. At startup it reads every
`chunks/<id>/MANIFEST.json` in the bucket and collects their
`raw_object_key` fields; a `raw/` object whose key appears there is done.
So a restart never re-processes a document, and a document whose manifest
you delete is processed again on the next poll. A `raw/` object that
raised during processing is remembered for the life of the process so the
daemon does not loop on it; restart the daemon (or fix and re-upload) to
retry it.

### Smoke test

```bash
aws s3 cp data/samples/finance_report.txt s3://my-data-onebucket/raw/ --endpoint-url "$AWS_ENDPOINT_URL" --profile "$AWS_PROFILE"
```

Within one poll interval: `[found] 1 new raw file(s)`, `[processing]`,
`[chunked] ... chunks=2`, `[embedded] ... rows_committed=2`.

## Stopping

Ctrl-C. The signal handler removes the heartbeat file and exits. If a
`kill -9` left the file behind, the MCP server reports `DAEMON_NOT_RUNNING`
after 30 seconds anyway (the `last_seen` timestamp ages out); delete it by
hand if you want that sooner:

```bash
rm -f /tmp/oneb-pipeline-daemon.heartbeat
```

## Health

```bash
python3 -m json.tool /tmp/oneb-pipeline-daemon.heartbeat
```

`mode` is `local-daemon`; `last_seen` is refreshed on every poll; `daemon_state`
is `processing` with `current_raw_file_id` set while a document is being
embedded; `processed_count` counts documents manifested, including those
found at startup. Every tenth poll logs a `[health]` line confirming the
bucket is reachable.

The daemon and the MCP server must run on the **same machine**: the server
checks the daemon through this file, not over the network. To move the
file (for example if `/tmp` is not persistent), set
`ONEBUCKET_DAEMON_HEARTBEAT_PATH` for **both** processes.

## Log line reference

| Tag | Meaning |
|---|---|
| `[startup]` | bucket being watched, poll interval, processed-set size |
| `[idle]` | nothing new; sleeping |
| `[found]` | new `raw/` objects detected |
| `[processing]` / `[chunked]` / `[embedded]` | the phases for one document |
| `[skip]` | gate rejected the file (type, size, empty) |
| `[error]` | chunking or embedding raised; the key is not retried until restart |
| `[health]` | periodic reachability check |
| `[warning]` | a Lambda notification exists on `raw/` (two modes active), or a manifest could not be read |

## Troubleshooting

**`DAEMON_NOT_RUNNING` from `ingest_document`.** No fresh heartbeat. Start
the daemon, or check that `ONEBUCKET_DAEMON_HEARTBEAT_PATH` is the same for
the daemon and the MCP server.

**`DAEMON_WATCHING_WRONG_BUCKET`.** The daemon's heartbeat names a
different bucket than the MCP server's `ONEBUCKET_CHUNK_ROOT`. Restart one
of them with the right value; otherwise uploads land where nobody looks.

**Uploads are never picked up.** Confirm the key is under `raw/` and ends in
`.txt` or `.md`; confirm the daemon's `AWS_ENDPOINT_URL` matches where you
uploaded (OneBucket vs AWS); look for `[skip]` lines.

**`merge_insert` or `open_table` fails.** The index was not initialised at
`ONEBUCKET_DATASET_ROOT`: run `scripts/init_index.py` against it once.

**A warning about a Lambda notification on `raw/`.** The bucket also has
the `lambda` mode wired; both would process every upload. Remove one
([aws-prerequisites.md](../aws-prerequisites.md) shows how to delete the
notification) or stop the daemon.

## Cost note

Polling is one `ListObjectsV2` call per interval. At the default five
seconds that is about 17,000 calls a day, which on AWS S3 is a few cents and
on OneBucket shows up as `s3:ListObjectsV2` events in the activity log.
Raise `--poll-interval` (or `ONEBUCKET_DAEMON_POLL_INTERVAL`) if that
matters; the event consumer avoids it entirely.

Design record: [ADR-013](../../architecture/decisions/ADR-013-execution-modes.md).
