# Execution modes

The pipeline has one implementation of its two phases
(`pipeline.chunk_and_write`, `pipeline.embed_and_commit`). The demo runs
them locally, triggered one of two ways; the same functions can also be
hosted elsewhere, and the repository ships AWS Lambda as a worked example.
Storage is always object storage; only the trigger and the place the code
runs differ. `ONEBUCKET_EXECUTION_MODE` tells the MCP server which one is
active so `ingest_document` can tell you when nothing is listening.

The live OneBucket demo uses **`onebucket-events`**. `local-daemon` is the
simplest way to start and works with any S3 bucket. `lambda` is optional
and is not part of the OneBucket demo.

## At a glance

| | `local-daemon` *(default)* | `onebucket-events` *(the demo)* | `lambda` *(optional example)* |
|---|---|---|---|
| Trigger | polls `raw/` every N seconds | long-polls OneBucket's events server (`tail_events`) | S3 ObjectCreated notifications |
| Runs on | your laptop | your laptop | AWS Lambda in your account |
| Works with | any S3 bucket or OneBucket | OneBucket only | AWS S3 only |
| Setup | export four variables, start a script | one browser sign-in, then the same | `deploy_aws.sh`, 10 to 20 minutes the first time |
| Trigger latency | up to the poll interval (5 s) | one poll cycle, usually seconds | about a second |
| Telemetry | none | per-peer replication timing in the event log | CloudWatch |
| Concurrency | one document at a time | one document at a time | Lambda scales out |
| Needs a long-running process | yes | yes | no |

Run **one mode per bucket at a time**. Both local modes write the same
heartbeat file and would both process every upload. A Lambda deployment's S3
triggers keep firing until you remove them, even when you have switched to
a local mode; the polling daemon warns at startup if it finds one on `raw/`,
and [aws-prerequisites.md](aws-prerequisites.md#cleanup) shows how to
remove it.

## Choosing

```
Is the bucket a OneBucket?
  no  -> local-daemon (plain S3 on a laptop) or lambda (serverless, BYO AWS account)
  yes -> onebucket-events for the full story (replication telemetry, event-driven);
         local-daemon if you just want to get going with an S3 key and no sign-in
```

## Per-mode setup

Common to all three: an index initialised once with
`uv run python scripts/init_index.py <dataset-root>`, and the MCP server's
`env` block in Claude Desktop carrying the same variables you export in the
shell (the server is a separate process; your shell does not reach it).

### `local-daemon`

```bash
export AWS_PROFILE=my-onebucket-profile
export AWS_ENDPOINT_URL=https://s3.onebucket.io        # omit for plain AWS S3
export ONEBUCKET_CHUNK_ROOT=s3://my-data-onebucket/chunks
export ONEBUCKET_DATASET_ROOT=s3://my-meta-onebucket/metadata
bash scripts/start-pipeline-daemon.sh                   # leave running
```

Claude Desktop `env`: the four variables above plus
`"ONEBUCKET_EXECUTION_MODE": "local-daemon"`. Runbook:
[runbooks/local-daemon.md](runbooks/local-daemon.md).

### `onebucket-events`

```bash
uv run python scripts/onebucket-login.py                # once per machine; opens the browser
export AWS_PROFILE=my-onebucket-profile
export ONEBUCKET_CHUNK_ROOT=s3://my-data-onebucket/chunks
export ONEBUCKET_DATASET_ROOT=s3://my-meta-onebucket/metadata
bash scripts/start-event-consumer.sh                    # leave running
```

The wrapper fills in the global hostnames for `AWS_ENDPOINT_URL`
(`https://s3.onebucket.io`) and `ONEBUCKET_EVENTS_URL`
(`https://events.onebucket.io`) when they are unset and says so; export
them yourself to pin a region (list at https://www.onebucket.io/support).
Claude Desktop `env`: the roots, `AWS_PROFILE`, `AWS_ENDPOINT_URL`,
`"ONEBUCKET_EXECUTION_MODE": "onebucket-events"`, and
`ONEBUCKET_EVENTS_URL` if you pinned one. Runbook:
[runbooks/event-consumer.md](runbooks/event-consumer.md).

### `lambda` (optional: running the compute elsewhere)

Any platform that can host a Python function and call it on "new object
under `raw/`" and "new `MANIFEST.json`" can run the pipeline; a GPU cloud or
serverless provider with an HTTP API works the same way: point
`dispatch_raw_object` in `scripts/oneb_event_consumer.py` (the one place the
consumer calls the pipeline) at your endpoint. The repository's worked
example is AWS Lambda against plain AWS S3:

```bash
bash scripts/deploy_aws.sh --bucket my-rag-bucket --region us-east-1 --profile my-aws-profile
```

Claude Desktop `env`: `AWS_PROFILE`, the two roots on the deployed bucket,
`"ONEBUCKET_EXECUTION_MODE": "lambda"`. No `AWS_ENDPOINT_URL`. Guide:
[aws-deployment-runbook.md](aws-deployment-runbook.md).

## What is the same in every mode

- The MCP tools, their arguments and return shapes.
- The key layout: `raw/<file>`, `chunks/<raw_file_id>/chunk_NNNN.txt`,
  `chunks/<raw_file_id>/MANIFEST.json`, `metadata/global_index.lance/`.
- The gates: `.txt` and `.md` only, 10 MB maximum.
- The index schema and the canary check.
- The heartbeat file both local consumers write
  (`/tmp/oneb-pipeline-daemon.heartbeat`, or `ONEBUCKET_DAEMON_HEARTBEAT_PATH`).

## Switching modes

Stop the running consumer (Ctrl-C; the heartbeat file is removed), start the
other one, change `ONEBUCKET_EXECUTION_MODE` in the Claude Desktop config,
and relaunch Claude Desktop. Switching to or from `lambda` additionally
means adding or removing the S3 notification on the bucket; the deploy
script adds it, and [aws-prerequisites.md](aws-prerequisites.md) shows how
to remove it.

Design record: [ADR-013](../architecture/decisions/ADR-013-execution-modes.md).
