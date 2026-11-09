# Demo Setup Runbook

How to take a freshly provisioned OneBucket account all the way to "the
demo is ready to run in Claude Desktop". The audience never sees any of
this; everything here is pre-demo preparation.

The on-stage demo is in [`onebucket_rag_demo_runbook.md`](onebucket_rag_demo_runbook.md).
This document is only the setup.

## Prerequisites (one-time)

On the operator's machine:

- macOS or Linux
- [`uv`](https://docs.astral.sh/uv/) installed (`brew install uv` or the official installer)
- This repo cloned and `uv sync --extra demo` run once (the `demo` extra adds DuckDB for `metrics.py`)
- A OneBucket account at <https://app.onebucket.io> with:
  - backing buckets on whichever providers you have, registered as Endpoints (console tab **Endpoints**) under any aliases; the example config uses AWS S3 and Wasabi aliased `EAST_S3` and `WEST_WASABI`. A single provider (one sync peer, no cache peers) still runs the whole pipeline and every tool; the cache-fill and cross-region segments then show nothing to measure and need a second provider
  - your OneBuckets (console tab **Policies**), named however you like and set up as described in [`onebucket-setup.md`, "What the pipeline needs from your policies"](../operations/onebucket-setup.md#what-the-pipeline-needs-from-your-policies): a data OneBucket (example `demo-data`), a metadata OneBucket kept separate and replicated eagerly to every peer (example `demo-meta`), and optionally a mirror (example `demo-west`). The scripts read the actual names from `variables.yml` (`pipeline.raw_writes_to_policy`, `chunks_writes_to_policy`, `metadata_writes_to_policy`)
  - an access key (console tab **Credentials**) for the S3 endpoint
- `docs/demo_runbook/variables.yml` created by copying `variables.example.yml` and replacing every `<placeholder>`. The file is gitignored. Loading fails closed while any placeholder remains, so nothing runs against a half-filled config.
- An AWS CLI profile holding the OneBucket access key (`aws configure --profile <name>`), named in `variables.yml` under `daemon.aws_profile`. `$AWS_PROFILE` in the shell takes precedence over that value; pass `--aws-profile` to be explicit.
- A OneBucket sign-in for the event consumer: run `uv run python scripts/onebucket-login.py` once. It opens the browser for OAuth and stores the token at `~/.config/onebucket/token.json` (mode 0600). Nothing is pasted anywhere.
- Claude Desktop installed with this repo's MCP server (`onebucket-demo`) and the two OneBucket connectors (`data.onebucket.io`, `events.onebucket.io`) registered. The full `claude_desktop_config.json` block, with every environment variable, is in the demo runbook's Prerequisites section.

## End-to-end run

### 1. Run the setup script (3 to 5 min on a clean run, about 10 s when idempotent)

```bash
cd /path/to/distributed-object-store-rag
bash scripts/demo-setup.sh
```

Phases (printed as `[N/7]` banners):

1. **Preflight**: `uv` binary, `variables.yml` load and validation (fails closed on placeholders), the OneBucket token at `~/.config/onebucket/token.json`, the AWS CLI profile
2. **fastembed cache and model pre-warm**: downloads BGE-small and SPLADE into `~/.cache/fastembed` (survives a macOS reboot, unlike `/tmp`) and warms both models
3. **LanceDB init**: creates the global index table at `s3://<metadata policy>/metadata/global_index.lance` (skipped if it already exists)
4. **Seed corpus ingest**: ingests the tickers in `operator_intent.seed_corpus_tickers` (skips tickers already present in the index)
5. **Event consumer**: starts `scripts/start-event-consumer.sh` as a detached background process and waits up to 30 s for a heartbeat with `connected: true`
6. **End-to-end verification**: `list_documents` plus one hybrid `search_global_index` query; both must return rows
7. **Ready summary**: prints the start prompt (see below) and the stop / reset commands

Common flags:

| Flag | Purpose |
|------|---------|
| `--force-rebuild` | Full reset: stop the consumer, wipe the LanceDB table and the source chunks, purge cache peers, re-ingest the seed corpus (about 10 min) |
| `--purge-cache` | Pre-demo cache reset only: leaves index, source and consumer alone, purges the cache peers so Act 2's cold-vs-warm measurement starts cold, then restarts the consumer (about 30 s) |
| `--skip-daemon` | Do not start the event consumer (for example when you already run one in a terminal) |
| `--tickers AAPL,MSFT` | Override `operator_intent.seed_corpus_tickers` |
| `--aws-profile <name>` | Override `$AWS_PROFILE` and `daemon.aws_profile` |
| `--token-file <path>` | Override `$ONEBUCKET_TOKEN_FILE` and the default `~/.config/onebucket/token.json` |

### 2. Verify from the shell (about 10 s)

```bash
python3 -m json.tool /tmp/oneb-pipeline-daemon.heartbeat   # connected: true, auth_state: ok, recent timestamp
tail -5 "${TMPDIR:-/tmp}/oneb-event-consumer-setup.log"      # no tracebacks
```

The consumer log is written to the system temp directory as
`oneb-event-consumer-setup.log`; the setup script prints the exact path
in phase 5.

### 3. Launch the demo in Claude Desktop (about 30 s)

Open Claude Desktop, start a fresh chat (Cmd-N) and paste the prompt in
[`preflight-prompt.md`](preflight-prompt.md). The setup script prints the
same prompt with absolute paths at the end of every successful run.

Only if this repo's MCP server code changed since your last run: fully
quit Claude Desktop (Cmd-Q) and relaunch first, so the `onebucket-demo`
subprocess restarts with the new code. Otherwise a fresh chat is enough.

The agent walks the pre-demo checklist and then Acts 0 to 4 from the
runbook.

## Operating the background consumer

The setup script starts the consumer detached (`start_new_session=True`),
so it survives after the script exits. Its output does not scroll past in
your terminal the way `bash scripts/start-event-consumer.sh` does; it is
captured to the log file.

**Watch live:**
```bash
tail -f "${TMPDIR:-/tmp}/oneb-event-consumer-setup.log"
```

**Health check:**
```bash
python3 -m json.tool /tmp/oneb-pipeline-daemon.heartbeat
```
Look for `connected: true`, `auth_state: "ok"` and a recent timestamp.

**Find the processes:**
```bash
pgrep -fl oneb_event_consumer
```
You will see two: the `uv run` wrapper and the Python child. That is
normal, not a duplicate consumer.

**Stop:**
```bash
pkill -f oneb_event_consumer
rm -f /tmp/oneb-pipeline-daemon.heartbeat
```

**Force-stop (if wedged):**
```bash
pkill -9 -f oneb_event_consumer
rm -f /tmp/oneb-pipeline-daemon.heartbeat
```

**Catch up on uploads that happened while the consumer was down:**
```bash
kill -USR1 $(pgrep -f oneb_event_consumer.py)
```

## Troubleshooting

### Preflight reports a different AWS profile than you expected

Precedence is `--aws-profile`, then `$AWS_PROFILE` in your shell, then
`daemon.aws_profile` in `variables.yml`. The script warns when the shell
value differs from the file. A personal profile exported globally that has
no rights on your OneBucket is the usual cause of LanceDB 403s.

### Consumer will not start, or the heartbeat stays `connected: false`

```bash
tail -50 "${TMPDIR:-/tmp}/oneb-event-consumer-setup.log"
```

Common causes:
- No token, or an expired sign-in: the consumer exits with code 3 and the heartbeat shows `auth_state: "expired"` or `"refresh_failed"`. Sign in again with `uv run python scripts/onebucket-login.py`.
- The events server is unreachable: check that `ONEBUCKET_EVENTS_URL` (default `https://events.onebucket.io`) is an `https://` URL and that the regional hostname, if you pinned one, is listed at <https://www.onebucket.io/support>.
- An old consumer still owns the heartbeat file: `pkill -f oneb_event_consumer && rm -f /tmp/oneb-pipeline-daemon.heartbeat`, then re-run.

### fastembed model download stalls or fails

```bash
rm -rf ~/.cache/fastembed
bash scripts/demo-setup.sh    # re-runs phase 2
```

### Quorum failure on the metadata policy

Your metadata OneBucket (example `demo-meta`) requires every sync peer to acknowledge every write. A 503 on
metadata writes means one backing bucket is unhealthy or misconfigured;
check the policy's peers in the console (and the `use_region_toggle`
note in `variables.example.yml`). Not fixable from this script.

### Mid-demo recovery

If the on-stage demo wedges and you need to reset between runs:

```bash
bash scripts/demo-setup.sh --force-rebuild
```

This is destructive (it wipes the LanceDB table and the chunk objects), so
run it only between demos, never live. For a cache-only reset that keeps
the index, use `--purge-cache`.

## What to capture when reporting issues

- Full stdout of the setup script (especially any `FATAL:` lines)
- Last 30 lines of the consumer log in the system temp directory
- Output of `cat /tmp/oneb-pipeline-daemon.heartbeat`
- Which phase failed (the `[N/7]` banner before the error)
- Whether the failure reproduces with `--force-rebuild`

## Related docs

- [`onebucket_rag_demo_runbook.md`](onebucket_rag_demo_runbook.md): the on-stage demo itself
- [`preflight-prompt.md`](preflight-prompt.md): the prompt to paste into Claude Desktop
- [`variables.example.yml`](variables.example.yml): the config schema (copy to `variables.yml`)
- [`../operations/onebucket-setup.md`](../operations/onebucket-setup.md): provisioning the OneBucket side (endpoints, policies, keys)
- [`../operations/runbooks/event-consumer.md`](../operations/runbooks/event-consumer.md): operator guide for the event consumer
