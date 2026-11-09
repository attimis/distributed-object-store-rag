# OneBucket setup

How to provision a OneBucket deployment for this pipeline: the backing
buckets on your storage providers, the OneBucket console objects that tie
them together, and the local configuration that points the code at them.
When you are done, `ONEBUCKET_CHUNK_ROOT` and `ONEBUCKET_DATASET_ROOT` name
OneBuckets instead of plain S3 buckets and nothing else changes.

Official references: the console at https://app.onebucket.io, the product
docs at https://docs.onebucket.io, and the region list at
https://www.onebucket.io/support. Console wording below follows the docs at
the time of writing; a newer console may label a screen slightly differently.

## The mental model

The console has three top-level objects. Getting them straight first saves
real confusion later.

| Console tab | What it is | Who uses it |
|---|---|---|
| **Credentials** | Access keys. Two kinds live here: the keys *your client* uses to talk to OneBucket (an S3 access key id + secret, like an AWS key), and the keys *OneBucket* uses to reach your backing storage. | Your shell, the MCP server, the consumers (client keys); endpoints (backing keys). |
| **Endpoints** | One backing storage provider: a name, its S3 (or Azure) URL, and the backing credentials for it. No bucket yet. | Referenced by peers inside a policy. |
| **Policies** | A **OneBucket**: the logical bucket name your application uses (`s3://demo-data/...`), mapped to one or more **peer buckets** at endpoints, with a mode per peer and a write quorum. | Every S3 call your application makes. |

A policy is where the interesting choices live. Each peer bucket has:

- **Mode.** `sync`: the write lands on this peer before the request returns
  and counts toward the quorum. `async`: the write is acknowledged first and
  copied to this peer in the background. `cache`: the peer holds copies of
  what has been read or prefetched so repeat reads are served locally; the
  source stays authoritative.
- **Permissions** (read, write, delete), **Active**, **Required** (every
  write must succeed on this peer regardless of quorum), **Sequence** (the
  order OneBucket tries peers), and **Opportunistic replication** (copy
  existing objects into this peer as they are read through the OneBucket).
- **Quorum**, on the policy: the minimum number of `sync` peers with write
  permission that must acknowledge a `PUT` for it to succeed. The console
  will not let the quorum drop below the number of `Required` peers, and the
  config loader in this repo refuses a file where it does.

Every policy needs at least one `sync` peer. Placement is per OneBucket, not
per object.

## What the pipeline needs from your policies

Name your OneBuckets whatever you like and use whichever providers you
have; the code never assumes a name, a provider or a region. Everything
deployment-specific is read from `docs/demo_runbook/variables.yml`, and
the environment variables name the policies explicitly. This document uses
`demo-data` and `demo-meta` as example names.

There is exactly **one** structural requirement, and it is the point of the
demo:

> **Keep the index in its own OneBucket, replicated eagerly; keep the
> documents and chunks in a different OneBucket, replicated lazily.**

| OneBucket | Holds | Peer shape | Why |
|---|---|---|---|
| **data** (`demo-data`) | `raw/` uploads and `chunks/` | one `sync` peer marked required (the canonical copy); any number of `cache` peers elsewhere | Writes land once. Other locations fill only with the chunks that are actually read, which is the "lazy tier" the demo measures. |
| **metadata** (`demo-meta`) | `metadata/` (the LanceDB index) | every peer `sync` and required; quorum = number of peers | The index must be byte-identical everywhere, so every write blocks until all peers acknowledge. |

Why they must be separate: a policy's placement is per OneBucket, not per
object, so one OneBucket cannot replicate its index eagerly and its chunks
lazily at the same time. Put them together and you either pay eager
replication on every chunk or run a search against an index copy that may
lag.

Everything else is optional. A second data OneBucket that mirrors the first
with the roles reversed (`sync` on provider B, `cache` on provider A) gives
the "read the same documents from the other side" segment of the live demo;
a single-provider setup, with the data OneBucket having one `sync` peer and
no cache peers, still runs the whole pipeline and every tool. Start with
what you have and add peers in the console later; the pipeline does not
need to change.

## Step 1: backing buckets and scoped credentials

On each provider, create the buckets and a scoped user that OneBucket will
use. Never hand OneBucket a root or admin key.

**AWS.** Two buckets (one for raw + chunks, one for metadata), block public
access on both, versioning on the metadata bucket (LanceDB writes
overlapping fragment files; versioning makes accidental-overwrite recovery
easy). One IAM user with programmatic access only and the inline policy in
[infra/iam/onebucket-backing-aws-policy.json](../../infra/iam/onebucket-backing-aws-policy.json)
after substituting the two bucket names:

```bash
export RAW_CHUNKS_BUCKET=<your-aws-bucket-for-raw-and-chunks>
export METADATA_BUCKET=<your-aws-bucket-for-metadata>
envsubst < infra/iam/onebucket-backing-aws-policy.json > /tmp/backing-policy.json
aws iam create-user --user-name onebucket-backing
aws iam put-user-policy --user-name onebucket-backing --policy-name onebucket-backing \
    --policy-document file:///tmp/backing-policy.json
aws iam create-access-key --user-name onebucket-backing   # save the secret; it is shown once
```

**Wasabi** (or any other S3-compatible provider). The same shape through
the provider's IAM-compatible API; the policy template is
[infra/iam/onebucket-backing-wasabi-policy.json](../../infra/iam/onebucket-backing-wasabi-policy.json)
(Wasabi rejects `s3:GetObjectAttributes`, so it is omitted there). Wasabi
needs `--create-bucket-configuration LocationConstraint=<region>` for any
region but `us-east-1`, and its regional URL pattern is
`https://s3.<region>.wasabisys.com`.

Verify a scoped key before you move on: listing the bucket should work,
`aws s3api list-buckets` should be denied.

## Step 2: console objects

1. **Credentials → Add Credentials.** Pick *S3 protocol*, *Generate* an
   access key id and secret, keep *Active* on. This is **your client key**:
   put it in an AWS CLI profile (`aws configure --profile my-onebucket-profile`).
   The secret is shown once.
2. **Endpoints → Create Endpoint**, once per provider: a friendly name
   (this is the alias the rest of the system uses, e.g. `EAST_S3`,
   `WEST_WASABI`), the provider URL, *S3 protocol*, the bucket addressing
   style, and the **backing** credentials from step 1.

   > **The "Use region" switch.** The endpoint form asks whether to insert
   > the per-peer region into the URL. Turn it **on** for a global URL such
   > as `https://s3.amazonaws.com:443` and set the region on each peer; turn
   > it **off** for a URL that already names its region, such as
   > `https://s3.us-west-1.wasabisys.com:443`. Getting this wrong composes a
   > hostname with the region twice, and the symptom is misleading: reads
   > keep working while every write returns 503. If writes fail and reads do
   > not, check this switch first.
3. **Policies → Create Policy**, once for the data OneBucket and once for
   the metadata OneBucket (plus any optional mirror), following the table
   above. The wizard asks for the OneBucket name (it cannot be changed
   later), the peer buckets (endpoint, backing key, peer bucket name, mode,
   permissions, required, opportunistic replication), the quorum, and the
   **core access keys**: tick your client key from step 2.1, or it cannot
   address this OneBucket.

Policies apply account-wide, in every region.

## Step 3: point the pipeline at it

```bash
export AWS_PROFILE=my-onebucket-profile          # the client key from step 2.1
export AWS_ENDPOINT_URL=https://s3.onebucket.io   # global; or s3.<region>.onebucket.io to pin
export AWS_DEFAULT_REGION=us-east-1               # used for request signing only
export ONEBUCKET_CHUNK_ROOT=s3://demo-data/chunks       # your data OneBucket
export ONEBUCKET_DATASET_ROOT=s3://demo-meta/metadata   # your metadata OneBucket

uv run python scripts/init_index.py "$ONEBUCKET_DATASET_ROOT"   # creates the table once
```

Both addressing styles work against OneBucket; the scripts use path style.
Then start one consumer, never two:

- `bash scripts/start-pipeline-daemon.sh` for `local-daemon` mode, or
- `uv run python scripts/onebucket-login.py` once, then
  `bash scripts/start-event-consumer.sh` for `onebucket-events` mode.

[execution-modes.md](execution-modes.md) compares the two and lists what
Claude Desktop's `env` block needs.

## Step 4: prove it end to end

```bash
echo "hello $(date)" > /tmp/probe.txt
aws s3 cp /tmp/probe.txt s3://demo-data/raw/probe.txt --endpoint-url "$AWS_ENDPOINT_URL" --profile "$AWS_PROFILE"
aws s3 ls s3://$RAW_CHUNKS_BUCKET/raw/ --profile <your-aws-admin-profile>   # landed on the backing bucket
aws s3 rm s3://demo-data/raw/probe.txt --endpoint-url "$AWS_ENDPOINT_URL" --profile "$AWS_PROFILE"
```

With a consumer running, the upload also produces `[processing]`,
`[chunked]` and `[embedded]` log lines and a `chunks/<id>/MANIFEST.json`
on the backing bucket; that is the pipeline working through OneBucket.

## Watching replication happen

The `events` MCP server (`https://events.onebucket.io`, or the regional
hostname) reports every request as one **gateway** event plus one
**backend** event per peer it reached, sharing a `reqId`. The event consumer
records them in `.runbook-state/events.jsonl`, and `get_replication_metrics`
turns the backend-minus-gateway timestamps into per-peer acknowledgement
latency. To browse objects and see which backends hold a copy, add the
`data` MCP server (`https://data.onebucket.io`) as a Claude connector and
use its `locate` tool. Both servers sign you in with OAuth in the browser;
never paste a key into a connector dialog.

## Checklist for a fresh deployment

- [ ] Backing buckets created on each provider; versioning on the metadata bucket
- [ ] One scoped backing user per provider with the policy template applied; keys captured
- [ ] Console: client access key created and stored in an AWS CLI profile
- [ ] Console: one endpoint per provider, "Use region" set per the rule above
- [ ] Console: a data OneBucket (one `sync` required peer, optional `cache` peers) and a separate metadata OneBucket (all peers `sync` + required, quorum = peer count); optionally a mirror data OneBucket
- [ ] Core access keys: your client key ticked on every policy
- [ ] `init_index.py` run against `s3://<your-metadata-onebucket>/metadata`
- [ ] Step 4 probe round-trips and lands on the backing bucket
- [ ] `docs/demo_runbook/variables.yml` copied from the example and filled in (live demo only)
