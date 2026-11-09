# AWS Deployment Runbook

> **Scope: the `lambda` execution mode.** This runbook is for deploying the OneBucket demo pipeline onto AWS Lambda, S3 and ECR in your own account. If you are running the demo on a laptop you probably want the default `local-daemon` mode instead, which needs no Lambda at all: see [runbooks/local-daemon.md](runbooks/local-daemon.md). For an overview of all three execution modes and how to pick one, see [execution-modes.md](execution-modes.md).

This is the end-to-end, read-top-to-bottom guide for a first deploy. It explains what the deploy script does, why the pieces are shaped the way they are, how to verify the result, and what to do when something goes wrong.

## Four docs, four jobs

| Doc | Use when |
|---|---|
| **This runbook** | First-time deploy, or a deploy that is not going smoothly. |
| [aws-prerequisites.md](aws-prerequisites.md) | Reference: tools, permissions, regions, cost, cleanup. |
| [aws-deployer-iam-setup.md](aws-deployer-iam-setup.md) | Reference: creating a least-privilege IAM user for the deploy. |
| [aws-manual-deploy.md](aws-manual-deploy.md) | The script cannot run (no Docker, restricted machine) and you need to do each step by hand or hand it to an administrator. |

---

## How the pipeline is shaped, and why

Three design choices show up everywhere in this runbook, so here is the reasoning once.

**Why two Lambda images.** The pipeline has two stages, chunking and embedding, and they have very different dependency footprints. The chunker only needs a tokenizer, boto3 and the storage helpers, so its image is about 200 MB. The embedder bakes the BGE-small and SPLADE model weights (about 630 MB) plus FastEmbed, LanceDB, pyarrow and numpy into the image, so it is roughly ten times larger. Keeping them separate means the chunker cold-starts in a few seconds and a change to the embedding stack does not force a rebuild of the chunker. The separation is enforced in code: the chunker's modules import the embedding stack lazily, and the chunker image build fails if `fastembed`, `lancedb` or `pyarrow` ever sneaks into its import graph.

**Why `MANIFEST.json` is written last.** The chunker writes every `chunk_NNNN.txt` object first and only then writes `chunks/<doc-id>/MANIFEST.json`. The embedder's S3 trigger fires on that one key (prefix `chunks/`, suffix `MANIFEST.json`), so the embedder fires exactly once per document, after all of its chunks are already in S3. Without this barrier the embedder would either fire once per chunk file or race the chunker and read a partial document. The same rule gives you a clean failure mode: a source that produces zero chunks writes no manifest, so no downstream work happens.

**Why the empty folder markers exist.** The deploy writes zero-byte `raw/`, `chunks/` and `metadata/` objects before it wires the triggers. Their only job is console visibility: a fresh bucket shows its three prefixes in the S3 console so you can find them, and because they are created before the notification configuration is attached they never trigger a Lambda.

---

## Part 1: Account and local setup

### 1.1 Install tools

See [aws-prerequisites.md](aws-prerequisites.md#tool-installs). On a fresh macOS:

```bash
brew install awscli
brew install --cask docker
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Start Docker Desktop before deploying:

```bash
open -a Docker
```

### 1.2 Pick a region

The default everywhere is `us-east-1`. Pass `--region` to the deploy script to override it; every example in these docs uses `us-east-1`.

### 1.3 Create a dedicated AWS profile (recommended)

If your existing AWS profile belongs to another project, or has admin permissions you would rather not use here, create a least-privilege user. The walkthrough is in [aws-deployer-iam-setup.md](aws-deployer-iam-setup.md) and the policy is [infra/iam/deployer-policy.json](../../infra/iam/deployer-policy.json). The policy file in git is the source of truth: when the deploy needs a new action, the action is added there and re-applied to the user.

### 1.4 Export `AWS_PROFILE` for the whole shell session

Do this rather than relying on `--profile` alone:

```bash
export AWS_PROFILE=onebucket-demo
aws sts get-caller-identity
# Arn should end in :user/onebucket-demo-deployer
```

Why it matters: the deploy is not one process. The bash wrapper runs the Python provisioner, which runs `scripts/init_index.py` as a subprocess, and inside that LanceDB's Rust object store resolves AWS credentials on its own rather than through boto3. The MCP server, the `aws s3` checks below and any re-run of `init_index.py` also resolve credentials from the environment. A profile that lives only in a flag on one command is the most common reason the deploy's sidecar check fails (see 4.10). One shell-wide export makes every layer agree.

After this you can drop `--profile` from every command in this guide.

---

## Part 2: First deploy

### 2.1 Run the deploy

From the repo root:

```bash
bash scripts/deploy_aws.sh --bucket <your-bucket> --region us-east-1
```

Flags:

| Flag | Default | Notes |
|---|---|---|
| `--bucket` | required | Must match the resource pattern in `deployer-policy.json` (`onebucket-*` as shipped). S3 names are globally unique, so add a suffix of your own. |
| `--region` | `us-east-1` | |
| `--profile` | `$AWS_PROFILE`, else `default` | Export `AWS_PROFILE` instead (1.4). |
| `--lambda-prefix` | `onebucket` | Prefix for the Lambda, role, queue, repo and log-group names. Must also match the policy's `onebucket-*` pattern. |

Environment:

| Variable | Effect |
|---|---|
| `ONEBUCKET_RUN_SMOKE=0` | Skip the post-deploy smoke test (default: run it). |
| `AWS_ENDPOINT_URL` | Must not point at a `*.onebucket.io` host. The deploy talks to IAM, Lambda, ECR and SQS and needs the real AWS control plane; both the wrapper and the provisioner refuse to run if it does. Unset it for this command. |

### 2.2 What happens, step by step

The bash wrapper does the parts that need a shell, then hands off to the Python provisioner.

**Wrapper** ([scripts/deploy_aws.sh](../../scripts/deploy_aws.sh)):

1. **Verify credentials** with `aws sts get-caller-identity`. If that fails and the profile is configured for SSO (it has `sso_start_url` or `sso_session`), it runs `aws sso login` for you. A static-key profile whose keys are rejected gets a clear error telling you to fix `~/.aws/credentials`; an unknown profile gets an error telling you to run `aws configure`.
2. **Build the chunker image**, then **the embedder image**. Each build script computes a content hash of `pyproject.toml`, `uv.lock`, the Dockerfile, the locked requirements and the `src/` tree, uses the first 12 hex characters as the image tag (with a `-dirty` suffix if the working tree has uncommitted changes), and writes the full local tag to `${TMPDIR:-/tmp}/onebucket_<which>_tag`. An unchanged build reuses the image already in ECR; any change rolls a new tag instead of overwriting the old one. The build context is the repo root, and the root `.dockerignore` denies everything except `src/onebucket_demo`, `pyproject.toml`, `uv.lock` and `lambda/requirements-*.txt`, so nothing else on your machine can reach the Docker daemon or the image.
3. **Log in to ECR, create the two repos if missing, and push both images** under their content-hash tags.

**Provisioner** ([scripts/aws_deploy.py](../../scripts/aws_deploy.py)), every step idempotent (check before mutate, log `[skip]` on a no-op):

1. **Bucket** with versioning and default SSE-S3 encryption.
2. **Folder markers** `raw/`, `chunks/`, `metadata/`.
3. **Dead-letter queues** `<prefix>-chunker-dlq` and `<prefix>-embedder-dlq` (30-minute visibility timeout).
4. **ECR repos**, the Lambda-pull repository policy ([infra/ecr/repository-policy.json](../../infra/ecr/repository-policy.json)), and a lifecycle policy that expires untagged images beyond the last three, so content-hash rebuilds do not accumulate forever.
5. **IAM execution roles** `<prefix>-chunker-role` and `<prefix>-embedder-role`, each with one inline least-privilege policy from [infra/iam/](../../infra/iam/).
6. **The two Lambda functions**: chunker at 1024 MB / 120 s, embedder at 3008 MB / 300 s, both pointed at their DLQ. On a re-run the provisioner reconciles the image **and** the environment, memory, timeout, role and DLQ, waiting for `LastUpdateStatus=Successful` between the code update and the configuration update because Lambda rejects overlapping updates. A fresh role can take 5 to 30 s to become assumable, so function creation retries on that specific error for up to about 60 s.
7. **14-day log retention** on `/aws/lambda/<prefix>-chunker` and `/aws/lambda/<prefix>-embedder`.
8. **Invoke permissions** letting `s3.amazonaws.com` call each function from this bucket.
9. **S3 notification configuration** from [infra/s3/notification-config.json](../../infra/s3/notification-config.json): `raw/*` fires the chunker, `chunks/*MANIFEST.json` fires the embedder. S3 validates the destination before accepting the configuration, and its view of the permission added in the previous step can lag, so this step retries on "Unable to validate" for up to about 60 s.
10. **`init_index`** against `s3://<bucket>/metadata`, creating the empty LanceDB table and the `_init_metadata.json` sidecar. The provisioner then HEADs the sidecar and fails loudly if it is missing (see 4.10).
11. **Smoke test** (unless `ONEBUCKET_RUN_SMOKE=0`): wait for both functions to be Active, upload `data/samples/finance_report.txt` to `raw/`, wait up to **60 s** for the manifest, then up to **120 s** for new objects under `metadata/` (the Lance commit). The expected document id is the first 16 hex characters of the SHA-256 of the file's **decoded text**, computed the same way the chunker does it. A failure raises with the stage name, `manifest` or `lance`.

### 2.3 Expected runtime

| Stage | First time | Cached |
|---|---|---|
| Chunker build | 3 to 5 min | about 10 s |
| Embedder build (model bake) | 15 to 30 min on an M-series Mac (QEMU cross-build) | about 30 s |
| ECR push (both) | 1 to 2 min | seconds |
| Provisioner | 1 to 2 min | under 30 s |
| Smoke test | up to 60 s + 120 s | same |

The first deploy on a fresh machine is dominated by the embedder build, which downloads about 700 MB of model weights.

### 2.4 Cold starts

The first upload after a deploy (or after a new image is pushed) can take 60 to 90 s before anything appears under `chunks/` or `metadata/`. Lambda has to pull the new image from ECR and start a fresh execution environment for each function, and the embedder's environment is large. Later invocations reuse warm environments and take seconds. The smoke test budgets allow for this; if you are watching by hand, wait before concluding a trigger is broken.

---

## Part 3: Verification

### 3.1 Smoke test output

The smoke test runs automatically as the last step. On success:

```
[smoke] manifest landed: chunks/<doc-id>/MANIFEST.json
[smoke] new metadata objects landed (Lance row committed)
[smoke] result: {'ok': True, 'raw_file_id': '<doc-id>'}
deploy complete — creates=N puts=M updates=K skips=L
```

On failure the provisioner raises `smoke test failed at stage manifest` (the chunker never wrote the manifest) or `smoke test failed at stage lance` (the embedder never committed). Go to Part 4.

### 3.2 Manual verification

A new terminal window will not have your profile exported, so start with:

```bash
export AWS_PROFILE=onebucket-demo BUCKET=<your-bucket>
aws sts get-caller-identity   # arn must end in :user/onebucket-demo-deployer
```

Then:

```bash
# 1. Upload a fixture under a fresh name
TS=$(date +%s)
aws s3 cp data/samples/finance_report.txt s3://${BUCKET}/raw/test-${TS}.txt

# 2. Wait for the chunker (allow up to 90 s the first time, see 2.4)
sleep 30 && aws s3 ls s3://${BUCKET}/chunks/ --recursive
# Expected: chunks/<doc-id>/MANIFEST.json plus chunks/<doc-id>/chunk_NNNN.txt

# 3. Wait for the embedder
sleep 60 && aws s3 ls s3://${BUCKET}/metadata/global_index.lance/data/ --recursive
# Expected: at least one .lance data file with a recent timestamp

# 4. Count rows
uv run python -c "import lancedb; db=lancedb.connect('s3://${BUCKET}/metadata'); t=db.open_table('global_index'); print('rows:', t.count_rows())"
# Expected: rows > 0
```

### 3.3 Loading a real 10-K

The fixtures in `data/samples/` are enough for a smoke test but too small to show off retrieval. A public 10-K filing chunks into 50 to 200 pieces and gives hybrid search something to work with.

The chunker accepts only `.txt` and `.md` (see [src/onebucket_demo/gates.py](../../src/onebucket_demo/gates.py)), and SEC EDGAR serves filings as HTML, so the HTML has to become text first. There are two ways to do that.

**The `ingest_document` MCP tool (easiest).** Once the MCP server is wired up (3.4), ask Claude to ingest a document by ticker, URL or local path. The tool resolves a ticker to its most recent 10-K through EDGAR's submissions API, fetches it, converts HTML to text, uploads it to `raw/` and returns a `raw_file_id` you can poll with `check_ingest_status`. A full 10-K takes about 4 to 5 minutes end to end in `lambda` mode because the embedder works in batches of 16 chunks to stay under its memory ceiling.

**By hand.** Convert the HTML to text, then upload it to `raw/`:

```bash
mkdir -p ./outputs/10k
uv run --with html2text python -c "
import html2text, pathlib, re
h = html2text.HTML2Text(); h.ignore_links = True; h.ignore_images = True; h.body_width = 0
src = pathlib.Path('/path/to/filing.html').read_text(errors='replace')
out = re.sub(r'\n{3,}', '\n\n', h.handle(src))
pathlib.Path('./outputs/10k/filing.txt').write_text(out)
"
aws s3 cp ./outputs/10k/filing.txt s3://${BUCKET}/raw/filing.txt
```

The same text file can also go through the pipeline without any Lambda at all, for example to compare results locally: `uv run python scripts/ingest.py ./outputs/10k/filing.txt` runs chunking and embedding in-process against the local `data/metadata` and `data/chunks` roots.

Then search from Python:

```bash
uv run python -c "
from onebucket_demo.search import search_global_index
hits = search_global_index('risks related to supply chain disruption',
    dataset_root='s3://${BUCKET}/metadata', limit=5)
print(hits)
"
```

### 3.4 Wiring the MCP server to the deployed bucket

The MCP server in this repo registers thirteen tools. In `lambda` mode no local process other than the server itself is needed, because the chunking and embedding run in your AWS account.

| Tool | What it does |
|---|---|
| `search_global_index(query, limit, scope)` | Hybrid dense + sparse search. `scope` restricts to a list of `raw_file_id`s. |
| `fetch_chunk(chunk_uri)` | Returns the text of one chunk by URI. |
| `ingest_document(source)` | Uploads a document by ticker, URL or local path and returns its `raw_file_id`. |
| `check_ingest_status(raw_file_id)` | Reports the pipeline phase for a document (`uploading_or_chunker_pending`, `chunking`, `embedding`, `ready`). |
| `list_documents()` | Lists every document in the index with its chunk count. |
| `delete_document(raw_file_id)` | Removes a document from the index, then its chunks, then its raw file. |
| `rebuild_index()` | Wipes the index and chunks and reprocesses everything under `raw/`. |
| `get_replication_metrics(raw_file_id)` | Replication latencies observed by the event consumer (`onebucket-events` mode). |
| `get_chunk_read_metrics(...)` | Chunk read latencies from the consumer's event log. |
| `purge_peer_cache(...)` | Deletes objects directly from a backing peer to demonstrate cache refill (OneBucket deployments). |
| `get_runbook_config()` | Reads the demo runbook's variables file. |
| `set_runbook_variable(path, value)` | Updates one variable in that file. |
| `validate_runbook_config()` | Reports inconsistencies and unfilled values in that file. |

The detailed reference with workflow patterns is [mcp-tools-guide.md](mcp-tools-guide.md). The presenter's walkthrough is [docs/demo-script.md](../demo-script.md).

Edit the Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS) and merge this into `mcpServers`:

```json
{
  "mcpServers": {
    "onebucket-demo": {
      "command": "uv",
      "args": [
        "--directory", "/absolute/path/to/this/repo",
        "run", "python", "-m", "onebucket_demo.mcp_server"
      ],
      "env": {
        "AWS_PROFILE": "onebucket-demo",
        "ONEBUCKET_DATASET_ROOT": "s3://<your-bucket>/metadata",
        "ONEBUCKET_CHUNK_ROOT": "s3://<your-bucket>/chunks",
        "ONEBUCKET_EXECUTION_MODE": "lambda",
        "ONEBUCKET_SEC_USER_AGENT": "OneBucket-Demo your-email@example.com"
      }
    }
  }
}
```

The environment variables:

- `AWS_PROFILE`: the server's S3 and SQS credentials. Your shell's `export` does not reach Claude Desktop's subprocess, so it has to be in this block.
- `ONEBUCKET_DATASET_ROOT`: where the LanceDB table lives. Used by `search_global_index`, `check_ingest_status`, `list_documents` and `delete_document`.
- `ONEBUCKET_CHUNK_ROOT`: `fetch_chunk` resolves relative `chunks/...` paths against it; `ingest_document` derives the bucket name and the `raw/` prefix from it.
- `ONEBUCKET_EXECUTION_MODE`: `lambda` here. The default, `local-daemon`, expects `scripts/start-pipeline-daemon.sh` to be running and refuses uploads when it is not; `onebucket-events` expects `scripts/start-event-consumer.sh` after a one-time `uv run python scripts/onebucket-login.py` sign-in. See [execution-modes.md](execution-modes.md).
- `ONEBUCKET_SEC_USER_AGENT`: SEC EDGAR rejects requests with an empty User-Agent, so `ingest_document` needs something identifying you.

Optional:

- `ONEBUCKET_RAW_ROOT`: only if you do not follow the `<bucket>/raw` + `<bucket>/chunks` convention.
- `ONEBUCKET_LAMBDA_PREFIX`: defaults to `onebucket`; `check_ingest_status` uses it to find the embedder's DLQ.

Fully quit and relaunch Claude Desktop (Cmd-Q, not just closing the window); it reads MCP config only at startup. Then, in a new conversation:

> Ingest the latest 10-K for AAPL. Once it is ready, tell me what risks Apple identifies for its business.

You should see `ingest_document` return a `raw_file_id`, `check_ingest_status` polled until `phase` is `ready`, a scoped `search_global_index`, a few `fetch_chunk` calls, and a grounded answer citing `source_uri` and `chunk_index`.

### 3.5 What is in the bucket

```
Claude Desktop
  └── MCP server: onebucket-demo (this repo, runs locally as a subprocess)
        env: AWS_PROFILE, ONEBUCKET_DATASET_ROOT (s3://), ONEBUCKET_CHUNK_ROOT (s3://),
             ONEBUCKET_EXECUTION_MODE=lambda

S3 (us-east-1)
  ├── raw/                                <- the chunker Lambda triggers on this prefix
  ├── chunks/<doc-id>/chunk_NNNN.txt      <- chunker output
  ├── chunks/<doc-id>/MANIFEST.json       <- written last; the embedder Lambda triggers on this suffix
  ├── metadata/_init_metadata.json        <- LanceDB sidecar (model versions, canary)
  └── metadata/global_index.lance/        <- LanceDB table (vectors, sparse weights, chunk URIs)
```

---

## Part 4: Troubleshooting

Each entry gives the symptom, the cause and the fix. Most of the fixes are already in the scripts; they are listed so you recognise the symptom if you are building on a fork.

### 4.1 `Cannot connect to the Docker daemon`

Docker Desktop is not running. `open -a Docker`, wait for it to start, retry.

### 4.2 `hf-xet: failed to compile from source` during a build

The Lambda base image has no C compiler, and on an arm64 Mac Docker defaults to `linux/arm64`, where `hf-xet` has no prebuilt wheel. Both build scripts pass `--platform=linux/amd64`, which is Lambda's default architecture anyway.

### 4.3 `pyarrow` or `numpy` fail to compile from source

The older AL2-based Lambda image (glibc 2.26) rejects `manylinux_2_28` wheels. Both Dockerfiles use `public.ecr.aws/lambda/python:3.12`, which is AL2023-based (glibc 2.34).

### 4.4 ECR push returns `403 Forbidden` on a manifest HEAD, or Lambda rejects the image with `image manifest, config or layer media type ... not supported`

BuildKit attaches SLSA provenance and SBOM attestations by default, which turns the image into an OCI manifest list. Pushing that list does a HEAD on the manifest (which needs `ecr:BatchGetImage`), and Lambda only accepts a single-manifest image. The build scripts pass `--provenance=false --sbom=false`, and `deployer-policy.json` includes `ecr:BatchGetImage`.

### 4.5 `CreateFunction: The role defined for the function cannot be assumed by Lambda`

IAM eventual consistency: the role was created seconds ago and its trust policy has not propagated. The provisioner retries this exact error with 5/5/10/10/15/15 s delays. Any other `InvalidParameterValue` is a real configuration error and is raised immediately.

### 4.6 `CreateFunction: MemorySize must be ≤ 3008`

3008 MB is the default per-account ceiling for Lambda memory on a new account; going higher (up to 10240 MB) needs a Service Quotas request. The embedder is configured at 3008 MB, which fits BGE-small, SPLADE and the LanceDB write path.

### 4.7 `CreateFunction: Lambda does not have permission to access the ECR image`

A fresh private ECR repo has no resource policy, so the Lambda service principal cannot pull from it. The provisioner applies [infra/ecr/repository-policy.json](../../infra/ecr/repository-policy.json), which grants `lambda.amazonaws.com` the `ecr:BatchGetImage` and `ecr:GetDownloadUrlForLayer` actions, conditioned on the calling function matching `<prefix>-*`. The deployer needs `ecr:GetRepositoryPolicy` and `ecr:SetRepositoryPolicy` for this.

### 4.8 `PutBucketNotification: Unable to validate the following destination configurations`

S3 checks that it may invoke the Lambda before it accepts the notification configuration, and its view of the permission added moments earlier can lag. The provisioner retries this exact error with the same backoff as 4.5.

### 4.9 Chunker fails with `OSError: [Errno 30] Read-only file system: /home/sbx_user...`

On Lambda everything outside `/tmp` is read-only, including the runtime user's home directory. Without `HF_HOME` set at build time the tokenizer cache lands in `/root/.cache`, the runtime cannot find it, tries to download again and crashes. Both Dockerfiles set `ENV HF_HOME=/var/task/.cache/huggingface` **before** the bake step so the cache lives inside the image.

### 4.10 `init_index returned 0 but the sidecar ... is missing`

`init_index.py` ran but wrote nothing. The usual cause is that the subprocess resolved a different AWS profile than the one you expected, because LanceDB's object store reads credentials from the environment on its own. Export `AWS_PROFILE` in your shell (1.4) and re-run the deploy, or re-run just the initialisation:

```bash
uv run python scripts/init_index.py s3://${BUCKET}/metadata
aws s3 ls s3://${BUCKET}/metadata/ --recursive
# Expect: _init_metadata.json and global_index.lance/_versions/...manifest
```

### 4.11 Embedder cold start takes about 160 s and the smoke test fails at stage `lance`

FastEmbed's model loader calls the Hugging Face Hub API on every construction, even when the model files are already cached, and Lambda's egress to huggingface.co is slow. [src/onebucket_demo/embed_dense.py](../../src/onebucket_demo/embed_dense.py) and [src/onebucket_demo/embed_sparse.py](../../src/onebucket_demo/embed_sparse.py) pass `local_files_only=True` whenever `AWS_LAMBDA_FUNCTION_NAME` is set, which Lambda sets automatically and the Docker build does not (the bake step needs network access to populate the cache). Expected cold start after this is 5 to 15 s for a small document. If you see the slow path on a fork, check that the model cache path baked into the image (`FASTEMBED_CACHE_PATH=/var/task/.cache/fastembed`) is the one the runtime reads.

### 4.12 Embedder hits `Runtime.OutOfMemory` at 3008 MB on a real document

FastEmbed embeds its whole input list in one ONNX pass, and a 140-chunk 10-K in one call peaks above 3008 MB. [src/onebucket_demo/pipeline.py](../../src/onebucket_demo/pipeline.py) embeds in batches of `ONEBUCKET_EMBED_BATCH_SIZE` (default 16), reusing the loaded models across batches, which keeps a 140-chunk document under the ceiling at the cost of about 4 to 5 minutes of wall clock. Raise the batch size in the Lambda environment only after raising the account's memory quota.

### 4.13 `aws sts get-caller-identity` shows the wrong user

The shell does not have `AWS_PROFILE` exported; a new terminal window inherits nothing from the last one. `export AWS_PROFILE=onebucket-demo`, or add it to your shell rc file if this is your usual context.

### 4.14 The smoke test waits for a manifest that never appears, but the chunker logs show success

The expected document id is the SHA-256 of the file's decoded text, not its raw bytes (`Path.read_text` normalises line endings). The deploy computes it the same way the chunker does. If you are polling by hand with `shasum` on a file with CRLF line endings, your id will not match the one under `chunks/`; use the Python one-liner in [aws-manual-deploy.md](aws-manual-deploy.md#step-13-smoke-test-provisioner-step-9).

### Reading the logs and queues

```bash
aws logs tail /aws/lambda/onebucket-chunker  --since 10m
aws logs tail /aws/lambda/onebucket-embedder --since 10m
for which in chunker embedder; do
  aws sqs get-queue-attributes \
    --queue-url "$(aws sqs get-queue-url --queue-name onebucket-${which}-dlq --query QueueUrl --output text)" \
    --attribute-names ApproximateNumberOfMessages
done
```

---

## Part 5: Iteration

### 5.1 Code change, rebuild, redeploy

The deploy is idempotent. After any source change, re-run the same command:

```bash
bash scripts/deploy_aws.sh --bucket ${BUCKET} --region us-east-1
```

On a re-run:

- An unchanged tree produces the same content-hash tag, Docker reuses its cache, and ECR already has the image.
- A changed tree produces a new tag and pushes only the changed layers.
- The provisioner logs `[skip]` for everything that already matches, `[update]` for a Lambda whose image changed, and a second `[update]` if its environment, memory, timeout, role or DLQ drifted from what the script expects. Configuration drift is reconciled too, so changing `--bucket` or the memory size in the script takes effect without deleting the function.

### 5.2 Updating the deployer IAM policy

When [infra/iam/deployer-policy.json](../../infra/iam/deployer-policy.json) changes, re-apply it to the user with the same command that created it; an inline user policy is replaced in place:

```bash
aws iam put-user-policy \
  --user-name onebucket-demo-deployer \
  --policy-name OneBucketDemoDeployerPolicy \
  --policy-document file://infra/iam/deployer-policy.json \
  --profile <your-admin-profile>
```

See [aws-deployer-iam-setup.md](aws-deployer-iam-setup.md#updating-the-policy-later).

### 5.3 Forcing a fresh image

If the content hash did not change but you want a new image anyway (for example the upstream base image was updated):

```bash
docker rmi "$(cat ${TMPDIR:-/tmp}/onebucket_chunker_tag)" "$(cat ${TMPDIR:-/tmp}/onebucket_embedder_tag)" 2>/dev/null
bash scripts/deploy_aws.sh --bucket ${BUCKET} --region us-east-1
```

### 5.4 Regenerating the locked Lambda dependencies

After editing `lambda/requirements-chunker.in` or `lambda/requirements-embedder.in`:

```bash
bash scripts/lock_lambda_deps.sh
```

Commit the resulting `.txt` files. They are part of the tag hash, so the image tag rolls on the next deploy.

---

## Part 6: Cleanup

To tear everything down, follow [aws-prerequisites.md](aws-prerequisites.md#cleanup). The bucket is versioned, so every object version and delete marker has to go before the bucket can be deleted, and most of the deletes need a profile with more permissions than the deployer has.

To remove the dedicated IAM user as well, see [aws-deployer-iam-setup.md](aws-deployer-iam-setup.md#cleaning-up-later).
