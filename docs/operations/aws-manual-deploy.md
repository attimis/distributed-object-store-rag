# AWS Manual Deploy: Console and CLI Walkthrough

Companion to `bash scripts/deploy_aws.sh`. The script is the recommended path; this document exists for when it cannot run (no Docker on the machine, a locked-down workstation, an administrator who has to perform some steps for you). Each step here does exactly what one step of the script does, in the same order, and says which script step it corresponds to, so you can hand any single step to someone else and still end up with the same deployment.

Before starting, read [aws-prerequisites.md](aws-prerequisites.md). For the reasoning behind the two images, the manifest-last rule and the folder markers, read the opening section of [aws-deployment-runbook.md](aws-deployment-runbook.md).

## Setup for every step

Export these once in the shell you will use for all of the steps. Every command below reads them.

```bash
export AWS_PROFILE=onebucket-demo        # or whichever profile has the deployer permissions
export AWS_DEFAULT_REGION=us-east-1
export BUCKET=<your-bucket>              # must match the onebucket-* pattern in deployer-policy.json
export LAMBDA_PREFIX=onebucket           # prefix for every function, role, queue, repo and log group
export REGION=$AWS_DEFAULT_REGION
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
mkdir -p ./outputs                       # substituted policy files and image URIs land here
```

Exporting `AWS_PROFILE` rather than passing `--profile` matters: `scripts/init_index.py` and LanceDB resolve credentials from the environment on their own, not through the AWS CLI.

The script's step list, for reference:

| Script step | Done by | Manual step |
|---|---|---|
| Verify credentials | wrapper | 1 |
| Build and push both images | wrapper | 6 |
| 1. Bucket (versioning + SSE) | provisioner | 2 |
| 2. Folder markers | provisioner | 3 |
| 3. Dead-letter queues | provisioner | 4 |
| 4. ECR repos, pull policy, lifecycle policy | provisioner | 5 |
| 5. IAM execution roles | provisioner | 7 |
| 6. Lambda functions and log retention | provisioner | 8, 9 |
| 7. Invoke permissions and S3 notification | provisioner | 10, 11 |
| 8. `init_index` | provisioner | 12 |
| 9. Smoke test | provisioner | 13 |

---

## Step 1: Verify AWS credentials (wrapper step 1)

```bash
aws sts get-caller-identity
```

The script does the same check and, when it fails on a profile that has `sso_start_url` or `sso_session` configured, runs `aws sso login --profile $AWS_PROFILE` for you. By hand: if the profile is SSO-based, run that login; if it is a static-key profile and the keys are rejected, fix `~/.aws/credentials`; if the CLI says `Unable to locate credentials`, no profile is configured at all, so run `aws configure` (access keys) or `aws configure sso` first.

Make sure `AWS_ENDPOINT_URL` is not set to a `*.onebucket.io` host in this shell. Every step below talks to the real AWS control plane.

## Step 2: Create the S3 bucket (provisioner step 1)

**Console:** S3, Create bucket, name `${BUCKET}`, region `${REGION}`, Block all public access (default on), Bucket Versioning **Enable**, Default encryption **SSE-S3 (AES-256)**, Create.

**CLI:**

```bash
aws s3api create-bucket --bucket ${BUCKET} --region ${REGION}
# Outside us-east-1 add: --create-bucket-configuration LocationConstraint=${REGION}
aws s3api put-bucket-versioning --bucket ${BUCKET} \
    --versioning-configuration Status=Enabled
aws s3api put-bucket-encryption --bucket ${BUCKET} \
    --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
```

Versioning is on because the index is append-only and a versioned bucket lets you recover an object that a bad run overwrote. It also means cleanup has to delete every version, see [aws-prerequisites.md](aws-prerequisites.md#cleanup).

## Step 3: Create the folder marker objects (provisioner step 2)

Zero-byte objects so the three prefixes show in the console before anything is uploaded. They are created now, before the triggers exist, so they never fire a Lambda.

```bash
for prefix in raw chunks metadata; do
    aws s3api put-object --bucket ${BUCKET} --key ${prefix}/
done
```

## Step 4: Create the dead-letter queues (provisioner step 3)

One standard SQS queue per Lambda. A failed asynchronous invocation lands here instead of disappearing, which is how `check_ingest_status` can tell you that a document failed rather than merely being slow.

**Console:** SQS, Create queue, Standard, name `${LAMBDA_PREFIX}-chunker-dlq`, Visibility timeout 30 minutes, Create. Repeat for `${LAMBDA_PREFIX}-embedder-dlq`.

**CLI:**

```bash
for which in chunker embedder; do
    aws sqs create-queue --queue-name ${LAMBDA_PREFIX}-${which}-dlq \
        --attributes VisibilityTimeout=1800
done

CHUNKER_DLQ_ARN=$(aws sqs get-queue-attributes \
    --queue-url "$(aws sqs get-queue-url --queue-name ${LAMBDA_PREFIX}-chunker-dlq --query QueueUrl --output text)" \
    --attribute-names QueueArn --query Attributes.QueueArn --output text)
EMBEDDER_DLQ_ARN=$(aws sqs get-queue-attributes \
    --queue-url "$(aws sqs get-queue-url --queue-name ${LAMBDA_PREFIX}-embedder-dlq --query QueueUrl --output text)" \
    --attribute-names QueueArn --query Attributes.QueueArn --output text)
```

## Step 5: Create the ECR repositories, pull policy and lifecycle policy (provisioner step 4)

**Console:** ECR, Create repository, name `${LAMBDA_PREFIX}-chunker`, Scan on push on, Create. Repeat for `${LAMBDA_PREFIX}-embedder`. Then on each repository: Permissions, Edit policy JSON, paste the substituted contents of `infra/ecr/repository-policy.json`; and Lifecycle policy, Create rule, "untagged" images, "image count more than 3", expire.

**CLI:**

```bash
sed -e "s/\${ACCOUNT_ID}/${ACCOUNT_ID}/g" \
    -e "s/\${REGION}/${REGION}/g" \
    -e "s/\${LAMBDA_PREFIX}/${LAMBDA_PREFIX}/g" \
    infra/ecr/repository-policy.json > ./outputs/ecr-repo-policy.json

cat > ./outputs/ecr-lifecycle.json <<'EOF'
{"rules":[{"rulePriority":1,"description":"keep the last 3 untagged images",
  "selection":{"tagStatus":"untagged","countType":"imageCountMoreThan","countNumber":3},
  "action":{"type":"expire"}}]}
EOF

for which in chunker embedder; do
    aws ecr create-repository --repository-name ${LAMBDA_PREFIX}-${which} \
        --image-scanning-configuration scanOnPush=true
    aws ecr set-repository-policy --repository-name ${LAMBDA_PREFIX}-${which} \
        --policy-text file://./outputs/ecr-repo-policy.json
    aws ecr put-lifecycle-policy --repository-name ${LAMBDA_PREFIX}-${which} \
        --lifecycle-policy-text file://./outputs/ecr-lifecycle.json
done
```

Why the repository policy: a private ECR repository has no resource policy by default, and without one the Lambda service principal cannot pull the image. `CreateFunction` then fails with "Lambda does not have permission to access the ECR image". Why the lifecycle policy: every content-hash build pushes a new image and the old layers would otherwise accumulate forever.

## Step 6: Build and push both images (wrapper step 2)

The build scripts tag each image with a content hash of the build inputs and write the local tag to `${TMPDIR:-/tmp}/onebucket_<which>_tag`. The build context is the repo root; the root `.dockerignore` allows only `src/onebucket_demo`, `pyproject.toml`, `uv.lock` and `lambda/requirements-*.txt` into it. The first embedder build takes 5 to 15 minutes (longer on an M-series Mac, which cross-builds for `linux/amd64`) because it downloads about 700 MB of model weights into the image.

```bash
aws ecr get-login-password --region ${REGION} | \
    docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com

bash scripts/build_chunker_image.sh
bash scripts/build_embedder_image.sh

for which in chunker embedder; do
    LOCAL_TAG=$(cat "${TMPDIR:-/tmp}/onebucket_${which}_tag")
    VERSION="${LOCAL_TAG##*:}"
    REMOTE="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${LAMBDA_PREFIX}-${which}:${VERSION}"
    docker tag "${LOCAL_TAG}" "${REMOTE}"
    docker push "${REMOTE}"
    echo "${REMOTE}" > "./outputs/${which}_image_uri"
done
CHUNKER_IMAGE_URI=$(cat ./outputs/chunker_image_uri)
EMBEDDER_IMAGE_URI=$(cat ./outputs/embedder_image_uri)
```

If the machine that builds is not the machine that deploys, copy the two image URIs across; they are the only output of this step the later steps need.

## Step 7: Create the IAM execution roles (provisioner step 5)

The trust documents and inline policies are in [infra/iam/](../../infra/iam/). Each role gets exactly one inline policy scoped to the prefixes that stage touches: the chunker reads `raw/` and writes `chunks/`; the embedder reads `chunks/` and reads, writes and deletes `metadata/`. Neither can see the other's inputs.

**Console:** IAM, Roles, Create role, Trusted entity AWS service: Lambda, skip permissions, name `${LAMBDA_PREFIX}-chunker-role`, Create. On the role: Add permissions, Create inline policy, JSON, paste the substituted `infra/iam/chunker-execution-policy.json`, name `${LAMBDA_PREFIX}-chunker-inline`. Repeat for the embedder with its two files.

**CLI:**

```bash
for which in chunker embedder; do
    sed -e "s/\${BUCKET}/${BUCKET}/g" \
        -e "s/\${ACCOUNT_ID}/${ACCOUNT_ID}/g" \
        -e "s/\${REGION}/${REGION}/g" \
        -e "s/\${LAMBDA_PREFIX}/${LAMBDA_PREFIX}/g" \
        infra/iam/${which}-execution-policy.json > ./outputs/${which}-policy.json

    aws iam create-role --role-name ${LAMBDA_PREFIX}-${which}-role \
        --assume-role-policy-document file://infra/iam/${which}-execution-role.json
    aws iam put-role-policy --role-name ${LAMBDA_PREFIX}-${which}-role \
        --policy-name ${LAMBDA_PREFIX}-${which}-inline \
        --policy-document file://./outputs/${which}-policy.json
done
```

A freshly created role can take 5 to 30 seconds before Lambda is allowed to assume it. If step 8 fails with "The role defined for the function cannot be assumed by Lambda", wait and retry; the script retries this exact error for about a minute.

## Step 8: Create the Lambda functions (provisioner step 6)

### Chunker

**Console:** Lambda, Create function, Container image, name `${LAMBDA_PREFIX}-chunker`, Container image URI: the chunker image from step 6, Architecture x86_64, Use an existing role: `${LAMBDA_PREFIX}-chunker-role`, Create. Then Configuration: Memory 1024 MB, Timeout 2 min, Environment variable `ONEBUCKET_CHUNK_ROOT=s3://${BUCKET}/chunks`; Asynchronous invocation: Dead-letter queue SQS `${LAMBDA_PREFIX}-chunker-dlq`.

**CLI:**

```bash
aws lambda create-function --function-name ${LAMBDA_PREFIX}-chunker \
    --package-type Image \
    --code ImageUri=${CHUNKER_IMAGE_URI} \
    --role arn:aws:iam::${ACCOUNT_ID}:role/${LAMBDA_PREFIX}-chunker-role \
    --memory-size 1024 --timeout 120 \
    --environment "Variables={ONEBUCKET_CHUNK_ROOT=s3://${BUCKET}/chunks}" \
    --dead-letter-config TargetArn=${CHUNKER_DLQ_ARN}
aws lambda wait function-active-v2 --function-name ${LAMBDA_PREFIX}-chunker
```

### Embedder

Same pattern, with 3008 MB, a 5-minute timeout and two environment variables. 3008 MB is the default per-account ceiling for Lambda memory on a new AWS account; the AWS-wide maximum is 10240 MB but anything above 3008 needs a Service Quotas request first. The embedder's resident set (BGE-small, SPLADE, FastEmbed, numpy, pyarrow) fits under 3008 because it embeds in batches of 16 chunks.

```bash
aws lambda create-function --function-name ${LAMBDA_PREFIX}-embedder \
    --package-type Image \
    --code ImageUri=${EMBEDDER_IMAGE_URI} \
    --role arn:aws:iam::${ACCOUNT_ID}:role/${LAMBDA_PREFIX}-embedder-role \
    --memory-size 3008 --timeout 300 \
    --environment "Variables={ONEBUCKET_DATASET_ROOT=s3://${BUCKET}/metadata,ONEBUCKET_CHUNK_ROOT=s3://${BUCKET}/chunks}" \
    --dead-letter-config TargetArn=${EMBEDDER_DLQ_ARN}
aws lambda wait function-active-v2 --function-name ${LAMBDA_PREFIX}-embedder
```

The `wait` matters: a container-image function spends several seconds in `Pending` after creation and in `InProgress` after every update, and Lambda rejects the next update or invocation during that window with `ResourceConflictException`. The script waits for `State=Active` and `LastUpdateStatus=Successful` between every change.

**Updating later.** To roll a new image, `aws lambda update-function-code --function-name ... --image-uri ...`, wait, then `aws lambda update-function-configuration ...` if memory, timeout, role, environment or DLQ changed, and wait again. The script does both on every re-run.

## Step 9: Set the log retention (provisioner step 6)

Lambda creates each log group on first invocation with retention set to "never expire". Creating the groups now and capping them at 14 days keeps an idle deployment from accumulating logs forever.

```bash
for which in chunker embedder; do
    aws logs create-log-group --log-group-name /aws/lambda/${LAMBDA_PREFIX}-${which} 2>/dev/null || true
    aws logs put-retention-policy --log-group-name /aws/lambda/${LAMBDA_PREFIX}-${which} \
        --retention-in-days 14
done
```

## Step 10: Let the bucket invoke each function (provisioner step 7)

```bash
for which in chunker embedder; do
    aws lambda add-permission --function-name ${LAMBDA_PREFIX}-${which} \
        --statement-id ${LAMBDA_PREFIX}-${which}-allow-s3-invoke \
        --action lambda:InvokeFunction --principal s3.amazonaws.com \
        --source-arn arn:aws:s3:::${BUCKET}
done
```

## Step 11: Configure the S3 triggers (provisioner step 7)

The configuration is [infra/s3/notification-config.json](../../infra/s3/notification-config.json): any object created under `raw/` invokes the chunker; any object created under `chunks/` whose key ends in `MANIFEST.json` invokes the embedder.

**Console:** S3, your bucket, Properties, Event notifications, Create event notification: name `ChunkerOnRawCreated`, Prefix `raw/`, no suffix, All object create events, Destination Lambda `${LAMBDA_PREFIX}-chunker`, Save. Repeat as `EmbedderOnManifestCreated` with Prefix `chunks/`, **Suffix `MANIFEST.json`**, Lambda `${LAMBDA_PREFIX}-embedder`.

**CLI:**

```bash
sed -e "s/\${ACCOUNT_ID}/${ACCOUNT_ID}/g" \
    -e "s/\${REGION}/${REGION}/g" \
    -e "s/\${LAMBDA_PREFIX}/${LAMBDA_PREFIX}/g" \
    infra/s3/notification-config.json | jq 'del(._comment)' > ./outputs/notification.json
aws s3api put-bucket-notification-configuration --bucket ${BUCKET} \
    --notification-configuration file://./outputs/notification.json
```

The embedder trigger must have **both** the `chunks/` prefix and the `MANIFEST.json` suffix. Without the suffix the embedder fires once per chunk file instead of once per document, which is the per-document batching that [ADR-012](../architecture/decisions/ADR-012-chained-lambda-architecture.md) describes. If S3 answers "Unable to validate the following destination configurations", its view of the permission from step 10 has not caught up yet; wait a few seconds and retry.

## Step 12: Initialise the LanceDB index (provisioner step 8)

```bash
uv run python scripts/init_index.py s3://${BUCKET}/metadata
aws s3api head-object --bucket ${BUCKET} --key metadata/_init_metadata.json
```

This writes the empty `global_index` table and the `_init_metadata.json` sidecar that records the model versions and a canary hash. The `head-object` is the same post-check the script performs: `init_index.py` can exit 0 and still write nothing if LanceDB resolved a different AWS profile than you expected, and the embedder would then fail later against an empty dataset. If the sidecar is missing, confirm `AWS_PROFILE` is exported and re-run.

## Step 13: Smoke test (provisioner step 9)

Upload the fixture and watch the two stages land. The budgets match the script: 60 s for the manifest, then 120 s for the Lance commit. The first invocation after a deploy can take 60 to 90 s while Lambda pulls the fresh images, which the budgets allow for.

```bash
# The document id is the sha256 of the DECODED text (line endings normalised),
# exactly as the chunker computes it. Hashing the raw bytes gives a different id
# for any file with CRLF line endings.
EXPECTED_ID=$(uv run python -c "
import hashlib, pathlib
text = pathlib.Path('data/samples/finance_report.txt').read_text(encoding='utf-8')
print(hashlib.sha256(text.encode('utf-8')).hexdigest()[:16])")

INITIAL=$(aws s3 ls s3://${BUCKET}/metadata/ --recursive | wc -l)
aws s3 cp data/samples/finance_report.txt s3://${BUCKET}/raw/finance_report.txt

# Stage 1: the manifest (60 s budget)
for i in $(seq 1 12); do
    if aws s3api head-object --bucket ${BUCKET} \
        --key chunks/${EXPECTED_ID}/MANIFEST.json > /dev/null 2>&1; then
        echo "manifest landed after about $((i * 5)) s"; break
    fi
    [ "$i" -eq 12 ] && echo "FAILED at stage manifest"
    sleep 5
done

# Stage 2: the Lance commit, seen as new objects under metadata/ (120 s budget)
for i in $(seq 1 24); do
    NOW=$(aws s3 ls s3://${BUCKET}/metadata/ --recursive | wc -l)
    if [ "$NOW" -gt "$INITIAL" ]; then
        echo "Lance commit landed after about $((i * 5)) s"; break
    fi
    [ "$i" -eq 24 ] && echo "FAILED at stage lance"
    sleep 5
done
```

## Optional: verify least privilege

The policies in `infra/iam/` are shape-tested in `tests/test_iam_policies.py`, but that does not exercise AWS's own evaluator. If you want to confirm the two roles really cannot reach each other's data, run one positive and one negative probe per role through `aws iam simulate-custom-policy`, which evaluates a policy document without attaching it to anything. This needs `iam:SimulateCustomPolicy`, which the deployer policy does not grant; use your admin profile.

```bash
# Chunker may read raw/: expect "allowed"
aws iam simulate-custom-policy --profile <your-admin-profile> \
    --policy-input-list "$(cat ./outputs/chunker-policy.json)" \
    --action-names s3:GetObject \
    --resource-arns "arn:aws:s3:::${BUCKET}/raw/finance_report.txt" \
    --query "EvaluationResults[0].EvalDecision"

# Chunker may not touch metadata/: expect "implicitDeny"
aws iam simulate-custom-policy --profile <your-admin-profile> \
    --policy-input-list "$(cat ./outputs/chunker-policy.json)" \
    --action-names s3:DeleteObject \
    --resource-arns "arn:aws:s3:::${BUCKET}/metadata/global_index.lance" \
    --query "EvaluationResults[0].EvalDecision"

# Embedder may write metadata/: expect "allowed"
aws iam simulate-custom-policy --profile <your-admin-profile> \
    --policy-input-list "$(cat ./outputs/embedder-policy.json)" \
    --action-names s3:PutObject \
    --resource-arns "arn:aws:s3:::${BUCKET}/metadata/global_index.lance" \
    --query "EvaluationResults[0].EvalDecision"

# Embedder may not read raw/: expect "implicitDeny"
aws iam simulate-custom-policy --profile <your-admin-profile> \
    --policy-input-list "$(cat ./outputs/embedder-policy.json)" \
    --action-names s3:GetObject \
    --resource-arns "arn:aws:s3:::${BUCKET}/raw/finance_report.txt" \
    --query "EvaluationResults[0].EvalDecision"
```

## Troubleshooting

If the smoke test times out:

```bash
aws logs tail /aws/lambda/${LAMBDA_PREFIX}-chunker  --since 10m
aws logs tail /aws/lambda/${LAMBDA_PREFIX}-embedder --since 10m
for which in chunker embedder; do
    aws sqs get-queue-attributes \
      --queue-url "$(aws sqs get-queue-url --queue-name ${LAMBDA_PREFIX}-${which}-dlq --query QueueUrl --output text)" \
      --attribute-names ApproximateNumberOfMessages
done
```

Common causes:

- **Cold start.** The first invocation after pushing a new image takes 60 to 90 s. Wait once and re-run the smoke test; later invocations take 5 to 15 s.
- **IAM denial in the logs.** Re-check the substitutions in step 7; account id and region must be yours exactly.
- **Trigger not firing.** Re-check step 11; the `chunks/` prefix and `MANIFEST.json` suffix pair must be exact.
- **Canary mismatch.** If you pushed an embedder image with new `fastembed` or model versions, the canary recorded in `s3://${BUCKET}/metadata/_init_metadata.json` is stale. Re-run step 12.
- **Commit conflicts under concurrent uploads.** LanceDB commits with optimistic concurrency: when two embedders commit at once one wins and the other retries. If you upload many documents at once and see `ConflictException` messages in the embedder DLQ, you can serialise the embedder with `aws lambda put-function-concurrency --function-name ${LAMBDA_PREFIX}-embedder --reserved-concurrent-executions 1`, trading throughput for guaranteed single-writer commits.

The longer catalogue, with causes and fixes, is in [aws-deployment-runbook.md](aws-deployment-runbook.md#part-4-troubleshooting).

## Cleanup

See [aws-prerequisites.md](aws-prerequisites.md#cleanup).
