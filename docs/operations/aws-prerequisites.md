# AWS Deployment Prerequisites

> **Scope: the `lambda` execution mode only.** This page covers what you need to run the OneBucket demo pipeline on AWS Lambda in your own account. In the default `local-daemon` mode no Lambda is involved; see [runbooks/local-daemon.md](runbooks/local-daemon.md) for what that mode needs. For an overview of all three execution modes, see [execution-modes.md](execution-modes.md).

The requirements are the same for the scripted path (`bash scripts/deploy_aws.sh`) and the manual path ([aws-manual-deploy.md](aws-manual-deploy.md)).

## Tool installs

| Tool | Needed for | macOS install |
|---|---|---|
| AWS CLI v2 | Both paths | `brew install awscli` |
| Docker Desktop | Both paths (image build and push) | `brew install --cask docker` or download from docker.com |
| `uv` | Both paths (runs the provisioner and `init_index`) | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| `jq` | Manual path only (strips the comment from the notification config) | `brew install jq` |
| Git | Both paths | bundled with macOS |

## AWS account requirements

- An AWS account and a profile whose permissions cover the list below.
- A region. The default is `us-east-1`; pass `--region` to the deploy script to change it. Every example in these docs uses `us-east-1`.
- Lambda container images and ECR are available in every major region, so any of them works.

### Permissions the deploying identity needs

The identity running `deploy_aws.sh` (or following the manual steps) must be allowed to do everything the provisioner does. The exact resource-scoped policy is [infra/iam/deployer-policy.json](../../infra/iam/deployer-policy.json), and [aws-deployer-iam-setup.md](aws-deployer-iam-setup.md) shows how to attach it to a dedicated user. In plain words the deploy needs to:

- **S3:** create the bucket, enable versioning and default encryption, read and write the bucket notification configuration, list the bucket, and put, get and delete objects in it. (API names and IAM action names differ: `HeadBucket` authorises as `s3:ListBucket`, `HeadObject` as `s3:GetObject`, `PutBucketEncryption` as `s3:PutEncryptionConfiguration`, `PutBucketNotificationConfiguration` as `s3:PutBucketNotification`.)
- **SQS:** create the two dead-letter queues and read their URL and attributes. Receiving and deleting messages is included so you can inspect and drain failed invocations while debugging.
- **ECR:** get an auth token, create and describe repositories, push images (`BatchCheckLayerAvailability`, `InitiateLayerUpload`, `UploadLayerPart`, `CompleteLayerUpload`, `PutImage`, `BatchGetImage`), and get and set the repository policy and lifecycle policy. `BatchGetImage` is needed because `docker push` HEADs the manifest; the repository policy is what lets the Lambda service pull the image.
- **IAM:** create and get the two execution roles, put and get their inline policies, and pass them to Lambda.
- **Lambda:** create, get, update (code and configuration) and invoke the two functions, add the S3 invoke permission and read the function policy.
- **CloudWatch Logs:** describe and create the two log groups, set their retention, and read their streams and events (for `aws logs tail` while debugging). The execution roles grant the functions their own `logs:Create*` and `PutLogEvents` separately.
- **STS:** `GetCallerIdentity`.

If a deploy step fails with `AccessDenied`, the error names the action; compare it against the policy file. If your administrator will not grant these permissions even temporarily, the [manual-deploy doc](aws-manual-deploy.md) is written so that any single step can be handed to them.

### AWS SSO

The deploy script does not detect SSO up front. It runs `aws sts get-caller-identity` first, and only if that fails does it look at the profile: a profile with `sso_start_url` or `sso_session` configured gets `aws sso login --profile <name>` run for it (which opens a browser), a static-key profile whose keys are rejected gets an error pointing you at `~/.aws/credentials`, and a profile name that is not configured at all gets an error telling you to run `aws configure sso` or `aws configure`.

To check your profile before deploying:

```bash
aws configure list-profiles               # should include your profile
aws sso login --profile <name>            # SSO profiles only
aws sts get-caller-identity --profile <name>
```

`Unable to locate credentials` means there is no identity configured at all, not that an SSO session expired. Telling those two apart is the most common first-time stumble.

## Cost expectations

An idle deployment costs very little.

| Resource | Approximate monthly cost when idle |
|---|---|
| S3 storage | nothing measurable for tens of MB; pennies per GB |
| ECR storage | about $0.30 (chunker about 200 MB, embedder about 2 GB, at $0.10 per GB-month); the lifecycle policy keeps only the last three untagged images |
| Lambda invocations | nothing when idle; well under $0.01 per document at demo volume |
| SQS dead-letter queues | nothing when empty |
| CloudWatch Logs | pennies; retention is capped at 14 days |

The deploy sets no provisioned concurrency, so there is no standing Lambda charge.

## Cleanup

The deployer policy grants no delete actions beyond `s3:DeleteObject` on current object versions, on purpose: a deployer that cannot delete cannot destroy a running deployment by accident. Run the cleanup below with a profile that can delete (your admin profile). Set the same variables as the deploy used:

```bash
export AWS_PROFILE=<your-admin-profile>
export AWS_DEFAULT_REGION=us-east-1
export BUCKET=<your-bucket>
export LAMBDA_PREFIX=onebucket
```

**1. Empty the bucket, including every object version and delete marker.** The deploy enabled versioning, so `aws s3 rm --recursive` is not enough: it only adds delete markers, and `delete-bucket` refuses while any version or marker remains.

```bash
for kind in Versions DeleteMarkers; do
    while :; do
        batch=$(aws s3api list-object-versions --bucket "${BUCKET}" --max-items 1000 \
            --query "{Objects: ${kind}[].{Key:Key,VersionId:VersionId}}" --output json)
        [ "$(echo "${batch}" | jq '.Objects | length')" -gt 0 ] || break
        aws s3api delete-objects --bucket "${BUCKET}" --delete "${batch}" > /dev/null
    done
done
aws s3api delete-bucket --bucket "${BUCKET}"
```

**2. Delete the functions, roles, queues, repositories and log groups.**

```bash
for which in chunker embedder; do
    aws lambda delete-function --function-name ${LAMBDA_PREFIX}-${which}
    aws iam delete-role-policy --role-name ${LAMBDA_PREFIX}-${which}-role \
        --policy-name ${LAMBDA_PREFIX}-${which}-inline
    aws iam delete-role --role-name ${LAMBDA_PREFIX}-${which}-role
    aws sqs delete-queue --queue-url "$(aws sqs get-queue-url \
        --queue-name ${LAMBDA_PREFIX}-${which}-dlq --query QueueUrl --output text)"
    aws ecr delete-repository --repository-name ${LAMBDA_PREFIX}-${which} --force
    aws logs delete-log-group --log-group-name /aws/lambda/${LAMBDA_PREFIX}-${which}
done
```

Which steps need the admin profile: all of step 1 except deleting current object versions (`s3:DeleteObjectVersion` and `s3:DeleteBucket` are not in the deployer policy), and every command in step 2 (`lambda:DeleteFunction`, `iam:DeleteRolePolicy`, `iam:DeleteRole`, `sqs:DeleteQueue`, `ecr:DeleteRepository`, `logs:DeleteLogGroup` are not granted either). In practice run the whole cleanup as admin.

To remove the dedicated deployer user afterwards, see [aws-deployer-iam-setup.md](aws-deployer-iam-setup.md#cleaning-up-later).
