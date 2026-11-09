"""AWS provisioner for the chained-Lambda execution mode.

Idempotent by construction: every step looks before it mutates, so running
the deploy twice against the same account produces zero ``Create*`` calls
the second time (``tests/test_deploy_idempotency.py`` proves this under moto).

Steps, in order:
  1. Bucket (versioning + default encryption).
  2. Folder marker objects (``raw/``, ``chunks/``, ``metadata/``) so the
     prefixes are visible in the console before anything is uploaded.
  3. Dead-letter queues for both Lambdas.
  4. ECR repositories, their Lambda-pull policy, and a lifecycle policy that
     expires untagged images.
  5. IAM execution roles with inline least-privilege policies.
  6. The two Lambda functions (image URIs come from the bash wrapper, which
     builds and pushes them), with configuration reconciled on re-runs and
     a 14-day CloudWatch log retention.
  7. Invoke permissions and the S3 notification configuration: ``raw/`` →
     chunker, ``chunks/*/MANIFEST.json`` → embedder.
  8. ``scripts/init_index.py`` against ``s3://<bucket>/metadata``.
  9. Optional smoke test: upload a fixture, wait for its manifest, then for
     the Lance commit.

Public entry: :func:`deploy`.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from string import Template

import boto3
from botocore.exceptions import ClientError

REPO_ROOT = Path(__file__).resolve().parent.parent
INFRA = REPO_ROOT / "infra"

logger = logging.getLogger(__name__)


@dataclass
class DeployConfig:
    bucket: str
    region: str
    account_id: str
    lambda_prefix: str
    chunker_image_uri: str
    embedder_image_uri: str


LOG_RETENTION_DAYS = 14
ECR_UNTAGGED_IMAGES_TO_KEEP = 3


@dataclass
class DeployStats:
    """Counts of mutating AWS calls — used by the idempotency double-run test."""
    creates: int = 0
    puts: int = 0
    updates: int = 0
    skips: int = 0


def _substituted_policy(path: Path, cfg: DeployConfig) -> str:
    raw = path.read_text()
    return Template(raw).safe_substitute(
        BUCKET=cfg.bucket,
        ACCOUNT_ID=cfg.account_id,
        REGION=cfg.region,
        LAMBDA_PREFIX=cfg.lambda_prefix,
    )


# ---------------------------------------------------------------------------
# Step helpers — each one is idempotent (check before mutate, log [skip] on no-op)
# ---------------------------------------------------------------------------


def ensure_bucket(s3, cfg: DeployConfig, stats: DeployStats) -> None:
    try:
        s3.head_bucket(Bucket=cfg.bucket)
        logger.info("[skip] bucket %s already exists", cfg.bucket)
        stats.skips += 1
        return
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") not in {"404", "NoSuchBucket", "NotFound"}:
            raise
    logger.info("[create] bucket %s in %s", cfg.bucket, cfg.region)
    if cfg.region == "us-east-1":
        s3.create_bucket(Bucket=cfg.bucket)
    else:
        s3.create_bucket(
            Bucket=cfg.bucket,
            CreateBucketConfiguration={"LocationConstraint": cfg.region},
        )
    s3.put_bucket_versioning(
        Bucket=cfg.bucket, VersioningConfiguration={"Status": "Enabled"}
    )
    s3.put_bucket_encryption(
        Bucket=cfg.bucket,
        ServerSideEncryptionConfiguration={
            "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
        },
    )
    stats.creates += 1


def ensure_folders(s3, cfg: DeployConfig, stats: DeployStats) -> None:
    for prefix in ("raw/", "chunks/", "metadata/"):
        try:
            s3.head_object(Bucket=cfg.bucket, Key=prefix)
            stats.skips += 1
            continue
        except ClientError:
            pass
        logger.info("[put] marker object %s", prefix)
        s3.put_object(Bucket=cfg.bucket, Key=prefix, Body=b"")
        stats.puts += 1


def ensure_dlqs(sqs, cfg: DeployConfig, stats: DeployStats) -> dict:
    arns = {}
    for which in ("chunker", "embedder"):
        name = f"{cfg.lambda_prefix}-{which}-dlq"
        try:
            url = sqs.get_queue_url(QueueName=name)["QueueUrl"]
            logger.info("[skip] DLQ %s already exists", name)
            stats.skips += 1
        except ClientError:
            logger.info("[create] DLQ %s", name)
            response = sqs.create_queue(
                QueueName=name,
                Attributes={"VisibilityTimeout": "1800"},  # 30 min
            )
            url = response["QueueUrl"]
            stats.creates += 1
        attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])
        arns[which] = attrs["Attributes"]["QueueArn"]
    return arns


def ensure_ecr_repos(ecr, cfg: DeployConfig, stats: DeployStats) -> None:
    repo_policy = _substituted_policy(INFRA / "ecr" / "repository-policy.json", cfg)
    desired = json.loads(repo_policy)
    for which in ("chunker", "embedder"):
        name = f"{cfg.lambda_prefix}-{which}"
        try:
            ecr.describe_repositories(repositoryNames=[name])
            logger.info("[skip] ECR repo %s already exists", name)
            stats.skips += 1
        except ClientError:
            logger.info("[create] ECR repo %s", name)
            ecr.create_repository(repositoryName=name, imageScanningConfiguration={"scanOnPush": True})
            stats.creates += 1
        # Always (re)assert the Lambda-pull repo policy. Without this the
        # Lambda service principal can't pull from a private ECR repo, and
        # CreateFunction fails with "Lambda does not have permission to access
        # the ECR image". Idempotent: SetRepositoryPolicy overwrites.
        try:
            current = ecr.get_repository_policy(repositoryName=name)
            current_policy = json.loads(current.get("policyText", "{}"))
        except ClientError:
            current_policy = None
        if current_policy == desired:
            logger.info("[skip] ECR repo policy %s unchanged", name)
            stats.skips += 1
        else:
            logger.info("[put] ECR repo policy %s (Lambda pull access)", name)
            ecr.set_repository_policy(repositoryName=name, policyText=repo_policy)
            stats.updates += 1
        # Every content-hash build pushes a new image; without a lifecycle
        # policy the old untagged layers accumulate in ECR forever.
        lifecycle = {
            "rules": [
                {
                    "rulePriority": 1,
                    "description": f"keep the last {ECR_UNTAGGED_IMAGES_TO_KEEP} untagged images",
                    "selection": {
                        "tagStatus": "untagged",
                        "countType": "imageCountMoreThan",
                        "countNumber": ECR_UNTAGGED_IMAGES_TO_KEEP,
                    },
                    "action": {"type": "expire"},
                }
            ]
        }
        try:
            current_lifecycle = json.loads(
                ecr.get_lifecycle_policy(repositoryName=name).get("lifecyclePolicyText", "{}")
            )
        except ClientError:
            current_lifecycle = None
        if current_lifecycle == lifecycle:
            logger.info("[skip] ECR lifecycle policy %s unchanged", name)
            stats.skips += 1
        else:
            logger.info("[put] ECR lifecycle policy %s", name)
            ecr.put_lifecycle_policy(repositoryName=name, lifecyclePolicyText=json.dumps(lifecycle))
            stats.updates += 1


def ensure_iam_role(iam, cfg: DeployConfig, which: str, stats: DeployStats) -> str:
    role_name = f"{cfg.lambda_prefix}-{which}-role"
    trust = json.loads(_substituted_policy(INFRA / "iam" / f"{which}-execution-role.json", cfg))
    policy = _substituted_policy(INFRA / "iam" / f"{which}-execution-policy.json", cfg)
    try:
        existing = iam.get_role(RoleName=role_name)
        logger.info("[skip] IAM role %s already exists", role_name)
        stats.skips += 1
        role_arn = existing["Role"]["Arn"]
    except ClientError:
        logger.info("[create] IAM role %s", role_name)
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust),
        )
        role_arn = response["Role"]["Arn"]
        stats.creates += 1

    # PutRolePolicy is idempotent; if content matches we still log skip.
    inline_name = f"{cfg.lambda_prefix}-{which}-inline"
    try:
        existing_policy = iam.get_role_policy(RoleName=role_name, PolicyName=inline_name)
        existing_doc = json.dumps(existing_policy["PolicyDocument"], sort_keys=True)
        new_doc = json.dumps(json.loads(policy), sort_keys=True)
        if existing_doc == new_doc:
            logger.info("[skip] IAM inline policy %s unchanged", inline_name)
            stats.skips += 1
            return role_arn
    except ClientError:
        pass
    logger.info("[put] IAM inline policy %s", inline_name)
    iam.put_role_policy(RoleName=role_name, PolicyName=inline_name, PolicyDocument=policy)
    stats.updates += 1
    return role_arn


def ensure_lambda(
    lam,
    cfg: DeployConfig,
    which: str,
    image_uri: str,
    role_arn: str,
    dlq_arn: str,
    stats: DeployStats,
) -> str:
    name = f"{cfg.lambda_prefix}-{which}"
    if which == "chunker":
        env = {"ONEBUCKET_CHUNK_ROOT": f"s3://{cfg.bucket}/chunks"}
        memory = 1024
        timeout = 120
    else:
        env = {
            "ONEBUCKET_DATASET_ROOT": f"s3://{cfg.bucket}/metadata",
            "ONEBUCKET_CHUNK_ROOT": f"s3://{cfg.bucket}/chunks",
        }
        # 3008 MB is the default service-quota ceiling for new AWS accounts
        # (the AWS-wide max is 10240 MB but requires a Service Quotas request).
        # The embedder's resident set — BGE-small (~130 MB) + SPLADE (~500 MB)
        # + FastEmbed/numpy/pyarrow runtime (~600 MB) — fits well under 3008.
        # Bump to 4096+ once the per-account quota is raised; profile first.
        memory = 3008
        timeout = 300

    try:
        existing = lam.get_function(FunctionName=name)
        existing_image = existing["Code"].get("ImageUri", "")
        if existing_image == image_uri:
            logger.info("[skip] Lambda %s already at %s", name, image_uri)
            stats.skips += 1
        else:
            logger.info("[update] Lambda %s code -> %s", name, image_uri)
            lam.update_function_code(FunctionName=name, ImageUri=image_uri)
            stats.updates += 1
            # Lambda rejects a configuration update while the code update is
            # still being applied (ResourceConflictException), so wait.
            wait_for_function_update(lam, name)
        # Reconcile the configuration too, otherwise a changed bucket, memory
        # size or role silently stays at whatever the first deploy set.
        conf = existing["Configuration"]
        drifted = (
            (conf.get("Environment") or {}).get("Variables", {}) != env
            or conf.get("MemorySize") != memory
            or conf.get("Timeout") != timeout
            or conf.get("Role") != role_arn
            # AWS always reports DeadLetterConfig once set; some emulators omit it.
            or ("DeadLetterConfig" in conf and conf["DeadLetterConfig"].get("TargetArn") != dlq_arn)
        )
        if drifted:
            logger.info("[update] Lambda %s configuration", name)
            lam.update_function_configuration(
                FunctionName=name,
                Role=role_arn,
                MemorySize=memory,
                Timeout=timeout,
                Environment={"Variables": env},
                DeadLetterConfig={"TargetArn": dlq_arn},
            )
            wait_for_function_update(lam, name)
            stats.updates += 1
        else:
            logger.info("[skip] Lambda %s configuration unchanged", name)
            stats.skips += 1
    except ClientError:
        logger.info("[create] Lambda %s", name)
        # IAM eventual-consistency workaround: when an IAM role is freshly
        # created, lambda:CreateFunction can return InvalidParameterValueException
        # ("The role defined for the function cannot be assumed by Lambda") for
        # 5–30s while the trust policy propagates. AWS does not expose a
        # deterministic "ready" signal; the recommended pattern is bounded retry.
        # Six attempts at 5s/5s/10s/10s/15s/15s = up to ~60s, which covers the
        # vast majority of cases without blocking idempotent re-runs.
        delays = [5, 5, 10, 10, 15, 15]
        for attempt, delay in enumerate(delays + [None]):
            try:
                lam.create_function(
                    FunctionName=name,
                    PackageType="Image",
                    Code={"ImageUri": image_uri},
                    Role=role_arn,
                    MemorySize=memory,
                    Timeout=timeout,
                    Environment={"Variables": env},
                    DeadLetterConfig={"TargetArn": dlq_arn},
                )
                break
            except ClientError as e:
                # Only the explicit "cannot be assumed" message is known to
                # mean IAM eventual consistency. Any other InvalidParameterValue
                # is a real config error (bad image URI, bad memory size, etc.)
                # and must not be retried — would just delay the real failure.
                if "cannot be assumed by Lambda" not in str(e) or delay is None:
                    raise
                logger.info(
                    "[retry] Lambda %s create attempt %d failed (IAM role propagation); sleeping %ds",
                    name, attempt + 1, delay,
                )
                time.sleep(delay)
        wait_for_function_update(lam, name)
        stats.creates += 1
    return f"arn:aws:lambda:{cfg.region}:{cfg.account_id}:function:{name}"


def wait_for_function_update(lam, name: str, timeout_seconds: int = 300) -> None:
    """Block until the function is Active and its last update has finished.

    A container-image Lambda spends several seconds in ``Pending`` after
    creation and in ``InProgress`` after every code or configuration update.
    Issuing the next update, or invoking it, during that window fails with
    ``ResourceConflictException`` / ``ResourceNotReadyException``.
    """
    deadline = time.time() + timeout_seconds
    while True:
        conf = lam.get_function_configuration(FunctionName=name)
        state = conf.get("State", "Active")
        update = conf.get("LastUpdateStatus", "Successful")
        if state == "Failed" or update == "Failed":
            raise RuntimeError(
                f"Lambda {name} entered a failed state: "
                f"{conf.get('StateReason') or conf.get('LastUpdateStatusReason')}"
            )
        if state == "Active" and update == "Successful":
            return
        if time.time() > deadline:
            raise RuntimeError(f"Lambda {name} still {state}/{update} after {timeout_seconds}s")
        time.sleep(3)


def ensure_log_retention(logs, cfg: DeployConfig, stats: DeployStats) -> None:
    """Cap CloudWatch log retention so an idle deployment stops accumulating."""
    for which in ("chunker", "embedder"):
        group = f"/aws/lambda/{cfg.lambda_prefix}-{which}"
        existing = logs.describe_log_groups(logGroupNamePrefix=group).get("logGroups", [])
        match = next((g for g in existing if g.get("logGroupName") == group), None)
        if match is None:
            logger.info("[create] log group %s", group)
            logs.create_log_group(logGroupName=group)
            stats.creates += 1
        elif match.get("retentionInDays") == LOG_RETENTION_DAYS:
            logger.info("[skip] log group %s retention already %d days", group, LOG_RETENTION_DAYS)
            stats.skips += 1
            continue
        logger.info("[put] log group %s retention %d days", group, LOG_RETENTION_DAYS)
        logs.put_retention_policy(logGroupName=group, retentionInDays=LOG_RETENTION_DAYS)
        if match is not None:
            stats.updates += 1


def ensure_lambda_invoke_permission(
    lam, cfg: DeployConfig, which: str, stats: DeployStats
) -> None:
    name = f"{cfg.lambda_prefix}-{which}"
    statement_id = f"{name}-allow-s3-invoke"
    try:
        existing = lam.get_policy(FunctionName=name)
        if statement_id in existing.get("Policy", ""):
            logger.info("[skip] Lambda invoke permission %s already attached", statement_id)
            stats.skips += 1
            return
    except ClientError:
        pass
    logger.info("[add] Lambda invoke permission %s", statement_id)
    lam.add_permission(
        FunctionName=name,
        StatementId=statement_id,
        Action="lambda:InvokeFunction",
        Principal="s3.amazonaws.com",
        SourceArn=f"arn:aws:s3:::{cfg.bucket}",
    )
    stats.updates += 1


def ensure_s3_trigger(s3, cfg: DeployConfig, stats: DeployStats) -> None:
    config_raw = _substituted_policy(INFRA / "s3" / "notification-config.json", cfg)
    desired = json.loads(config_raw)
    desired.pop("_comment", None)

    existing = s3.get_bucket_notification_configuration(Bucket=cfg.bucket)
    existing.pop("ResponseMetadata", None)
    desired_lambda = desired.get("LambdaFunctionConfigurations", [])
    existing_lambda = existing.get("LambdaFunctionConfigurations", [])

    # Compare by sorted JSON; if equal, skip
    if json.dumps(desired_lambda, sort_keys=True) == json.dumps(existing_lambda, sort_keys=True):
        logger.info("[skip] S3 notification config unchanged")
        stats.skips += 1
        return
    logger.info("[put] S3 notification config")
    # Eventual-consistency workaround mirroring the Lambda role-propagation
    # retry above. When this step runs immediately after lambda:add_permission,
    # S3's pre-flight ("can I invoke this Lambda?") sometimes fails with
    # "Unable to validate the following destination configurations" because
    # S3 hasn't refreshed its view of the Lambda's resource policy yet. The
    # policy IS attached — S3's cache just hasn't caught up. Same backoff.
    delays = [5, 5, 10, 10, 15, 15]
    for attempt, delay in enumerate(delays + [None]):
        try:
            s3.put_bucket_notification_configuration(
                Bucket=cfg.bucket,
                NotificationConfiguration={"LambdaFunctionConfigurations": desired_lambda},
            )
            break
        except ClientError as e:
            # Only the explicit "Unable to validate" message matches eventual
            # consistency — any other InvalidArgument is a real config bug
            # (bad ARN, bad filter, etc.) and must surface immediately.
            if "Unable to validate" not in str(e) or delay is None:
                raise
            logger.info(
                "[retry] S3 notification put attempt %d failed (Lambda permission propagation); sleeping %ds",
                attempt + 1, delay,
            )
            time.sleep(delay)
    stats.updates += 1


def run_init_index(s3, cfg: DeployConfig, stats: DeployStats) -> None:
    """Initialize the LanceDB dataset against the s3:// dataset_root.

    Post-condition is verified by HEAD-ing the sidecar in S3 — earlier we hit
    a silent failure mode where ``subprocess.run(init_index.py, check=True)``
    returned 0 but the dataset was never actually written (caller's shell had
    a different AWS_PROFILE than the deploy expected, or LanceDB's underlying
    Rust object_store resolved creds differently than boto3). The post-check
    surfaces this immediately rather than letting the embedder no-op silently
    later in the pipeline.
    """
    dataset_root = f"s3://{cfg.bucket}/metadata"
    logger.info("[run] init_index against %s", dataset_root)
    subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "init_index.py"), dataset_root],
        check=True,
        cwd=str(REPO_ROOT),
    )
    sidecar_key = "metadata/_init_metadata.json"
    try:
        s3.head_object(Bucket=cfg.bucket, Key=sidecar_key)
    except ClientError as e:
        raise RuntimeError(
            f"init_index returned 0 but the sidecar at "
            f"s3://{cfg.bucket}/{sidecar_key} is missing. The most common cause is "
            f"that the subprocess inherited a different AWS profile than the deploy "
            f"expected — `export AWS_PROFILE=<your-profile>` in your shell before "
            f"running scripts/deploy_aws.sh and re-run."
        ) from e
    logger.info("[verified] sidecar present at s3://%s/%s", cfg.bucket, sidecar_key)
    stats.updates += 1


def expected_raw_file_id(fixture: Path) -> str:
    """The document id the chunker will assign to this file.

    The chunker hashes the *decoded text* (``Path.read_text`` normalises
    line endings), not the raw bytes, so the smoke test must do the same or a
    CRLF fixture would wait forever for a manifest that lands under a
    different id.
    """
    import hashlib

    text = fixture.read_text(encoding="utf-8")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def smoke_test(s3, cfg: DeployConfig, fixture: Path, timeout_seconds: int = 180) -> dict:
    """Two-stage smoke: wait for the manifest (60 s), then for the Lance commit
    (the remainder of ``timeout_seconds``, 120 s by default)."""
    body = fixture.read_bytes()
    raw_file_id = expected_raw_file_id(fixture)
    raw_key = f"raw/{fixture.name}"
    logger.info("[smoke] uploading %s → %s/%s", fixture.name, cfg.bucket, raw_key)
    s3.put_object(Bucket=cfg.bucket, Key=raw_key, Body=body)

    deadline = time.time() + 60  # Stage 1: 60s for manifest
    manifest_key = f"chunks/{raw_file_id}/MANIFEST.json"
    while time.time() < deadline:
        try:
            s3.head_object(Bucket=cfg.bucket, Key=manifest_key)
            logger.info("[smoke] manifest landed: %s", manifest_key)
            break
        except ClientError:
            time.sleep(5)
    else:
        return {"ok": False, "stage": "manifest", "expected_key": manifest_key}

    # Stage 2: 120s for the Lance commit, detected as new objects under
    # metadata/. The budget covers a cold start of a freshly pushed embedder
    # image (~25s) plus the embedding work itself with headroom.
    deadline = time.time() + max(timeout_seconds - 60, 30)
    metadata_seen_initial = _count_metadata_objects(s3, cfg.bucket)
    while time.time() < deadline:
        if _count_metadata_objects(s3, cfg.bucket) > metadata_seen_initial:
            logger.info("[smoke] new metadata objects landed (Lance row committed)")
            return {"ok": True, "raw_file_id": raw_file_id}
        time.sleep(5)
    return {"ok": False, "stage": "lance", "raw_file_id": raw_file_id}


def _count_metadata_objects(s3, bucket: str) -> int:
    response = s3.list_objects_v2(Bucket=bucket, Prefix="metadata/")
    return response.get("KeyCount", 0)


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------


def deploy(cfg: DeployConfig, *, run_smoke: bool = False, fixture: Path | None = None) -> DeployStats:
    """Run all idempotent provisioning steps. Returns mutation stats."""
    endpoint_override = os.environ.get("AWS_ENDPOINT_URL", "")
    if "onebucket.io" in endpoint_override:
        raise RuntimeError(
            "AWS_ENDPOINT_URL points at a OneBucket endpoint. The AWS deploy talks "
            "to IAM, Lambda, ECR and SQS and must use the real AWS control plane; "
            "unset AWS_ENDPOINT_URL for this command."
        )
    stats = DeployStats()
    s3 = boto3.client("s3", region_name=cfg.region)
    sqs = boto3.client("sqs", region_name=cfg.region)
    ecr = boto3.client("ecr", region_name=cfg.region)
    iam = boto3.client("iam", region_name=cfg.region)
    lam = boto3.client("lambda", region_name=cfg.region)
    logs = boto3.client("logs", region_name=cfg.region)

    ensure_bucket(s3, cfg, stats)
    ensure_folders(s3, cfg, stats)
    dlq_arns = ensure_dlqs(sqs, cfg, stats)
    ensure_ecr_repos(ecr, cfg, stats)

    chunker_role_arn = ensure_iam_role(iam, cfg, "chunker", stats)
    embedder_role_arn = ensure_iam_role(iam, cfg, "embedder", stats)

    ensure_lambda(
        lam, cfg, "chunker", cfg.chunker_image_uri, chunker_role_arn, dlq_arns["chunker"], stats
    )
    ensure_lambda(
        lam, cfg, "embedder", cfg.embedder_image_uri, embedder_role_arn, dlq_arns["embedder"], stats
    )
    ensure_log_retention(logs, cfg, stats)
    ensure_lambda_invoke_permission(lam, cfg, "chunker", stats)
    ensure_lambda_invoke_permission(lam, cfg, "embedder", stats)
    ensure_s3_trigger(s3, cfg, stats)
    run_init_index(s3, cfg, stats)

    if run_smoke and fixture is not None:
        for which in ("chunker", "embedder"):
            wait_for_function_update(lam, f"{cfg.lambda_prefix}-{which}")
        result = smoke_test(s3, cfg, fixture)
        logger.info("[smoke] result: %s", result)
        if not result.get("ok"):
            raise RuntimeError(f"smoke test failed at stage {result.get('stage')}: {result}")

    return stats


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str]) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="aws_deploy")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--lambda-prefix", default="onebucket")
    parser.add_argument("--chunker-image-uri", required=True)
    parser.add_argument("--embedder-image-uri", required=True)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument(
        "--smoke-fixture",
        type=Path,
        default=REPO_ROOT / "data" / "samples" / "finance_report.txt",
    )
    args = parser.parse_args(argv)

    sts = boto3.client("sts", region_name=args.region)
    account_id = sts.get_caller_identity()["Account"]

    cfg = DeployConfig(
        bucket=args.bucket,
        region=args.region,
        account_id=account_id,
        lambda_prefix=args.lambda_prefix,
        chunker_image_uri=args.chunker_image_uri,
        embedder_image_uri=args.embedder_image_uri,
    )

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    stats = deploy(cfg, run_smoke=args.smoke, fixture=args.smoke_fixture)
    print(
        f"deploy complete — creates={stats.creates} puts={stats.puts} "
        f"updates={stats.updates} skips={stats.skips}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
