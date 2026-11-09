"""IAM policy validation + structural-shape tests.

Positive (allowed-for-allowed) and negative (denied-for-denied) coverage of
the chunker + embedder execution policies.

**Note on coverage:** moto 5.x does not implement ``iam:SimulateCustomPolicy``,
so live-AWS policy simulation has to be run manually against a deployed
account. Here we assert the same invariants structurally — walking
the policy Statements and verifying the right Action/Resource shapes are
present (positive cases) or absent (negative cases). This catches the same
class of policy-drift bugs as the moto-simulation pass would, with the caveat
that AWS's actual evaluation engine is not exercised.
"""

from __future__ import annotations

import json
from pathlib import Path
from string import Template

REPO_ROOT = Path(__file__).resolve().parent.parent
INFRA = REPO_ROOT / "infra"

TEST_BUCKET = "ob-test-bucket"
TEST_ACCOUNT = "123456789012"
TEST_REGION = "us-east-1"
TEST_PREFIX = "onebucket-test"


def _load_policy(path: Path) -> dict:
    raw = path.read_text()
    substituted = Template(raw).safe_substitute(
        BUCKET=TEST_BUCKET,
        ACCOUNT_ID=TEST_ACCOUNT,
        REGION=TEST_REGION,
        LAMBDA_PREFIX=TEST_PREFIX,
    )
    return json.loads(substituted)


def _statements_for(policy: dict, action: str, resource_substr: str) -> list[dict]:
    """Find Allow Statements whose Action+Resource match the request."""
    matches = []
    for stmt in policy["Statement"]:
        if stmt["Effect"] != "Allow":
            continue
        actions = stmt["Action"] if isinstance(stmt["Action"], list) else [stmt["Action"]]
        resources = stmt["Resource"] if isinstance(stmt["Resource"], list) else [stmt["Resource"]]
        if action in actions and any(resource_substr in r for r in resources):
            matches.append(stmt)
    return matches


def _allowed(policy: dict, action: str, resource_substr: str) -> bool:
    return bool(_statements_for(policy, action, resource_substr))


# ---------------------------------------------------------------------------
# JSON shape validation
# ---------------------------------------------------------------------------


def test_chunker_policy_json_shape():
    policy = _load_policy(INFRA / "iam" / "chunker-execution-policy.json")
    assert policy["Version"] == "2012-10-17"
    assert isinstance(policy["Statement"], list)
    for stmt in policy["Statement"]:
        assert stmt["Effect"] in {"Allow", "Deny"}
        assert "Action" in stmt
        assert "Resource" in stmt


def test_embedder_policy_json_shape():
    policy = _load_policy(INFRA / "iam" / "embedder-execution-policy.json")
    assert policy["Version"] == "2012-10-17"
    assert isinstance(policy["Statement"], list)
    for stmt in policy["Statement"]:
        assert stmt["Effect"] in {"Allow", "Deny"}
        assert "Action" in stmt
        assert "Resource" in stmt


def test_chunker_trust_policy_assumes_lambda():
    trust = json.loads((INFRA / "iam" / "chunker-execution-role.json").read_text())
    [stmt] = trust["Statement"]
    assert stmt["Principal"] == {"Service": "lambda.amazonaws.com"}
    assert stmt["Action"] == "sts:AssumeRole"


def test_embedder_trust_policy_assumes_lambda():
    trust = json.loads((INFRA / "iam" / "embedder-execution-role.json").read_text())
    [stmt] = trust["Statement"]
    assert stmt["Principal"] == {"Service": "lambda.amazonaws.com"}
    assert stmt["Action"] == "sts:AssumeRole"


# ---------------------------------------------------------------------------
# Negative: no wildcards
# ---------------------------------------------------------------------------


def test_chunker_no_wildcard_actions_or_resources():
    _assert_no_wildcards(INFRA / "iam" / "chunker-execution-policy.json")


def test_embedder_no_wildcard_actions_or_resources():
    _assert_no_wildcards(INFRA / "iam" / "embedder-execution-policy.json")


def _assert_no_wildcards(policy_path):
    policy = _load_policy(policy_path)
    for stmt in policy["Statement"]:
        actions = stmt["Action"] if isinstance(stmt["Action"], list) else [stmt["Action"]]
        for action in actions:
            assert action != "*", f"naked wildcard Action in {policy_path.name}"
            assert action != "s3:*", f"s3:* wildcard in {policy_path.name}"
            assert action != "iam:*", f"iam:* wildcard in {policy_path.name}"
        resources = stmt["Resource"] if isinstance(stmt["Resource"], list) else [stmt["Resource"]]
        assert "*" not in resources, f"naked Resource: '*' in {policy_path.name}"


# ---------------------------------------------------------------------------
# Chunker policy — positive + negative cases
# ---------------------------------------------------------------------------


def test_chunker_can_read_raw():
    policy = _load_policy(INFRA / "iam" / "chunker-execution-policy.json")
    assert _allowed(policy, "s3:GetObject", f"{TEST_BUCKET}/raw/")


def test_chunker_can_write_chunks():
    policy = _load_policy(INFRA / "iam" / "chunker-execution-policy.json")
    assert _allowed(policy, "s3:PutObject", f"{TEST_BUCKET}/chunks/")


def test_chunker_cannot_touch_metadata():
    """Cross-role isolation: chunker has NO statement granting metadata/ access."""
    policy = _load_policy(INFRA / "iam" / "chunker-execution-policy.json")
    for action in ("s3:GetObject", "s3:PutObject", "s3:DeleteObject"):
        assert not _allowed(policy, action, f"{TEST_BUCKET}/metadata/"), (
            f"chunker policy unexpectedly grants {action} on metadata/"
        )


def test_chunker_cannot_delete_raw():
    """Chunker reads raw/ but does not own it — no DeleteObject on raw/."""
    policy = _load_policy(INFRA / "iam" / "chunker-execution-policy.json")
    assert not _allowed(policy, "s3:DeleteObject", f"{TEST_BUCKET}/raw/")


def test_chunker_list_bucket_is_prefix_scoped():
    policy = _load_policy(INFRA / "iam" / "chunker-execution-policy.json")
    [list_stmt] = [s for s in policy["Statement"] if "s3:ListBucket" in (
        s["Action"] if isinstance(s["Action"], list) else [s["Action"]]
    )]
    assert "Condition" in list_stmt, "ListBucket must be Condition-scoped"
    prefixes = list_stmt["Condition"]["StringLike"]["s3:prefix"]
    assert "raw/*" in prefixes
    assert "chunks/*" in prefixes
    # Chunker explicitly does NOT have metadata/* in its prefix scope
    assert "metadata/*" not in prefixes


# ---------------------------------------------------------------------------
# Embedder policy — positive + negative cases
# ---------------------------------------------------------------------------


def test_embedder_can_read_chunks():
    policy = _load_policy(INFRA / "iam" / "embedder-execution-policy.json")
    assert _allowed(policy, "s3:GetObject", f"{TEST_BUCKET}/chunks/")


def test_embedder_can_write_metadata():
    policy = _load_policy(INFRA / "iam" / "embedder-execution-policy.json")
    for action in ("s3:GetObject", "s3:PutObject", "s3:DeleteObject"):
        assert _allowed(policy, action, f"{TEST_BUCKET}/metadata/"), (
            f"embedder policy unexpectedly missing {action} on metadata/"
        )


def test_embedder_cannot_touch_raw():
    """Cross-role isolation: embedder has NO access to raw/."""
    policy = _load_policy(INFRA / "iam" / "embedder-execution-policy.json")
    for action in ("s3:GetObject", "s3:PutObject", "s3:DeleteObject"):
        assert not _allowed(policy, action, f"{TEST_BUCKET}/raw/"), (
            f"embedder policy unexpectedly grants {action} on raw/"
        )


def test_embedder_cannot_delete_chunks():
    """Embedder reads chunks/ but chunker owns them; no DeleteObject on chunks/."""
    policy = _load_policy(INFRA / "iam" / "embedder-execution-policy.json")
    assert not _allowed(policy, "s3:DeleteObject", f"{TEST_BUCKET}/chunks/")


def test_embedder_list_bucket_is_prefix_scoped():
    policy = _load_policy(INFRA / "iam" / "embedder-execution-policy.json")
    [list_stmt] = [s for s in policy["Statement"] if "s3:ListBucket" in (
        s["Action"] if isinstance(s["Action"], list) else [s["Action"]]
    )]
    assert "Condition" in list_stmt
    prefixes = list_stmt["Condition"]["StringLike"]["s3:prefix"]
    assert "chunks/*" in prefixes
    assert "metadata/*" in prefixes
    # Embedder explicitly does NOT have raw/* in its prefix scope
    assert "raw/*" not in prefixes


# ---------------------------------------------------------------------------
# DLQ + log-group ARN structure
# ---------------------------------------------------------------------------


def test_chunker_dlq_send_message_present():
    policy = _load_policy(INFRA / "iam" / "chunker-execution-policy.json")
    assert _allowed(
        policy,
        "sqs:SendMessage",
        f"{TEST_PREFIX}-chunker-dlq",
    )


def test_embedder_dlq_send_message_present():
    policy = _load_policy(INFRA / "iam" / "embedder-execution-policy.json")
    assert _allowed(
        policy,
        "sqs:SendMessage",
        f"{TEST_PREFIX}-embedder-dlq",
    )


def test_chunker_log_group_arn_includes_account_and_region():
    """Log-group ARNs must be account-id + region bound, not wildcarded."""
    policy = _load_policy(INFRA / "iam" / "chunker-execution-policy.json")
    [log_stmt] = [
        s for s in policy["Statement"]
        if "logs:PutLogEvents" in (s["Action"] if isinstance(s["Action"], list) else [s["Action"]])
    ]
    resource = log_stmt["Resource"] if isinstance(log_stmt["Resource"], str) else log_stmt["Resource"][0]
    assert TEST_REGION in resource
    assert TEST_ACCOUNT in resource
    assert f"/aws/lambda/{TEST_PREFIX}-chunker" in resource


def test_embedder_log_group_arn_includes_account_and_region():
    policy = _load_policy(INFRA / "iam" / "embedder-execution-policy.json")
    [log_stmt] = [
        s for s in policy["Statement"]
        if "logs:PutLogEvents" in (s["Action"] if isinstance(s["Action"], list) else [s["Action"]])
    ]
    resource = log_stmt["Resource"] if isinstance(log_stmt["Resource"], str) else log_stmt["Resource"][0]
    assert TEST_REGION in resource
    assert TEST_ACCOUNT in resource
    assert f"/aws/lambda/{TEST_PREFIX}-embedder" in resource


# ---------------------------------------------------------------------------
# S3 notification config
# ---------------------------------------------------------------------------


def test_notification_config_has_chunker_and_embedder():
    config = _load_policy(INFRA / "s3" / "notification-config.json")
    confs = config["LambdaFunctionConfigurations"]
    ids = {c["Id"] for c in confs}
    assert "ChunkerOnRawCreated" in ids
    assert "EmbedderOnManifestCreated" in ids


def test_chunker_trigger_filters_raw_prefix_only():
    config = _load_policy(INFRA / "s3" / "notification-config.json")
    chunker = next(
        c for c in config["LambdaFunctionConfigurations"] if c["Id"] == "ChunkerOnRawCreated"
    )
    rules = chunker["Filter"]["Key"]["FilterRules"]
    assert any(r["Name"] == "prefix" and r["Value"] == "raw/" for r in rules)
    assert not any(r["Name"] == "suffix" for r in rules)


def test_embedder_trigger_filters_chunks_prefix_and_manifest_suffix():
    """The chained-architecture-defining trigger: prefix=chunks/ AND suffix=MANIFEST.json."""
    config = _load_policy(INFRA / "s3" / "notification-config.json")
    embedder = next(
        c for c in config["LambdaFunctionConfigurations"] if c["Id"] == "EmbedderOnManifestCreated"
    )
    rules = embedder["Filter"]["Key"]["FilterRules"]
    assert any(r["Name"] == "prefix" and r["Value"] == "chunks/" for r in rules)
    assert any(r["Name"] == "suffix" and r["Value"] == "MANIFEST.json" for r in rules)
