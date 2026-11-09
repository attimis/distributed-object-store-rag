#!/usr/bin/env bash
# One-command AWS deploy for the chained-Lambda execution mode.
#
# The Python module scripts/aws_deploy.py makes the AWS API calls and is
# idempotent (every step checks before it mutates, so re-running against a
# deployed stack only logs [skip]). This wrapper does the parts that need a
# shell: the credential pre-check, the two Docker image builds, and the ECR
# push. It then hands the pushed image URIs to the provisioner.
#
# Usage:
#   bash scripts/deploy_aws.sh --bucket my-bucket [--region us-east-1] \
#                              [--lambda-prefix onebucket] \
#                              [--profile <aws-profile>]
#
# Environment:
#   AWS_PROFILE            AWS named profile (default: --profile, else "default")
#   ONEBUCKET_RUN_SMOKE=0  skip the post-deploy smoke test (default: run it)
#
# The deploy talks to AWS control-plane services (IAM, Lambda, ECR, SQS). It
# must NOT run with AWS_ENDPOINT_URL pointed at a OneBucket S3 endpoint; the
# provisioner refuses if it is.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

BUCKET=""
REGION="us-east-1"
PROFILE="${AWS_PROFILE:-default}"
LAMBDA_PREFIX="onebucket"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --bucket) BUCKET="$2"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        --profile) PROFILE="$2"; shift 2 ;;
        --lambda-prefix) LAMBDA_PREFIX="$2"; shift 2 ;;
        --help|-h)
            sed -n '2,/^$/p' "${BASH_SOURCE[0]}"
            exit 0
            ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

[[ -n "$BUCKET" ]] || { echo "ERROR: --bucket is required" >&2; exit 2; }

if [[ "${AWS_ENDPOINT_URL:-}" == *onebucket.io* ]]; then
    echo "ERROR: AWS_ENDPOINT_URL points at a OneBucket endpoint. The deploy needs" >&2
    echo "       the real AWS control plane; unset it for this command." >&2
    exit 2
fi

export AWS_PROFILE="$PROFILE"
export AWS_DEFAULT_REGION="$REGION"

# 1. Verify AWS credentials. For an SSO profile with no active session, start
#    the browser login; for anything else, explain what to fix.
echo "==> Verifying AWS credentials (profile=$PROFILE region=$REGION)"
if ! aws sts get-caller-identity --output text > /dev/null 2>&1; then
    if aws configure list-profiles 2>/dev/null | grep -qx "$PROFILE"; then
        if aws configure get sso_start_url --profile "$PROFILE" > /dev/null 2>&1 \
           || aws configure get sso_session --profile "$PROFILE" > /dev/null 2>&1; then
            echo "    No active SSO session; running: aws sso login --profile $PROFILE"
            aws sso login --profile "$PROFILE"
        else
            echo "ERROR: profile '$PROFILE' exists but its credentials were rejected." >&2
            echo "       Check ~/.aws/credentials for that profile, or re-run 'aws configure --profile $PROFILE'." >&2
            exit 1
        fi
    else
        echo "ERROR: profile '$PROFILE' is not configured." >&2
        echo "       Run 'aws configure sso' (SSO) or 'aws configure' (access keys), or set AWS_PROFILE." >&2
        exit 1
    fi
fi
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "    Authenticated to account $ACCOUNT_ID"

# 2. Build both images and push them to ECR under their content-hash tags.
echo "==> Building chunker image"
bash scripts/build_chunker_image.sh
echo "==> Building embedder image (first build bakes ~700 MB of model weights; 5-15 min)"
bash scripts/build_embedder_image.sh

aws ecr get-login-password --region "$REGION" | \
    docker login --username AWS --password-stdin "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"

CHUNKER_IMAGE_URI=""
EMBEDDER_IMAGE_URI=""
for which in chunker embedder; do
    REPO="${LAMBDA_PREFIX}-${which}"
    # The provisioner also ensures the repo exists; do it here so the push works first.
    aws ecr describe-repositories --repository-names "$REPO" --region "$REGION" > /dev/null 2>&1 \
        || aws ecr create-repository --repository-name "$REPO" --region "$REGION" \
            --image-scanning-configuration scanOnPush=true > /dev/null
    TAG_FILE="${TMPDIR:-/tmp}/onebucket_${which}_tag"
    [[ -f "$TAG_FILE" ]] || { echo "ERROR: $TAG_FILE missing; the build script did not run" >&2; exit 1; }
    LOCAL_TAG=$(cat "$TAG_FILE")
    VERSION="${LOCAL_TAG##*:}"
    REMOTE_TAG="$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO:$VERSION"
    echo "==> Pushing $LOCAL_TAG as $REMOTE_TAG"
    docker tag "$LOCAL_TAG" "$REMOTE_TAG"
    docker push "$REMOTE_TAG"
    case "$which" in
        chunker)  CHUNKER_IMAGE_URI="$REMOTE_TAG"  ;;
        embedder) EMBEDDER_IMAGE_URI="$REMOTE_TAG" ;;
    esac
done

# 3. Provision everything else (idempotent), then smoke-test unless disabled.
echo "==> Provisioning AWS resources (idempotent)"
SMOKE_FLAG="--smoke"
if [[ "${ONEBUCKET_RUN_SMOKE:-1}" != "1" ]]; then
    SMOKE_FLAG=""
fi
uv run python scripts/aws_deploy.py \
    --bucket "$BUCKET" \
    --region "$REGION" \
    --lambda-prefix "$LAMBDA_PREFIX" \
    --chunker-image-uri "$CHUNKER_IMAGE_URI" \
    --embedder-image-uri "$EMBEDDER_IMAGE_URI" \
    $SMOKE_FLAG

echo
echo "Deploy complete. Point the MCP server at the deployed index with:"
echo "  ONEBUCKET_DATASET_ROOT=s3://$BUCKET/metadata"
echo "  ONEBUCKET_CHUNK_ROOT=s3://$BUCKET/chunks"
echo "  ONEBUCKET_EXECUTION_MODE=lambda"
echo "(see README.md, section 'Wire it into Claude Desktop')"
