# Creating a dedicated AWS profile for OneBucket demo deploys

If your existing AWS profile belongs to another project, or carries broader permissions than you want to use here, create a dedicated IAM user and a local profile for this demo. The user is scoped by [infra/iam/deployer-policy.json](../../infra/iam/deployer-policy.json) to exactly the actions the deploy performs, which are described in plain words in [aws-prerequisites.md](aws-prerequisites.md#permissions-the-deploying-identity-needs).

Two paths are given: the AWS CLI (recommended, reproducible) and the AWS Console (clickable). Both attach the policy as an **inline user policy**, not a managed policy. The difference matters later: an inline policy is a single document that lives on the user, and updating it means re-running the same `put-user-policy` command with the new file. There are no policy versions to manage and no second AWS-side artifact to keep in sync with the file in git.

You need an existing admin profile that can create IAM users, usually the one you used to bootstrap the account. The commands below call it `<your-admin-profile>`.

## Naming used throughout

| Thing | Value |
|---|---|
| IAM user (created in AWS) | `onebucket-demo-deployer` |
| Inline policy name | `OneBucketDemoDeployerPolicy` |
| Local profile name (in `~/.aws/credentials`) | `onebucket-demo` |
| Resource pattern the policy allows | `onebucket-*` |

The `onebucket-*` pattern appears in every resource ARN in `deployer-policy.json`: the bucket, the SQS queues, the ECR repositories, the IAM roles, the Lambda functions and the log groups. It has to match what you pass to the deploy script: `--bucket` must start with `onebucket-`, and `--lambda-prefix` (default `onebucket`) must start with it too, because every other resource name is derived from that prefix. If you want a different scheme (say your bucket is `acme-rag-demo`), edit the pattern in `deployer-policy.json` before applying it, and keep the file and your flags in agreement from then on.

---

## Path A: AWS CLI (recommended)

Run these from the repo root with your admin profile.

### A1. Create the IAM user

```bash
aws iam create-user \
  --user-name onebucket-demo-deployer \
  --profile <your-admin-profile>
```

### A2. Attach the deployer policy inline

```bash
aws iam put-user-policy \
  --user-name onebucket-demo-deployer \
  --policy-name OneBucketDemoDeployerPolicy \
  --policy-document file://infra/iam/deployer-policy.json \
  --profile <your-admin-profile>
```

`put-user-policy` creates the inline policy the first time and replaces it on every later run, which is why updating it (see below) is the same command.

### A3. Generate access keys

```bash
aws iam create-access-key \
  --user-name onebucket-demo-deployer \
  --profile <your-admin-profile>
```

Save the `AccessKeyId` and `SecretAccessKey` from the output now. AWS shows the secret once; if you lose it, delete the key and create a new one.

### A4. Configure the local profile

```bash
aws configure --profile onebucket-demo
# AWS Access Key ID:     <AccessKeyId from A3>
# AWS Secret Access Key: <SecretAccessKey from A3>
# Default region name:   us-east-1
# Default output format: json
```

This adds an `onebucket-demo` section to `~/.aws/credentials` and `~/.aws/config` and leaves your other profiles alone.

### A5. Verify

```bash
aws sts get-caller-identity --profile onebucket-demo
```

Expect an `Arn` ending in `:user/onebucket-demo-deployer`.

Optionally confirm that a few of the ECR permissions resolve. This simulation needs `iam:SimulatePrincipalPolicy` on the profile that runs it, so it uses the admin profile to ask about the deployer:

```bash
aws iam simulate-principal-policy \
  --policy-source-arn "$(aws sts get-caller-identity --query Arn --output text --profile onebucket-demo)" \
  --action-names ecr:GetAuthorizationToken ecr:CreateRepository ecr:PutImage \
  --query 'EvaluationResults[].[EvalActionName,EvalDecision]' \
  --output table \
  --profile <your-admin-profile>
```

All three rows should say `allowed`.

### A6. Deploy

```bash
export AWS_PROFILE=onebucket-demo
bash scripts/deploy_aws.sh --bucket <your-bucket> --region us-east-1
```

Export the profile rather than only passing `--profile`: parts of the deploy resolve credentials from the environment on their own (see [aws-deployment-runbook.md](aws-deployment-runbook.md#14-export-aws_profile-for-the-whole-shell-session)). `<your-bucket>` must start with `onebucket-` and, because S3 bucket names are globally unique, needs a suffix of your own.

---

## Path B: AWS Console

1. **Create the user.** IAM, Users, Create user. Name `onebucket-demo-deployer`. Do not enable console access; this user is for programmatic access only. Skip the permissions step for now and create the user.
2. **Attach the policy inline.** Open the user, Permissions, Add permissions, **Create inline policy**. Switch to the JSON editor, paste the contents of [infra/iam/deployer-policy.json](../../infra/iam/deployer-policy.json), name it `OneBucketDemoDeployerPolicy`, Create. (Do not use "Attach policies directly" with a new managed policy; that creates a second artifact with its own versions, which Path A and the update instructions below do not expect.)
3. **Create an access key.** Security credentials, Access keys, Create access key, "Command Line Interface (CLI)", confirm, download the CSV.
4. **Configure the local profile.** `aws configure --profile onebucket-demo`, paste from the CSV, region `us-east-1`, output `json`.
5. **Verify and deploy** as in A5 and A6.

---

## Updating the policy later

When `infra/iam/deployer-policy.json` changes (for example a new action is added to the deploy), re-run the attach command; the inline policy is replaced in place:

```bash
aws iam put-user-policy \
  --user-name onebucket-demo-deployer \
  --policy-name OneBucketDemoDeployerPolicy \
  --policy-document file://infra/iam/deployer-policy.json \
  --profile <your-admin-profile>
```

In the console the equivalent is: open the user, Permissions, click `OneBucketDemoDeployerPolicy`, Edit, replace the JSON, Save. Either way there is only ever one copy of the document on the user.

---

## Cleaning up later

Removing the deployer user is separate from tearing down the deployed stack, which the deployer itself is not allowed to delete; see [aws-prerequisites.md](aws-prerequisites.md#cleanup) for that.

```bash
ADMIN=<your-admin-profile>

# delete the access key first (list, then delete by id)
aws iam list-access-keys --user-name onebucket-demo-deployer --profile $ADMIN
aws iam delete-access-key --user-name onebucket-demo-deployer --access-key-id <AccessKeyId> --profile $ADMIN

# remove the inline policy
aws iam delete-user-policy \
  --user-name onebucket-demo-deployer \
  --policy-name OneBucketDemoDeployerPolicy \
  --profile $ADMIN

# delete the user
aws iam delete-user --user-name onebucket-demo-deployer --profile $ADMIN

# optionally remove the [onebucket-demo] sections from ~/.aws/credentials and ~/.aws/config
```

---

## Troubleshooting

**`AccessDenied` on `iam:CreateUser`.** The profile you are using cannot create IAM users. Ask whoever administers the account, or use a profile that can.

**`AccessDenied` on `s3:CreateBucket` during the deploy.** Either the bucket name does not match the `onebucket-*` pattern in the policy, or the name is already taken by another AWS account (S3 names are global). Pick a name that matches the pattern and is more specific.

**`AccessDenied` on `lambda:CreateFunction` (or on a role, queue or repository).** You passed a `--lambda-prefix` that does not start with `onebucket`, so the derived resource names fall outside the policy. Either use the default prefix or edit the pattern in `infra/iam/deployer-policy.json` to match and re-run `put-user-policy`.

**`AccessDenied` on a delete.** Expected. The deployer policy grants no delete actions beyond `s3:DeleteObject`; cleanup runs with the admin profile.
