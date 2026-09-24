# Shared Storage Real AWS S3 Release Evidence Runbook

This runbook configures the **release-evidence-only** AWS path used by
`Shared Storage Real S3 Compatibility`. It is intentionally narrower than a
production Shared Storage IAM policy.

The current workflow uses GitHub Actions OIDC, the protected GitHub Environment
`shared-storage-aws-s3`, and the dedicated evidence branch
`shared-wal-s3-4.3.1-real-s3`.

## 1. GitHub Environment

Create or update the GitHub Environment:

`shared-storage-aws-s3`

Configure:

- Environment variable `SHARED_STORAGE_AWS_S3_BUCKET`: an existing general-purpose S3 bucket used only for release evidence.
- Optional environment variable `SHARED_STORAGE_AWS_S3_REGION`: bucket region; defaults to `us-east-1`.
- Environment secret `SHARED_STORAGE_AWS_ROLE_ARN`: IAM role assumed through GitHub OIDC.

The workflow writes only below:

`ga-compatibility/<random-uuid>/objects/`

Each run uses a new UUID and deletes its test objects in cleanup.

### Environment protection

The workflow job references a GitHub Environment. For the standard GitHub OIDC
subject format this means the token subject is based on the environment, not the
branch:

`repo:funky-eyes/kafka:environment:shared-storage-aws-s3`

Use Environment deployment branch/tag rules as an additional boundary. For the
automatic release-evidence path, allow only
`shared-wal-s3-4.3.1-real-s3`. Add another branch only if a deliberate
`workflow_dispatch` flow needs it.

If the repository or organization uses customized or immutable OIDC subject
claims, use the actual emitted subject rather than assuming the standard format.

## 2. AWS OIDC trust

The AWS account must have the GitHub Actions OIDC provider:

- Provider URL: `https://token.actions.githubusercontent.com`
- Audience: `sts.amazonaws.com`

A minimal trust relationship for the standard environment subject is:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::<AWS_ACCOUNT_ID>:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
          "token.actions.githubusercontent.com:sub": "repo:funky-eyes/kafka:environment:shared-storage-aws-s3"
        }
      }
    }
  ]
}
```

Do not replace the scoped `sub` condition with a repository-wide wildcard
unless there is a separately reviewed reason to do so.

GitHub reference:

- <https://docs.github.com/en/actions/how-tos/secure-your-work/security-harden-deployments/oidc-in-aws>
- <https://docs.github.com/en/actions/reference/security/oidc>

## 3. Minimal S3 permissions for the compatibility proof

The current compatibility proof exercises:

- single-object PUT;
- ranged GET;
- multipart create/upload/complete;
- multipart abort on failure;
- DELETE.

The corresponding least-privilege identity policy for this test path is:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "KafkaSharedStorageRealS3Evidence",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": "arn:aws:s3:::<BUCKET_NAME>/ga-compatibility/*"
    }
  ]
}
```

The current code does not list buckets or objects, so this evidence policy does
not require `s3:ListBucket`.

For Amazon S3 multipart upload, `CreateMultipartUpload`, `UploadPart`, and
`CompleteMultipartUpload` require `s3:PutObject`;
`AbortMultipartUpload` requires `s3:AbortMultipartUpload`.

AWS references:

- <https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html>
- <https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html>

### KMS-encrypted buckets

If the test bucket enforces SSE-KMS with a customer-managed KMS key, the role
also needs the KMS permissions required by that bucket/key policy. Multipart
uploads can require `kms:GenerateDataKey` and `kms:Decrypt`.

For the narrowest release-evidence setup, a dedicated bucket using standard S3
server-side encryption avoids coupling the compatibility proof to unrelated KMS
policy configuration.

## 4. Trigger exact-candidate evidence

First make sure the development branch is at the intended canonical candidate.
Then move the dedicated evidence branch to that exact candidate.

Conceptually:

```text
shared-wal-s3-4.3.1
        |
        | exact candidate SHA
        v
shared-wal-s3-4.3.1-real-s3
```

With Git:

```bash
git fetch origin \
  shared-wal-s3-4.3.1 \
  shared-wal-s3-4.3.1-real-s3

candidate="$(git rev-parse origin/shared-wal-s3-4.3.1)"
previous_evidence="$(git rev-parse origin/shared-wal-s3-4.3.1-real-s3)"

git push \
  --force-with-lease="refs/heads/shared-wal-s3-4.3.1-real-s3:${previous_evidence}" \
  origin "${candidate}:refs/heads/shared-wal-s3-4.3.1-real-s3"
```

Moving the evidence branch triggers:

1. `Shared Storage Real S3 Compatibility`
2. `Shared Storage Real S3 GA Seal`

The strict seal requires the Real S3 result in addition to the normal 19
mandatory GA gates.

## 5. Expected compatibility proof

A successful `S3RealCompatibilityTest` proves the current production client
path against AWS S3 using normal AWS endpoint resolution, TLS, and
virtual-hosted bucket addressing. It verifies:

- small-object PUT followed by Range GET;
- known-size multipart upload with a range crossing the 5 MiB part boundary;
- zero-length read behavior;
- DELETE followed by an expected 404 read;
- cleanup of test object IDs.

The test does not configure an S3-compatible endpoint override and does not
enable path-style addressing.

## 6. Failure diagnosis

### Preflight reports missing bucket or role

Configure the corresponding GitHub Environment value:

- `SHARED_STORAGE_AWS_S3_BUCKET`
- `SHARED_STORAGE_AWS_ROLE_ARN`

The preflight reports all missing required values in one run and exits before
requesting AWS credentials.

### `Configure AWS workload credentials` fails

Check:

- the IAM OIDC provider URL;
- audience `sts.amazonaws.com`;
- the role trust `sub` condition;
- whether the Environment deployment rule permits the evidence branch;
- whether the repository uses a customized or immutable OIDC subject.

### S3 returns AccessDenied

Map the failing API to the evidence role policy:

- PUT / CreateMultipartUpload / UploadPart / CompleteMultipartUpload -> `s3:PutObject`
- Range GET -> `s3:GetObject`
- DELETE -> `s3:DeleteObject`
- multipart cleanup -> `s3:AbortMultipartUpload`

Also check bucket policy, organization SCPs, VPC endpoint policies if applicable,
and KMS key policy when the bucket uses SSE-KMS.

## 7. Release claim

Do not claim real AWS S3 release compatibility until both are true:

1. `Shared Storage Real S3 Compatibility` is successful for a
   production- and contract-equivalent candidate.
2. The strict GA manifest generated with `--require-real-s3` is PASS.

A green MinIO-oriented 19/19 GA manifest by itself is not real AWS S3 evidence.
