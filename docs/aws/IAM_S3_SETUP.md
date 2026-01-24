# AWS IAM and S3 Setup for Zombi

This guide covers the minimal AWS permissions and S3 configuration required to run Zombi with S3 cold storage.

---

## Minimal IAM Policy

Zombi requires the following S3 permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR-BUCKET-NAME",
        "arn:aws:s3:::YOUR-BUCKET-NAME/*"
      ]
    }
  ]
}
```

Replace `YOUR-BUCKET-NAME` with your actual S3 bucket name.

### Permission Details

| Permission | Purpose |
|------------|---------|
| `s3:PutObject` | Write flushed events to cold storage |
| `s3:GetObject` | Read events from cold storage |
| `s3:ListBucket` | List objects for compaction and recovery |
| `s3:DeleteObject` | Remove old files after compaction |

---

## S3 Bucket Configuration

### Required Settings

1. **Bucket Name**: Choose a globally unique name
2. **Region**: Deploy in the same region as your Zombi instance for lower latency

### Recommended Settings

For production deployments:

- **Versioning**: Enable for data protection (allows recovery from accidental deletes)
- **Encryption**: Enable server-side encryption (SSE-S3 or SSE-KMS)
- **Lifecycle Rules**: Optional - configure to transition old data to Glacier

For test/development:

- **Versioning**: Can be disabled for faster cleanup
- **Force Destroy**: Enable if using Terraform for easy teardown

---

## Attaching the Policy

### Option 1: EC2 Instance Profile (Recommended)

Use an IAM role with an instance profile. This avoids storing credentials on the instance.

1. Create the IAM role with EC2 trust policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

2. Attach the S3 policy (from above) to the role
3. Create an instance profile and associate the role
4. Launch your EC2 instance with the instance profile

The AWS SDK will automatically use the instance profile credentials.

### Option 2: Access Keys (Development Only)

For local development or non-EC2 environments:

1. Create an IAM user
2. Attach the S3 policy to the user
3. Generate access keys
4. Set environment variables:

```bash
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
```

**Warning**: Avoid using access keys in production. Prefer instance profiles or IAM roles.

### Option 3: ECS/EKS Task Role

For containerized deployments:

- **ECS**: Attach the policy to a task execution role
- **EKS**: Use IAM Roles for Service Accounts (IRSA)

---

## Automated Setup with Terraform

The `infra/terraform/` directory provides a complete setup including:

- S3 bucket
- IAM role with the required policy
- EC2 instance profile
- EC2 instance with Zombi pre-configured

```bash
cd infra/terraform
terraform init
terraform apply \
  -var "s3_bucket_name=my-zombi-bucket" \
  -var "key_pair_name=zombi-key" \
  -var "public_key_path=~/.ssh/id_rsa.pub"
```

See [infra/terraform/README.md](../../infra/terraform/README.md) for full details.

---

## Verification

After setup, verify Zombi can access S3:

```bash
# Check Zombi health
curl http://localhost:8080/health

# Write an event
curl -X POST http://localhost:8080/tables/events \
  -H "Content-Type: application/json" \
  -d '{"payload":"test"}'

# Trigger a flush (if manual flush is enabled)
curl -X POST http://localhost:8080/flush

# List objects in S3
aws s3 ls s3://YOUR-BUCKET-NAME/ --recursive
```
