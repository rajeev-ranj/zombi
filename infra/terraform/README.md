# Zombi AWS (Terraform)

## Prereqs
- Terraform
- AWS credentials in your shell (e.g., `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- SSH public key (e.g., `~/.ssh/id_rsa.pub`)

## Usage

```bash
cd infra/terraform
terraform init
terraform apply \
  -var "s3_bucket_name=<unique-bucket>" \
  -var "key_pair_name=zombi-key" \
  -var "public_key_path=/Users/<you>/.ssh/id_rsa.pub"
```

Outputs include the EC2 public IP and S3 bucket name.

## Test

```bash
curl http://<public-ip>:8080/health
curl -X POST http://<public-ip>:8080/tables/events \
  -H "Content-Type: application/json" \
  -d '{"payload":"hello aws"}'
```
