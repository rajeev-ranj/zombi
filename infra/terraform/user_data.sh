#!/usr/bin/env bash
set -euo pipefail

apt-get update
apt-get install -y docker.io
systemctl enable docker
systemctl start docker

# Run Zombi
/usr/bin/docker run -d --name zombi \
  -p 8080:8080 \
  -e ZOMBI_S3_BUCKET=${s3_bucket} \
  -e ZOMBI_S3_REGION=${region} \
  -e ZOMBI_ICEBERG_ENABLED=true \
  -e ZOMBI_FLUSH_INTERVAL_SECS=5 \
  -e ZOMBI_FLUSH_BATCH_SIZE=100 \
  -e ZOMBI_FLUSH_MAX_SEGMENT=1000 \
  ${zombi_image}
