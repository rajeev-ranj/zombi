#!/usr/bin/env bash
set -euo pipefail

# Log everything
exec > >(tee /var/log/user-data.log) 2>&1
echo "Starting user-data script at $(date)"

# Install dependencies
apt-get update
apt-get install -y docker.io python3-pip protobuf-compiler git jq hey
systemctl enable docker
systemctl start docker

# Clone repo for producer tools
git clone https://github.com/rajeev-ranj/zombi.git /opt/zombi
pip3 install requests protobuf

# Generate protobuf code
cd /opt/zombi/tools/producer
protoc --python_out=. events.proto

# Run Zombi container
echo "Starting Zombi container..."
# Use --network host to access EC2 instance metadata for IAM role credentials
/usr/bin/docker run -d --name zombi \
  --restart always \
  --network host \
  -v /data/zombi:/var/lib/zombi \
  -e ZOMBI_S3_BUCKET=${s3_bucket} \
  -e ZOMBI_S3_REGION=${region} \
  -e ZOMBI_ICEBERG_ENABLED=true \
  -e ZOMBI_FLUSH_INTERVAL_SECS=5 \
  -e ZOMBI_FLUSH_BATCH_SIZE=100 \
  -e ZOMBI_FLUSH_MAX_SEGMENT=1000 \
  -e RUST_LOG=zombi=info \
  ${zombi_image}

# Wait for Zombi to be healthy
echo "Waiting for Zombi to be healthy..."
for i in $(seq 1 60); do
  if curl -sf http://localhost:8080/health > /dev/null 2>&1; then
    echo "Zombi is healthy!"
    break
  fi
  sleep 5
done

# Create benchmark script
cat > /opt/run_benchmark.sh << 'BENCHMARK'
#!/bin/bash
set -e
PROFILE=$${1:-steady}
DURATION=$${2:-60}
WORKERS=$${3:-5}

echo "Running benchmark: profile=$$PROFILE duration=$$DURATION workers=$$WORKERS"
cd /opt/zombi/tools
python3 load_test.py --url http://localhost:8080 --profile "$$PROFILE" --duration "$$DURATION" --workers "$$WORKERS"
BENCHMARK
chmod +x /opt/run_benchmark.sh

# Create stats collection script
cat > /opt/collect_stats.sh << 'STATS'
#!/bin/bash
echo "=== Zombi Stats ==="
curl -s http://localhost:8080/stats | jq .

echo ""
echo "=== Docker Stats ==="
docker stats zombi --no-stream

echo ""
echo "=== Recent Logs ==="
docker logs zombi --tail 20
STATS
chmod +x /opt/collect_stats.sh

echo "User-data script completed at $(date)"
