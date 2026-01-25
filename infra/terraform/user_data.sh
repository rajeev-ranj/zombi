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

# Peak test script (runs FROM EC2, not local)
cat > /opt/run_peak_test.sh << 'PEAKTEST'
#!/bin/bash
set -e
CONCURRENCY="$${1:-50,100,200}"
DURATION="$${2:-30}"
OUTPUT_DIR="$${3:-/opt/results}"

mkdir -p "$$OUTPUT_DIR"
cd /opt/zombi/tools

echo "=== Peak Performance Test (Single Write API) ==="
python3 peak_performance.py \
    --url http://localhost:8080 \
    --concurrency "$$CONCURRENCY" \
    --duration "$$DURATION" \
    --output "$$OUTPUT_DIR/peak_single.json"

echo "=== Peak Performance Test (Bulk Write API) ==="
python3 peak_performance_bulk.py \
    --url http://localhost:8080 \
    --concurrency "$$CONCURRENCY" \
    --duration "$$DURATION" \
    --batch-size 100 \
    --output "$$OUTPUT_DIR/peak_bulk.json"

echo "=== Read Throughput Test ==="
python3 benchmark.py \
    --url http://localhost:8080 \
    --test read-throughput
cp benchmark_results.json "$$OUTPUT_DIR/read_results.json" 2>/dev/null || true

echo "=== Waiting for Iceberg Flush (120s) ==="
sleep 120

echo "=== Iceberg Verification ==="
/opt/verify_iceberg.sh "$$OUTPUT_DIR"

echo "=== Final Server Stats ==="
curl -s http://localhost:8080/stats > "$$OUTPUT_DIR/server_stats.json"

echo "Results saved to: $$OUTPUT_DIR"
PEAKTEST
chmod +x /opt/run_peak_test.sh

# Iceberg verification script
cat > /opt/verify_iceberg.sh << 'VERIFY'
#!/bin/bash
OUTPUT_DIR="$${1:-.}"
BUCKET="$${ZOMBI_S3_BUCKET:-zombi-events-test1}"

echo "=== Iceberg Verification: s3://$$BUCKET ==="

METADATA=$$(aws s3 ls "s3://$$BUCKET/tables/" --recursive | grep -c "metadata.json" || echo 0)
PARQUET=$$(aws s3 ls "s3://$$BUCKET/tables/" --recursive | grep -c ".parquet" || echo 0)
TOTAL_SIZE=$$(aws s3 ls "s3://$$BUCKET/tables/" --recursive --summarize 2>/dev/null | grep "Total Size" | awk '{print $$3}' || echo 0)

echo "  Metadata files: $$METADATA"
echo "  Parquet files: $$PARQUET"
echo "  Total size: $$TOTAL_SIZE bytes"

# List actual files
echo ""
echo "S3 Contents:"
aws s3 ls "s3://$$BUCKET/tables/" --recursive | head -20

cat > "$$OUTPUT_DIR/iceberg_verification.json" << EOF
{
  "bucket": "$$BUCKET",
  "metadata_files": $$METADATA,
  "parquet_files": $$PARQUET,
  "total_bytes": $$TOTAL_SIZE,
  "verified_at": "$$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
VERIFY
chmod +x /opt/verify_iceberg.sh

echo "User-data script completed at $(date)"
