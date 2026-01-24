# Zombi Load Testing Tool - Quick Start Guide

## Overview

The load testing tool (`tools/load_test.py`) is now ready to test Zombi's write throughput on EC2.

## Quick Start

```bash
cd tools
# Find maximum sustainable throughput (5 minute test)
python3 load_test.py --url http://18.143.135.197:8080 --profile max --duration 300

# Or use the interactive menu
python3 run_load_tests.py

# Or use the quick shell script
./quick_test.sh max 300
```

## Test Profiles

| Profile | Description | Best For |
|---------|-------------|-----------|
| `steady` | Constant rate load | Baseline performance |
| `ramp` | Linear ramp from min to max | Finding breaking point |
| `spike` | Baseline with periodic 10x spikes | Testing recovery |
| `max` | Adaptive, finds sustainable throughput | Production sizing |

## Sample Commands

```bash
# Quick sanity check (30s, ~100 events/sec)
python3 load_test.py --url http://18.143.135.197:8080 --profile steady --rate 100 --duration 30

# Steady load test (2 min, 500 events/sec)
python3 load_test.py --url http://18.143.135.197:8080 --profile steady --rate 500 --duration 120

# Ramp test (5 min, 100 -> 5000 events/sec)
python3 load_test.py --url http://18.143.135.197:8080 --profile ramp --min-rate 100 --max-rate 5000 --duration 300

# Spike test (2 min)
python3 load_test.py --url http://18.143.135.197:8080 --profile spike --min-rate 100 --max-rate 1000 --duration 120

# Find max throughput (5-10 min recommended)
python3 load_test.py --url http://18.143.135.197:8080 --profile max --duration 600
```

## Understanding the Output

```
======================================================================
FINAL RESULTS
======================================================================
Duration: 30.0s
Total Requests: 1,485
  Successful: 1,485 (100.0%)
  Errors: 0 (0.0%)
Throughput: 49.5 events/sec
Total Bytes: 263,340 (0.25 MB)
Avg Event Size: 177 bytes

Latency (ms):
  Min: 58.2
  P50: 82.7      # Median latency
  P90: 101.0     # 90th percentile
  P95: 187.1     # 95th percentile
  P99: 383.5     # 99th percentile
  Max: 2262.6
======================================================================
```

## What We've Learned from Testing

### t3.micro Performance (Singapore Region)

From our initial tests:
- **Throughput**: ~50 events/sec from local machine to Singapore EC2
- **Latency**: P50 ~83ms, P95 ~187ms (network latency to Singapore)
- **Bottleneck**: Network latency (180ms RTT) from test location to EC2

### Recommendations

1. **Run tests from same region** (Singapore) to eliminate network latency
2. **Use multiple test machines** to test network limits
3. **Monitor EC2 resources** during tests:
   ```bash
   ssh ubuntu@18.143.135.197 "sudo docker stats zombi"
   ssh ubuntu@18.143.135.197 "top -b -n1"
   ```

## Monitoring While Testing

### Check Zombi Logs
```bash
ssh -i ~/.ssh/id_ed25519 ubuntu@18.143.135.197 'sudo docker logs zombi -f'
```

### Check Container Stats
```bash
ssh -i ~/.ssh/id_ed25519 ubuntu@18.143.135.197 'sudo docker stats zombi'
```

### Check S3 Objects
```bash
aws s3 ls s3://zombi-events-test1/ --recursive --summarize
```

### Force a Flush
```bash
curl -X POST http://18.143.135.197:8080/tables/test/flush
```

## Troubleshooting

### All Requests Failing
- Check Zombi is running: `curl http://18.143.135.197:8080/health`
- Check logs: `ssh ubuntu@18.143.135.197 "sudo docker logs zombi"`

### Low Throughput
- Check network latency to EC2
- Run test from closer region
- Check EC2 CPU/memory limits

### High Error Rate
- Check flusher is working
- Verify S3 bucket access
- Check RocksDB disk space

## Files

| File | Description |
|------|-------------|
| `load_test.py` | Main load testing tool |
| `run_load_tests.py` | Interactive menu for common tests |
| `quick_test.sh` | Quick bash wrapper |
| `LOAD_TEST_README.md` | Detailed documentation |

## Customization

### Change Payload Size
Modify `generate_event()` in `load_test.py` to create larger/smaller payloads.

### Change Number of Partitions
Events are randomly distributed across partitions 0-3. Modify the random.randint call in `send_event_worker()`.

### Test Multiple Tables
Use `--table` parameter to write to different tables.

## Next Steps

1. Run a 10-minute max throughput test
2. Test from Singapore region machine
3. Test with larger payloads (1KB, 10KB, 100KB)
4. Test with multiple concurrent clients
5. Monitor CPU credit usage on t3.micro

## Support

For issues or questions, check the detailed documentation in `LOAD_TEST_README.md`.
