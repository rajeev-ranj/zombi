# Zombi Load Testing Tools

This directory contains tools for load testing Zombi.

## Load Test Tool (`load_test.py`)

A comprehensive load testing tool that tests Zombi's write throughput and latency.

### Features

- **Multiple Load Profiles**
  - `steady`: Constant rate
  - `ramp`: Linear ramp from min to max rate
  - `spike`: Baseline with periodic 10x spikes
  - `max`: Adaptive test to find sustainable max throughput

- **Detailed Metrics**
  - Events per second throughput
  - Latency percentiles (P50, P90, P95, P99, P99.9)
  - Error rates
  - Bytes transferred

- **Multi-threaded**
  - Configurable worker threads
  - Connection pooling
  - Automatic retry logic

### Installation

```bash
cd tools
pip install requests urllib3
```

### Quick Start

```bash
# Find maximum sustainable throughput (5 minute test)
python load_test.py --url http://18.143.135.197:8080 --profile max --duration 300

# Steady load at 1000 events/sec
python load_test.py --url http://18.143.135.197:8080 --profile steady --rate 1000 --duration 60

# Ramp from 100 to 5000 events/sec
python load_test.py --url http://18.143.135.197:8080 --profile ramp --min-rate 100 --max-rate 5000 --duration 300

# Spike test
python load_test.py --url http://18.143.135.197:8080 --profile spike --min-rate 100 --max-rate 2000 --duration 120
```

### Usage

```
usage: load_test.py [-h] [--url URL] [--table TABLE] [--duration DURATION]
                    [--workers WORKERS] [--profile {steady,ramp,spike,max}]
                    [--rate RATE] [--min-rate MIN_RATE]
                    [--max-rate MAX_RATE] [--max-latency-ms MAX_LATENCY_MS]

Zombi Load Test Tool

options:
  -h, --help            show this help message and exit
  --url URL              Zombi base URL (default: http://18.143.135.197:8080)
  --table TABLE          Table name to write to (default: test)
  --duration DURATION    Test duration in seconds (default: 60)
  --workers WORKERS      Number of worker threads (default: 10)
  --profile {steady,ramp,spike,max}
                        Load profile (default: max)
  --rate RATE            Target rate for steady profile (events/sec) (default: 1000)
  --min-rate MIN_RATE    Minimum rate for ramp profile (default: 100)
  --max-rate MAX_RATE    Maximum rate for ramp/spike profile (default: 5000)
  --max-latency-ms MAX_LATENCY_MS
                        Target P95 latency for max profile (default: 500)
```

### Example Output

```
======================================================================
ZOMBI LOAD TEST
======================================================================
URL: http://18.143.135.197:8080
Table: test
Profile: max
Duration: 300s
Workers: 10
Target P95 Latency: 500ms
======================================================================

Starting adaptive load test...
Workers will automatically adjust rate based on P95 latency
Target P95 latency: 500ms

  [5s] Rate: 487.2/s | Sent: 2,436 | P50: 12.3ms | P95: 45.2ms | P99: 89.1ms
  [10s] Rate: 892.3/s | Sent: 6,897 | P50: 15.8ms | P95: 52.4ms | P99: 98.7ms
  ...

======================================================================
FINAL RESULTS
======================================================================
Duration: 300.0s
Total Requests: 125,432
  Successful: 125,398 (99.97%)
  Errors: 34 (0.03%)
Throughput: 418.0 events/sec
Total Bytes: 12,543,200 (11.96 MB)
Avg Event Size: 100 bytes

Latency (ms):
  Min: 2.1
  P50: 14.5
  P90: 32.8
  P95: 48.2
  P99: 89.5
  P99.9: 156.3
  Max: 342.1
======================================================================
```

## Testing Strategy

### For t3.micro EC2 Instance

A t3.micro has:
- 2 vCPUs (burstable)
- 1 GB RAM
- Network up to 5 Gbps

Expected limits:
- **Write throughput**: ~200-500 events/sec with small payloads
- **Latency**: <50ms P95 under normal load
- **Memory**: Limited by 1GB total

### Recommended Test Sequence

1. **Quick sanity check** (30 seconds)
   ```bash
   python load_test.py --url http://18.143.135.197:8080 --profile steady --rate 100 --duration 30
   ```

2. **Steady load test** (2 minutes)
   ```bash
   python load_test.py --url http://18.143.135.197:8080 --profile steady --rate 500 --duration 120
   ```

3. **Ramp test** (5 minutes)
   ```bash
   python load_test.py --url http://18.143.135.197:8080 --profile ramp --min-rate 100 --max-rate 2000 --duration 300
   ```

4. **Max throughput test** (5-10 minutes)
   ```bash
   python load_test.py --url http://18.143.135.197:8080 --profile max --duration 300
   ```

5. **Spike test** (2 minutes)
   ```bash
   python load_test.py --url http://18.143.135.197:8080 --profile spike --min-rate 100 --max-rate 1000 --duration 120
   ```

### Running from Multiple Machines

To test network/bandwidth limits, run the load test from multiple machines simultaneously:

```bash
# Machine 1
python load_test.py --url http://18.143.135.197:8080 --profile max --duration 600

# Machine 2 (run at same time)
python load_test.py --url http://18.143.135.197:8080 --profile max --duration 600

# Machine 3 (run at same time)
python load_test.py --url http://18.143.135.197:8080 --profile max --duration 600
```

### Monitoring EC2 During Tests

While tests are running, monitor the EC2 instance:

```bash
# Check CPU and memory
ssh -i ~/.ssh/id_ed25519 ubuntu@18.143.135.197 "top -b -n1"

# Check Zombi logs
ssh -i ~/.ssh/id_ed25519 ubuntu@18.143.135.197 "sudo docker logs zombi --tail 50 -f"

# Check RocksDB size
ssh -i ~/.ssh/id_ed25519 ubuntu@18.143.135.197 "sudo du -sh /var/lib/zombi"

# Check S3 objects
aws s3 ls s3://zombi-events-test1/ --recursive | wc -l
```

## Tips

1. **Use longer durations** (300s+) for max tests to see steady-state performance
2. **Watch CPU credits** on t3.micro - bursts will deplete credits and throttle
3. **Monitor S3** - if flush is slow, it will affect write latency
4. **Test different payload sizes** - current events are ~100 bytes, modify `generate_event()` to test larger payloads
5. **Test multiple tables** - use `--table` to write to different tables and test partitioning
