# Analysis: Reaching 2 Gbps Throughput

## Current State

| Metric | Value |
|--------|-------|
| Peak Events/sec | 166,222 |
| Payload size | 30 bytes (JSON overhead) |
| Actual throughput | **4.76 MB/s = 0.038 Gbps** |
| Target | **250 MB/s = 2 Gbps** |
| **Gap** | **52x improvement needed** |

---

## Theoretical Maximum on t3.micro

### Network
- t3.micro: Up to 5 Gbps burst
- **Not the bottleneck** (using 0.038 Gbps)

### CPU
- 2 vCPU, observed 27% utilization
- Could push 3-4x more work → ~15 MB/s max
- **Partial bottleneck**

### Disk (EBS gp2)
- 8GB gp2: 100 IOPS baseline, 3000 burst
- RocksDB write: ~3-4KB per event (WAL + memtable flush)
- At 166K ev/s: ~600 MB/s disk write demand
- EBS gp2 max: 250 MB/s
- **PRIMARY BOTTLENECK**

### Memory
- 1GB RAM, 278MB available
- RocksDB block cache: ~100-200MB effective
- **Significant bottleneck** for read-heavy workloads

---

## Optimization Strategies

### Level 1: Configuration Changes (No Code)

| Change | Expected Impact | Effort |
|--------|-----------------|--------|
| Larger instance (c5.xlarge) | 4x CPU, 8GB RAM → ~2-3x | Low |
| gp3 EBS (16000 IOPS, 1000 MB/s) | Removes disk bottleneck | Low |
| Mount /data on NVMe | 10x disk throughput | Medium |

**Estimated result**: 15-20 MB/s (~0.15 Gbps)

### Level 2: Application Tuning (Minor Code)

| Change | Expected Impact | Effort |
|--------|-----------------|--------|
| Larger batch size (100 → 1000) | Reduce HTTP overhead → 2x | Low |
| Larger payload (30 → 1000 bytes) | Better bandwidth utilization → 30x | N/A (data) |
| RocksDB write buffer pooling | Reduce memory allocations → 1.2x | Medium |
| Disable WAL (risky) | 2-3x write throughput | Low |

**Estimated result**: 50-100 MB/s (~0.5-0.8 Gbps)

### Level 3: Architecture Changes (Significant Code)

| Change | Expected Impact | Effort |
|--------|-----------------|--------|
| Partitioned writes (N servers) | Linear scaling → Nx | High |
| Async S3 flush | Remove flush backpressure | Medium |
| Memory-mapped writes | Bypass RocksDB for hot path | High |
| Direct Parquet writes (skip RocksDB) | Eliminate hot storage → 5-10x | High |

**Estimated result**: 250+ MB/s (2 Gbps achievable)

---

## Recommended Path to 2 Gbps

### Phase 1: Quick Wins (1-2 days)
```bash
# Use larger instance
terraform apply -var="instance_type=c5.xlarge"

# Increase batch size
# In load test: --batch-size 500

# Use gp3 EBS with provisioned IOPS
# 500 GB gp3 with 16000 IOPS, 1000 MB/s
```
**Expected**: 20-30 MB/s

### Phase 2: RocksDB Tuning (1 week)
```rust
// In storage/rocksdb.rs
let mut opts = Options::default();
opts.set_write_buffer_size(256 * 1024 * 1024);  // 256MB vs default 64MB
opts.set_max_write_buffer_number(4);
opts.set_level_zero_file_num_compaction_trigger(8);
opts.set_level_zero_slowdown_writes_trigger(20);
opts.set_level_zero_stop_writes_trigger(36);
opts.set_target_file_size_base(256 * 1024 * 1024);  // 256MB
opts.set_max_background_jobs(4);
```
**Expected**: 50-80 MB/s

### Phase 3: Partitioned Architecture (2-4 weeks)
```
          Load Balancer
               │
    ┌──────────┼──────────┐
    │          │          │
Zombi-1    Zombi-2    Zombi-3
    │          │          │
   S3         S3         S3
    │          │          │
    └──────────┴──────────┘
         Iceberg Catalog
         (single table)
```
**Expected**: 150-250 MB/s (scaling with nodes)

---

## Cost Analysis

| Configuration | Throughput | Monthly Cost | $/GB ingested |
|---------------|-----------|--------------|---------------|
| t3.micro (current) | 0.038 Gbps | $10 | $0.10 |
| c5.xlarge + gp3 | 0.15 Gbps | $150 | $0.04 |
| 3x c5.xlarge | 0.5 Gbps | $450 | $0.03 |
| 3x c5.2xlarge | 2.0 Gbps | $1,200 | $0.02 |

**Insight**: Cost per GB decreases with scale due to better resource utilization.

---

## Benchmarks Needed

To validate these estimates, run:

1. **Batch size sweep**: 100, 500, 1000, 5000 events/batch
2. **Payload size sweep**: 100B, 1KB, 10KB, 100KB
3. **Instance comparison**: t3.micro vs c5.large vs c5.xlarge
4. **Disk comparison**: gp2 vs gp3 vs io2

---

## Conclusion

**2 Gbps is achievable** but requires:

1. **Immediate**: Larger instance (c5.xlarge) + gp3 EBS
2. **Short-term**: RocksDB tuning + larger batches
3. **Medium-term**: Partitioned architecture for horizontal scaling

The current t3.micro is fundamentally **disk-bound** and can only reach ~5-15 MB/s with any optimization. To reach 2 Gbps, **hardware upgrade is mandatory**.
