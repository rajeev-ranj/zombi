# gp2 → gp3 Migration Guide and Instance Sizing

> **Summary:** Zombi’s hot path is disk‑bound on small instances. gp3 removes the
> gp2 IOPS cliff and gives predictable throughput. This guide explains how to
> migrate and how to pick instance/volume sizes for common workloads.

---

## Why Migrate

gp2 uses burst credits; when credits are depleted, sustained write throughput
drops sharply. The performance analysis shows disk I/O as the primary bottleneck
on t3.micro. gp3 provides **consistent baseline IOPS** with optional
provisioned IOPS/throughput, which stabilizes write latency under load.

---

## Quick Recommendations (Starting Points)

| Workload | Instance | gp3 Size | IOPS | Throughput | Notes |
|---------|----------|----------|------|------------|-------|
| Dev / test | t3.micro | 20–50 GB | 3,000 | 125 MB/s | Lowest cost, avoids burst cliff |
| Small prod | t3.small / t3.medium | 100 GB | 3,000–6,000 | 125–250 MB/s | Good baseline for steady ingest |
| Medium prod | c5.large | 200 GB | 6,000–10,000 | 250–500 MB/s | Headroom for bursts |
| High throughput | c5.xlarge+ | 500 GB | 12,000–16,000 | 500–1,000 MB/s | For sustained high ingest |

**Notes:**
- gp3 baseline is 3,000 IOPS and 125 MB/s; you can provision higher values.
- Increase volume size if RocksDB compaction or hot retention causes growth.

---

## Terraform Migration

The Terraform config uses the **default root volume** (gp2 on many AMIs).
Override it by adding `root_block_device` to the instance:

```hcl
resource "aws_instance" "zombi" {
  # ... existing fields ...

  root_block_device {
    volume_type = "gp3"
    volume_size = 100
    iops        = 3000
    throughput  = 125
  }
}
```

If you prefer a dedicated data volume, add an `ebs_block_device` and mount it in
`user_data.sh` (e.g., `/data`), then point `ZOMBI_DATA_DIR` to that mount.

---

## AWS Console Migration (Existing Instance)

1. Stop writes (or stop the instance) to minimize risk.
2. In EC2 → **Volumes**, select the root volume.
3. **Modify volume** → change type to `gp3`.
4. Set size, IOPS, and throughput (start with 3,000 / 125 MB/s).
5. Apply changes and restart the instance.

---

## Validate the Upgrade

After migration:

- `df -h` to confirm available disk.
- `iostat -x 1` to watch IOPS/latency under load.
- Zombi metrics: monitor `zombi_write_latency_us` and RocksDB stalls.

If latency still degrades under sustained writes, increase gp3 IOPS or upgrade
instance class.
