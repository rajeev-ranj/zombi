# AWS Scaling: ALB + Redis Architecture

This document describes how to scale Zombi horizontally on AWS using an Application Load Balancer (ALB) and Amazon ElastiCache Redis for distributed coordination.

---

## Overview

The current single-instance deployment handles moderate workloads well, but for high availability (HA) and high throughput scenarios, you need multiple Zombi instances behind a load balancer with shared coordination via Redis.

### When to Use This Architecture

| Scenario | Single Instance | ALB + Redis |
|----------|-----------------|-------------|
| Events/sec | < 50,000 | > 50,000 |
| Availability | Single point of failure | Multi-AZ HA |
| Development/Testing | Recommended | Overkill |
| Production (critical) | Risky | Recommended |
| Cost sensitivity | Lower | Higher |

---

## Architecture Diagram

```
                                    Internet / VPC
                                         |
                                         v
                              +---------------------+
                              |   Application       |
                              |   Load Balancer     |
                              |   (ALB)             |
                              |                     |
                              |   Health: /health   |
                              +----------+----------+
                                         |
                    +--------------------+--------------------+
                    |                    |                    |
                    v                    v                    v
          +-----------------+  +-----------------+  +-----------------+
          |  Zombi Instance |  |  Zombi Instance |  |  Zombi Instance |
          |  (EC2 / ECS)    |  |  (EC2 / ECS)    |  |  (EC2 / ECS)    |
          |                 |  |                 |  |                 |
          |  +-----------+  |  |  +-----------+  |  |  +-----------+  |
          |  |  RocksDB  |  |  |  |  RocksDB  |  |  |  |  RocksDB  |  |
          |  |  (local)  |  |  |  |  (local)  |  |  |  |  (local)  |  |
          |  +-----------+  |  |  +-----------+  |  |  +-----------+  |
          +--------+--------+  +--------+--------+  +--------+--------+
                   |                    |                    |
                   +--------------------+--------------------+
                                        |
                    +-------------------+-------------------+
                    |                                       |
                    v                                       v
          +-----------------+                     +-----------------+
          |  ElastiCache    |                     |  Amazon S3      |
          |  Redis          |                     |  (Iceberg)      |
          |                 |                     |                 |
          |  - Offsets      |                     |  - Parquet data |
          |  - Consumer     |                     |  - Metadata     |
          |    groups       |                     |                 |
          |  - Flush coord  |                     |                 |
          +-----------------+                     +-----------------+
                                                          |
                                                          v
                                                 +-----------------+
                                                 |  Query Engines  |
                                                 |  (Spark/Trino/  |
                                                 |   Athena)       |
                                                 +-----------------+
```

---

## Application Load Balancer (ALB) Configuration

### Target Group Settings

| Setting | Value | Notes |
|---------|-------|-------|
| Target type | `instance` or `ip` | Use `ip` for ECS Fargate |
| Protocol | HTTP | Or HTTPS for TLS termination at ALB |
| Port | 8080 | Zombi default port |
| Health check path | `/health` | Returns 200 when ready |
| Health check interval | 30 seconds | Adjust based on sensitivity |
| Healthy threshold | 2 | Consecutive successes to mark healthy |
| Unhealthy threshold | 3 | Consecutive failures to mark unhealthy |
| Timeout | 5 seconds | Health check timeout |
| Deregistration delay | 30 seconds | Allow in-flight requests to complete |

### Listener Configuration

```
Listener: HTTPS:443 (recommended for production)
  - SSL Certificate: ACM certificate
  - Default action: Forward to target group

Listener: HTTP:80 (optional)
  - Default action: Redirect to HTTPS
```

### Sticky Sessions

**For write operations:** Sticky sessions are **not required**. Any instance can handle writes because Redis coordinates offset generation.

**For streaming reads:** Consider enabling sticky sessions if consumers frequently poll the same partition, as the local RocksDB cache will be warm.

| Session Type | Setting | Notes |
|--------------|---------|-------|
| Duration-based | 1 hour | Cookie-based stickiness |
| Application-based | Custom cookie | If using client-side affinity |

To enable sticky sessions in the target group:
```
Stickiness type: Load balancer generated cookie
Stickiness duration: 3600 seconds (1 hour)
```

### Security Group for ALB

```
Inbound:
  - Port 443 (HTTPS) from 0.0.0.0/0 or your CIDR
  - Port 80 (HTTP) from 0.0.0.0/0 (for redirect only)

Outbound:
  - Port 8080 to EC2 security group
```

---

## ElastiCache Redis Setup

### Cluster Configuration

| Setting | Recommended Value | Notes |
|---------|-------------------|-------|
| Engine | Redis 7.x | Latest stable version |
| Node type | `cache.r6g.large` | Start here, scale based on load |
| Number of replicas | 2 | For HA across AZs |
| Multi-AZ | Enabled | Automatic failover |
| Cluster mode | Disabled | Single shard sufficient for coordination |
| Encryption in-transit | Enabled | TLS for security |
| Encryption at-rest | Enabled | For compliance |
| Auth token | Required | Set via Secrets Manager |

### Cluster Mode Considerations

**Cluster mode disabled (recommended for Zombi):**
- Simpler operations
- Single endpoint
- Sufficient for offset coordination workloads
- Replicas for read scaling and HA

**Cluster mode enabled (for very high throughput):**
- Multiple shards with automatic partitioning
- More complex client configuration
- Consider if > 100K Redis operations/sec

### Security Group for Redis

```
Inbound:
  - Port 6379 from EC2 security group

Outbound:
  - None required (Redis doesn't initiate connections)
```

### Subnet Group

Create a subnet group spanning at least 2 Availability Zones for HA:
```
Subnets: private-subnet-1a, private-subnet-1b, private-subnet-1c
```

### Parameter Group Settings

| Parameter | Value | Notes |
|-----------|-------|-------|
| `maxmemory-policy` | `noeviction` | Never evict offset data |
| `notify-keyspace-events` | `""` | Disable if not needed |
| `timeout` | `0` | No idle timeout |

---

## Redis Data Model

Zombi stores coordination data in Redis with the following key patterns:

```
# Offset generation (atomic increment)
offset:{topic}:{partition} -> next_offset (int64)

# Consumer group offsets
consumer:{group}:{topic}:{partition} -> committed_offset (int64)

# Flush coordination
flush:{topic}:{partition} -> last_flushed_offset (int64)

# Node registration (optional)
node:{node_id} -> {last_heartbeat, status}
```

### Example Redis Commands

```redis
# Get next offset (atomic)
INCR offset:events:0
> 12346

# Commit consumer offset
SET consumer:my-group:events:0 12340

# Get consumer offset
GET consumer:my-group:events:0
> "12340"

# Update flush watermark
SET flush:events:0 12000
```

---

## Environment Variables for Zombi

Configure each Zombi instance with these environment variables:

### Required for Multi-Node

| Variable | Example | Description |
|----------|---------|-------------|
| `ZOMBI_REDIS_URL` | `rediss://user:pass@cluster.xxx.cache.amazonaws.com:6379` | ElastiCache endpoint (TLS) |
| `ZOMBI_NODE_ID` | `zombi-1a-001` | Unique identifier per instance |

### Full Configuration Example

```bash
# Core settings
export ZOMBI_HOST=0.0.0.0
export ZOMBI_PORT=8080
export ZOMBI_DATA_DIR=/data/zombi

# S3/Iceberg settings
export ZOMBI_S3_BUCKET=my-zombi-bucket
export ZOMBI_S3_REGION=us-east-1
export ZOMBI_STORAGE_PATH=tables
export ZOMBI_ICEBERG_ENABLED=true
export ZOMBI_TARGET_FILE_SIZE_MB=128

# Redis coordination (enables multi-node)
export ZOMBI_REDIS_URL=rediss://:AUTH_TOKEN@my-cluster.xxx.cache.amazonaws.com:6379
export ZOMBI_NODE_ID=$(hostname)

# Logging
export RUST_LOG=zombi=info
```

### Using AWS Secrets Manager

For the Redis auth token, use AWS Secrets Manager:

```bash
# Retrieve secret at startup
export REDIS_AUTH=$(aws secretsmanager get-secret-value \
  --secret-id zombi/redis-auth \
  --query SecretString --output text)

export ZOMBI_REDIS_URL="rediss://:${REDIS_AUTH}@my-cluster.xxx.cache.amazonaws.com:6379"
```

---

## EC2 Instance Configuration

### Security Group for EC2

```
Inbound:
  - Port 8080 from ALB security group
  - Port 22 from bastion (optional, for SSH)

Outbound:
  - Port 6379 to Redis security group
  - Port 443 to S3 (via VPC endpoint or NAT)
  - Port 443 to Secrets Manager (if using)
```

### IAM Role Permissions

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
        "arn:aws:s3:::my-zombi-bucket",
        "arn:aws:s3:::my-zombi-bucket/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:*:*:secret:zombi/*"
    }
  ]
}
```

### Instance Storage

For RocksDB, use instance storage (NVMe) or EBS gp3:

| Storage Type | Use Case | Notes |
|--------------|----------|-------|
| Instance NVMe | Highest performance | Data lost on termination (OK - S3 is source of truth) |
| EBS gp3 | Persistent hot cache | Survives restarts, higher latency |

Recommended: NVMe instance storage (e.g., `i3.xlarge`, `i3en.xlarge`) since RocksDB is ephemeral and all durable data is in S3/Iceberg.

---

## Deployment Checklist

### Pre-Deployment

- [ ] VPC with private subnets in 2+ AZs
- [ ] S3 bucket created with versioning enabled
- [ ] ElastiCache Redis cluster deployed
- [ ] Redis auth token stored in Secrets Manager
- [ ] ALB and target group created
- [ ] Security groups configured
- [ ] IAM role with S3 and Secrets Manager access
- [ ] ACM certificate for HTTPS (if using)

### Per-Instance

- [ ] EC2 instance launched in private subnet
- [ ] Attached to target group
- [ ] Environment variables configured
- [ ] Zombi binary deployed and running
- [ ] Health check passing (`/health` returns 200)

### Validation

- [ ] Write events via ALB endpoint
- [ ] Verify events appear in S3/Iceberg
- [ ] Test consumer offset commit/retrieve
- [ ] Simulate instance failure (terminate one instance)
- [ ] Verify ALB routes to healthy instances
- [ ] Verify data integrity after failover

---

## Monitoring

### CloudWatch Metrics to Watch

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| `HealthyHostCount` | ALB | < 2 |
| `UnHealthyHostCount` | ALB | > 0 |
| `TargetResponseTime` | ALB | > 100ms |
| `RequestCount` | ALB | Baseline + 50% |
| `CPUUtilization` | EC2 | > 80% |
| `CurrConnections` | ElastiCache | > 1000 |
| `EngineCPUUtilization` | ElastiCache | > 70% |
| `CacheHits/CacheMisses` | ElastiCache | Miss ratio > 10% |

### Application Logs

Configure CloudWatch Logs agent to ship Zombi logs:

```
/var/log/zombi/zombi.log -> /aws/zombi/{instance-id}
```

---

## Cost Estimation

| Component | Size | Monthly Cost (us-east-1) |
|-----------|------|--------------------------|
| ALB | Base + LCU | ~$20 + usage |
| EC2 (3x i3.xlarge) | On-demand | ~$750 |
| EC2 (3x i3.xlarge) | Reserved 1yr | ~$450 |
| ElastiCache (r6g.large, 2 replicas) | On-demand | ~$350 |
| S3 | 1TB + requests | ~$25 |
| **Total (on-demand)** | | ~$1,145/month |
| **Total (reserved)** | | ~$845/month |

Note: Costs are approximate. Use AWS Pricing Calculator for accurate estimates.

---

## Related Issues

- #13 - Redis client implementation
- #14 - Redis-based offset generation
- #15 - Redis consumer offset storage
- #16 - Redis flush coordination

---

## Future Considerations

### Auto Scaling

Add Auto Scaling Group with policies based on:
- ALB request count per target
- CPU utilization
- Custom metric (events/sec from CloudWatch)

### ECS/Fargate Deployment

For containerized deployment:
- Use ECS service with ALB integration
- Fargate for serverless containers
- EFS for shared RocksDB (if needed)

### Cross-Region

For disaster recovery:
- S3 cross-region replication
- ElastiCache Global Datastore
- Route 53 health checks and failover
