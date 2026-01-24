# Iceberg Verification on AWS

This guide documents how to verify Zombi's Iceberg data on S3 using DuckDB and AWS Athena.

## Overview

Zombi writes Iceberg v2 tables to S3 with the following structure:

```
s3://bucket/tables/{topic}/
├── metadata/
│   ├── v1.metadata.json
│   ├── v2.metadata.json
│   └── snap-{id}-{uuid}.avro
└── data/
    └── event_date=YYYY-MM-DD/
        └── event_hour=HH/
            └── partition=N/
                └── {uuid}.parquet
```

### Schema

| Column | Type | Description |
|--------|------|-------------|
| sequence | long | Monotonically increasing event ID |
| topic | string | Topic name |
| partition | int | Partition number |
| payload | binary | Event data |
| timestamp_ms | long | Event timestamp (milliseconds since epoch) |
| idempotency_key | string (nullable) | Optional deduplication key |
| event_date | date | Partition column (derived from timestamp) |
| event_hour | int | Partition column (0-23) |

---

## Verification with DuckDB

DuckDB provides fast, local querying of Iceberg tables on S3 without requiring a server.

### Installation

```bash
# macOS
brew install duckdb

# Linux (Ubuntu/Debian)
wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
chmod +x duckdb

# Verify installation
duckdb --version
```

### Configure AWS Credentials

DuckDB uses standard AWS credential chain. Set credentials via environment variables or AWS config:

```bash
# Option 1: Environment variables
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"

# Option 2: AWS CLI profile
aws configure
```

### Load Iceberg Extension

```sql
-- Start DuckDB
duckdb

-- Install and load required extensions
INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;

-- Configure S3 access
SET s3_region = 'us-east-1';
-- For custom endpoints (MinIO, LocalStack):
-- SET s3_endpoint = 'localhost:9000';
-- SET s3_use_ssl = false;
-- SET s3_url_style = 'path';
```

### Query Iceberg Tables

```sql
-- Read latest snapshot
SELECT *
FROM iceberg_scan('s3://zombi-events/tables/events/')
LIMIT 10;

-- Count total events
SELECT COUNT(*) as total_events
FROM iceberg_scan('s3://zombi-events/tables/events/');

-- Check sequence continuity (no gaps)
SELECT
    MIN(sequence) as min_seq,
    MAX(sequence) as max_seq,
    COUNT(*) as event_count,
    MAX(sequence) - MIN(sequence) + 1 as expected_count
FROM iceberg_scan('s3://zombi-events/tables/events/');

-- Verify partition distribution
SELECT
    partition,
    COUNT(*) as event_count,
    MIN(sequence) as min_seq,
    MAX(sequence) as max_seq
FROM iceberg_scan('s3://zombi-events/tables/events/')
GROUP BY partition
ORDER BY partition;

-- Check events by date and hour
SELECT
    event_date,
    event_hour,
    COUNT(*) as event_count
FROM iceberg_scan('s3://zombi-events/tables/events/')
GROUP BY event_date, event_hour
ORDER BY event_date, event_hour;

-- Verify timestamps are valid
SELECT
    MIN(timestamp_ms) as earliest_event,
    MAX(timestamp_ms) as latest_event,
    COUNT(*) FILTER (WHERE timestamp_ms <= 0) as invalid_timestamps
FROM iceberg_scan('s3://zombi-events/tables/events/');

-- Query specific time range (uses partition pruning)
SELECT *
FROM iceberg_scan('s3://zombi-events/tables/events/')
WHERE event_date = '2024-01-15'
  AND event_hour BETWEEN 10 AND 14
LIMIT 100;

-- Check for duplicate sequences (should return 0)
SELECT sequence, COUNT(*) as cnt
FROM iceberg_scan('s3://zombi-events/tables/events/')
GROUP BY sequence
HAVING COUNT(*) > 1;

-- View table metadata
SELECT * FROM iceberg_metadata('s3://zombi-events/tables/events/');

-- List snapshots
SELECT * FROM iceberg_snapshots('s3://zombi-events/tables/events/');
```

### Query with MinIO (Local Development)

```sql
-- Configure for MinIO
SET s3_endpoint = 'localhost:9000';
SET s3_use_ssl = false;
SET s3_url_style = 'path';
SET s3_access_key_id = 'minioadmin';
SET s3_secret_access_key = 'minioadmin';

-- Query local Iceberg table
SELECT * FROM iceberg_scan('s3://zombi-events/tables/events/') LIMIT 10;
```

---

## Verification with AWS Athena

Athena provides serverless SQL queries on Iceberg tables in S3.

### Prerequisites

1. AWS account with Athena access
2. S3 bucket with Iceberg data
3. IAM permissions for Athena and S3

### Create Database

```sql
-- In Athena Query Editor
CREATE DATABASE IF NOT EXISTS zombi;
```

### Create Iceberg Table

```sql
-- Create external table pointing to Zombi's Iceberg location
CREATE TABLE zombi.events (
    sequence BIGINT,
    topic STRING,
    partition INT,
    payload BINARY,
    timestamp_ms BIGINT,
    idempotency_key STRING,
    event_date DATE,
    event_hour INT
)
PARTITIONED BY (event_date, event_hour)
LOCATION 's3://zombi-events/tables/events/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);
```

**Note:** If Zombi manages the table metadata, use `MSCK REPAIR TABLE` or create an external table that reads the existing metadata:

```sql
-- Alternative: Use existing Iceberg metadata
CREATE TABLE zombi.events
LOCATION 's3://zombi-events/tables/events/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG'
);
```

### Query in Athena

```sql
-- Count events
SELECT COUNT(*) FROM zombi.events;

-- Verify data integrity
SELECT
    MIN(sequence) as min_seq,
    MAX(sequence) as max_seq,
    COUNT(*) as total,
    MAX(sequence) - MIN(sequence) + 1 as expected
FROM zombi.events;

-- Events by partition
SELECT
    partition,
    COUNT(*) as event_count
FROM zombi.events
GROUP BY partition
ORDER BY partition;

-- Events by date (uses partition pruning)
SELECT
    event_date,
    COUNT(*) as events
FROM zombi.events
WHERE event_date >= DATE '2024-01-01'
GROUP BY event_date
ORDER BY event_date;

-- Time travel: Query previous snapshot
SELECT * FROM zombi.events FOR TIMESTAMP AS OF TIMESTAMP '2024-01-15 10:00:00';

-- Query specific snapshot by ID
SELECT * FROM zombi.events FOR VERSION AS OF 1234567890;

-- Check for duplicate sequences
SELECT sequence, COUNT(*)
FROM zombi.events
GROUP BY sequence
HAVING COUNT(*) > 1;
```

### Using AWS Glue Catalog

If registering with AWS Glue Data Catalog:

1. Create a Glue Crawler pointing to your S3 location
2. Configure the crawler to recognize Iceberg format
3. Run the crawler to populate the catalog
4. Query via Athena using the Glue catalog

```sql
-- After Glue crawler runs, table is available
SELECT * FROM glue_catalog.zombi.events LIMIT 10;
```

---

## Data Integrity Validation

### Verify Invariants

Run these queries to validate Zombi's invariants:

```sql
-- INV-1: Sequences are monotonic (no duplicates within partition)
SELECT partition, sequence, COUNT(*) as cnt
FROM zombi.events
GROUP BY partition, sequence
HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- INV-3: Order preserved (check sequence gaps per partition)
WITH sequenced AS (
    SELECT
        partition,
        sequence,
        LAG(sequence) OVER (PARTITION BY partition ORDER BY sequence) as prev_seq
    FROM zombi.events
)
SELECT partition, prev_seq, sequence, sequence - prev_seq as gap
FROM sequenced
WHERE sequence - prev_seq > 1
LIMIT 100;
-- Expected: Should show expected gaps from flush boundaries only

-- INV-5: Partition isolation (verify topic/partition consistency)
SELECT topic, partition, COUNT(*) as events
FROM zombi.events
GROUP BY topic, partition
ORDER BY topic, partition;
```

### Verify Parquet File Health

```sql
-- Check file statistics via metadata
SELECT * FROM iceberg_snapshots('s3://zombi-events/tables/events/');

-- Verify row counts match
WITH snapshot_stats AS (
    SELECT
        snapshot_id,
        CAST(summary['total-records'] AS BIGINT) as expected_records
    FROM iceberg_snapshots('s3://zombi-events/tables/events/')
    WHERE snapshot_id = (
        SELECT MAX(snapshot_id)
        FROM iceberg_snapshots('s3://zombi-events/tables/events/')
    )
)
SELECT
    s.expected_records,
    (SELECT COUNT(*) FROM iceberg_scan('s3://zombi-events/tables/events/')) as actual_records
FROM snapshot_stats s;
```

---

## Troubleshooting

### Common Issues

#### 1. "No Iceberg metadata found"

**Cause:** Missing or invalid metadata files at the expected location.

**Fix:**
```bash
# Verify metadata exists
aws s3 ls s3://zombi-events/tables/events/metadata/

# Check for latest metadata file
aws s3 ls s3://zombi-events/tables/events/metadata/ | grep metadata.json
```

#### 2. "Access Denied" errors

**Cause:** Missing S3 permissions or incorrect credentials.

**Fix:**
```bash
# Verify credentials
aws sts get-caller-identity

# Check bucket access
aws s3 ls s3://zombi-events/

# Required IAM permissions
# - s3:GetObject
# - s3:ListBucket
# - s3:GetBucketLocation
```

#### 3. Empty query results

**Cause:** No snapshots committed or querying wrong location.

**Fix:**
```bash
# Check if data files exist
aws s3 ls s3://zombi-events/tables/events/data/ --recursive | head

# Verify snapshot was committed
aws s3 cat s3://zombi-events/tables/events/metadata/v1.metadata.json | jq '.snapshots'
```

#### 4. DuckDB Iceberg extension errors

**Cause:** Extension version mismatch or missing installation.

**Fix:**
```sql
-- Force reinstall
FORCE INSTALL iceberg;
LOAD iceberg;

-- Check extension version
SELECT * FROM duckdb_extensions() WHERE extension_name = 'iceberg';
```

#### 5. Schema mismatch errors

**Cause:** Query engine schema differs from Iceberg schema.

**Fix:**
```sql
-- View actual schema in DuckDB
DESCRIBE SELECT * FROM iceberg_scan('s3://zombi-events/tables/events/') LIMIT 1;

-- Check metadata schema
SELECT * FROM iceberg_metadata('s3://zombi-events/tables/events/');
```

#### 6. MinIO/LocalStack connection issues

**Fix:**
```sql
-- DuckDB: Use path-style URLs
SET s3_url_style = 'path';
SET s3_use_ssl = false;

-- Verify endpoint is reachable
-- In terminal: curl http://localhost:9000/minio/health/live
```

### Performance Tips

1. **Use partition pruning:** Always filter by `event_date` and `event_hour` when possible
2. **Limit scans:** Use `LIMIT` for exploratory queries
3. **Check file sizes:** Small files (<1MB) impact query performance
4. **Run compaction:** Use Zombi's compaction API for better read performance

```bash
# Trigger compaction
curl -X POST http://localhost:8080/tables/events/compact
```

---

## Quick Reference

### DuckDB Commands

```sql
-- Load extensions
INSTALL iceberg; INSTALL httpfs;
LOAD iceberg; LOAD httpfs;

-- Query table
SELECT * FROM iceberg_scan('s3://bucket/path/') LIMIT 10;

-- View metadata
SELECT * FROM iceberg_metadata('s3://bucket/path/');
SELECT * FROM iceberg_snapshots('s3://bucket/path/');
```

### Athena DDL

```sql
-- Create database
CREATE DATABASE zombi;

-- Create Iceberg table
CREATE TABLE zombi.events
LOCATION 's3://bucket/tables/events/'
TBLPROPERTIES ('table_type' = 'ICEBERG');
```

### AWS CLI Commands

```bash
# List data files
aws s3 ls s3://zombi-events/tables/events/data/ --recursive

# View metadata
aws s3 cp s3://zombi-events/tables/events/metadata/v1.metadata.json -

# Count files
aws s3 ls s3://zombi-events/tables/events/data/ --recursive | wc -l
```
