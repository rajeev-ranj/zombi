# Zombi Grafana Dashboard

This directory contains a Grafana dashboard for monitoring Zombi event streaming metrics.

## Dashboard Overview

The dashboard (`zombi-dashboard.json`) displays:

### Application Metrics (from `/stats` endpoint)
- **Uptime** - Server uptime in seconds
- **Total Errors** - Cumulative error count
- **Total Writes** - Number of write operations
- **Total Reads** - Number of read operations
- **Total Bytes Written** - Cumulative bytes written
- **Total Records Read** - Cumulative records returned
- **Write Rate** - Events written per second
- **Write Latency** - Average write latency in microseconds
- **Read Rate** - Read requests per second
- **Read Latency** - Average read latency in microseconds
- **Error Rate** - Errors per second (5m rolling)

### Infrastructure Metrics (from CloudWatch)
- **CPU Usage** - User and system CPU utilization
- **Memory Usage** - Memory utilization percentage
- **Disk Usage** - Disk space utilization
- **Network I/O** - Bytes sent and received

## Prerequisites

1. **Grafana** (v9.0.0 or later)
2. **Prometheus** - For scraping Zombi `/stats` endpoint
3. **CloudWatch datasource** - For AWS infrastructure metrics (optional)

## Setup Instructions

### Step 1: Configure Prometheus to Scrape Zombi

Add the following to your Prometheus configuration (`prometheus.yml`):

```yaml
scrape_configs:
  - job_name: 'zombi'
    scrape_interval: 15s
    metrics_path: /stats
    static_configs:
      - targets: ['localhost:8080']  # Replace with your Zombi host:port
    metric_relabel_configs:
      # Convert JSON response to Prometheus metrics
      - source_labels: [__name__]
        regex: '(.*)'
        target_label: __name__
        replacement: 'zombi_$1'
```

Alternatively, use a JSON exporter or custom scraper to convert the `/stats` JSON to Prometheus metrics format.

### Step 2: Add Datasources in Grafana

1. Navigate to **Configuration > Data Sources** in Grafana
2. Add a **Prometheus** datasource pointing to your Prometheus server
3. (Optional) Add a **CloudWatch** datasource for infrastructure metrics

### Step 3: Import the Dashboard

1. In Grafana, navigate to **Dashboards > Import**
2. Click **Upload JSON file** and select `zombi-dashboard.json`
3. Select your Prometheus datasource for `DS_PROMETHEUS`
4. Select your CloudWatch datasource for `DS_CLOUDWATCH` (or skip if not using)
5. Click **Import**

## Alternative: Direct JSON Polling

If you prefer not to use Prometheus, you can use the **JSON API datasource** plugin:

1. Install the [JSON API datasource](https://grafana.com/grafana/plugins/marcusolsson-json-datasource/) plugin
2. Configure it to poll `http://<zombi-host>:8080/stats`
3. Modify the dashboard panels to use JSONPath expressions:
   - `$.uptime_secs` for uptime
   - `$.writes.total` for total writes
   - `$.writes.rate_per_sec` for write rate
   - `$.writes.avg_latency_us` for write latency
   - `$.reads.total` for total reads
   - `$.reads.rate_per_sec` for read rate
   - `$.reads.avg_latency_us` for read latency
   - `$.errors_total` for errors

## Metrics Reference

The `/stats` endpoint returns:

```json
{
  "uptime_secs": 3600.5,
  "writes": {
    "total": 150000,
    "bytes_total": 75000000,
    "rate_per_sec": 41.67,
    "avg_latency_us": 85.3
  },
  "reads": {
    "total": 50000,
    "records_total": 500000,
    "rate_per_sec": 13.89,
    "avg_latency_us": 120.5
  },
  "errors_total": 5
}
```

## Customization

Feel free to modify the dashboard to suit your needs:
- Adjust thresholds for latency alerts
- Add additional CloudWatch metrics
- Create custom panels for specific use cases
- Set up alerting rules based on error rates or latency

## Troubleshooting

- **No data in Prometheus panels**: Verify Prometheus is scraping the `/stats` endpoint correctly
- **No data in CloudWatch panels**: Ensure the CloudWatch agent is installed and configured (see `../cloudwatch-agent.json`)
- **Import errors**: Verify Grafana version is 9.0.0 or later
