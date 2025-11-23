# ADR-006: InfluxDB Setup Automation

## Status
Accepted

## Context
Need automated InfluxDB bucket setup and data lifecycle management following best practices.

## Decision

### Automated Setup Script
Create `scripts/setup_influx.py` to:
- Create required buckets with proper retention
- Set up aggregation tasks
- Configure measurement schemas
- Validate connection and permissions

### Bucket Configuration
```python
BUCKETS = {
    'homeassistant-recent': {
        'retention': '90d',
        'description': 'Recent HA data (3 months)',
        'schema_type': 'implicit'
    },
    'homeassistant-historical': {
        'retention': '0s',  # Infinite retention
        'description': 'Historical aggregated HA data',
        'schema_type': 'explicit'
    }
}
```

### Flux Tasks for Data Lifecycle
1. **Hourly Aggregation Task**
   - Source: homeassistant-recent
   - Target: homeassistant-historical
   - Trigger: Every hour for data older than 30 days
   - Apply sensor-specific aggregation rules

2. **Data Cleanup Task**
   - Remove duplicates in historical bucket
   - Validate aggregation quality
   - Log aggregation statistics

### Task Template (config/influx_tasks.flux)
```flux
// Hourly aggregation for temperature sensors
option task = {name: "aggregate-temperature-hourly", every: 1h}

from(bucket: "homeassistant-recent")
  |> range(start: -30d, stop: -29d)
  |> filter(fn: (r) => r["_measurement"] == "temperature")
  |> aggregateWindow(every: 1h, fn: mean)
  |> to(bucket: "homeassistant-historical")
```

### Monitoring & Alerts
- Task execution monitoring
- Data quality checks
- Storage usage tracking
- Failed aggregation alerts

## Best Practices Applied
- Separate buckets for different retention needs
- Automated data lifecycle management
- Measurement-specific aggregation strategies
- Monitoring and alerting for data pipeline health

## Consequences
- Zero-maintenance data lifecycle after initial setup
- Industry-standard retention and aggregation
- Scalable architecture for growing data volumes
- Easy troubleshooting with proper monitoring

## Changes
- 2025-11-22: Task creation API issue fixed - removed invalid 'name' parameter
- 2025-11-22: Updated to use TaskCreateRequest with correct parameters (org_id, flux, description, status)
- 2025-11-22: Added proper existing task detection by searching Flux scripts
- 2025-11-22: Enhanced error handling for task creation with fallback instructions
- 2025-11-22: Task creation now works with current InfluxDB client library