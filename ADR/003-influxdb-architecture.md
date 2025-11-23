# ADR-003: InfluxDB Architecture

## Status
Accepted

## Context
Need dual-bucket strategy for short-term vs long-term data with automatic aggregation.

## Decision

### Bucket Strategy
- **Short-term bucket**: `homeassistant-recent`
  - Retention: 90 days
  - Data resolution: Original HA intervals (typically 5-15 minutes)
  - All measurement fields preserved

- **Long-term bucket**: `homeassistant-historical`
  - Retention: Unlimited
  - Data resolution: Aggregated (hourly/daily based on sensor type)
  - Compressed data for storage efficiency

### Aggregation Rules by Sensor Type
```python
AGGREGATION_RULES = {
    'kWh': {'method': 'last', 'window': '1h'},      # Energy: preserve cumulative
    'W': {'method': 'mean', 'window': '1h'},        # Power: average over hour
    '°C': {'method': 'mean', 'window': '1h'},       # Temperature: hourly average
    'A': {'method': 'mean', 'window': '1h'},        # Current: average
    'V': {'method': 'mean', 'window': '1h'},        # Voltage: average
    '%': {'method': 'mean', 'window': '1h'},        # Percentage: average
    'kB/s': {'method': 'mean', 'window': '1h'},     # Bandwidth: average
    'GB': {'method': 'last', 'window': '1d'},       # Data usage: daily snapshot
}
```

### Data Migration Flow
1. Export to short-term bucket first
2. InfluxDB Task aggregates data after 30 days
3. Original data aged out after 90 days

## Configuration
- Connection via .env file
- Separate write permissions for each bucket
- Batch writes for performance

## Consequences
- Automatic data lifecycle management
- Balanced between detail and storage efficiency
- Industry-standard retention policies

## Changes
- 2025-11-22: InfluxDB client implementation completed
- 2025-11-22: Bucket management with automated setup script created
- 2025-11-22: Flux aggregation tasks template with 8 sensor category rules
- 2025-11-22: Point creation logic implemented with proper tagging strategy
- 2025-11-22: Offline testing completed - ready for live InfluxDB connection