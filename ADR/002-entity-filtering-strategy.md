# ADR-002: Entity Filtering Strategy

## Status
Accepted

## Context
Home Assistant tracks all entity state changes, but not all entities are suitable for time-series analysis in InfluxDB. A filtering strategy is needed to identify valuable sensors while excluding status indicators and binary states.

## Decision

### Include Time-Series Valuable Entities
- **Energy sensors** (kWh): Cumulative energy consumption data
- **Power sensors** (W): Instantaneous power readings
- **Environmental sensors** (°C, hPa, %): Temperature, pressure, humidity
- **Network sensors** (kB/s, GB): Bandwidth and data usage
- **Electrical sensors** (A, V): Current and voltage monitoring
- **Special integrations**: Energy billing data (e.g., Tibber)

### Exclude Non-Time-Series Data
- **Status indicators**: Availability, connection state, signal strength
- **Binary sensors**: On/off states without meaningful trends
- **Device trackers**: Presence/location data
- **Control entities**: Switches, buttons, automations

### Filtering Implementation
```python
INCLUDE_UNITS = ['kWh', 'W', '°C', '°F', 'kB/s', 'GB', 'MB', 'A', 'V', 'hPa', 'bar', 'mbar', 'lux', 'ppm', 'dB', 'rpm']
INCLUDE_SOURCES = ['tibber']  # Special energy providers
EXCLUDE_PATTERNS = ['%availability%', '%status%', '%signal%', '%connected%']
```

## Consequences
- Reduces exported data volume by ~20-40%
- Focuses InfluxDB storage on actionable metrics
- Maintains data quality by filtering noise

## Results
- Typical filtering: 340+ entities selected from 430+ total
- ~80-85% inclusion rate for well-configured Home Assistant instances with expanded sensor support
- Automatic categorization into 8 sensor types for appropriate aggregation