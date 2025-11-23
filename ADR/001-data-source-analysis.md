# ADR-001: Data Source Analysis

## Status
Accepted

## Context
Home Assistant stores statistics in two separate SQLite tables with different aggregation levels and retention periods. Understanding this structure is crucial for designing an effective export strategy.

## Decision
Export from both tables to preserve different data granularities:
- **Short-term data** (`statistics_short_term`): Recent detailed records for immediate analysis
- **Long-term data** (`statistics`): Historical compressed records for trend analysis

## Schema Analysis
Both tables share identical schema:
- `start_ts`: Unix timestamp for measurement window
- `mean`, `min`, `max`: Statistical aggregations
- `sum`: Cumulative values (primarily for energy sensors)
- `state`: Current/last known value
- `metadata_id`: Foreign key to `statistics_meta` for entity information

## Data Characteristics
- Records span multiple months to years of Home Assistant operation
- Energy sensors typically have the highest record count
- Data quality varies by sensor type and integration

## Consequences
- Export strategy must handle both granular and pre-aggregated data
- Different retention policies map naturally to InfluxDB bucket strategy
- Preserves Home Assistant's native aggregation approach

## Implementation Notes
- Batch processing required for large datasets (1M+ records)
- Entity metadata resolution needed for proper categorization
- Timestamp validation ensures data quality