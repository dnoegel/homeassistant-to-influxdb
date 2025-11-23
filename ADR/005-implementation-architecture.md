# ADR-005: Implementation Architecture

## Status
Accepted

## Context
Need maintainable, modular Python implementation with progress tracking and resume capability.

## Decision

### Project Structure
```
hastats/
├── src/
│   ├── __init__.py
│   ├── config.py           # Configuration management
│   ├── database.py         # HA SQLite interface
│   ├── influxdb_client.py  # InfluxDB operations
│   ├── entity_filter.py    # Entity filtering logic
│   ├── data_processor.py   # Data transformation
│   └── exporter.py         # Main export orchestration
├── scripts/
│   ├── export.py           # CLI entry point
│   └── setup_influx.py     # InfluxDB bucket setup
├── config/
│   ├── .env.template       # Environment template
│   └── influx_tasks.flux   # InfluxDB aggregation tasks
├── ADR/                    # Architecture decisions
├── CLAUDE.md              # AI context
└── project.md             # Project documentation
```

### Core Components

#### 1. Configuration (config.py)
- Load .env file
- Validate InfluxDB connection
- Entity filtering rules
- Aggregation strategies

#### 2. Database Interface (database.py)
- SQLite connection management
- Statistics table queries
- Metadata resolution
- Batch data retrieval

#### 3. Entity Filter (entity_filter.py)
- Apply inclusion/exclusion rules
- Categorize by sensor type
- Determine aggregation strategy

#### 4. Data Processor (data_processor.py)
- Data quality validation
- Timestamp conversion
- InfluxDB point creation
- Batch preparation

#### 5. Exporter (exporter.py)
- Progress tracking
- Resume capability
- Error handling
- Logging

### CLI Interface
```bash
python scripts/export.py [options]
  --resume                 # Resume interrupted export
  --entities <filter>      # Export specific entities
  --date-range <start:end> # Limit time range
  --dry-run               # Validate without writing
  --progress              # Show progress bar
```

### Resume Strategy
- Track export progress in SQLite checkpoint table
- Record last exported timestamp per entity
- Support incremental exports
- Validate data consistency on resume

## Consequences
- Modular design enables testing and maintenance
- CLI flexibility for different use cases
- Resume capability handles large datasets
- Clear separation of concerns

## Changes
- 2025-11-22: Complete implementation of all core components
- 2025-11-22: CLI interface with click framework implemented
- 2025-11-22: Export orchestration with resume capability completed
- 2025-11-22: Comprehensive testing pipeline - all 6 test suites passing
- 2025-11-22: Ready for production use with dry-run validation