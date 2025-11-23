# Home Assistant Statistics Exporter

A comprehensive tool for migrating Home Assistant statistical data to InfluxDB 2.x with intelligent entity filtering and data quality validation.

## Overview

This project provides a one-time migration tool that exports Home Assistant's SQLite statistics database to InfluxDB 2.x. It's designed for users who want to migrate their historical sensor data to InfluxDB for advanced analytics, visualization, and long-term storage.

## Key Features

### Smart Data Migration
- **Dual-Source Export**: Processes both short-term (recent) and long-term (historical) statistics tables
- **Intelligent Entity Filtering**: Automatically identifies time-series valuable sensors while excluding status indicators
- **Data Quality Validation**: Detects and corrects common data issues before export
- **Resume Capability**: Safely interrupt and resume large migrations

### InfluxDB Integration
- **Dual-Bucket Architecture**: Separates recent detailed data from historical compressed data
- **Automated Setup**: Creates buckets and configures data lifecycle policies
- **Flux Aggregation Tasks**: Automatically generates tasks for data rollup and retention
- **Schema Optimization**: Uses appropriate tags and fields for efficient querying

### Production Ready
- **Batch Processing**: Handles millions of records efficiently
- **Progress Tracking**: Real-time export progress and performance metrics
- **Comprehensive CLI**: Multiple export modes and analysis tools
- **Configurable**: Extensive configuration options via environment variables

## Use Cases

- **Data Analytics**: Migrate HA data for advanced analysis with tools like Grafana, Tableau, or Jupyter
- **Long-term Storage**: Preserve historical sensor data with InfluxDB's efficient time-series storage
- **Performance**: Improve Home Assistant performance by offloading historical data analysis
- **Integration**: Connect HA data with other time-series data sources in a unified platform

## Architecture

The exporter implements a robust data processing pipeline:

1. **Entity Discovery**: Scans Home Assistant database for statistical entities
2. **Smart Filtering**: Categorizes entities into 8 sensor types based on units and patterns
3. **Data Quality**: Validates and corrects common issues (NaN values, out-of-range data)
4. **Batch Export**: Processes data in configurable batches with checkpoint support
5. **InfluxDB Integration**: Writes to appropriate buckets with proper schema design

## Entity Filtering Strategy

The tool automatically categorizes sensors into meaningful groups:

- **Energy Sensors** (kWh): Cumulative energy consumption tracking
- **Power Sensors** (W): Real-time power monitoring
- **Environmental** (°C, hPa, %): Temperature, pressure, humidity
- **Network** (kB/s, GB): Bandwidth and data usage metrics
- **Electrical** (A, V): Current and voltage monitoring
- **Special Integrations**: Energy providers and smart meter data
- **Other Numeric**: Miscellaneous measurement sensors

**Result**: Typically filters ~80-85% of entities (340+ from 430+ total) as time-series relevant with expanded sensor support.

## Technical Specifications

- **Database Support**: Home Assistant SQLite statistics database
- **Target Platform**: InfluxDB 2.x with API token authentication
- **Data Processing**: Configurable batch sizes (default: 1000 records)
- **Performance**: Processes 100K+ records per hour on typical hardware
- **Memory Efficiency**: Streaming processing with minimal memory footprint
- **Resume Support**: JSON checkpoint system for interrupted exports

## Project Structure

```
hastats/
├── src/                    # Core library modules
├── scripts/               # CLI tools and utilities  
├── config/                # Configuration templates
├── ADR/                   # Architecture decision records
└── requirements.txt       # Dependencies
```

## Getting Started

1. **Prerequisites**: Python 3.8+, Home Assistant database, InfluxDB 2.x
2. **Installation**: Clone repository and install dependencies
3. **Configuration**: Set up InfluxDB credentials and database paths
4. **Analysis**: Run entity analysis to understand your data
5. **Migration**: Execute the export with dry-run first, then full migration

## Documentation

- **README.md**: Complete installation and usage guide
- **ADR/**: Detailed architectural decisions and rationale
- **Configuration**: Comprehensive environment variable reference
- **CLI Reference**: Complete command-line interface documentation

## License

[License information to be added]
