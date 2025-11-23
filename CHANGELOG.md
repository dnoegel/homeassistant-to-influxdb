# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-11-23

### Added
- **Initial Release** 🎉
- Complete Home Assistant to InfluxDB 2.x migration tool
- Intelligent entity filtering with 8 sensor categories (energy, power, temperature, etc.)
- Smart schema design with unit-based measurements (°C, kWh, W)
- Dual-bucket architecture (recent vs historical data)
- Comprehensive data quality validation and auto-correction
- Batch processing with resume capability for large datasets
- Progress tracking and checkpoint system
- CLI interface with dry-run, analysis, and filtering options
- Automatic InfluxDB bucket creation and Flux task setup
- Enhanced metadata extraction including friendly names and device classes
- Configurable entity filtering by domain, unit, and patterns
- Memory-efficient streaming processing
- Production-ready error handling and logging

### Technical Highlights
- **Schema Compatibility**: Friendly names stored as tags to match HA native format
- **Performance**: Handles millions of records with constant memory usage
- **Data Quality**: Automatic NaN/infinity detection and range validation
- **Filtering Intelligence**: ~80-85% entity inclusion rate with smart exclusions
- **Resume Support**: Interrupt and resume large exports safely
- **Batch Optimization**: Configurable batch sizes with progress reporting

### Supported Features
- **Entity Types**: sensor, counter, weather, climate, utility_meter
- **Measurement Units**: kWh, W, °C, °F, hPa, bar, A, V, lux, ppm, dB, rpm, and more
- **Data Sources**: Home Assistant recorder, special integrations (Tibber)
- **InfluxDB Operations**: Bucket management, data lifecycle, Flux aggregations
- **Export Options**: Full migration, selective filtering, time ranges

### Architecture
- Modular Python design with clear separation of concerns
- Configuration-driven approach with environment variables
- Comprehensive CLI with multiple operation modes
- Robust error handling with detailed logging
- Clean project structure with ADR documentation