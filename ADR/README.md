# Architecture Decision Records (ADRs)

This directory contains Architecture Decision Records documenting the key architectural decisions made during the development of the Home Assistant to InfluxDB exporter.

## Format

Each ADR follows this structure:
- **Title**: A short descriptive title
- **Status**: Current status (Proposed | Accepted | Superseded)
- **Context**: Background and problem statement
- **Decision**: What was decided and why
- **Consequences**: Results and impacts
- **Changes**: Evolution of the decision over time

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [000](000-adr-process.md) | ADR Process and Maintenance | Accepted |
| [001](001-data-source-analysis.md) | Data Source Analysis | Accepted |
| [002](002-entity-filtering-strategy.md) | Entity Filtering Strategy | Accepted |
| [003](003-influxdb-architecture.md) | InfluxDB Architecture | Accepted |
| [004](004-data-processing-strategy.md) | Data Processing Strategy | Accepted |
| [005](005-implementation-architecture.md) | Implementation Architecture | Accepted |
| [006](006-influxdb-setup-automation.md) | InfluxDB Setup Automation | Accepted |

## Contributing

When making significant architectural changes:
1. Create a new ADR using the next available number
2. Update existing ADRs if they're superseded
3. Update this index
4. Follow the established format