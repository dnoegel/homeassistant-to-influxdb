#!/usr/bin/env python3
"""
CLI entry point for Home Assistant to InfluxDB statistics export.
"""

import sys
import os
import click
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import src.config as config_module
from src.exporter import exporter

# Initialize config
config = config_module.config


@click.command()
@click.option('--resume', is_flag=True, default=False, 
              help='Resume from previous checkpoint')
@click.option('--dry-run', is_flag=True, default=False,
              help='Validate data without writing to InfluxDB')
@click.option('--entities', type=str, default=None,
              help='Filter entities by pattern (substring match)')
@click.option('--config-file', type=str, default=None,
              help='Path to custom .env configuration file')
@click.option('--progress', is_flag=True, default=True,
              help='Show progress during export')
@click.option('--verbose', '-v', is_flag=True, default=False,
              help='Enable verbose logging')
def main(resume, dry_run, entities, config_file, progress, verbose):
    """
    Export Home Assistant statistics to InfluxDB.
    
    This tool exports filtered Home Assistant statistics from SQLite to InfluxDB 2
    with proper data quality validation, aggregation strategies, and resume capability.
    
    Examples:
        # Basic export
        python export.py
        
        # Dry run to validate data
        python export.py --dry-run
        
        # Resume interrupted export
        python export.py --resume
        
        # Export only energy sensors
        python export.py --entities energy
        
        # Verbose logging
        python export.py --verbose
    """
    
    # Use global config, but allow override
    current_config = config
    
    # Load custom config if specified
    if config_file:
        if not Path(config_file).exists():
            click.echo(f"Error: Configuration file not found: {config_file}")
            sys.exit(1)
        
        # Reload config with custom file  
        from src.config import Config
        current_config = Config(config_file)
    
    # Configure logging level
    if verbose:
        current_config.log_level = 'DEBUG'
    
    # Setup logging
    current_config.setup_logging()
    
    # Validate configuration
    try:
        # This will trigger validation in config
        current_config.print_summary()
    except Exception as e:
        click.echo(f"Configuration error: {e}")
        sys.exit(1)
    
    # Check if this is a resume without checkpoint
    if resume and not Path(current_config.checkpoint_file).exists():
        click.echo("Warning: --resume specified but no checkpoint file found. Starting fresh export.")
        resume = False
    
    # Confirm settings for non-dry-run
    if not dry_run:
        click.echo("\n" + "="*60)
        click.echo("EXPORT CONFIRMATION")
        click.echo("="*60)
        click.echo(f"InfluxDB URL: {current_config.influx_url}")
        click.echo(f"InfluxDB Org: {current_config.influx_org}")
        click.echo(f"Recent Bucket: {current_config.influx_bucket_recent}")
        click.echo(f"Historical Bucket: {current_config.influx_bucket_historical}")
        click.echo(f"HA Database: {current_config.ha_database_path}")
        
        if entities:
            click.echo(f"Entity Filter: {entities}")
        
        if resume:
            click.echo("Mode: RESUME from checkpoint")
        
        click.echo("\nThis will write data to InfluxDB. Continue?")
        if not click.confirm("Proceed with export?"):
            click.echo("Export cancelled.")
            sys.exit(0)
    
    # Run export
    try:
        success = exporter.export_statistics(
            resume=resume,
            dry_run=dry_run,
            entity_filter_pattern=entities
        )
        
        if success:
            click.echo("\n✓ Export completed successfully!")
            if dry_run:
                click.echo("Note: This was a dry run. No data was written to InfluxDB.")
                click.echo("Remove --dry-run flag to perform actual export.")
            sys.exit(0)
        else:
            click.echo("\n✗ Export failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        click.echo("\n\nExport interrupted by user.")
        if current_config.resume_enabled:
            click.echo("Progress has been saved. Use --resume to continue.")
        sys.exit(130)  # Standard exit code for SIGINT
        
    except Exception as e:
        click.echo(f"\nUnexpected error: {e}")
        import traceback
        if verbose:
            traceback.print_exc()
        sys.exit(1)


@click.command()
@click.option('--config-file', type=str, default=None, help='Path to custom .env configuration file')
@click.option('--verbose', '-v', is_flag=True, default=False, help='Show detailed entity information')
def analyze_entities(config_file, verbose):
    """Analyze Home Assistant entities and show filtering results."""
    
    # Use global config, but allow override
    current_config = config_module.config
    
    if config_file:
        if not Path(config_file).exists():
            click.echo(f"Error: Configuration file not found: {config_file}")
            sys.exit(1)
        from src.config import Config
        current_config = Config(config_file)
    
    # Setup logging
    current_config.setup_logging()
    
    try:
        from src.database import db
        from src.entity_filter import entity_filter
        
        click.echo("=" * 60)
        click.echo("HOME ASSISTANT ENTITY ANALYSIS")
        click.echo("=" * 60)
        
        if current_config.use_latest_metadata_only:
            click.echo("Using fast mode: latest metadata only per entity...")
            
            # Fast mode: get latest metadata per entity
            metadata_list = db.get_statistics_metadata_latest_only()
            click.echo(f"Loaded {len(metadata_list):,} latest metadata records")
            
        else:
            click.echo("Using complete mode: streaming all metadata records...")
            
            # Get fast metadata count for analysis
            total_count = db.get_statistics_metadata_count()
            click.echo(f"Total metadata records: {total_count:,}")
            
            # Stream metadata for analysis
            metadata_list = []
            batch_count = 0
            total_loaded = 0
            
            import time
            
            click.echo(f"Processing {total_count:,} metadata records in batches of {current_config.metadata_batch_size:,}")
            
            for batch in db.iter_statistics_metadata():
                batch_start_time = time.time()
                metadata_list.extend(batch)
                batch_count += 1
                total_loaded += len(batch)
                
                # Progress reporting (matching export format)
                if batch_count % current_config.progress_interval == 0:
                    batch_time = time.time() - batch_start_time
                    rate = len(batch) / batch_time if batch_time > 0 else 0
                    click.echo(f"  Batch {batch_count}: {len(batch)} metadata records processed ({rate:.0f} rec/sec)")
        
        if not metadata_list:
            click.echo("No entities found in the database.")
            sys.exit(1)
        
        # Filter entities
        filtered_entities, summary_stats = entity_filter.filter_entities(metadata_list)
        
        # Print brief summary only
        click.echo(f"\n✓ Filtering completed: {summary_stats['included_entities']}/{summary_stats['total_entities']} entities selected ({summary_stats['inclusion_rate']:.1f}%)")
        
        # Show unit breakdown
        if verbose:
            click.echo("\n" + "=" * 60)
            click.echo("DETAILED UNIT BREAKDOWN")
            click.echo("=" * 60)
            
            unit_counts = {}
            excluded_units = {}
            
            for metadata in metadata_list:
                unit = metadata.unit_of_measurement or "no unit"
                entity_id = metadata.statistic_id.lower()
                
                # Check if this entity would be included
                category = entity_filter.categorize_entity(metadata)
                if category.name == "EXCLUDED":
                    excluded_units[unit] = excluded_units.get(unit, 0) + 1
                else:
                    unit_counts[unit] = unit_counts.get(unit, 0) + 1
            
            click.echo("\nIncluded units:")
            for unit, count in sorted(unit_counts.items(), key=lambda x: x[1], reverse=True):
                click.echo(f"  {unit:<15}: {count:>4} entities")
            
            click.echo("\nExcluded units (top 10):")
            for unit, count in sorted(excluded_units.items(), key=lambda x: x[1], reverse=True)[:10]:
                click.echo(f"  {unit:<15}: {count:>4} entities")
    
    except Exception as e:
        click.echo(f"Analysis failed: {e}")
        import traceback
        if verbose:
            traceback.print_exc()
        sys.exit(1)


@click.command()
@click.option('--bucket', type=str, help='Bucket to query')
@click.option('--entity', type=str, help='Entity ID to query')
@click.option('--days', type=int, default=7, help='Number of days to query')
def query(bucket, entity, days):
    """Query exported data from InfluxDB (utility command)."""
    try:
        from src.influxdb_client import get_influx_manager
        
        bucket = bucket or config.influx_bucket_recent
        
        query_str = f'''
        from(bucket: "{bucket}")
          |> range(start: -{days}d)
        '''
        
        if entity:
            query_str += f'|> filter(fn: (r) => r["entity_id"] == "{entity}")'
        
        query_str += '''
          |> filter(fn: (r) => r["_field"] == "value")
          |> limit(n: 10)
        '''
        
        with get_influx_manager() as influx:
            results = influx.query_data(bucket, query_str)
            
            if results:
                click.echo(f"Sample data from bucket '{bucket}':")
                for i, record in enumerate(results[:10]):
                    entity_id = record.get('entity_id', 'unknown')
                    value = record.get('_value', 'N/A')
                    time = record.get('_time', 'unknown')
                    click.echo(f"  {entity_id}: {value} at {time}")
            else:
                click.echo(f"No data found in bucket '{bucket}'")
        
    except Exception as e:
        click.echo(f"Query failed: {e}")
        sys.exit(1)


@click.group()
def cli():
    """Home Assistant to InfluxDB Statistics Exporter."""
    pass


cli.add_command(main, name='export')
cli.add_command(analyze_entities, name='analyze-entities')
cli.add_command(query, name='query')


if __name__ == "__main__":
    cli()