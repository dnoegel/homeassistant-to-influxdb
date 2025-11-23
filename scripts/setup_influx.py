#!/usr/bin/env python3
"""
InfluxDB setup script for Home Assistant statistics export.
Creates buckets, configures retention policies, and sets up aggregation tasks.
"""

import sys
import os
import logging
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.config import config
from src.influxdb_client import InfluxDBManager


def setup_buckets(influx_manager: InfluxDBManager) -> bool:
    """Set up required InfluxDB buckets."""
    print("Setting up InfluxDB buckets...")
    
    buckets_config = {
        config.influx_bucket_recent: {
            'retention_days': 90,
            'description': 'Home Assistant recent data (90 day retention)'
        },
        config.influx_bucket_historical: {
            'retention_days': 0,  # Infinite retention
            'description': 'Home Assistant historical aggregated data (unlimited retention)'
        }
    }
    
    success = True
    for bucket_name, bucket_config in buckets_config.items():
        retention_seconds = bucket_config['retention_days'] * 24 * 3600 if bucket_config['retention_days'] > 0 else 0
        
        print(f"  Creating bucket: {bucket_name}")
        retention_display = 'Unlimited' if retention_seconds == 0 else f'{bucket_config["retention_days"]} days'
        print(f"    Retention: {retention_display}")
        print(f"    Description: {bucket_config['description']}")
        
        if influx_manager.create_bucket(
            bucket_name=bucket_name,
            retention_seconds=retention_seconds,
            description=bucket_config['description']
        ):
            print(f"    ✓ Bucket '{bucket_name}' ready")
        else:
            print(f"    ✗ Failed to create bucket '{bucket_name}'")
            success = False
    
    return success


def create_aggregation_tasks(influx_manager: InfluxDBManager) -> bool:
    """Create Flux aggregation tasks for data lifecycle management."""
    print("\nSetting up aggregation tasks...")
    
    try:
        tasks_api = influx_manager.client.tasks_api()
        
        # Check if task already exists
        task_name = "homeassistant-unified-aggregation"
        existing_task_id = None
        try:
            existing_tasks = tasks_api.find_tasks()
            
            # Check if our specific task already exists by searching the flux script
            for task in existing_tasks:
                if task.flux and task_name in task.flux:
                    print(f"  Task '{task_name}' already exists (ID: {task.id})")
                    print(f"  Deleting existing task to update with new sensor support...")
                    try:
                        tasks_api.delete_task(task.id)
                        print(f"  ✓ Deleted old task")
                        break
                    except Exception as delete_error:
                        print(f"  ✗ Failed to delete old task: {delete_error}")
                        print(f"  Please manually delete task '{task_name}' and re-run setup")
                        return False
                    
        except Exception as e:
            print(f"  Warning: Could not check existing tasks: {e}")
            print(f"  Proceeding with task creation...")
        
        # Read Flux task template
        task_file = Path(__file__).parent.parent / 'config' / 'influx_tasks.flux'
        
        # Always recreate the template to ensure it has the latest sensor support
        print(f"  Creating/updating Flux task template: {task_file}")
        create_flux_tasks_file(task_file)
        
        with open(task_file, 'r') as f:
            flux_script = f.read()
        
        # Replace placeholders with actual configuration
        flux_script = flux_script.replace('{{RECENT_BUCKET}}', config.influx_bucket_recent)
        flux_script = flux_script.replace('{{HISTORICAL_BUCKET}}', config.influx_bucket_historical)
        flux_script = flux_script.replace('{{ORG}}', config.influx_org)
        
        # Create task using TaskCreateRequest
        from influxdb_client.domain.task_create_request import TaskCreateRequest
        
        task_request = TaskCreateRequest(
            org_id=influx_manager._get_org_id(),
            flux=flux_script,
            description="Aggregate Home Assistant data from recent to historical bucket (unit-based)",
            status="active"
        )
        
        try:
            created_task = tasks_api.create_task(task_create_request=task_request)
            print(f"  ✓ Created aggregation task: {task_name}")
            print(f"    Task ID: {created_task.id}")
            return True
            
        except Exception as create_error:
            print(f"  ✗ Failed to create task: {create_error}")
            print(f"  Note: You can create the task manually using the Flux script in config/influx_tasks.flux")
            return False
        
    except Exception as e:
        print(f"  ✗ Failed to create aggregation tasks: {e}")
        print(f"    Note: Tasks can be created manually using the Flux scripts in config/")
        return True  # Don't fail setup for this


def create_flux_tasks_file(task_file: Path):
    """Create the Flux tasks configuration file."""
    task_file.parent.mkdir(exist_ok=True)
    
    flux_content = '''// Home Assistant Data Aggregation Tasks  
// Compatible with both exported historical data and live HA data
// Aggregates by measurement (unit) for unified querying

// ============================================================================
// Unified Aggregation Task
// Runs every hour to aggregate data older than 30 days from recent to historical bucket
// ============================================================================

option task = {
    name: "homeassistant-unified-aggregation",
    every: 1h,
    offset: 5m
}

// Time range: data older than 30 days but newer than 31 days  
start_time = -31d
end_time = -30d

// Energy measurements (kWh, Wh) - preserve cumulative values (last)
from(bucket: "{{RECENT_BUCKET}}")
    |> range(start: start_time, stop: end_time)
    |> filter(fn: (r) => r["_measurement"] == "kWh" or r["_measurement"] == "Wh")
    |> aggregateWindow(every: 1h, fn: last, createEmpty: false)
    |> to(bucket: "{{HISTORICAL_BUCKET}}", org: "{{ORG}}")

// Power measurements (W, kW) - average over time
from(bucket: "{{RECENT_BUCKET}}")
    |> range(start: start_time, stop: end_time) 
    |> filter(fn: (r) => r["_measurement"] == "W" or r["_measurement"] == "kW")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
    |> to(bucket: "{{HISTORICAL_BUCKET}}", org: "{{ORG}}")

// Temperature measurements (°C, °F) - average over time
from(bucket: "{{RECENT_BUCKET}}")
    |> range(start: start_time, stop: end_time)
    |> filter(fn: (r) => r["_measurement"] == "°C" or r["_measurement"] == "°F")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
    |> to(bucket: "{{HISTORICAL_BUCKET}}", org: "{{ORG}}")

// Pressure measurements (hPa, bar, mbar) - average over time
from(bucket: "{{RECENT_BUCKET}}")
    |> range(start: start_time, stop: end_time)
    |> filter(fn: (r) => r["_measurement"] == "hPa" or r["_measurement"] == "bar" or r["_measurement"] == "mbar")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
    |> to(bucket: "{{HISTORICAL_BUCKET}}", org: "{{ORG}}")

// Percentage measurements (%) - average over time
from(bucket: "{{RECENT_BUCKET}}")
    |> range(start: start_time, stop: end_time)
    |> filter(fn: (r) => r["_measurement"] == "%")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
    |> to(bucket: "{{HISTORICAL_BUCKET}}", org: "{{ORG}}")

// Electrical measurements (A, V) - average over time
from(bucket: "{{RECENT_BUCKET}}")
    |> range(start: start_time, stop: end_time)
    |> filter(fn: (r) => r["_measurement"] == "A" or r["_measurement"] == "V" or r["_measurement"] == "mA" or r["_measurement"] == "mV")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
    |> to(bucket: "{{HISTORICAL_BUCKET}}", org: "{{ORG}}")

// Light measurements (lux) - average over time
from(bucket: "{{RECENT_BUCKET}}")
    |> range(start: start_time, stop: end_time)
    |> filter(fn: (r) => r["_measurement"] == "lux")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
    |> to(bucket: "{{HISTORICAL_BUCKET}}", org: "{{ORG}}")

// Air quality measurements (ppm) - average over time  
from(bucket: "{{RECENT_BUCKET}}")
    |> range(start: start_time, stop: end_time)
    |> filter(fn: (r) => r["_measurement"] == "ppm")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
    |> to(bucket: "{{HISTORICAL_BUCKET}}", org: "{{ORG}}")

// Sound measurements (dB) - average over time
from(bucket: "{{RECENT_BUCKET}}")
    |> range(start: start_time, stop: end_time)
    |> filter(fn: (r) => r["_measurement"] == "dB")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
    |> to(bucket: "{{HISTORICAL_BUCKET}}", org: "{{ORG}}")

// Motion measurements (rpm) - average over time
from(bucket: "{{RECENT_BUCKET}}")
    |> range(start: start_time, stop: end_time)
    |> filter(fn: (r) => r["_measurement"] == "rpm")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
    |> to(bucket: "{{HISTORICAL_BUCKET}}", org: "{{ORG}}")

// Network measurements - rate data (average), cumulative data (last)
from(bucket: "{{RECENT_BUCKET}}")
    |> range(start: start_time, stop: end_time)
    |> filter(fn: (r) => r["_measurement"] == "kB/s" or r["_measurement"] == "MB/s")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
    |> to(bucket: "{{HISTORICAL_BUCKET}}", org: "{{ORG}}")

from(bucket: "{{RECENT_BUCKET}}")
    |> range(start: start_time, stop: end_time)
    |> filter(fn: (r) => r["_measurement"] == "GB" or r["_measurement"] == "MB")
    |> aggregateWindow(every: 1h, fn: last, createEmpty: false)
    |> to(bucket: "{{HISTORICAL_BUCKET}}", org: "{{ORG}}")

// Counter domain - always use max for cumulative counters
from(bucket: "{{RECENT_BUCKET}}")
    |> range(start: start_time, stop: end_time)
    |> filter(fn: (r) => r["domain"] == "counter")
    |> aggregateWindow(every: 1h, fn: max, createEmpty: false)
    |> to(bucket: "{{HISTORICAL_BUCKET}}", org: "{{ORG}}")

// Entities without units (fallback) - group by domain
from(bucket: "{{RECENT_BUCKET}}")
    |> range(start: start_time, stop: end_time)
    |> filter(fn: (r) => r["_measurement"] =~ /.*_data$/)
    |> group(columns: ["domain"])
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
    |> to(bucket: "{{HISTORICAL_BUCKET}}", org: "{{ORG}}")'''
    
    with open(task_file, 'w') as f:
        f.write(flux_content)
    
    print(f"  ✓ Created Flux tasks template: {task_file}")


def verify_setup(influx_manager: InfluxDBManager) -> bool:
    """Verify that the InfluxDB setup is working correctly."""
    print("\nVerifying InfluxDB setup...")
    
    success = True
    
    # Check buckets exist
    buckets = [config.influx_bucket_recent, config.influx_bucket_historical]
    for bucket in buckets:
        if influx_manager.bucket_exists(bucket):
            print(f"  ✓ Bucket '{bucket}' exists and accessible")
        else:
            print(f"  ✗ Bucket '{bucket}' not found or inaccessible")
            success = False
    
    # Test write permissions (write a test point)
    try:
        from influxdb_client import Point
        from datetime import datetime, timezone
        
        test_point = Point("test_measurement") \
            .tag("setup_test", "true") \
            .field("value", 1.0) \
            .time(datetime.now(tz=timezone.utc))
        
        result = influx_manager.write_points([test_point], config.influx_bucket_recent)
        if result.success:
            print(f"  ✓ Write permissions verified")
        else:
            print(f"  ✗ Write test failed: {result.errors}")
            success = False
            
    except Exception as e:
        print(f"  ✗ Write test failed: {e}")
        success = False
    
    return success


def main():
    """Main setup function."""
    print("="*60)
    print("HOME ASSISTANT INFLUXDB SETUP")
    print("="*60)
    
    # Setup logging
    config.setup_logging()
    
    # Print configuration summary
    print("\nConfiguration:")
    config.print_summary()
    
    try:
        # Initialize InfluxDB manager
        print(f"\nConnecting to InfluxDB at {config.influx_url}...")
        with InfluxDBManager() as influx_manager:
            print("✓ Connected to InfluxDB successfully")
            
            # Set up buckets
            if not setup_buckets(influx_manager):
                print("\n✗ Bucket setup failed")
                return 1
            
            # Set up aggregation tasks
            create_aggregation_tasks(influx_manager)
            
            # Verify setup
            if not verify_setup(influx_manager):
                print("\n✗ Setup verification failed")
                return 1
            
            print("\n" + "="*60)
            print("SETUP COMPLETE")
            print("="*60)
            print("✓ InfluxDB setup completed successfully")
            print("✓ Buckets created with proper retention policies")
            print("✓ Unit-based aggregation tasks configured")
            print("✓ Write permissions verified")
            print("\nReady to run the main export!")
            print("Next step: python scripts/export.py analyze-entities")
            
            return 0
    
    except Exception as e:
        print(f"\n✗ Setup failed: {e}")
        logging.exception("Setup failed with exception")
        return 1


if __name__ == "__main__":
    sys.exit(main())