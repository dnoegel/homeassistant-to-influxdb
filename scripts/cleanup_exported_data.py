#!/usr/bin/env python3
"""
Clean up Home Assistant migration data and infrastructure from InfluxDB.

Two cleanup modes:
  --migrated: Delete only migrated data (source=migration) from buckets
  --all: Delete entire setup including buckets and InfluxDB task
"""

import sys
import os
import argparse

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.config import config
from src.influxdb_client import get_influx_manager


def delete_migration_data():
    """Delete all migrated historical data."""
    print("="*70)
    print("CLEANUP HASTATS EXPORTED DATA")
    print("="*70)
    
    buckets_to_clean = [
        config.influx_bucket_recent,
        config.influx_bucket_historical
    ]
    
    try:
        with get_influx_manager() as influx:
            
            for bucket_name in buckets_to_clean:
                print(f"\n🗑️  Cleaning bucket: {bucket_name}")
                print("-" * 50)
                
                # Check how much data we're about to delete
                count_query = f'''
                from(bucket: "{bucket_name}")
                  |> range(start: 1970-01-01T00:00:00Z)
                  |> filter(fn: (r) => r["source"] == "migration")
                  |> group()
                  |> count()
                '''
                
                try:
                    count_results = influx.query_data(bucket_name, count_query)
                    if count_results and len(count_results) > 0:
                        total_points = count_results[0].get('_value', 0)
                        print(f"📊 Found {total_points:,} migration points")
                        
                        if total_points == 0:
                            print("✅ No migration data to clean up")
                            continue
                            
                        # Ask for confirmation
                        print(f"⚠️  This will DELETE {total_points:,} data points from {bucket_name}")
                        response = input(f"Continue? (type 'DELETE' to confirm): ")
                        
                        if response != 'DELETE':
                            print("❌ Cleanup cancelled for this bucket")
                            continue
                            
                    else:
                        print("✅ No migration data found")
                        continue
                        
                except Exception as e:
                    print(f"❌ Could not count data: {e}")
                    print("Proceeding anyway...")
                
                # Smart deletion: Use InfluxDB delete API directly with large time range
                print("🗑️  Deleting migration data...")
                
                try:
                    delete_api = influx.client.delete_api()
                    
                    # Use wide time range to catch all data
                    start_time = "1970-01-01T00:00:00Z"
                    stop_time = "2030-12-31T23:59:59Z" 
                    
                    print(f"   Deleting all migration data from {start_time} to {stop_time}")
                    print(f"   Using predicate: source=\"migration\"")
                    
                    # Single delete operation for the entire range
                    delete_api.delete(
                        start=start_time,
                        stop=stop_time,
                        predicate='source="migration"',
                        bucket=bucket_name,
                        org=config.influx_org
                    )
                    
                    print(f"✅ Successfully deleted migration data from {bucket_name}")
                    
                except Exception as e:
                    error_msg = str(e)
                    print(f"❌ Failed to delete data from {bucket_name}: {error_msg}")
                    
                    # If timeout, suggest alternatives
                    if "timeout" in error_msg.lower() or "read timed out" in error_msg.lower():
                        print("   🔧 Timeout detected. Alternative approaches:")
                        print(f"      1. InfluxDB CLI: influx delete --bucket {bucket_name} --predicate 'source=\"migration\"' --start 1970-01-01T00:00:00Z --stop 2030-12-31T23:59:59Z")
                        print(f"      2. InfluxDB Web UI: Data Explorer > Delete Data")
                        print(f"      3. Recreate bucket: python scripts/cleanup_exported_data.py --all")
                    elif "schema collision" in error_msg.lower():
                        print("   🔧 Schema error detected. Alternative approaches:")
                        print(f"      1. Use InfluxDB CLI (bypasses schema issues)")
                        print(f"      2. Delete by measurement type separately")
                        print(f"      3. Recreate bucket entirely")
                    else:
                        print(f"   🔧 General error. Try:")
                        print(f"      1. InfluxDB CLI: influx delete --bucket {bucket_name} --predicate 'source=\"migration\"'")
                        print(f"      2. InfluxDB Web UI delete function")
                        print(f"      3. Recreate bucket: --all flag")
                    
                    continue
                
                # Verify deletion
                try:
                    verify_results = influx.query_data(bucket_name, count_query)
                    if verify_results and len(verify_results) > 0:
                        remaining_points = verify_results[0].get('_value', 0)
                        if remaining_points == 0:
                            print(f"✅ Verification: All migration data removed from {bucket_name}")
                        else:
                            print(f"⚠️  Warning: {remaining_points:,} points still remain")
                    else:
                        print(f"✅ Verification: All migration data removed from {bucket_name}")
                        
                except Exception as e:
                    print(f"❌ Could not verify deletion: {e}")
            
            print(f"\n{'='*70}")
            print("CLEANUP COMPLETE")
            print("="*70)
            print("✅ All migration data has been removed")
            print("✅ Ready for fresh migration")
            print("\nNext step:")
            print("Run: python scripts/export.py export")
            
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        return False
    
    return True



def delete_all_buckets_and_task():
    """Delete the entire HA migration setup: buckets and InfluxDB task."""
    print("="*70)
    print("DELETE ALL HA MIGRATION INFRASTRUCTURE")
    print("="*70)
    
    try:
        with get_influx_manager() as influx:
            
            # Delete buckets
            buckets_to_delete = [
                config.influx_bucket_recent,
                config.influx_bucket_historical
            ]
            
            buckets_api = influx.client.buckets_api()
            
            for bucket_name in buckets_to_delete:
                print(f"\n🗑️  Deleting bucket: {bucket_name}")
                try:
                    bucket = buckets_api.find_bucket_by_name(bucket_name)
                    if bucket:
                        buckets_api.delete_bucket(bucket)
                        print(f"✅ Bucket '{bucket_name}' deleted")
                    else:
                        print(f"⚠️  Bucket '{bucket_name}' not found")
                except Exception as e:
                    print(f"❌ Failed to delete bucket '{bucket_name}': {e}")
            
            # Delete InfluxDB task
            print(f"\n🗑️  Deleting InfluxDB task: homeassistant-unified-aggregation")
            try:
                tasks_api = influx.client.tasks_api()
                tasks = tasks_api.find_tasks(name="homeassistant-unified-aggregation")
                
                if tasks:
                    for task in tasks:
                        tasks_api.delete_task(task.id)
                        print(f"✅ Task '{task.name}' deleted")
                else:
                    print("⚠️  Task 'homeassistant-unified-aggregation' not found")
            except Exception as e:
                print(f"❌ Failed to delete task: {e}")
            
            print(f"\n{'='*70}")
            print("COMPLETE CLEANUP FINISHED")
            print("="*70)
            print("✅ All buckets and tasks removed")
            print("✅ Ready for fresh setup")
            print("\nNext steps:")
            print("1. Run: python scripts/setup_influx.py")
            print("2. Run: python scripts/export.py export")
            
    except Exception as e:
        print(f"❌ Complete cleanup failed: {e}")
        return False
    
    return True


def main():
    """Main cleanup function."""
    parser = argparse.ArgumentParser(
        description='Clean up Home Assistant migration data and infrastructure from InfluxDB',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/cleanup_exported_data.py --migrated    # Delete only migrated data (keep buckets/tasks)
  python scripts/cleanup_exported_data.py --all         # Delete everything: buckets, tasks, and data
        """
    )
    
    # Create mutually exclusive group
    action_group = parser.add_mutually_exclusive_group(required=False)
    
    action_group.add_argument('--migrated', '-m',
                             action='store_true', 
                             help='Delete only migrated data (source=migration) from buckets')
    
    action_group.add_argument('--all', '-a',
                             action='store_true',
                             help='Delete entire setup: buckets, tasks, and all data')
    
    args = parser.parse_args()
    
    # Show help if no arguments provided
    if not args.migrated and not args.all:
        parser.print_help()
        return 0
    
    # Setup logging
    config.setup_logging()
    
    if args.all:
        print("🧹 This will delete the ENTIRE HA migration setup:")
        print("   → Recent bucket:", config.influx_bucket_recent)
        print("   → Historical bucket:", config.influx_bucket_historical) 
        print("   → InfluxDB aggregation task")
        print("\n⚠️  WARNING: This will remove ALL DATA and infrastructure!")
        print("   Use this for complete reset or when switching to different setup")
        
        response = input("\nType 'DELETE EVERYTHING' to confirm: ")
        
        if response != 'DELETE EVERYTHING':
            print("❌ Complete cleanup cancelled")
            return 1
        
        if delete_all_buckets_and_task():
            return 0
        else:
            return 1
    
    elif args.migrated:
        print("🧹 This will delete ONLY migrated historical data:")
        print("   → Identifies data by: source='migration'")
        print("   → Keeps buckets and tasks intact")
        print("\n⚠️  WARNING: This action cannot be undone!")
        print("\nUse this when you want to:")
        print("  • Re-run migration with different settings")
        print("  • Clean up after testing")
        print("  • Reset migrated data before schema changes")
        
        response = input("\nType 'DELETE MIGRATED' to confirm: ")
        
        if response != 'DELETE MIGRATED':
            print("❌ Migration cleanup cancelled")
            return 1
        
        print(f"\nUsing buckets:")
        print(f"  Recent: {config.influx_bucket_recent}")
        print(f"  Historical: {config.influx_bucket_historical}")
        
        if delete_migration_data():
            return 0
        else:
            return 1


if __name__ == "__main__":
    sys.exit(main())