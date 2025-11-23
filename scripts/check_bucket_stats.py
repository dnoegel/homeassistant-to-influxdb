#!/usr/bin/env python3
"""
Check InfluxDB bucket statistics and sizes.
"""

import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.config import config
from src.influxdb_client import get_influx_manager


def check_bucket_stats():
    """Check statistics for both InfluxDB buckets."""
    print("="*60)
    print("INFLUXDB BUCKET STATISTICS")
    print("="*60)
    
    try:
        with get_influx_manager() as influx:
            
            buckets = [config.influx_bucket_recent, config.influx_bucket_historical]
            
            for bucket_name in buckets:
                print(f"\n📊 Bucket: {bucket_name}")
                print("-" * 40)
                
                if influx.bucket_exists(bucket_name):
                    print("✅ Bucket exists")
                    
                    # Get basic point count
                    try:
                        query = f'''
                        from(bucket: "{bucket_name}")
                          |> range(start: -30d)
                          |> group()
                          |> count()
                        '''
                        
                        results = influx.query_data(bucket_name, query)
                        
                        if results:
                            total_points = results[0].get('_value', 0)
                            print(f"📈 Data points (last 30 days): {total_points:,}")
                        else:
                            print("📈 Data points: No data in last 30 days")
                        
                        # Try to get all-time count
                        query_all = f'''
                        from(bucket: "{bucket_name}")
                          |> range(start: 1970-01-01T00:00:00Z)
                          |> group()
                          |> count()
                        '''
                        
                        results_all = influx.query_data(bucket_name, query_all)
                        if results_all:
                            total_all = results_all[0].get('_value', 0)
                            print(f"📈 Total data points (all time): {total_all:,}")
                        
                        # Get measurement info
                        measurements_query = f'''
                        import "influxdata/influxdb/schema"
                        schema.measurements(bucket: "{bucket_name}")
                        '''
                        
                        try:
                            measurements = influx.query_data(bucket_name, measurements_query)
                            if measurements:
                                measurement_names = [m.get('_value', '') for m in measurements]
                                print(f"📋 Measurements: {', '.join(measurement_names)}")
                            else:
                                print("📋 Measurements: None found")
                        except:
                            print("📋 Measurements: Unable to query")
                        
                        # Get time range
                        time_query = f'''
                        from(bucket: "{bucket_name}")
                          |> range(start: 1970-01-01T00:00:00Z)
                          |> group()
                          |> first()
                        '''
                        
                        try:
                            first_result = influx.query_data(bucket_name, time_query)
                            if first_result:
                                first_time = first_result[0].get('_time', 'Unknown')
                                print(f"📅 Earliest data: {first_time}")
                        except:
                            print("📅 Earliest data: Unable to query")
                        
                        last_query = f'''
                        from(bucket: "{bucket_name}")
                          |> range(start: 1970-01-01T00:00:00Z)
                          |> group()
                          |> last()
                        '''
                        
                        try:
                            last_result = influx.query_data(bucket_name, last_query)
                            if last_result:
                                last_time = last_result[0].get('_time', 'Unknown')
                                print(f"📅 Latest data: {last_time}")
                        except:
                            print("📅 Latest data: Unable to query")
                        
                    except Exception as e:
                        print(f"❌ Error querying bucket: {e}")
                        
                else:
                    print("❌ Bucket does not exist")
            
            # Get organization info
            try:
                print(f"\n🏢 Organization: {config.influx_org}")
                print(f"🔗 InfluxDB URL: {config.influx_url}")
            except:
                pass
                
        print("\n" + "="*60)
        print("📝 NOTE: For detailed storage sizes, check the InfluxDB Web UI:")
        print(f"   {config.influx_url} → Settings → Buckets")
        print("="*60)
                
    except Exception as e:
        print(f"❌ Failed to connect to InfluxDB: {e}")
        print(f"\nCheck your configuration:")
        print(f"  URL: {config.influx_url}")
        print(f"  Org: {config.influx_org}")
        print(f"  Token: {'*' * 20}...")


if __name__ == "__main__":
    check_bucket_stats()