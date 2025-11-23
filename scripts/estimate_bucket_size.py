#!/usr/bin/env python3
"""
Estimate InfluxDB bucket sizes by analyzing data patterns.
"""

import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.config import config
from src.influxdb_client import get_influx_manager


def estimate_bucket_sizes():
    """Estimate bucket sizes based on data points and field counts."""
    print("="*70)
    print("INFLUXDB BUCKET SIZE ESTIMATION")
    print("="*70)
    
    try:
        with get_influx_manager() as influx:
            
            buckets = [config.influx_bucket_recent, config.influx_bucket_historical]
            
            for bucket_name in buckets:
                print(f"\n📊 Analyzing bucket: {bucket_name}")
                print("-" * 50)
                
                try:
                    # Count total points
                    count_query = f'''
                    from(bucket: "{bucket_name}")
                      |> range(start: 1970-01-01T00:00:00Z)
                      |> group()
                      |> count()
                    '''
                    
                    count_results = influx.query_data(bucket_name, count_query)
                    total_points = count_results[0].get('_value', 0) if count_results else 0
                    
                    print(f"📈 Total data points: {total_points:,}")
                    
                    # Count unique entities
                    entities_query = f'''
                    import "influxdata/influxdb/schema"
                    
                    from(bucket: "{bucket_name}")
                      |> range(start: 1970-01-01T00:00:00Z)
                      |> group(columns: ["entity_id"])
                      |> distinct(column: "entity_id")
                      |> group()
                      |> count()
                    '''
                    
                    try:
                        entity_results = influx.query_data(bucket_name, entities_query)
                        unique_entities = entity_results[0].get('_value', 0) if entity_results else 0
                        print(f"🏷️  Unique entities: {unique_entities:,}")
                    except:
                        print("🏷️  Unique entities: Unable to determine")
                    
                    # Count unique fields
                    fields_query = f'''
                    import "influxdata/influxdb/schema"
                    schema.fieldKeys(bucket: "{bucket_name}")
                    '''
                    
                    try:
                        field_results = influx.query_data(bucket_name, fields_query)
                        unique_fields = len(field_results) if field_results else 0
                        field_names = [f.get('_value', '') for f in field_results] if field_results else []
                        print(f"📊 Unique fields: {unique_fields} ({', '.join(field_names[:5])}{'...' if len(field_names) > 5 else ''})")
                    except:
                        print("📊 Unique fields: Unable to determine")
                    
                    # Sample some data to estimate field usage
                    sample_query = f'''
                    from(bucket: "{bucket_name}")
                      |> range(start: -7d)
                      |> limit(n: 1000)
                    '''
                    
                    try:
                        sample_results = influx.query_data(bucket_name, sample_query)
                        if sample_results:
                            sample_size = len(sample_results)
                            print(f"🔬 Sample size (last 7 days): {sample_size:,} points")
                            
                            # Rough size estimation
                            # Each point: ~100-200 bytes average (timestamp + tags + fields)
                            avg_point_size = 150  # bytes
                            estimated_size_bytes = total_points * avg_point_size
                            estimated_size_mb = estimated_size_bytes / (1024 * 1024)
                            
                            print(f"💾 Estimated size: ~{estimated_size_mb:.1f} MB")
                            
                            if estimated_size_mb > 1024:
                                estimated_size_gb = estimated_size_mb / 1024
                                print(f"💾 Estimated size: ~{estimated_size_gb:.1f} GB")
                            
                    except Exception as e:
                        print(f"🔬 Sample analysis failed: {e}")
                    
                    # Time span
                    time_span_query = f'''
                    timeRange = from(bucket: "{bucket_name}")
                      |> range(start: 1970-01-01T00:00:00Z)
                      |> group()
                    
                    earliest = timeRange |> first() |> findColumn(fn: (key) => key._field == "_time", column: "_time")
                    latest = timeRange |> last() |> findColumn(fn: (key) => key._field == "_time", column: "_time")
                    
                    earliest |> yield(name: "earliest")
                    latest |> yield(name: "latest")
                    '''
                    
                    # Get time range differently
                    first_query = f'''
                    from(bucket: "{bucket_name}")
                      |> range(start: 1970-01-01T00:00:00Z)
                      |> first()
                      |> keep(columns: ["_time"])
                    '''
                    
                    last_query = f'''
                    from(bucket: "{bucket_name}")
                      |> range(start: 1970-01-01T00:00:00Z)
                      |> last()
                      |> keep(columns: ["_time"])
                    '''
                    
                    try:
                        first_result = influx.query_data(bucket_name, first_query)
                        last_result = influx.query_data(bucket_name, last_query)
                        
                        if first_result and last_result:
                            first_time = first_result[0].get('_time', '')
                            last_time = last_result[0].get('_time', '')
                            
                            print(f"📅 Data range: {first_time} to {last_time}")
                            
                            # Calculate days
                            from datetime import datetime
                            try:
                                first_dt = datetime.fromisoformat(first_time.replace('Z', '+00:00'))
                                last_dt = datetime.fromisoformat(last_time.replace('Z', '+00:00'))
                                days_span = (last_dt - first_dt).days
                                print(f"📅 Time span: {days_span} days")
                                
                                if total_points > 0 and days_span > 0:
                                    points_per_day = total_points / days_span
                                    print(f"📈 Average points per day: {points_per_day:.0f}")
                                
                            except:
                                pass
                            
                    except Exception as e:
                        print(f"📅 Time range analysis failed: {e}")
                
                except Exception as e:
                    print(f"❌ Error analyzing bucket {bucket_name}: {e}")
            
            print(f"\n{'='*70}")
            print("📝 NOTES:")
            print("• Size estimates are rough calculations (150 bytes/point average)")
            print("• Actual storage may vary due to compression and indexing")
            print("• InfluxDB uses columnar storage and compression")
            print(f"{'='*70}")
                
    except Exception as e:
        print(f"❌ Failed to connect to InfluxDB: {e}")


if __name__ == "__main__":
    estimate_bucket_sizes()